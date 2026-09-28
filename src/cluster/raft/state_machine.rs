//! Raft state machine: applies ClusterCommand to local SQLite app tables.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};
use parking_lot::Mutex;
use rusqlite::{OptionalExtension, params};

use crate::cluster::command::{ClusterCommand, ClusterResponse};
use crate::cluster::raft::TypeConfig;
use crate::cluster::raft::snapshot::AppSnapshot;
use crate::db::{Db, OwnerError, StreamAddError, ViewerAddError};

/// Side effects that must run after a Raft apply on every node (session markers,
/// in-memory bearer refresh). Delivered via an optional channel set by
/// [`ClusterManager`](crate::cluster::ClusterManager).
#[derive(Debug, Clone)]
pub enum StateEffect {
    DrainStream(String),
    ClearDrainStream(String),
    RevokeViewer(String),
    ApiToken(String),
    /// Ownership rows changed — refresh in-memory tracker immediately.
    OwnershipChanged,
}

#[derive(Clone)]
pub struct SqliteStateMachine {
    db: Arc<Db>,
    last_applied: Arc<Mutex<Option<LogId<u64>>>>,
    last_membership: Arc<Mutex<StoredMembership<u64, openraft::BasicNode>>>,
    current_snapshot: Arc<Mutex<Option<StoredSnapshot>>>,
    snapshot_idx: Arc<AtomicU64>,
    effects: Arc<Mutex<Option<std::sync::mpsc::Sender<StateEffect>>>>,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, openraft::BasicNode>,
    data: Vec<u8>,
}

impl SqliteStateMachine {
    pub fn new(db: Arc<Db>) -> Result<Self, StorageError<u64>> {
        let last_applied = Self::load_last_applied(&db)?;
        let last_membership = Self::load_last_membership(&db)?;
        Ok(Self {
            db,
            last_applied: Arc::new(Mutex::new(last_applied)),
            last_membership: Arc::new(Mutex::new(last_membership)),
            current_snapshot: Arc::new(Mutex::new(None)),
            snapshot_idx: Arc::new(AtomicU64::new(0)),
            effects: Arc::new(Mutex::new(None)),
        })
    }

    fn load_last_applied(db: &Db) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        db.with_conn(|conn| {
            match conn.query_row(
                "SELECT val FROM raft_meta WHERE key='last_applied'",
                [],
                |r| r.get::<_, String>(0),
            ) {
                Ok(j) => serde_json::from_str(&j).map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::read_state_machine(&e),
                }),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(StorageError::IO {
                    source: StorageIOError::<u64>::read_state_machine(&e),
                }),
            }
        })
    }

    fn load_last_membership(
        db: &Db,
    ) -> Result<StoredMembership<u64, openraft::BasicNode>, StorageError<u64>> {
        db.with_conn(|conn| {
            match conn.query_row(
                "SELECT val FROM raft_meta WHERE key='last_membership'",
                [],
                |r| r.get::<_, String>(0),
            ) {
                Ok(j) => serde_json::from_str(&j).map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::read_state_machine(&e),
                }),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    Ok(StoredMembership::new(None, openraft::Membership::default()))
                }
                Err(e) => Err(StorageError::IO {
                    source: StorageIOError::<u64>::read_state_machine(&e),
                }),
            }
        })
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    /// Register a channel for post-apply session/token side effects.
    pub fn set_effects_tx(&self, tx: std::sync::mpsc::Sender<StateEffect>) {
        *self.effects.lock() = Some(tx);
    }

    pub fn last_membership(&self) -> StoredMembership<u64, openraft::BasicNode> {
        self.last_membership.lock().clone()
    }

    fn emit_effect(&self, effect: StateEffect) {
        if let Some(tx) = self.effects.lock().as_ref() {
            let _ = tx.send(effect);
        }
    }

    /// Persist `last_applied` (+ optional membership) in one SQLite transaction.
    fn persist_applied_tx(
        &self,
        log_id: LogId<u64>,
        membership_json: Option<&str>,
    ) -> Result<(), StorageError<u64>> {
        let applied_json = serde_json::to_string(&log_id).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_state_machine(&e),
        })?;
        self.db.with_conn(|conn| {
            let tx = conn.unchecked_transaction().map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_state_machine(&e),
            })?;
            if let Some(mem) = membership_json {
                tx.execute(
                    "INSERT INTO raft_meta(key,val) VALUES('last_membership',?) \
                     ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                    params![mem],
                )
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::write_state_machine(&e),
                })?;
            }
            tx.execute(
                "INSERT INTO raft_meta(key,val) VALUES('last_applied',?) \
                 ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                params![applied_json],
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_state_machine(&e),
            })?;
            tx.commit().map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_state_machine(&e),
            })?;
            Ok::<(), StorageError<u64>>(())
        })?;
        *self.last_applied.lock() = Some(log_id);
        Ok(())
    }

    fn apply_command(&self, cmd: &ClusterCommand) -> ClusterResponse {
        match cmd {
            ClusterCommand::CreateStream {
                stream,
                default_viewer,
            } => match self.db.stream_add_with_viewer(stream, default_viewer) {
                Ok(()) => ClusterResponse::Ok,
                Err(StreamAddError::Duplicate) => ClusterResponse::Duplicate,
                Err(StreamAddError::Db) => ClusterResponse::Error("db".into()),
            },
            ClusterCommand::BeginDeleteStream { id } => match self.db.stream_disable(id) {
                Some(true) => {
                    self.emit_effect(StateEffect::DrainStream(id.clone()));
                    ClusterResponse::Ok
                }
                Some(false) => ClusterResponse::NotFound,
                None => ClusterResponse::Error("db".into()),
            },
            ClusterCommand::FinalizeDeleteStream { id } => {
                // Only delete while still pending — a stale recovery proposal
                // from a lagging replica must not wipe a stream that was
                // re-enabled (or never deleted) on the leader.
                match self.db.stream_delete_if_pending(id) {
                    Some(true) => {
                        self.emit_effect(StateEffect::ClearDrainStream(id.clone()));
                        ClusterResponse::Ok
                    }
                    // Authoritative on the leader: distinguish a genuinely
                    // absent row (NotFound) from one that still exists but is
                    // no longer pending (Conflict), so a requesting follower
                    // never has to re-check its possibly-stale local state.
                    Some(false) => match self.db.stream_get(id) {
                        crate::db::DbLookup::Missing => ClusterResponse::NotFound,
                        crate::db::DbLookup::Failed => ClusterResponse::Error("db".into()),
                        crate::db::DbLookup::Ok(_) => ClusterResponse::Conflict,
                    },
                    None => ClusterResponse::Error("db".into()),
                }
            }
            ClusterCommand::SetStreamEnabled { id, enabled } => {
                if self.db.stream_set_enabled(id, *enabled) {
                    ClusterResponse::Ok
                } else {
                    // Missing stream is idempotent (e.g. delete-rollback after
                    // finalize already removed the row). Do not return Error —
                    // that stalls Raft apply for every follower.
                    match self.db.stream_get(id) {
                        crate::db::DbLookup::Missing => ClusterResponse::NotFound,
                        _ => ClusterResponse::Error("db".into()),
                    }
                }
            }
            ClusterCommand::CreateViewer { viewer } => match self.db.viewer_add(viewer) {
                Ok(()) => ClusterResponse::Ok,
                Err(ViewerAddError::Duplicate) => ClusterResponse::Duplicate,
                Err(ViewerAddError::Db) => ClusterResponse::Error("db".into()),
            },
            ClusterCommand::DeleteViewer {
                stream_id,
                viewer_id,
            } => {
                // The HTTP layer only pre-checks "not the last viewer" before
                // proposing this command, so two concurrent last-viewer deletes
                // submitted through different nodes can both pass that check.
                // Raft serializes the actual applies, so recheck here — the
                // second one to apply must not leave the stream with zero
                // usable play keys.
                let viewers = self.db.viewer_list(stream_id);
                let would_remove_last =
                    viewers.len() == 1 && viewers.iter().any(|v| v.id == *viewer_id);
                if would_remove_last {
                    return ClusterResponse::Conflict;
                }
                match self.db.viewer_delete(stream_id, viewer_id) {
                    Some(true) => {
                        self.db.players_deactivate_for_viewer(viewer_id);
                        self.emit_effect(StateEffect::RevokeViewer(viewer_id.clone()));
                        ClusterResponse::Ok
                    }
                    Some(false) => ClusterResponse::NotFound,
                    None => ClusterResponse::Error("db".into()),
                }
            }
            ClusterCommand::SetApiToken { token } => match self.db.token_replace(token) {
                Ok(()) => {
                    self.emit_effect(StateEffect::ApiToken(token.clone()));
                    ClusterResponse::Ok
                }
                Err(e) => ClusterResponse::Error(e),
            },
            ClusterCommand::AcquireStreamOwner {
                stream_id,
                node_id,
                epoch,
                acquired_at,
            } => match self
                .db
                .stream_owner_acquire(stream_id, *node_id, *epoch, *acquired_at)
            {
                Ok(ep) => ClusterResponse::OwnerEpoch(ep),
                Err(crate::db::OwnerError::Conflict) => ClusterResponse::Conflict,
                Err(crate::db::OwnerError::NotFound) => ClusterResponse::NotFound,
                Err(crate::db::OwnerError::Db) => ClusterResponse::Error("db".into()),
            },
            ClusterCommand::ReleaseStreamOwner { stream_id, epoch } => {
                match self.db.stream_owner_release(stream_id, *epoch) {
                    // Idempotent: already released or epoch mismatch on follower catch-up.
                    Ok(_) => {
                        self.emit_effect(StateEffect::OwnershipChanged);
                        ClusterResponse::Ok
                    }
                    Err(OwnerError::Db) => ClusterResponse::Error("db".into()),
                    Err(OwnerError::Conflict) => ClusterResponse::Error("db".into()),
                    // stream_owner_release never actually returns NotFound (it
                    // only fails with Db), but OwnerError is shared with the
                    // acquire path so the match must stay exhaustive.
                    Err(OwnerError::NotFound) => ClusterResponse::Error("db".into()),
                }
            }
            ClusterCommand::ReleaseOwnersForNode { node_id } => {
                match self.db.stream_owners_release_for_node(*node_id) {
                    Ok(_) => {
                        self.emit_effect(StateEffect::OwnershipChanged);
                        ClusterResponse::Ok
                    }
                    Err(OwnerError::Db) => ClusterResponse::Error("db".into()),
                    Err(OwnerError::Conflict) => ClusterResponse::Error("db".into()),
                    // Same rationale as ReleaseStreamOwner above.
                    Err(OwnerError::NotFound) => ClusterResponse::Error("db".into()),
                }
            }
            ClusterCommand::SeedFromStandalone {
                streams,
                viewers,
                api_token,
                pending_delete_stream_ids,
            } => {
                // Bootstrap node already has these rows; Duplicate is expected.
                // Any other failure must surface so last_applied does not advance.
                for s in streams {
                    match self.db.stream_insert_only(s) {
                        Ok(()) | Err(StreamAddError::Duplicate) => {}
                        Err(StreamAddError::Db) => {
                            return ClusterResponse::Error(
                                "stream_insert_only failed during SeedFromStandalone".into(),
                            );
                        }
                    }
                }
                for v in viewers {
                    match self.db.viewer_add(v) {
                        Ok(()) | Err(ViewerAddError::Duplicate) => {}
                        Err(ViewerAddError::Db) => {
                            return ClusterResponse::Error(
                                "viewer_add failed during SeedFromStandalone".into(),
                            );
                        }
                    }
                }
                if let Some(token) = api_token {
                    if let Err(e) = self.db.token_replace(token) {
                        return ClusterResponse::Error(e);
                    }
                    self.emit_effect(StateEffect::ApiToken(token.clone()));
                }
                // A standalone DB can hold streams left mid-delete; replicate
                // that state so followers mark them pending too and
                // FinalizeDeleteStream can complete instead of leaving a
                // disabled ghost stream behind.
                for stream_id in pending_delete_stream_ids {
                    if self.db.stream_disable(&stream_id).is_none() {
                        return ClusterResponse::Error(
                            "stream_disable failed during SeedFromStandalone".into(),
                        );
                    }
                    // Mirror BeginDeleteStream: kick local sessions and stop
                    // advertising the stream so the leader can finalize.
                    self.emit_effect(StateEffect::DrainStream(stream_id.clone()));
                }
                ClusterResponse::Ok
            }
            ClusterCommand::SetClusterId { id } => match self.db.setting_set("cluster_id", id) {
                Ok(()) => ClusterResponse::Ok,
                Err(e) => ClusterResponse::Error(e),
            },
        }
    }

    fn build_app_snapshot(&self) -> Result<AppSnapshot, String> {
        let (streams, viewers, owners, api_token, cluster_id, pending_delete_stream_ids) =
            self.db.read_replicated_snapshot()?;
        Ok(AppSnapshot {
            streams,
            viewers,
            owners,
            api_token,
            cluster_id,
            last_applied_index: self.last_applied.lock().map(|l| l.index),
            pending_delete_stream_ids,
        })
    }

    fn install_app_snapshot(&self, snap: &AppSnapshot) -> Result<(), String> {
        // Everything below runs inside one locked connection + one transaction
        // so a concurrently running RTMP connection on this node can never
        // observe a torn state: streams/local sessions deleted but not yet
        // reinserted, or a captured pre-delete session row reinserted after
        // (and overwriting) a legitimate concurrent update. `Db::with_conn`
        // holds the connection mutex for the whole closure, which is what
        // actually provides that exclusion — every other `db.rs` accessor
        // goes through the same mutex.
        //
        // Side effects collected here are emitted after commit so the RTMP
        // poll loop drains/revokes in-memory sessions that SQLite no longer
        // (or no longer fully) authorizes.
        let (revoke_viewers, drain_streams, dropped_player_viewers) = self.db.with_conn(|conn| -> Result<(Vec<String>, Vec<String>, Vec<String>), String> {
            let mut revoke_viewers = Vec::new();
            let mut drain_streams = Vec::new();
            // Active player rows whose parent stream or viewer didn't
            // survive the snapshot are skipped below rather than
            // reinserted (see the `local_players` loop) — permanently
            // dropped from `players`, not just deactivated. Track them so
            // `active_player_counts` (Db's in-memory viewer session
            // counter) can be resynced after commit; otherwise it would
            // keep counting a session whose row no longer exists.
            let mut dropped_player_viewers = Vec::new();
            let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;

            // Snapshot local session children before deleting `streams` — FK
            // CASCADE would otherwise wipe publishers/players/stats_samples.
            let mut stmt = tx
                .prepare(
                    "SELECT id,stream_id,app,stream_name,video_codec,audio_codec,\
                     video_width,video_height,fps,audio_sample_rate,audio_channels,\
                     bytes_in,bitrate_kbps,rtt_ms,connected_at,active FROM publishers",
                )
                .map_err(|e| e.to_string())?;
            let local_pubs: Vec<crate::db::Publisher> = stmt
                .query_map([], |row| {
                    Ok(crate::db::Publisher {
                        id: row.get(0)?,
                        stream_id: row.get(1)?,
                        app: row.get(2)?,
                        stream_name: row.get(3)?,
                        video_codec: row.get(4)?,
                        audio_codec: row.get(5)?,
                        video_width: row.get(6)?,
                        video_height: row.get(7)?,
                        fps: row.get(8)?,
                        audio_sample_rate: row.get(9)?,
                        audio_channels: row.get(10)?,
                        bytes_in: u64::try_from(row.get::<_, i64>(11)?).unwrap_or(0),
                        bitrate_kbps: row.get(12)?,
                        rtt_ms: row.get(13)?,
                        connected_at: row.get(14)?,
                        active: row.get(15)?,
                    })
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            drop(stmt);

            let mut stmt = tx
                .prepare(
                    "SELECT id,stream_id,viewer_id,app,stream_name,bytes_out,\
                     bitrate_kbps,rtt_ms,connected_at,active FROM players",
                )
                .map_err(|e| e.to_string())?;
            let local_players: Vec<crate::db::Player> = stmt
                .query_map([], |row| {
                    Ok(crate::db::Player {
                        id: row.get(0)?,
                        stream_id: row.get(1)?,
                        viewer_id: row.get(2)?,
                        app: row.get(3)?,
                        stream_name: row.get(4)?,
                        bytes_out: u64::try_from(row.get::<_, i64>(5)?).unwrap_or(0),
                        bitrate_kbps: row.get(6)?,
                        rtt_ms: row.get(7)?,
                        connected_at: row.get(8)?,
                        active: row.get(9)?,
                    })
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            drop(stmt);

            let mut stmt = tx
                .prepare(
                    "SELECT stream_id,bitrate_in_kbps,fps,width,height,video_codec,\
                     audio_codec,player_count,ts FROM stats_samples",
                )
                .map_err(|e| e.to_string())?;
            let local_stats: Vec<crate::db::StatSample> = stmt
                .query_map([], |row| {
                    Ok(crate::db::StatSample {
                        stream_id: row.get(0)?,
                        bitrate_in_kbps: row.get(1)?,
                        fps: row.get(2)?,
                        width: row.get(3)?,
                        height: row.get(4)?,
                        video_codec: row.get(5)?,
                        audio_codec: row.get(6)?,
                        player_count: row.get(7)?,
                        ts: row.get(8)?,
                    })
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            drop(stmt);

            // Only wipe durable replicated tables.
            tx.execute_batch(
                "DELETE FROM stream_owners;
                 DELETE FROM stream_viewers;
                 DELETE FROM streams;",
            )
            .map_err(|e| e.to_string())?;
            for s in &snap.streams {
                tx.execute(
                    "INSERT INTO streams (id,name,app,publish_key,play_key,stats_key,enabled,created_at) \
                     VALUES (?,?,?,?,?,?,?,?)",
                    params![
                        s.id,
                        s.name,
                        s.app,
                        s.publish_key,
                        s.play_key,
                        s.stats_key,
                        s.enabled,
                        s.created_at,
                    ],
                )
                .map_err(|e| e.to_string())?;
            }
            for stream_id in &snap.pending_delete_stream_ids {
                tx.execute(
                    "UPDATE streams SET pending_delete=1 WHERE id=?",
                    params![stream_id],
                )
                .map_err(|e| e.to_string())?;
                // Mirror BeginDeleteStream: lagging nodes that install a
                // snapshot taken mid-delete must populate deleted_streams so
                // local sessions are kicked and heartbeats stop advertising
                // them (otherwise the leader waits forever to finalize).
                drain_streams.push(stream_id.clone());
            }
            // Viewers must be reinserted before local player rows are
            // reconsidered below, so the players loop can check that a
            // player's viewer_id actually survived the snapshot (and not
            // just its stream) before preserving it.
            for v in &snap.viewers {
                tx.execute(
                    "INSERT INTO stream_viewers (id,stream_id,name,play_key,enabled,created_at) \
                     VALUES (?,?,?,?,?,?)",
                    params![
                        v.id,
                        v.stream_id,
                        v.name,
                        v.play_key,
                        v.enabled,
                        v.created_at,
                    ],
                )
                .map_err(|e| e.to_string())?;
            }

            // Reinsert local session rows whose parent stream survived the snapshot.
            for p in &local_pubs {
                let keep: bool = tx
                    .query_row(
                        "SELECT 1 FROM streams WHERE id=?",
                        params![p.stream_id],
                        |_| Ok(true),
                    )
                    .optional()
                    .map_err(|e| e.to_string())?
                    .unwrap_or(false);
                if !keep {
                    // Fully deleted streams are absent from pending_delete_ids;
                    // still drain the live RTMP publisher socket.
                    drain_streams.push(p.stream_id.clone());
                    continue;
                }
                let bytes_in = i64::try_from(p.bytes_in).unwrap_or(i64::MAX);
                tx.execute(
                    "INSERT INTO publishers \
                     (id,stream_id,app,stream_name,video_codec,audio_codec,video_width,\
                      video_height,fps,audio_sample_rate,audio_channels,bytes_in,\
                      bitrate_kbps,rtt_ms,connected_at,active) \
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    params![
                        p.id,
                        p.stream_id,
                        p.app,
                        p.stream_name,
                        p.video_codec,
                        p.audio_codec,
                        p.video_width,
                        p.video_height,
                        p.fps,
                        p.audio_sample_rate,
                        p.audio_channels,
                        bytes_in,
                        p.bitrate_kbps,
                        p.rtt_ms,
                        p.connected_at,
                        p.active,
                    ],
                )
                .map_err(|e| e.to_string())?;
            }
            for p in &local_players {
                let stream_keep: bool = tx
                    .query_row(
                        "SELECT 1 FROM streams WHERE id=?",
                        params![p.stream_id],
                        |_| Ok(true),
                    )
                    .optional()
                    .map_err(|e| e.to_string())?
                    .unwrap_or(false);
                // A viewer deleted through the snapshot (e.g. a concurrent
                // DeleteViewer that committed before this snapshot) must not
                // let its still-connected player session survive reinsertion
                // — that would keep a revoked play key authenticated and
                // relaying media until the connection happens to drop.
                let viewer_keep: bool = tx
                    .query_row(
                        "SELECT 1 FROM stream_viewers WHERE id=?",
                        params![p.viewer_id],
                        |_| Ok(true),
                    )
                    .optional()
                    .map_err(|e| e.to_string())?
                    .unwrap_or(false);
                if !stream_keep || !viewer_keep {
                    // Skipping the SQLite row is not enough — DbRtmpBridge may
                    // still hold the authenticated socket. RevokeViewer makes
                    // the RTMP poll loop kick it; DrainStream covers a stream
                    // that the snapshot fully removed.
                    if !stream_keep {
                        drain_streams.push(p.stream_id.clone());
                    } else if !viewer_keep {
                        revoke_viewers.push(p.viewer_id.clone());
                    }
                    if p.active {
                        dropped_player_viewers.push(p.viewer_id.clone());
                    }
                    continue;
                }
                let bytes_out = i64::try_from(p.bytes_out).unwrap_or(i64::MAX);
                tx.execute(
                    "INSERT INTO players \
                     (id,stream_id,viewer_id,app,stream_name,bytes_out,bitrate_kbps,\
                      rtt_ms,connected_at,active) \
                     VALUES (?,?,?,?,?,?,?,?,?,?)",
                    params![
                        p.id,
                        p.stream_id,
                        p.viewer_id,
                        p.app,
                        p.stream_name,
                        bytes_out,
                        p.bitrate_kbps,
                        p.rtt_ms,
                        p.connected_at,
                        p.active,
                    ],
                )
                .map_err(|e| e.to_string())?;
            }
            for s in &local_stats {
                let keep: bool = tx
                    .query_row(
                        "SELECT 1 FROM streams WHERE id=?",
                        params![s.stream_id],
                        |_| Ok(true),
                    )
                    .optional()
                    .map_err(|e| e.to_string())?
                    .unwrap_or(false);
                if !keep {
                    continue;
                }
                tx.execute(
                    "INSERT INTO stats_samples \
                     (stream_id,bitrate_in_kbps,fps,width,height,video_codec,\
                      audio_codec,player_count,ts) \
                     VALUES (?,?,?,?,?,?,?,?,?)",
                    params![
                        s.stream_id,
                        s.bitrate_in_kbps,
                        s.fps,
                        s.width,
                        s.height,
                        s.video_codec,
                        s.audio_codec,
                        s.player_count,
                        s.ts,
                    ],
                )
                .map_err(|e| e.to_string())?;
            }
            for o in &snap.owners {
                tx.execute(
                    "INSERT INTO stream_owners(stream_id, owner_node_id, epoch, acquired_at) \
                     VALUES (?,?,?,?)",
                    params![
                        o.stream_id,
                        o.owner_node_id as i64,
                        o.epoch as i64,
                        o.acquired_at,
                    ],
                )
                .map_err(|e| e.to_string())?;
            }
            if let Some(token) = &snap.api_token {
                tx.execute(
                    "INSERT INTO settings(key, val) VALUES('api_token', ?) \
                     ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                    params![token],
                )
                .map_err(|e| e.to_string())?;
            }
            if let Some(cid) = &snap.cluster_id {
                tx.execute(
                    "INSERT INTO settings(key, val) VALUES('cluster_id', ?) \
                     ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                    params![cid],
                )
                .map_err(|e| e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())?;
            Ok((revoke_viewers, drain_streams, dropped_player_viewers))
        })?;

        for stream_id in drain_streams {
            self.emit_effect(StateEffect::DrainStream(stream_id));
        }
        for viewer_id in revoke_viewers {
            self.emit_effect(StateEffect::RevokeViewer(viewer_id));
        }
        // Resync Db's in-memory active-viewer counter for every session
        // dropped above instead of reinserted — `players_deactivate_for_viewer`
        // zeroes it unconditionally, which is correct here even though the
        // SQLite row is already gone (its UPDATE just matches zero rows).
        for viewer_id in dropped_player_viewers {
            self.db.players_deactivate_for_viewer(&viewer_id);
        }
        if let Some(token) = &snap.api_token {
            self.emit_effect(StateEffect::ApiToken(token.clone()));
        }
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let last_applied = *self.last_applied.lock();
        let last_membership = self.last_membership.lock().clone();
        let data = self.build_app_snapshot().map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_state_machine(&std::io::Error::other(e)),
        })?;
        let bytes = serde_json::to_vec(&data).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_state_machine(&e),
        })?;
        let idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match last_applied {
            Some(last) => format!("{}-{}-{}", last.leader_id, last.index, idx),
            None => format!("--{idx}"),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        *self.current_snapshot.lock() = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        });
        // Persist snapshot blob for crash recovery.
        let meta_json = serde_json::to_string(&meta).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(None, &e),
        })?;
        self.db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO raft_snapshots(id, meta_json, data) VALUES(1, ?, ?) \
                 ON CONFLICT(id) DO UPDATE SET meta_json=excluded.meta_json, data=excluded.data",
                params![meta_json, bytes],
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(None, &e),
            })?;
            Ok::<(), StorageError<u64>>(())
        })?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for SqliteStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        Ok((
            *self.last_applied.lock(),
            self.last_membership.lock().clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClusterResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut responses = Vec::new();
        for entry in entries {
            // Apply first, then advance last_applied. Never persist_applied before
            // the command commits — a crash between would skip the mutation.
            match entry.payload {
                EntryPayload::Blank => {
                    self.persist_applied_tx(entry.log_id, None)?;
                    responses.push(ClusterResponse::Ok);
                }
                EntryPayload::Normal(ref cmd) => {
                    let resp = match cmd {
                        ClusterCommand::AcquireStreamOwner {
                            stream_id,
                            node_id,
                            epoch: _,
                            acquired_at,
                        } => {
                            // Raft log index is cluster-wide monotonic — use it as
                            // the fencing epoch so release cannot recycle tokens.
                            let epoch = entry.log_id.index.max(1);
                            match self.db.stream_owner_acquire(
                                stream_id,
                                *node_id,
                                epoch,
                                *acquired_at,
                            ) {
                                Ok(ep) => {
                                    self.emit_effect(StateEffect::OwnershipChanged);
                                    ClusterResponse::OwnerEpoch(ep)
                                }
                                Err(crate::db::OwnerError::Conflict) => ClusterResponse::Conflict,
                                Err(crate::db::OwnerError::NotFound) => ClusterResponse::NotFound,
                                Err(crate::db::OwnerError::Db) => {
                                    ClusterResponse::Error("db".into())
                                }
                            }
                        }
                        other => self.apply_command(other),
                    };
                    // Any apply-time Error (not only the literal "db") must
                    // fail the storage apply so last_applied does not advance
                    // past a partially omitted mutation.
                    if let ClusterResponse::Error(ref msg) = resp {
                        return Err(StorageError::IO {
                            source: StorageIOError::<u64>::write_state_machine(
                                &std::io::Error::other(msg.clone()),
                            ),
                        });
                    }
                    // Command SQL already committed via Db helpers; persist index next.
                    // Membership+index use a single tx; command+index cannot share a
                    // conn with Db helpers (non-reentrant mutex) — order still safe:
                    // lagging last_applied re-applies idempotently after crash.
                    self.persist_applied_tx(entry.log_id, None)?;
                    responses.push(resp);
                }
                EntryPayload::Membership(ref mem) => {
                    let stored = StoredMembership::new(Some(entry.log_id), mem.clone());
                    let json = serde_json::to_string(&stored).map_err(|e| StorageError::IO {
                        source: StorageIOError::<u64>::write_state_machine(&e),
                    })?;
                    self.persist_applied_tx(entry.log_id, Some(&json))?;
                    *self.last_membership.lock() = stored;
                    responses.push(ClusterResponse::Ok);
                }
            }
        }
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let app: AppSnapshot = serde_json::from_slice(&data).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), &e),
        })?;
        self.install_app_snapshot(&app)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e),
                ),
            })?;
        *self.last_membership.lock() = meta.last_membership.clone();
        // Persist both values in a single transaction (persist_applied_tx):
        // a crash between two separate writes here would leave the snapshot
        // boundary recorded with stale membership, and once logs through
        // that boundary are compacted, restart could resume with the wrong
        // voter set.
        if let Some(log_id) = meta.last_log_id {
            let membership_json =
                serde_json::to_string(&meta.last_membership).map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::write_state_machine(&e),
                })?;
            self.persist_applied_tx(log_id, Some(&membership_json))?;
        } else {
            *self.last_applied.lock() = meta.last_log_id;
        }
        *self.current_snapshot.lock() = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        // Persist the received snapshot the same way build_snapshot() does —
        // otherwise a restart before this node next builds its own snapshot
        // would lose it even though last_applied already records the newer
        // boundary and the corresponding logs may already be compacted,
        // leaving this node unable to supply a lagging peer's recovery point.
        let meta_json = serde_json::to_string(meta).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), &e),
        })?;
        self.db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO raft_snapshots(id, meta_json, data) VALUES(1, ?, ?) \
                 ON CONFLICT(id) DO UPDATE SET meta_json=excluded.meta_json, data=excluded.data",
                params![meta_json, data],
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), &e),
            })?;
            Ok::<(), StorageError<u64>>(())
        })?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        if let Some(snap) = self.current_snapshot.lock().clone() {
            return Ok(Some(Snapshot {
                meta: snap.meta,
                snapshot: Box::new(Cursor::new(snap.data)),
            }));
        }
        // Try load from DB. A genuine read failure (I/O, corruption,
        // row-decoding) must not be swallowed into "no snapshot" — that
        // would make OpenRaft believe recovery data is absent instead of
        // surfacing the underlying storage failure.
        let loaded = self
            .db
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT meta_json, data FROM raft_snapshots WHERE id=1",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )
                .optional()
            })
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::read_snapshot(None, &e),
            })?;
        if let Some((meta_json, data)) = loaded {
            let meta: SnapshotMeta<u64, openraft::BasicNode> = serde_json::from_str(&meta_json)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::read_snapshot(None, &e),
                })?;
            *self.current_snapshot.lock() = Some(StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            });
            return Ok(Some(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            }));
        }
        Ok(None)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::mpsc::Receiver;

    use openraft::{BasicNode, CommittedLeaderId, Membership};

    use super::*;
    use crate::db::{Player, Publisher, StatSample, Stream, StreamViewer};

    fn key(prefix: &str, id: &str) -> String {
        format!("{prefix}_{id:x<32}")
    }

    fn stream(id: &str) -> Stream {
        Stream {
            id: id.into(),
            name: format!("name-{id}"),
            app: "live".into(),
            publish_key: key("pub", id),
            play_key: key("play", id),
            stats_key: key("stat", id),
            enabled: true,
            created_at: 1,
        }
    }

    fn default_viewer(s: &Stream) -> StreamViewer {
        StreamViewer {
            id: format!("v-{}", s.id),
            stream_id: s.id.clone(),
            name: "Player 1".into(),
            play_key: s.play_key.clone(),
            enabled: true,
            created_at: 1,
        }
    }

    fn extra_viewer(stream_id: &str, id: &str) -> StreamViewer {
        StreamViewer {
            id: id.into(),
            stream_id: stream_id.into(),
            name: id.into(),
            play_key: key("pk", id),
            enabled: true,
            created_at: 2,
        }
    }

    fn log_id(index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(1, 1), index)
    }

    fn normal(index: u64, cmd: ClusterCommand) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    fn membership(voters: &[u64]) -> Membership<u64, BasicNode> {
        let nodes: BTreeMap<u64, BasicNode> = voters
            .iter()
            .map(|id| {
                (
                    *id,
                    BasicNode {
                        addr: format!("127.0.0.1:{}", 7000 + id),
                    },
                )
            })
            .collect();
        Membership::new(vec![voters.iter().copied().collect::<BTreeSet<_>>()], nodes)
    }

    fn sm_with_effects() -> (SqliteStateMachine, Receiver<StateEffect>) {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let sm = SqliteStateMachine::new(db).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        sm.set_effects_tx(tx);
        (sm, rx)
    }

    fn drain(rx: &Receiver<StateEffect>) -> Vec<StateEffect> {
        rx.try_iter().collect()
    }

    fn exec(sm: &SqliteStateMachine, sql: &str) {
        sm.db().with_conn(|c| c.execute_batch(sql)).unwrap();
    }

    /// Apply one command through the Raft apply path, returning its response.
    async fn apply_one(
        sm: &mut SqliteStateMachine,
        index: u64,
        cmd: ClusterCommand,
    ) -> Result<ClusterResponse, StorageError<u64>> {
        sm.apply(vec![normal(index, cmd)])
            .await
            .map(|mut v| v.pop().unwrap())
    }

    fn create(s: &Stream) -> ClusterCommand {
        ClusterCommand::CreateStream {
            stream: s.clone(),
            default_viewer: default_viewer(s),
        }
    }

    #[tokio::test]
    async fn fresh_state_machine_has_no_applied_state() {
        let (mut sm, _rx) = sm_with_effects();
        let (applied, mem) = sm.applied_state().await.unwrap();
        assert!(applied.is_none());
        assert!(mem.log_id().is_none());
        assert_eq!(sm.last_membership().membership().nodes().count(), 0);
        assert!(sm.get_current_snapshot().await.unwrap().is_none());
        let cursor = sm.begin_receiving_snapshot().await.unwrap();
        assert!(cursor.get_ref().is_empty());
    }

    #[tokio::test]
    async fn blank_and_membership_entries_persist_and_reload() {
        let (mut sm, _rx) = sm_with_effects();
        let resp = sm
            .apply(vec![
                Entry {
                    log_id: log_id(1),
                    payload: EntryPayload::Blank,
                },
                Entry {
                    log_id: log_id(2),
                    payload: EntryPayload::Membership(membership(&[1, 2])),
                },
            ])
            .await
            .unwrap();
        assert_eq!(resp, vec![ClusterResponse::Ok, ClusterResponse::Ok]);
        let (applied, mem) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(2)));
        assert_eq!(mem.log_id(), &Some(log_id(2)));
        let voters: Vec<u64> = mem.membership().voter_ids().collect();
        assert_eq!(voters, vec![1, 2]);

        // A new state machine over the same DB restores both values.
        let mut reopened = SqliteStateMachine::new(Arc::clone(sm.db())).unwrap();
        let (applied2, mem2) = reopened.applied_state().await.unwrap();
        assert_eq!(applied2, Some(log_id(2)));
        assert_eq!(mem2, mem);
    }

    #[test]
    fn corrupt_persisted_meta_fails_construction() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.with_conn(|c| {
            c.execute(
                "INSERT INTO raft_meta(key,val) VALUES('last_applied','not json')",
                [],
            )
        })
        .unwrap();
        assert!(SqliteStateMachine::new(Arc::clone(&db)).is_err());

        let db = Arc::new(Db::open(":memory:").unwrap());
        db.with_conn(|c| {
            c.execute(
                "INSERT INTO raft_meta(key,val) VALUES('last_membership','{bad')",
                [],
            )
        })
        .unwrap();
        assert!(SqliteStateMachine::new(db).is_err());
    }

    #[test]
    fn missing_meta_table_fails_construction() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.with_conn(|c| c.execute_batch("DROP TABLE raft_meta"))
            .unwrap();
        assert!(SqliteStateMachine::new(Arc::clone(&db)).is_err());
        assert!(SqliteStateMachine::load_last_membership(&db).is_err());
    }

    #[tokio::test]
    async fn stream_lifecycle_commands_and_effects() {
        let (mut sm, rx) = sm_with_effects();
        let s = stream("s1");
        assert_eq!(
            apply_one(&mut sm, 1, create(&s)).await.unwrap(),
            ClusterResponse::Ok
        );
        assert_eq!(
            apply_one(&mut sm, 2, create(&s)).await.unwrap(),
            ClusterResponse::Duplicate
        );
        assert_eq!(sm.db().viewer_list("s1").len(), 1);

        // Disable / re-enable, and a missing stream is an idempotent NotFound.
        assert_eq!(
            apply_one(
                &mut sm,
                3,
                ClusterCommand::SetStreamEnabled {
                    id: "s1".into(),
                    enabled: false
                }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert_eq!(
            apply_one(
                &mut sm,
                4,
                ClusterCommand::SetStreamEnabled {
                    id: "missing".into(),
                    enabled: true
                }
            )
            .await
            .unwrap(),
            ClusterResponse::NotFound
        );

        // Finalize on a live (non-pending) stream is a Conflict; on a missing
        // stream NotFound.
        assert_eq!(
            apply_one(
                &mut sm,
                5,
                ClusterCommand::FinalizeDeleteStream { id: "s1".into() }
            )
            .await
            .unwrap(),
            ClusterResponse::Conflict
        );
        assert_eq!(
            apply_one(
                &mut sm,
                6,
                ClusterCommand::FinalizeDeleteStream { id: "nope".into() }
            )
            .await
            .unwrap(),
            ClusterResponse::NotFound
        );
        assert_eq!(
            apply_one(
                &mut sm,
                7,
                ClusterCommand::BeginDeleteStream { id: "nope".into() }
            )
            .await
            .unwrap(),
            ClusterResponse::NotFound
        );
        assert!(drain(&rx).is_empty());

        assert_eq!(
            apply_one(
                &mut sm,
                8,
                ClusterCommand::BeginDeleteStream { id: "s1".into() }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert_eq!(sm.db().stream_pending_delete("s1"), Some(true));
        assert!(matches!(
            drain(&rx).as_slice(),
            [StateEffect::DrainStream(id)] if id == "s1"
        ));
        assert_eq!(
            apply_one(
                &mut sm,
                9,
                ClusterCommand::FinalizeDeleteStream { id: "s1".into() }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert!(matches!(
            sm.db().stream_get("s1"),
            crate::db::DbLookup::Missing
        ));
        assert!(matches!(
            drain(&rx).as_slice(),
            [StateEffect::ClearDrainStream(id)] if id == "s1"
        ));
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(9)));
    }

    #[tokio::test]
    async fn viewer_commands_and_last_viewer_guard() {
        let (mut sm, rx) = sm_with_effects();
        let s = stream("sv");
        apply_one(&mut sm, 1, create(&s)).await.unwrap();
        let dv = default_viewer(&s);

        // Deleting the only viewer must be refused.
        assert_eq!(
            apply_one(
                &mut sm,
                2,
                ClusterCommand::DeleteViewer {
                    stream_id: "sv".into(),
                    viewer_id: dv.id.clone()
                }
            )
            .await
            .unwrap(),
            ClusterResponse::Conflict
        );

        let v2 = extra_viewer("sv", "v2");
        assert_eq!(
            apply_one(
                &mut sm,
                3,
                ClusterCommand::CreateViewer { viewer: v2.clone() }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert_eq!(
            apply_one(
                &mut sm,
                4,
                ClusterCommand::CreateViewer { viewer: v2.clone() }
            )
            .await
            .unwrap(),
            ClusterResponse::Duplicate
        );
        assert_eq!(
            apply_one(
                &mut sm,
                5,
                ClusterCommand::DeleteViewer {
                    stream_id: "sv".into(),
                    viewer_id: "ghost".into()
                }
            )
            .await
            .unwrap(),
            ClusterResponse::NotFound
        );
        assert_eq!(
            apply_one(
                &mut sm,
                6,
                ClusterCommand::DeleteViewer {
                    stream_id: "sv".into(),
                    viewer_id: "v2".into()
                }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert!(matches!(
            drain(&rx).as_slice(),
            [StateEffect::RevokeViewer(id)] if id == "v2"
        ));
        assert_eq!(sm.db().viewer_list("sv").len(), 1);
    }

    #[tokio::test]
    async fn token_and_cluster_id_commands() {
        let (mut sm, rx) = sm_with_effects();
        assert_eq!(
            apply_one(
                &mut sm,
                1,
                ClusterCommand::SetApiToken {
                    token: "tok-1".into()
                }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert_eq!(sm.db().token_get().unwrap().as_deref(), Some("tok-1"));
        assert!(matches!(
            drain(&rx).as_slice(),
            [StateEffect::ApiToken(t)] if t == "tok-1"
        ));
        assert_eq!(
            apply_one(
                &mut sm,
                2,
                ClusterCommand::SetClusterId { id: "cid".into() }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert_eq!(sm.db().setting_get("cluster_id").as_deref(), Some("cid"));
    }

    #[tokio::test]
    async fn ownership_commands_use_log_index_as_epoch() {
        let (mut sm, rx) = sm_with_effects();
        apply_one(&mut sm, 1, create(&stream("o1"))).await.unwrap();
        apply_one(&mut sm, 2, create(&stream("o2"))).await.unwrap();

        let acquire = |stream_id: &str, node_id: u64| ClusterCommand::AcquireStreamOwner {
            stream_id: stream_id.into(),
            node_id,
            epoch: 999,
            acquired_at: 5,
        };
        // Epoch is the log index, not the proposed value.
        assert_eq!(
            apply_one(&mut sm, 7, acquire("o1", 1)).await.unwrap(),
            ClusterResponse::OwnerEpoch(7)
        );
        assert!(matches!(
            drain(&rx).as_slice(),
            [StateEffect::OwnershipChanged]
        ));
        assert_eq!(
            apply_one(&mut sm, 8, acquire("o1", 2)).await.unwrap(),
            ClusterResponse::Conflict
        );
        assert_eq!(
            apply_one(&mut sm, 9, acquire("missing", 1)).await.unwrap(),
            ClusterResponse::NotFound
        );
        assert_eq!(
            apply_one(&mut sm, 10, acquire("o2", 1)).await.unwrap(),
            ClusterResponse::OwnerEpoch(10)
        );
        drain(&rx);

        // Release with a stale epoch is idempotent Ok and leaves the row.
        assert_eq!(
            apply_one(
                &mut sm,
                11,
                ClusterCommand::ReleaseStreamOwner {
                    stream_id: "o1".into(),
                    epoch: 1
                }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert!(sm.db().stream_owner_get("o1").is_some());
        assert_eq!(
            apply_one(
                &mut sm,
                12,
                ClusterCommand::ReleaseStreamOwner {
                    stream_id: "o1".into(),
                    epoch: 7
                }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert!(sm.db().stream_owner_get("o1").is_none());
        assert_eq!(
            apply_one(
                &mut sm,
                13,
                ClusterCommand::ReleaseOwnersForNode { node_id: 1 }
            )
            .await
            .unwrap(),
            ClusterResponse::Ok
        );
        assert!(sm.db().stream_owner_list().is_empty());
        // Every release apply (even the stale-epoch no-op) refreshes owners.
        let effects = drain(&rx);
        assert_eq!(effects.len(), 3);
        assert!(
            effects
                .iter()
                .all(|e| matches!(e, StateEffect::OwnershipChanged))
        );
    }

    #[test]
    fn apply_command_acquire_path_keeps_proposed_epoch() {
        // `apply` special-cases AcquireStreamOwner; the generic apply_command
        // arm (used only when called directly) stores the proposed epoch.
        let (sm, _rx) = sm_with_effects();
        let s = stream("ac");
        assert_eq!(sm.apply_command(&create(&s)), ClusterResponse::Ok);
        let cmd = |node_id| ClusterCommand::AcquireStreamOwner {
            stream_id: "ac".into(),
            node_id,
            epoch: 42,
            acquired_at: 1,
        };
        assert_eq!(sm.apply_command(&cmd(1)), ClusterResponse::OwnerEpoch(42));
        assert_eq!(sm.apply_command(&cmd(2)), ClusterResponse::Conflict);
        assert_eq!(
            sm.apply_command(&ClusterCommand::AcquireStreamOwner {
                stream_id: "none".into(),
                node_id: 1,
                epoch: 1,
                acquired_at: 1,
            }),
            ClusterResponse::NotFound
        );
        exec(&sm, "DROP TABLE stream_owners");
        assert_eq!(
            sm.apply_command(&cmd(1)),
            ClusterResponse::Error("db".into())
        );
    }

    #[tokio::test]
    async fn seed_from_standalone_tolerates_duplicates_and_marks_pending() {
        let (mut sm, rx) = sm_with_effects();
        let a = stream("a");
        let b = stream("b");
        // `a` already exists locally (bootstrap node case).
        apply_one(&mut sm, 1, create(&a)).await.unwrap();
        drain(&rx);
        let cmd = ClusterCommand::SeedFromStandalone {
            streams: vec![a.clone(), b.clone()],
            viewers: vec![default_viewer(&a), default_viewer(&b)],
            api_token: Some("seed-token".into()),
            pending_delete_stream_ids: vec!["b".into()],
        };
        assert_eq!(
            apply_one(&mut sm, 2, cmd).await.unwrap(),
            ClusterResponse::Ok
        );
        assert!(matches!(
            sm.db().stream_get("b"),
            crate::db::DbLookup::Ok(_)
        ));
        assert_eq!(sm.db().viewer_list("b").len(), 1);
        assert_eq!(sm.db().stream_pending_delete("b"), Some(true));
        assert_eq!(sm.db().token_get().unwrap().as_deref(), Some("seed-token"));
        let effects = drain(&rx);
        assert!(matches!(
            effects.as_slice(),
            [StateEffect::ApiToken(t), StateEffect::DrainStream(id)]
                if t == "seed-token" && id == "b"
        ));

        // Without a token nothing token-related is emitted.
        let cmd = ClusterCommand::SeedFromStandalone {
            streams: vec![],
            viewers: vec![],
            api_token: None,
            pending_delete_stream_ids: vec![],
        };
        assert_eq!(
            apply_one(&mut sm, 3, cmd).await.unwrap(),
            ClusterResponse::Ok
        );
        assert!(drain(&rx).is_empty());
    }

    #[tokio::test]
    async fn seed_from_standalone_surfaces_storage_errors() {
        let seed = |streams: Vec<Stream>,
                    viewers: Vec<StreamViewer>,
                    api_token: Option<String>,
                    pending: Vec<String>| ClusterCommand::SeedFromStandalone {
            streams,
            viewers,
            api_token,
            pending_delete_stream_ids: pending,
        };

        // Stream insert failing for a non-constraint reason.
        let (mut sm, _rx) = sm_with_effects();
        exec(&sm, "ALTER TABLE streams RENAME TO streams_gone");
        assert!(
            apply_one(&mut sm, 1, seed(vec![stream("x")], vec![], None, vec![]))
                .await
                .is_err()
        );
        assert!(sm.applied_state().await.unwrap().0.is_none());

        // Viewer insert failing.
        let (mut sm, _rx) = sm_with_effects();
        exec(&sm, "DROP TABLE stream_viewers");
        assert!(
            apply_one(
                &mut sm,
                1,
                seed(vec![], vec![extra_viewer("x", "vx")], None, vec![])
            )
            .await
            .is_err()
        );

        // Token write failing.
        let (mut sm, _rx) = sm_with_effects();
        exec(
            &sm,
            "CREATE TRIGGER no_settings BEFORE INSERT ON settings \
             BEGIN SELECT RAISE(ABORT, 'blocked'); END;",
        );
        assert!(
            apply_one(&mut sm, 1, seed(vec![], vec![], Some("t".into()), vec![]))
                .await
                .is_err()
        );

        // Pending-delete re-mark failing.
        let (mut sm, _rx) = sm_with_effects();
        apply_one(&mut sm, 1, create(&stream("p"))).await.unwrap();
        exec(
            &sm,
            "CREATE TRIGGER no_update BEFORE UPDATE ON streams \
             BEGIN SELECT RAISE(ABORT, 'blocked'); END;",
        );
        assert!(
            apply_one(&mut sm, 2, seed(vec![], vec![], None, vec!["p".into()]))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn command_storage_errors_fail_apply_without_advancing() {
        // CreateStream with a mismatched default viewer is a Db error.
        let (mut sm, _rx) = sm_with_effects();
        let s = stream("e1");
        let mut bad_viewer = default_viewer(&s);
        bad_viewer.stream_id = "other".into();
        assert!(
            apply_one(
                &mut sm,
                1,
                ClusterCommand::CreateStream {
                    stream: s.clone(),
                    default_viewer: bad_viewer
                }
            )
            .await
            .is_err()
        );
        assert!(sm.applied_state().await.unwrap().0.is_none());

        apply_one(&mut sm, 1, create(&s)).await.unwrap();
        apply_one(
            &mut sm,
            2,
            ClusterCommand::CreateViewer {
                viewer: extra_viewer("e1", "ev2"),
            },
        )
        .await
        .unwrap();

        // Mark pending first so the finalize DELETE actually matches a row
        // and trips the trigger below.
        exec(
            &sm,
            "UPDATE streams SET pending_delete=1 WHERE id='e1';
             CREATE TRIGGER no_update BEFORE UPDATE ON streams \
             BEGIN SELECT RAISE(ABORT, 'blocked'); END;
             CREATE TRIGGER no_delete BEFORE DELETE ON streams \
             BEGIN SELECT RAISE(ABORT, 'blocked'); END;
             CREATE TRIGGER no_viewer_delete BEFORE DELETE ON stream_viewers \
             BEGIN SELECT RAISE(ABORT, 'blocked'); END;
             CREATE TRIGGER no_settings BEFORE INSERT ON settings \
             BEGIN SELECT RAISE(ABORT, 'blocked'); END;",
        );
        for cmd in [
            ClusterCommand::BeginDeleteStream { id: "e1".into() },
            ClusterCommand::FinalizeDeleteStream { id: "e1".into() },
            ClusterCommand::SetStreamEnabled {
                id: "e1".into(),
                enabled: false,
            },
            ClusterCommand::DeleteViewer {
                stream_id: "e1".into(),
                viewer_id: "ev2".into(),
            },
            ClusterCommand::SetApiToken { token: "t".into() },
            ClusterCommand::SetClusterId { id: "c".into() },
        ] {
            assert!(
                apply_one(&mut sm, 3, cmd.clone()).await.is_err(),
                "{cmd:?} must fail the apply"
            );
        }
        assert_eq!(sm.applied_state().await.unwrap().0, Some(log_id(2)));

        // Make the stream acquirable again, then break the owner/viewer tables.
        exec(
            &sm,
            "DROP TRIGGER no_update;
             UPDATE streams SET pending_delete=0 WHERE id='e1';
             DROP TABLE stream_viewers;
             DROP TABLE stream_owners;",
        );
        for cmd in [
            ClusterCommand::CreateViewer {
                viewer: extra_viewer("e1", "ev3"),
            },
            ClusterCommand::AcquireStreamOwner {
                stream_id: "e1".into(),
                node_id: 1,
                epoch: 1,
                acquired_at: 1,
            },
            ClusterCommand::ReleaseStreamOwner {
                stream_id: "e1".into(),
                epoch: 1,
            },
            ClusterCommand::ReleaseOwnersForNode { node_id: 1 },
        ] {
            assert!(
                apply_one(&mut sm, 3, cmd.clone()).await.is_err(),
                "{cmd:?} must fail the apply"
            );
        }
    }

    #[tokio::test]
    async fn persist_failure_fails_blank_and_membership_entries() {
        let (mut sm, _rx) = sm_with_effects();
        exec(&sm, "DROP TABLE raft_meta");
        assert!(
            sm.apply(vec![Entry {
                log_id: log_id(1),
                payload: EntryPayload::Blank,
            }])
            .await
            .is_err()
        );
        assert!(
            sm.apply(vec![Entry {
                log_id: log_id(1),
                payload: EntryPayload::Membership(membership(&[1])),
            }])
            .await
            .is_err()
        );
        assert!(
            apply_one(&mut sm, 1, ClusterCommand::SetClusterId { id: "x".into() })
                .await
                .is_err()
        );
        assert!(sm.applied_state().await.unwrap().0.is_none());
    }

    #[tokio::test]
    async fn snapshot_build_and_reload_from_db() {
        let (mut sm, _rx) = sm_with_effects();

        // Snapshot before anything was applied has an index-less id.
        let mut builder = sm.get_snapshot_builder().await;
        let empty = builder.build_snapshot().await.unwrap();
        assert_eq!(empty.meta.snapshot_id, "--1");
        assert!(empty.meta.last_log_id.is_none());

        sm.apply(vec![Entry {
            log_id: log_id(1),
            payload: EntryPayload::Membership(membership(&[1])),
        }])
        .await
        .unwrap();
        let s = stream("snap");
        apply_one(&mut sm, 2, create(&s)).await.unwrap();
        apply_one(
            &mut sm,
            3,
            ClusterCommand::AcquireStreamOwner {
                stream_id: "snap".into(),
                node_id: 1,
                epoch: 0,
                acquired_at: 9,
            },
        )
        .await
        .unwrap();
        apply_one(
            &mut sm,
            4,
            ClusterCommand::SetApiToken { token: "tk".into() },
        )
        .await
        .unwrap();
        apply_one(
            &mut sm,
            5,
            ClusterCommand::SetClusterId { id: "cid".into() },
        )
        .await
        .unwrap();

        let snap = builder.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id, Some(log_id(5)));
        assert!(snap.meta.snapshot_id.ends_with("-5-2"));
        let app: AppSnapshot = serde_json::from_slice(snap.snapshot.get_ref()).unwrap();
        assert_eq!(app.streams.len(), 1);
        assert_eq!(app.viewers.len(), 1);
        assert_eq!(app.owners.len(), 1);
        assert_eq!(app.api_token.as_deref(), Some("tk"));
        assert_eq!(app.cluster_id.as_deref(), Some("cid"));
        assert_eq!(app.last_applied_index, Some(5));

        // Cached copy.
        let cur = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(cur.meta, snap.meta);

        // A new instance (after restart) loads the persisted blob from SQLite.
        let mut reopened = SqliteStateMachine::new(Arc::clone(sm.db())).unwrap();
        let loaded = reopened.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(loaded.meta, snap.meta);
        assert_eq!(loaded.snapshot.get_ref(), snap.snapshot.get_ref());
        // Second call is served from the cache populated by the first.
        assert!(reopened.get_current_snapshot().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn snapshot_storage_failures_are_reported() {
        // Unreadable persisted meta.
        let (sm, _rx) = sm_with_effects();
        exec(
            &sm,
            "INSERT INTO raft_snapshots(id, meta_json, data) VALUES(1, 'garbage', x'00')",
        );
        let mut fresh = SqliteStateMachine::new(Arc::clone(sm.db())).unwrap();
        assert!(fresh.get_current_snapshot().await.is_err());

        // Snapshot table gone: both load and persist fail.
        let (mut sm, _rx) = sm_with_effects();
        exec(&sm, "DROP TABLE raft_snapshots");
        assert!(sm.get_current_snapshot().await.is_err());
        let mut builder = sm.get_snapshot_builder().await;
        assert!(builder.build_snapshot().await.is_err());

        // Replicated tables unreadable.
        let (mut sm, _rx) = sm_with_effects();
        exec(&sm, "DROP TABLE stream_owners");
        let mut builder = sm.get_snapshot_builder().await;
        assert!(builder.build_snapshot().await.is_err());
    }

    fn publisher(id: &str, stream_id: &str) -> Publisher {
        Publisher {
            id: id.into(),
            stream_id: stream_id.into(),
            app: "live".into(),
            stream_name: stream_id.into(),
            video_codec: "avc1".into(),
            bytes_in: 100,
            active: true,
            ..Default::default()
        }
    }

    fn player(id: &str, stream_id: &str, viewer_id: &str) -> Player {
        Player {
            id: id.into(),
            stream_id: stream_id.into(),
            viewer_id: viewer_id.into(),
            app: "live".into(),
            stream_name: stream_id.into(),
            bytes_out: 50,
            active: true,
            ..Default::default()
        }
    }

    fn stat(stream_id: &str) -> StatSample {
        StatSample {
            stream_id: stream_id.into(),
            bitrate_in_kbps: 1.0,
            ts: 1,
            ..Default::default()
        }
    }

    fn snap_meta(
        last: Option<LogId<u64>>,
        mem: Membership<u64, BasicNode>,
    ) -> SnapshotMeta<u64, BasicNode> {
        SnapshotMeta {
            last_log_id: last,
            last_membership: StoredMembership::new(last, mem),
            snapshot_id: "test-snap".into(),
        }
    }

    #[tokio::test]
    async fn install_snapshot_preserves_surviving_sessions_and_kicks_others() {
        let (mut sm, rx) = sm_with_effects();
        let keep = stream("keep");
        let gone = stream("gone");
        apply_one(&mut sm, 1, create(&keep)).await.unwrap();
        apply_one(&mut sm, 2, create(&gone)).await.unwrap();
        let revoked = extra_viewer("keep", "revoked");
        apply_one(&mut sm, 3, ClusterCommand::CreateViewer { viewer: revoked })
            .await
            .unwrap();
        drain(&rx);

        let db = Arc::clone(sm.db());
        let keep_viewer = default_viewer(&keep);
        let gone_viewer = default_viewer(&gone);
        assert!(db.publisher_try_acquire(&publisher("pub-keep", "keep")));
        assert!(db.publisher_try_acquire(&publisher("pub-gone", "gone")));
        assert!(db.player_try_acquire(&player("pl-keep", "keep", &keep_viewer.id)));
        assert!(db.player_try_acquire(&player("pl-revoked", "keep", "revoked")));
        assert!(db.player_try_acquire(&player("pl-gone", "gone", &gone_viewer.id)));
        assert!(db.stat_add(&stat("keep")));
        assert!(db.stat_add(&stat("gone")));

        // Leader-side snapshot: `gone` fully deleted, `revoked` viewer removed,
        // `pend` mid-delete, plus owners, token and cluster id.
        let pend = stream("pend");
        let snap = AppSnapshot {
            streams: vec![keep.clone(), pend.clone()],
            viewers: vec![keep_viewer.clone(), default_viewer(&pend)],
            owners: vec![crate::db::StreamOwner {
                stream_id: "keep".into(),
                owner_node_id: 2,
                epoch: 11,
                acquired_at: 3,
            }],
            api_token: Some("snap-token".into()),
            cluster_id: Some("snap-cluster".into()),
            last_applied_index: Some(20),
            pending_delete_stream_ids: vec!["pend".into()],
        };
        let meta = snap_meta(Some(log_id(20)), membership(&[1, 2]));
        let data = serde_json::to_vec(&snap).unwrap();
        sm.install_snapshot(&meta, Box::new(Cursor::new(data.clone())))
            .await
            .unwrap();

        // Durable state replaced.
        let ids: Vec<String> = db.stream_list().into_iter().map(|s| s.id).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"keep".to_string()) && ids.contains(&"pend".to_string()));
        assert_eq!(db.stream_pending_delete("pend"), Some(true));
        assert_eq!(db.stream_owner_get("keep").map(|o| o.epoch), Some(11));
        assert_eq!(db.token_get().unwrap().as_deref(), Some("snap-token"));
        assert_eq!(
            db.setting_get("cluster_id").as_deref(),
            Some("snap-cluster")
        );

        // Local sessions: only rows whose parents survived were kept.
        let pubs: Vec<String> = db.publisher_list_all().into_iter().map(|p| p.id).collect();
        assert_eq!(pubs, vec!["pub-keep".to_string()]);
        let players: Vec<String> = db.player_list_all().into_iter().map(|p| p.id).collect();
        assert_eq!(players, vec!["pl-keep".to_string()]);
        assert_eq!(db.stat_recent("keep", 10).len(), 1);
        assert!(db.stat_recent("gone", 10).is_empty());
        assert_eq!(db.player_active_count_for_viewer("revoked"), 0);
        assert_eq!(db.player_active_count_for_viewer(&gone_viewer.id), 0);

        // Effects: drains for pend + gone (publisher, player), revoke, token.
        let effects = drain(&rx);
        let drains: Vec<&str> = effects
            .iter()
            .filter_map(|e| match e {
                StateEffect::DrainStream(id) => Some(id.as_str()),
                _ => None,
            })
            .collect();
        assert!(drains.contains(&"pend") && drains.contains(&"gone"));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, StateEffect::RevokeViewer(v) if v == "revoked"))
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, StateEffect::ApiToken(t) if t == "snap-token"))
        );

        // Raft bookkeeping updated and snapshot persisted.
        let (applied, mem) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(20)));
        assert_eq!(mem, meta.last_membership);
        let cur = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(cur.meta, meta);
        let mut reopened = SqliteStateMachine::new(Arc::clone(&db)).unwrap();
        assert_eq!(reopened.applied_state().await.unwrap().0, Some(log_id(20)));
        let persisted = reopened.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(persisted.snapshot.get_ref(), &data);
    }

    #[tokio::test]
    async fn install_snapshot_without_log_id_and_failure_paths() {
        let (mut sm, rx) = sm_with_effects();
        let empty = serde_json::to_vec(&AppSnapshot::default()).unwrap();
        let meta = snap_meta(None, Membership::default());
        sm.install_snapshot(&meta, Box::new(Cursor::new(empty.clone())))
            .await
            .unwrap();
        assert!(sm.applied_state().await.unwrap().0.is_none());
        assert!(drain(&rx).is_empty());

        // Undecodable payload.
        assert!(
            sm.install_snapshot(&meta, Box::new(Cursor::new(b"not json".to_vec())))
                .await
                .is_err()
        );

        // Snapshot with conflicting rows fails inside the install transaction
        // and leaves the previous state untouched.
        let (mut sm, _rx) = sm_with_effects();
        apply_one(&mut sm, 1, create(&stream("orig")))
            .await
            .unwrap();
        let dup = AppSnapshot {
            streams: vec![stream("d"), stream("d")],
            ..Default::default()
        };
        let meta = snap_meta(Some(log_id(9)), membership(&[1]));
        assert!(
            sm.install_snapshot(
                &meta,
                Box::new(Cursor::new(serde_json::to_vec(&dup).unwrap()))
            )
            .await
            .is_err()
        );
        assert!(matches!(
            sm.db().stream_get("orig"),
            crate::db::DbLookup::Ok(_)
        ));
        assert_eq!(sm.applied_state().await.unwrap().0, Some(log_id(1)));

        // Snapshot blob persistence failing after install.
        let (mut sm, _rx) = sm_with_effects();
        exec(&sm, "DROP TABLE raft_snapshots");
        assert!(
            sm.install_snapshot(&meta, Box::new(Cursor::new(empty.clone())))
                .await
                .is_err()
        );

        // last_applied persistence failing after install.
        let (mut sm, _rx) = sm_with_effects();
        exec(&sm, "DROP TABLE raft_meta");
        assert!(
            sm.install_snapshot(&meta, Box::new(Cursor::new(empty)))
                .await
                .is_err()
        );
    }

    #[test]
    fn effects_without_channel_are_dropped() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let sm = SqliteStateMachine::new(db).unwrap();
        // No channel registered: emitting must be a silent no-op.
        sm.emit_effect(StateEffect::OwnershipChanged);
        assert_eq!(
            sm.apply_command(&ClusterCommand::SetApiToken { token: "x".into() }),
            ClusterResponse::Ok
        );
    }
}
