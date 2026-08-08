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
            ClusterCommand::FinalizeDeleteStream { id } => match self.db.stream_delete(id) {
                Some(true) => {
                    self.emit_effect(StateEffect::ClearDrainStream(id.clone()));
                    ClusterResponse::Ok
                }
                Some(false) => ClusterResponse::NotFound,
                None => ClusterResponse::Error("db".into()),
            },
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
                }
            }
            ClusterCommand::SeedFromStandalone {
                streams,
                viewers,
                api_token,
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
                ClusterResponse::Ok
            }
            ClusterCommand::SetClusterId { id } => match self.db.setting_set("cluster_id", id) {
                Ok(()) => ClusterResponse::Ok,
                Err(e) => ClusterResponse::Error(e),
            },
        }
    }

    fn build_app_snapshot(&self) -> Result<AppSnapshot, String> {
        let (streams, viewers, owners, api_token, cluster_id) =
            self.db.read_replicated_snapshot()?;
        let pending_delete_stream_ids = self.db.stream_ids_pending_delete();
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
        self.db.with_conn(|conn| -> Result<(), String> {
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
            Ok::<(), String>(())
        })?;

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
                                Err(crate::db::OwnerError::Db) => {
                                    ClusterResponse::Error("db".into())
                                }
                            }
                        }
                        other => self.apply_command(other),
                    };
                    if let ClusterResponse::Error(ref msg) = resp {
                        if msg == "db" {
                            return Err(StorageError::IO {
                                source: StorageIOError::<u64>::write_state_machine(
                                    &std::io::Error::other(msg.clone()),
                                ),
                            });
                        }
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
