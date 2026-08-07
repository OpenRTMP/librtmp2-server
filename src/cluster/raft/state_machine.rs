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
use crate::db::{Db, StreamAddError};

#[derive(Clone)]
pub struct SqliteStateMachine {
    db: Arc<Db>,
    last_applied: Arc<Mutex<Option<LogId<u64>>>>,
    last_membership: Arc<Mutex<StoredMembership<u64, openraft::BasicNode>>>,
    current_snapshot: Arc<Mutex<Option<StoredSnapshot>>>,
    snapshot_idx: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, openraft::BasicNode>,
    data: Vec<u8>,
}

impl SqliteStateMachine {
    pub fn new(db: Arc<Db>) -> Self {
        let last_applied = db.with_conn(|conn| {
            conn.query_row(
                "SELECT val FROM raft_meta WHERE key='last_applied'",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|j| serde_json::from_str(&j).ok())
        });
        let last_membership = db.with_conn(|conn| {
            conn.query_row(
                "SELECT val FROM raft_meta WHERE key='last_membership'",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|j| serde_json::from_str(&j).ok())
            .unwrap_or_else(|| StoredMembership::new(None, openraft::Membership::default()))
        });
        Self {
            db,
            last_applied: Arc::new(Mutex::new(last_applied)),
            last_membership: Arc::new(Mutex::new(last_membership)),
            current_snapshot: Arc::new(Mutex::new(None)),
            snapshot_idx: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    fn persist_applied(&self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let json = serde_json::to_string(&log_id).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_state_machine(&e),
        })?;
        self.db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO raft_meta(key,val) VALUES('last_applied',?) \
                 ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                params![json],
            )
            .map_err(|e| StorageError::IO {
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
                Some(true) => ClusterResponse::Ok,
                Some(false) => ClusterResponse::NotFound,
                None => ClusterResponse::Error("db".into()),
            },
            ClusterCommand::FinalizeDeleteStream { id } => match self.db.stream_delete(id) {
                Some(true) => ClusterResponse::Ok,
                Some(false) => ClusterResponse::NotFound,
                None => ClusterResponse::Error("db".into()),
            },
            ClusterCommand::SetStreamEnabled { id, enabled } => {
                if self.db.stream_set_enabled(id, *enabled) {
                    ClusterResponse::Ok
                } else {
                    ClusterResponse::Error("db".into())
                }
            }
            ClusterCommand::CreateViewer { viewer } => {
                if self.db.viewer_add(viewer) {
                    ClusterResponse::Ok
                } else {
                    ClusterResponse::Duplicate
                }
            }
            ClusterCommand::DeleteViewer {
                stream_id,
                viewer_id,
            } => match self.db.viewer_delete(stream_id, viewer_id) {
                Some(true) => {
                    self.db.players_deactivate_for_viewer(viewer_id);
                    ClusterResponse::Ok
                }
                Some(false) => ClusterResponse::NotFound,
                None => ClusterResponse::Error("db".into()),
            },
            ClusterCommand::SetApiToken { token } => match self.db.token_set(token) {
                Ok(_) => ClusterResponse::Ok,
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
                if self.db.stream_owner_release(stream_id, *epoch) {
                    ClusterResponse::Ok
                } else {
                    // Idempotent: already released or epoch mismatch on follower catch-up.
                    ClusterResponse::Ok
                }
            }
            ClusterCommand::ReleaseOwnersForNode { node_id } => {
                self.db.stream_owners_release_for_node(*node_id);
                ClusterResponse::Ok
            }
            ClusterCommand::SeedFromStandalone {
                streams,
                viewers,
                api_token,
            } => {
                for s in streams {
                    let _ = self.db.stream_insert_only(s);
                }
                for v in viewers {
                    let _ = self.db.viewer_add(v);
                }
                if let Some(token) = api_token {
                    let _ = self.db.token_set(token);
                }
                ClusterResponse::Ok
            }
        }
    }

    fn build_app_snapshot(&self) -> AppSnapshot {
        AppSnapshot {
            streams: self.db.stream_list(),
            viewers: self.db.viewer_list_all(),
            owners: self.db.stream_owner_list(),
            api_token: self.db.token_get().ok().flatten(),
            last_applied_index: self.last_applied.lock().map(|l| l.index),
        }
    }

    fn install_app_snapshot(&self, snap: &AppSnapshot) -> Result<(), String> {
        self.db.with_conn(|conn| {
            let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
            tx.execute_batch(
                "DELETE FROM stream_owners;
                 DELETE FROM stream_viewers;
                 DELETE FROM players;
                 DELETE FROM publishers;
                 DELETE FROM stats_samples;
                 DELETE FROM streams;",
            )
            .map_err(|e| e.to_string())?;
            tx.commit().map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        })?;
        for s in &snap.streams {
            self.db
                .stream_insert_only(s)
                .map_err(|_| "stream_insert_only failed during snapshot install".to_string())?;
        }
        for v in &snap.viewers {
            if !self.db.viewer_add(v) {
                return Err("viewer_add failed during snapshot install".into());
            }
        }
        for o in &snap.owners {
            self.db
                .stream_owner_acquire(&o.stream_id, o.owner_node_id, o.epoch, o.acquired_at)
                .map_err(|_| "owner restore failed".to_string())?;
        }
        if let Some(token) = &snap.api_token {
            let _ = self.db.token_set(token);
        }
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let last_applied = *self.last_applied.lock();
        let last_membership = self.last_membership.lock().clone();
        let data = self.build_app_snapshot();
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
            self.persist_applied(entry.log_id)?;
            match entry.payload {
                EntryPayload::Blank => responses.push(ClusterResponse::Ok),
                EntryPayload::Normal(ref cmd) => responses.push(self.apply_command(cmd)),
                EntryPayload::Membership(ref mem) => {
                    let stored = StoredMembership::new(Some(entry.log_id), mem.clone());
                    let json = serde_json::to_string(&stored).map_err(|e| StorageError::IO {
                        source: StorageIOError::<u64>::write_state_machine(&e),
                    })?;
                    self.db.with_conn(|conn| {
                        conn.execute(
                            "INSERT INTO raft_meta(key,val) VALUES('last_membership',?) \
                             ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                            params![json],
                        )
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::<u64>::write_state_machine(&e),
                        })?;
                        Ok::<(), StorageError<u64>>(())
                    })?;
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
        *self.last_applied.lock() = meta.last_log_id;
        *self.last_membership.lock() = meta.last_membership.clone();
        if let Some(log_id) = meta.last_log_id {
            self.persist_applied(log_id)?;
        }
        let json = serde_json::to_string(&meta.last_membership).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_state_machine(&e),
        })?;
        self.db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO raft_meta(key,val) VALUES('last_membership',?) \
                 ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                params![json],
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_state_machine(&e),
            })?;
            Ok::<(), StorageError<u64>>(())
        })?;
        *self.current_snapshot.lock() = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
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
        // Try load from DB.
        let loaded = self.db.with_conn(|conn| {
            conn.query_row(
                "SELECT meta_json, data FROM raft_snapshots WHERE id=1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .ok()
            .flatten()
        });
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
