//! SQLite-backed OpenRaft log store (same `server.db` as app state).

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    Entry, LogId, LogState, RaftLogId, RaftLogReader, StorageError, StorageIOError, Vote,
};
use rusqlite::params;

use crate::cluster::raft::TypeConfig;
use crate::db::Db;

#[derive(Clone)]
pub struct SqliteLogStore {
    db: Arc<Db>,
}

impl SqliteLogStore {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    fn io_err<E: std::error::Error + 'static>(e: E) -> StorageError<u64> {
        StorageError::IO {
            source: StorageIOError::write_logs(&e),
        }
    }

    fn read_err<E: std::error::Error + 'static>(e: E) -> StorageError<u64> {
        StorageError::IO {
            source: StorageIOError::read_logs(&e),
        }
    }
}

impl RaftLogReader<TypeConfig> for SqliteLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(v) => *v,
            std::ops::Bound::Excluded(v) => v.saturating_add(1),
            std::ops::Bound::Unbounded => 0,
        };
        let end_excl = match range.end_bound() {
            std::ops::Bound::Included(v) => v.saturating_add(1),
            std::ops::Bound::Excluded(v) => *v,
            // u64::MAX as i64 is -1 and would make `log_index < -1` match nothing.
            std::ops::Bound::Unbounded => i64::MAX as u64,
        };
        self.db.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT entry_json FROM raft_log WHERE log_index >= ? AND log_index < ? ORDER BY log_index",
                )
                .map_err(Self::read_err)?;
            let end_bind = if end_excl >= i64::MAX as u64 {
                i64::MAX
            } else {
                end_excl as i64
            };
            let rows = stmt
                .query_map(params![start as i64, end_bind], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(Self::read_err)?;
            let mut out = Vec::new();
            for row in rows {
                let json = row.map_err(Self::read_err)?;
                let entry: Entry<TypeConfig> =
                    serde_json::from_str(&json).map_err(Self::read_err)?;
                out.push(entry);
            }
            Ok(out)
        })
    }
}

impl RaftLogStorage<TypeConfig> for SqliteLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        self.db.with_conn(|conn| {
            let last_purged: Option<String> = conn
                .query_row(
                    "SELECT val FROM raft_meta WHERE key='last_purged_log_id'",
                    [],
                    |r| r.get(0),
                )
                .ok();
            let last_purged_log_id = match last_purged {
                Some(j) => Some(serde_json::from_str(&j).map_err(Self::read_err)?),
                None => None,
            };
            let last_index: Option<i64> = conn
                .query_row("SELECT MAX(log_index) FROM raft_log", [], |r| r.get(0))
                .ok()
                .flatten();
            let last_log_id = if let Some(idx) = last_index {
                let json: String = conn
                    .query_row(
                        "SELECT entry_json FROM raft_log WHERE log_index=?",
                        params![idx],
                        |r| r.get(0),
                    )
                    .map_err(Self::read_err)?;
                let entry: Entry<TypeConfig> =
                    serde_json::from_str(&json).map_err(Self::read_err)?;
                Some(*entry.get_log_id())
            } else {
                last_purged_log_id
            };
            Ok(LogState {
                last_purged_log_id,
                last_log_id,
            })
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let json = serde_json::to_string(&committed).map_err(Self::io_err)?;
        self.db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO raft_meta(key,val) VALUES('committed',?) \
                 ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                params![json],
            )
            .map_err(Self::io_err)?;
            Ok(())
        })
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        self.db.with_conn(|conn| {
            let json: Option<String> = conn
                .query_row("SELECT val FROM raft_meta WHERE key='committed'", [], |r| {
                    r.get(0)
                })
                .ok();
            match json {
                Some(j) => Ok(serde_json::from_str(&j).map_err(Self::read_err)?),
                None => Ok(None),
            }
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let json = serde_json::to_string(vote).map_err(Self::io_err)?;
        self.db.with_conn(|conn| {
            conn.execute(
                "INSERT INTO raft_vote(id, vote_json) VALUES(1, ?) \
                 ON CONFLICT(id) DO UPDATE SET vote_json=excluded.vote_json",
                params![json],
            )
            .map_err(Self::io_err)?;
            Ok(())
        })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.db.with_conn(|conn| {
            use rusqlite::OptionalExtension;
            let json: Option<String> = conn
                .query_row("SELECT vote_json FROM raft_vote WHERE id=1", [], |r| {
                    r.get(0)
                })
                .optional()
                .map_err(Self::read_err)?;
            match json {
                Some(j) => Ok(Some(serde_json::from_str(&j).map_err(Self::read_err)?)),
                None => Ok(None),
            }
        })
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let collected: Vec<_> = entries.into_iter().collect();
        let result = self.db.with_conn(|conn| {
            let tx = conn.unchecked_transaction().map_err(Self::io_err)?;
            for entry in &collected {
                let idx = entry.get_log_id().index as i64;
                let json = serde_json::to_string(entry).map_err(Self::io_err)?;
                tx.execute(
                    "INSERT INTO raft_log(log_index, entry_json) VALUES(?,?) \
                     ON CONFLICT(log_index) DO UPDATE SET entry_json=excluded.entry_json",
                    params![idx, json],
                )
                .map_err(Self::io_err)?;
            }
            tx.commit().map_err(Self::io_err)?;
            Ok(())
        });
        match &result {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(e) => {
                callback.log_io_completed(Err(std::io::Error::other(format!("{e}"))));
            }
        }
        result
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.db.with_conn(|conn| {
            conn.execute(
                "DELETE FROM raft_log WHERE log_index >= ?",
                params![log_id.index as i64],
            )
            .map_err(Self::io_err)?;
            Ok(())
        })
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let json = serde_json::to_string(&log_id).map_err(Self::io_err)?;
        self.db.with_conn(|conn| {
            let tx = conn.unchecked_transaction().map_err(Self::io_err)?;
            tx.execute(
                "INSERT INTO raft_meta(key,val) VALUES('last_purged_log_id',?) \
                 ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                params![json],
            )
            .map_err(Self::io_err)?;
            tx.execute(
                "DELETE FROM raft_log WHERE log_index <= ?",
                params![log_id.index as i64],
            )
            .map_err(Self::io_err)?;
            tx.commit().map_err(Self::io_err)?;
            Ok(())
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
