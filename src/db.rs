//! SQLite persistence layer.
//!
//! Stores streams, publishers, players and stats samples. `Db` wraps a
//! single connection behind a mutex; SQLite itself only allows one writer
//! at a time anyway, so this matches the C version's locking model without
//! needing a connection pool.

use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Db {
    conn: Mutex<Connection>,
}

/// Max simultaneous RTMP play connections per play key (not configurable).
pub const MAX_CONNECTIONS_PER_PLAY_KEY: usize = 5;

#[derive(Debug, Clone, Default, Serialize)]
pub struct Stream {
    pub id: String,
    pub name: String,
    pub app: String,
    pub publish_key: String,
    pub play_key: String,
    pub stats_key: String,
    pub enabled: bool,
    pub created_at: i64,
}

/// Panel-managed play key for a stream (auto-generated `play_*` key).
#[derive(Debug, Clone, Default, Serialize)]
pub struct StreamViewer {
    pub id: String,
    pub stream_id: String,
    pub name: String,
    pub play_key: String,
    pub enabled: bool,
    pub created_at: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Publisher {
    pub id: String,
    pub stream_id: String,
    pub app: String,
    pub stream_name: String,
    pub video_codec: String,
    pub audio_codec: String,
    pub video_width: u32,
    pub video_height: u32,
    pub fps: f64,
    pub bytes_in: u64,
    pub bitrate_kbps: f64,
    pub rtt_ms: f64,
    pub connected_at: i64,
    pub active: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Player {
    pub id: String,
    pub stream_id: String,
    /// Configured viewer slot this session authenticated with.
    pub viewer_id: String,
    pub app: String,
    pub stream_name: String,
    pub bytes_out: u64,
    pub bitrate_kbps: f64,
    pub rtt_ms: f64,
    pub connected_at: i64,
    pub active: bool,
}

// Stats-sample plumbing and a handful of CRUD methods below are part of the
// persistence API but have no production call site yet — they're exercised
// by tests and will be driven once the RTMP frame-stats and stream-management
// paths are wired up.
#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize)]
pub struct StatSample {
    pub stream_id: String,
    pub bitrate_in_kbps: f64,
    pub fps: f64,
    pub width: u32,
    pub height: u32,
    pub video_codec: String,
    pub audio_codec: String,
    pub player_count: i32,
    pub ts: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamAddError {
    Duplicate,
    Db,
}

/// Result of a single-row lookup: found, not found, or a real DB error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbLookup<T> {
    Ok(T),
    Missing,
    Failed,
}

fn map_optional<T>(result: rusqlite::Result<T>) -> DbLookup<T> {
    match result.optional() {
        Ok(Some(v)) => DbLookup::Ok(v),
        Ok(None) => DbLookup::Missing,
        Err(e) => {
            crate::log_error!("DB query error: {e}");
            DbLookup::Failed
        }
    }
}

#[allow(dead_code)]
pub fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  val TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS streams (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  app TEXT NOT NULL DEFAULT 'live',
  publish_key TEXT UNIQUE NOT NULL,
  play_key TEXT UNIQUE NOT NULL,
  stats_key TEXT UNIQUE NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  pending_delete INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS publishers (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  app TEXT NOT NULL DEFAULT '',
  stream_name TEXT NOT NULL DEFAULT '',
  video_codec TEXT NOT NULL DEFAULT '',
  audio_codec TEXT NOT NULL DEFAULT '',
  video_width INTEGER NOT NULL DEFAULT 0,
  video_height INTEGER NOT NULL DEFAULT 0,
  fps REAL NOT NULL DEFAULT 0,
  bytes_in INTEGER NOT NULL DEFAULT 0,
  bitrate_kbps REAL NOT NULL DEFAULT 0,
  rtt_ms REAL NOT NULL DEFAULT 0,
  connected_at INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS players (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  viewer_id TEXT NOT NULL DEFAULT '',
  app TEXT NOT NULL DEFAULT '',
  stream_name TEXT NOT NULL DEFAULT '',
  bytes_out INTEGER NOT NULL DEFAULT 0,
  bitrate_kbps REAL NOT NULL DEFAULT 0,
  rtt_ms REAL NOT NULL DEFAULT 0,
  connected_at INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS stats_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stream_id TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  bitrate_in_kbps REAL NOT NULL DEFAULT 0,
  fps REAL NOT NULL DEFAULT 0,
  width INTEGER NOT NULL DEFAULT 0,
  height INTEGER NOT NULL DEFAULT 0,
  video_codec TEXT NOT NULL DEFAULT '',
  audio_codec TEXT NOT NULL DEFAULT '',
  player_count INTEGER NOT NULL DEFAULT 0,
  ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pub_stream ON publishers(stream_id);
CREATE INDEX IF NOT EXISTS idx_player_stream ON players(stream_id);
CREATE INDEX IF NOT EXISTS idx_stats_stream ON stats_samples(stream_id);
CREATE INDEX IF NOT EXISTS idx_pub_active ON publishers(active);
CREATE INDEX IF NOT EXISTS idx_player_active ON players(active);
CREATE TABLE IF NOT EXISTS stream_viewers (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  name TEXT NOT NULL DEFAULT '',
  play_key TEXT UNIQUE NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_viewer_stream ON stream_viewers(stream_id);
CREATE INDEX IF NOT EXISTS idx_viewer_play_key ON stream_viewers(play_key);
CREATE INDEX IF NOT EXISTS idx_player_viewer ON players(viewer_id);
";

/// WAL mode creates sibling `-wal`/`-shm` files; restrict all three so stream
/// keys stored in SQLite are not world-readable on multi-user hosts.
#[cfg(unix)]
fn restrict_db_file_permissions(path: &str) {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    // In-memory and URI-style databases have no backing file to restrict.
    if path.is_empty() || path == ":memory:" || path.starts_with("file:") {
        return;
    }

    let mode = fs::Permissions::from_mode(0o600);
    for candidate in [path, &format!("{path}-wal"), &format!("{path}-shm")] {
        if let Err(e) = fs::set_permissions(candidate, mode.clone()) {
            // -wal/-shm may not exist yet on a brand-new database.
            if candidate == path {
                crate::log_warn!("Could not restrict permissions on {candidate}: {e}");
            }
        }
    }
}

#[cfg(windows)]
fn restrict_db_file_permissions(path: &str) {
    use std::path::Path;
    use std::process::Command;

    if path.is_empty() || path == ":memory:" || path.starts_with("file:") {
        return;
    }

    let username = std::env::var("USERNAME").unwrap_or_default();
    if username.is_empty() {
        return;
    }

    let grant = format!("{username}:(F)");
    for candidate in [path, &format!("{path}-wal"), &format!("{path}-shm")] {
        if !Path::new(candidate).exists() {
            continue;
        }
        match Command::new("icacls")
            .args([candidate, "/inheritance:r", "/grant:r", &grant])
            .status()
        {
            Ok(status) if status.success() => {}
            Ok(status) => {
                crate::log_warn!(
                    "Could not restrict permissions on {candidate}: icacls exited with {status}"
                );
            }
            Err(e) => {
                crate::log_warn!("Could not restrict permissions on {candidate}: {e}");
            }
        }
    }
}

impl Db {
    pub fn open(path: &str) -> rusqlite::Result<Db> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_millis(1000))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        conn.execute_batch(SCHEMA)?;
        // Migrate pre-existing databases created before `pending_delete` was
        // added to the `streams` table (CREATE TABLE IF NOT EXISTS above is a
        // no-op on an already-existing table). Ignore the "duplicate column"
        // error on databases that already have it.
        match conn.execute(
            "ALTER TABLE streams ADD COLUMN pending_delete INTEGER NOT NULL DEFAULT 0",
            [],
        ) {
            Ok(_) => {}
            Err(e) if e.to_string().contains("duplicate column name") => {}
            Err(e) => return Err(e),
        }
        let stale = conn
            .execute("UPDATE publishers SET active=0 WHERE active=1", [])
            .unwrap_or(0)
            + conn
                .execute("UPDATE players SET active=0 WHERE active=1", [])
                .unwrap_or(0);
        if stale > 0 {
            crate::log_info!("Cleared {stale} stale active publisher/player row(s) from prior run");
        }
        restrict_db_file_permissions(path);
        crate::log_info!("Database opened: {path}");
        Ok(Db {
            conn: Mutex::new(conn),
        })
    }

    // ==================== SETTINGS ====================

    /// Returns the stored API token, or `Ok(None)` if none has been generated
    /// yet. Returns `Err` on a real database read failure so callers can
    /// distinguish "not found" from "broken DB".
    pub fn token_get(&self) -> Result<Option<String>, String> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT val FROM settings WHERE key='api_token'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| format!("DB error reading API token: {e}"))
        .map(|opt| opt.filter(|v| !v.is_empty()))
    }

    /// Persists `token` as the API token. Inserts a new row, or updates an
    /// existing row only when its value is empty (repairing a corrupted state).
    /// Returns `Ok(true)` if the token was written, `Ok(false)` if a non-empty
    /// token was already present (the caller should re-read with [`token_get`]
    /// to get the winner's value).
    pub fn token_set(&self, token: &str) -> Result<bool, String> {
        let conn = self.conn.lock();
        let rows = conn
            .execute(
                "INSERT INTO settings(key,val) VALUES('api_token',?) \
                 ON CONFLICT(key) DO UPDATE SET val=excluded.val \
                 WHERE settings.val=''",
                rusqlite::params![token],
            )
            .map_err(|e| format!("DB error persisting API token: {e}"))?;
        Ok(rows > 0)
    }

    // ==================== STREAMS ====================

    pub fn stream_add(&self, s: &Stream) -> std::result::Result<(), StreamAddError> {
        let viewer_id = match crate::keygen::keygen_stream_key(crate::keygen::PREFIX_VIEWER_ID) {
            Ok(id) => id,
            Err(e) => {
                crate::log_error!("Failed to generate viewer id for stream {}: {e}", s.id);
                return Err(StreamAddError::Db);
            }
        };
        let conn = self.conn.lock();
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(e) => {
                crate::log_error!("stream_add: begin tx failed: {e}");
                return Err(StreamAddError::Db);
            }
        };
        let stream_rc = tx.execute(
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
                s.created_at
            ],
        );
        match stream_rc {
            Ok(_) => {}
            Err(e) => {
                crate::log_error!("Failed to add stream {}: {e}", s.id);
                if matches!(
                    e,
                    rusqlite::Error::SqliteFailure(ref err, _)
                        if err.code == rusqlite::ErrorCode::ConstraintViolation
                ) {
                    return Err(StreamAddError::Duplicate);
                }
                return Err(StreamAddError::Db);
            }
        }
        if tx
            .execute(
                "INSERT INTO stream_viewers (id,stream_id,name,play_key,enabled,created_at) \
                 VALUES (?,?,?,?,?,?)",
                params![viewer_id, s.id, "Player 1", s.play_key, true, s.created_at],
            )
            .is_err()
        {
            crate::log_error!("Failed to add default play key for stream {}", s.id);
            return Err(StreamAddError::Db);
        }
        match tx.commit() {
            Ok(()) => {
                crate::log_info!("Stream added: id={} app={}", s.id, s.app);
                Ok(())
            }
            Err(e) => {
                crate::log_error!("stream_add commit failed for {}: {e}", s.id);
                Err(StreamAddError::Db)
            }
        }
    }

    fn load_stream_row(row: &rusqlite::Row) -> rusqlite::Result<Stream> {
        Ok(Stream {
            id: row.get(0)?,
            name: row.get(1)?,
            app: row.get(2)?,
            publish_key: row.get(3)?,
            play_key: row.get(4)?,
            stats_key: row.get(5)?,
            enabled: row.get(6)?,
            created_at: row.get(7)?,
        })
    }

    const STREAM_COLS: &'static str =
        "id,name,app,publish_key,play_key,stats_key,enabled,created_at";

    pub fn stream_get(&self, id: &str) -> DbLookup<Stream> {
        let conn = self.conn.lock();
        map_optional(conn.query_row(
            &format!("SELECT {} FROM streams WHERE id=?", Self::STREAM_COLS),
            params![id],
            Self::load_stream_row,
        ))
    }

    #[allow(dead_code)]
    pub fn stream_get_by_app(&self, app: &str, stream_name: &str) -> DbLookup<Stream> {
        let conn = self.conn.lock();
        map_optional(conn.query_row(
            &format!(
                "SELECT {} FROM streams WHERE app=? AND name=?",
                Self::STREAM_COLS
            ),
            params![app, stream_name],
            Self::load_stream_row,
        ))
    }

    fn stream_find_by(&self, column: &str, key: &str) -> DbLookup<Stream> {
        if !matches!(column, "publish_key" | "stats_key") {
            crate::log_error!("stream_find_by: rejected disallowed column '{column}'");
            return DbLookup::Failed;
        }
        let conn = self.conn.lock();
        map_optional(conn.query_row(
            &format!(
                "SELECT {} FROM streams WHERE {column}=? AND enabled=1",
                Self::STREAM_COLS
            ),
            params![key],
            Self::load_stream_row,
        ))
    }

    pub fn stream_find_by_publish_key(&self, key: &str) -> DbLookup<Stream> {
        self.stream_find_by("publish_key", key)
    }

    /// Like `stream_find_by_publish_key`, but does not filter on `enabled` —
    /// used to distinguish a truly unknown/invalid key from one that
    /// belongs to a disabled/pending-delete stream, so the RTMP auth-failure
    /// rate limiter only counts the former as a credential mismatch.
    pub fn stream_find_by_publish_key_any(&self, key: &str) -> DbLookup<Stream> {
        let conn = self.conn.lock();
        map_optional(conn.query_row(
            &format!(
                "SELECT {} FROM streams WHERE publish_key=?",
                Self::STREAM_COLS
            ),
            params![key],
            Self::load_stream_row,
        ))
    }

    pub fn stream_find_by_stats_key(&self, key: &str) -> DbLookup<Stream> {
        self.stream_find_by("stats_key", key)
    }

    /// Disable a stream and mark it as pending deletion, so new publish/play
    /// attempts are rejected while RTMP sessions drain and a crash before the
    /// delete finishes can be recovered on the next startup (see
    /// `stream_ids_pending_delete`). Returns `Some(true)` if updated,
    /// `Some(false)` if not found, `None` on DB error.
    pub fn stream_disable(&self, id: &str) -> Option<bool> {
        let conn = self.conn.lock();
        match conn.execute(
            "UPDATE streams SET enabled=0, pending_delete=1 WHERE id=?",
            params![id],
        ) {
            Ok(rows) => Some(rows > 0),
            Err(e) => {
                crate::log_error!("stream_disable error for {id}: {e}");
                None
            }
        }
    }

    /// Re-enable a stream after a failed delete rollback, clearing the
    /// pending-delete marker set by `stream_disable`.
    pub fn stream_set_enabled(&self, id: &str, enabled: bool) -> bool {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE streams SET enabled=?, pending_delete=0 WHERE id=?",
            params![enabled, id],
        )
        .map(|rows| rows > 0)
        .unwrap_or(false)
    }

    /// Stream ids left mid-delete (`pending_delete=1`) by a prior process
    /// that crashed or was redeployed before finishing an async delete — see
    /// `handle_stream_delete`'s `202` path in `http.rs`. Distinct from
    /// `enabled=0`, which may also mark a stream an operator disabled
    /// without deleting it.
    pub fn stream_ids_pending_delete(&self) -> Vec<String> {
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare("SELECT id FROM streams WHERE pending_delete=1") {
            Ok(s) => s,
            Err(e) => {
                crate::log_error!("stream_ids_pending_delete: prepare failed: {e}");
                return Vec::new();
            }
        };
        match stmt.query_map([], |row| row.get::<_, String>(0)) {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(e) => {
                crate::log_error!("stream_ids_pending_delete: query failed: {e}");
                Vec::new()
            }
        }
    }

    #[allow(dead_code)]
    pub fn stream_update(&self, id: &str, s: &Stream) -> bool {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE streams SET name=?,app=?,publish_key=?,play_key=?,stats_key=?,enabled=? WHERE id=?",
            params![s.name, s.app, s.publish_key, s.play_key, s.stats_key, s.enabled, id],
        )
        .is_ok()
    }

    /// Cascade: remove dependent rows so deleted streams cannot leave ghost
    /// active publishers/players that pollute stats after stream re-creation.
    ///
    /// Returns `Some(true)` = deleted, `Some(false)` = not found, `None` = DB error.
    pub fn stream_delete(&self, id: &str) -> Option<bool> {
        let conn = self.conn.lock();
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(e) => {
                crate::log_error!("DB error starting cascade delete transaction: {e}");
                return None;
            }
        };

        let result = (|| -> rusqlite::Result<usize> {
            tx.execute("DELETE FROM publishers WHERE stream_id=?", params![id])?;
            tx.execute("DELETE FROM players WHERE stream_id=?", params![id])?;
            tx.execute("DELETE FROM stats_samples WHERE stream_id=?", params![id])?;
            tx.execute("DELETE FROM streams WHERE id=?", params![id])
        })();

        match result {
            Ok(rows) => match tx.commit() {
                Ok(()) => Some(rows > 0),
                Err(e) => {
                    crate::log_error!("DB cascade delete commit error for {id}: {e}");
                    None
                }
            },
            Err(e) => {
                crate::log_error!("DB cascade delete error for {id}: {e}");
                let _ = tx.rollback();
                None
            }
        }
    }

    pub fn stream_list(&self) -> Vec<Stream> {
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(&format!(
            "SELECT {} FROM streams ORDER BY created_at",
            Self::STREAM_COLS
        )) {
            Ok(s) => s,
            Err(e) => {
                crate::log_error!("stream_list: prepare failed: {e}");
                return Vec::new();
            }
        };
        stmt.query_map([], Self::load_stream_row)
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
    }

    // ==================== STREAM VIEWERS (configured play keys) ====================

    fn load_viewer_row(row: &rusqlite::Row) -> rusqlite::Result<StreamViewer> {
        Ok(StreamViewer {
            id: row.get(0)?,
            stream_id: row.get(1)?,
            name: row.get(2)?,
            play_key: row.get(3)?,
            enabled: row.get(4)?,
            created_at: row.get(5)?,
        })
    }

    const VIEWER_COLS: &'static str = "id,stream_id,name,play_key,enabled,created_at";

    pub fn viewer_add(&self, v: &StreamViewer) -> bool {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO stream_viewers (id,stream_id,name,play_key,enabled,created_at) \
             VALUES (?,?,?,?,?,?)",
            params![
                v.id,
                v.stream_id,
                v.name,
                v.play_key,
                v.enabled,
                v.created_at
            ],
        )
        .is_ok()
    }

    pub fn viewer_list(&self, stream_id: &str) -> Vec<StreamViewer> {
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(&format!(
            "SELECT {} FROM stream_viewers WHERE stream_id=? ORDER BY created_at",
            Self::VIEWER_COLS
        )) {
            Ok(s) => s,
            Err(e) => {
                crate::log_error!("viewer_list: prepare failed: {e}");
                return Vec::new();
            }
        };
        stmt.query_map(params![stream_id], Self::load_viewer_row)
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
    }

    pub fn viewer_find_by_play_key(&self, key: &str) -> DbLookup<StreamViewer> {
        let conn = self.conn.lock();
        map_optional(conn.query_row(
            &format!(
                "SELECT {} FROM stream_viewers WHERE play_key=? AND enabled=1",
                Self::VIEWER_COLS
            ),
            params![key],
            Self::load_viewer_row,
        ))
    }

    pub fn viewer_get(&self, stream_id: &str, viewer_id: &str) -> DbLookup<StreamViewer> {
        let conn = self.conn.lock();
        map_optional(conn.query_row(
            &format!(
                "SELECT {} FROM stream_viewers WHERE stream_id=? AND id=?",
                Self::VIEWER_COLS
            ),
            params![stream_id, viewer_id],
            Self::load_viewer_row,
        ))
    }

    /// Returns `Some(true)` if deleted, `Some(false)` if not found, `None` on DB error.
    pub fn viewer_delete(&self, stream_id: &str, viewer_id: &str) -> Option<bool> {
        let conn = self.conn.lock();
        match conn.execute(
            "DELETE FROM stream_viewers WHERE stream_id=? AND id=?",
            params![stream_id, viewer_id],
        ) {
            Ok(rows) => Some(rows > 0),
            Err(e) => {
                crate::log_error!("viewer_delete error for {viewer_id}: {e}");
                None
            }
        }
    }

    /// Mark active player sessions for a revoked viewer slot inactive.
    pub fn players_deactivate_for_viewer(&self, viewer_id: &str) -> bool {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE players SET active=0 WHERE viewer_id=? AND active=1",
            params![viewer_id],
        )
        .is_ok()
    }

    /// Atomically insert an active publisher only when the stream has none.
    pub fn publisher_try_acquire(&self, p: &Publisher) -> bool {
        let Ok(bytes_in) = i64::try_from(p.bytes_in) else {
            crate::log_error!(
                "publisher_try_acquire: bytes_in {} overflows i64",
                p.bytes_in
            );
            return false;
        };
        let conn = self.conn.lock();
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(e) => {
                crate::log_error!("publisher_try_acquire: begin tx failed: {e}");
                return false;
            }
        };
        let active: i64 = match tx.query_row(
            "SELECT COUNT(*) FROM publishers WHERE stream_id=? AND active=1",
            params![p.stream_id],
            |row| row.get(0),
        ) {
            Ok(count) => count,
            Err(e) => {
                crate::log_error!("publisher_try_acquire: count query failed: {e}");
                return false;
            }
        };
        if active > 0 {
            return false;
        }
        if tx
            .execute(
                "INSERT INTO publishers \
                 (id,stream_id,app,stream_name,video_codec,audio_codec,video_width,video_height,fps,bytes_in,bitrate_kbps,rtt_ms,connected_at,active) \
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
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
                    bytes_in,
                    p.bitrate_kbps,
                    p.rtt_ms,
                    p.connected_at
                ],
            )
            .is_err()
        {
            return false;
        }
        tx.commit().is_ok()
    }

    pub fn publisher_update(&self, id: &str, p: &Publisher) -> bool {
        let Ok(bytes_in) = i64::try_from(p.bytes_in) else {
            crate::log_error!("publisher_update: bytes_in {} overflows i64", p.bytes_in);
            return false;
        };
        let conn = self.conn.lock();
        match conn.execute(
            "UPDATE publishers SET stream_id=?,app=?,stream_name=?,\
             video_codec=?,audio_codec=?,video_width=?,video_height=?,fps=?,\
             bytes_in=?,bitrate_kbps=?,rtt_ms=?,active=? WHERE id=?",
            params![
                p.stream_id,
                p.app,
                p.stream_name,
                p.video_codec,
                p.audio_codec,
                p.video_width,
                p.video_height,
                p.fps,
                bytes_in,
                p.bitrate_kbps,
                p.rtt_ms,
                p.active,
                id
            ],
        ) {
            Ok(rows) if rows > 0 => true,
            Ok(_) => {
                crate::log_warn!("publisher_update: no row updated for id={id}");
                false
            }
            Err(e) => {
                crate::log_error!("publisher_update error for {id}: {e}");
                false
            }
        }
    }

    #[allow(dead_code)]
    pub fn publisher_remove(&self, id: &str) -> bool {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM publishers WHERE id=?", params![id])
            .is_ok()
    }

    fn load_publisher_row(row: &rusqlite::Row) -> rusqlite::Result<Publisher> {
        Ok(Publisher {
            id: row.get(0)?,
            stream_id: row.get(1)?,
            app: row.get(2)?,
            stream_name: row.get(3)?,
            video_codec: row.get(4)?,
            audio_codec: row.get(5)?,
            video_width: row.get(6)?,
            video_height: row.get(7)?,
            fps: row.get(8)?,
            bytes_in: u64::try_from(row.get::<_, i64>(9)?).map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Integer,
                    "negative bytes_in".into(),
                )
            })?,
            bitrate_kbps: row.get(10)?,
            rtt_ms: row.get(11)?,
            connected_at: row.get(12)?,
            active: row.get(13)?,
        })
    }

    const PUBLISHER_COLS: &'static str = "id,stream_id,app,stream_name,video_codec,audio_codec,video_width,video_height,fps,bytes_in,bitrate_kbps,rtt_ms,connected_at,active";

    pub fn publisher_list(&self, stream_id: Option<&str>) -> Vec<Publisher> {
        let conn = self.conn.lock();
        match stream_id {
            Some(sid) => {
                let mut stmt = match conn.prepare(&format!(
                    "SELECT {} FROM publishers WHERE stream_id=? AND active=1",
                    Self::PUBLISHER_COLS
                )) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::log_error!("publisher_list: prepare failed: {e}");
                        return Vec::new();
                    }
                };
                stmt.query_map(params![sid], Self::load_publisher_row)
                    .map(|rows| rows.filter_map(|r| r.ok()).collect())
                    .unwrap_or_default()
            }
            None => {
                let mut stmt = match conn.prepare(&format!(
                    "SELECT {} FROM publishers WHERE active=1",
                    Self::PUBLISHER_COLS
                )) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::log_error!("publisher_list: prepare failed: {e}");
                        return Vec::new();
                    }
                };
                stmt.query_map([], Self::load_publisher_row)
                    .map(|rows| rows.filter_map(|r| r.ok()).collect())
                    .unwrap_or_default()
            }
        }
    }

    pub fn publisher_list_all(&self) -> Vec<Publisher> {
        self.publisher_list(None)
    }

    // ==================== PLAYERS (active RTMP viewer sessions) ====================

    /// Atomically insert a player session when the play key is below its connection cap.
    pub fn player_try_acquire(&self, p: &Player) -> bool {
        if p.viewer_id.is_empty() {
            crate::log_error!("player_try_acquire: missing viewer_id");
            return false;
        }
        let Ok(bytes_out) = i64::try_from(p.bytes_out) else {
            crate::log_error!(
                "player_try_acquire: bytes_out {} overflows i64",
                p.bytes_out
            );
            return false;
        };
        let conn = self.conn.lock();
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(e) => {
                crate::log_error!("player_try_acquire: begin tx failed: {e}");
                return false;
            }
        };
        let active: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM players WHERE viewer_id=? AND active=1",
                params![p.viewer_id],
                |row| row.get(0),
            )
            .unwrap_or(MAX_CONNECTIONS_PER_PLAY_KEY as i64);
        if active >= MAX_CONNECTIONS_PER_PLAY_KEY as i64 {
            return false;
        }
        if tx
            .execute(
                "INSERT INTO players \
                 (id,stream_id,viewer_id,app,stream_name,bytes_out,bitrate_kbps,rtt_ms,connected_at,active) \
                 VALUES (?,?,?,?,?,?,?,?,?,1)",
                params![
                    p.id,
                    p.stream_id,
                    p.viewer_id,
                    p.app,
                    p.stream_name,
                    bytes_out,
                    p.bitrate_kbps,
                    p.rtt_ms,
                    p.connected_at
                ],
            )
            .is_err()
        {
            return false;
        }
        tx.commit().is_ok()
    }

    #[allow(dead_code)]
    pub fn player_add(&self, p: &Player) -> bool {
        self.player_try_acquire(p)
    }

    pub fn player_update(&self, id: &str, p: &Player) -> bool {
        let Ok(bytes_out) = i64::try_from(p.bytes_out) else {
            crate::log_error!("player_update: bytes_out {} overflows i64", p.bytes_out);
            return false;
        };
        let conn = self.conn.lock();
        match conn.execute(
            "UPDATE players SET stream_id=?,viewer_id=?,app=?,stream_name=?,\
             bytes_out=?,bitrate_kbps=?,rtt_ms=?,active=? WHERE id=?",
            params![
                p.stream_id,
                p.viewer_id,
                p.app,
                p.stream_name,
                bytes_out,
                p.bitrate_kbps,
                p.rtt_ms,
                p.active,
                id
            ],
        ) {
            Ok(rows) if rows > 0 => true,
            Ok(_) => {
                crate::log_warn!("player_update: no row updated for id={id}");
                false
            }
            Err(e) => {
                crate::log_error!("player_update error for {id}: {e}");
                false
            }
        }
    }

    #[allow(dead_code)]
    pub fn player_remove(&self, id: &str) -> bool {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM players WHERE id=?", params![id])
            .is_ok()
    }

    fn load_player_row(row: &rusqlite::Row) -> rusqlite::Result<Player> {
        Ok(Player {
            id: row.get(0)?,
            stream_id: row.get(1)?,
            viewer_id: row.get(2)?,
            app: row.get(3)?,
            stream_name: row.get(4)?,
            bytes_out: u64::try_from(row.get::<_, i64>(5)?).map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Integer,
                    "negative bytes_out".into(),
                )
            })?,
            bitrate_kbps: row.get(6)?,
            rtt_ms: row.get(7)?,
            connected_at: row.get(8)?,
            active: row.get(9)?,
        })
    }

    const PLAYER_COLS: &'static str =
        "id,stream_id,viewer_id,app,stream_name,bytes_out,bitrate_kbps,rtt_ms,connected_at,active";

    pub fn player_list(&self, stream_id: Option<&str>) -> Vec<Player> {
        let conn = self.conn.lock();
        match stream_id {
            Some(sid) => {
                let mut stmt = match conn.prepare(&format!(
                    "SELECT {} FROM players WHERE stream_id=? AND active=1",
                    Self::PLAYER_COLS
                )) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::log_error!("player_list: prepare failed: {e}");
                        return Vec::new();
                    }
                };
                stmt.query_map(params![sid], Self::load_player_row)
                    .map(|rows| rows.filter_map(|r| r.ok()).collect())
                    .unwrap_or_default()
            }
            None => {
                let mut stmt = match conn.prepare(&format!(
                    "SELECT {} FROM players WHERE active=1",
                    Self::PLAYER_COLS
                )) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::log_error!("player_list: prepare failed: {e}");
                        return Vec::new();
                    }
                };
                stmt.query_map([], Self::load_player_row)
                    .map(|rows| rows.filter_map(|r| r.ok()).collect())
                    .unwrap_or_default()
            }
        }
    }

    pub fn player_list_all(&self) -> Vec<Player> {
        self.player_list(None)
    }

    // ==================== STATS SAMPLES ====================

    #[allow(dead_code)]
    pub fn stat_add(&self, s: &StatSample) -> bool {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO stats_samples \
             (stream_id,bitrate_in_kbps,fps,width,height,video_codec,audio_codec,player_count,ts) \
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
                s.ts
            ],
        )
        .is_ok()
    }

    #[allow(dead_code)]
    pub fn stat_recent(&self, stream_id: &str, limit: i64) -> Vec<StatSample> {
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(
            "SELECT stream_id,bitrate_in_kbps,fps,width,height,video_codec,audio_codec,player_count,ts \
             FROM stats_samples WHERE stream_id=? ORDER BY ts DESC LIMIT ?",
        ) {
            Ok(s) => s,
            Err(e) => {
                crate::log_error!("stat_recent: prepare failed: {e}");
                return Vec::new();
            }
        };
        stmt.query_map(params![stream_id, limit], |row| {
            Ok(StatSample {
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
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn db_file_permissions_restricted() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("perms.db");
        let path_str = path.to_str().unwrap();
        let _db = Db::open(path_str).unwrap();

        let mode = std::fs::metadata(path_str).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "database must not be world-readable");
    }

    fn sample_stream(id: &str, pub_key: &str, play_key: &str, stats_key: &str) -> Stream {
        Stream {
            id: id.to_string(),
            name: format!("{id} name"),
            app: "live".to_string(),
            publish_key: pub_key.to_string(),
            play_key: play_key.to_string(),
            stats_key: stats_key.to_string(),
            enabled: true,
            created_at: now_ts(),
        }
    }

    #[test]
    fn publisher_try_acquire_rejects_second_slot() {
        let db = Db::open(":memory:").unwrap();
        db.stream_add(&sample_stream(
            "stream1",
            "live_key_123",
            "play_key_456",
            "sts_key_789",
        ))
        .unwrap();

        let first = Publisher {
            id: "pub1".to_string(),
            stream_id: "stream1".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        };
        let second = Publisher {
            id: "pub2".to_string(),
            stream_id: "stream1".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        };
        assert!(db.publisher_try_acquire(&first));
        assert!(!db.publisher_try_acquire(&second));
        assert_eq!(db.publisher_list(Some("stream1")).len(), 1);
    }

    #[test]
    fn stream_crud_and_keys() {
        let db = Db::open(":memory:").unwrap();

        let s = sample_stream("stream1", "pub_key_123", "pl_key_456", "st_key_789");
        assert!(db.stream_add(&s).is_ok());
        assert_eq!(db.stream_add(&s), Err(StreamAddError::Duplicate));

        let DbLookup::Ok(got) = db.stream_get("stream1") else {
            panic!("stream not found");
        };
        assert_eq!(got.name, "stream1 name");

        assert_eq!(
            match db.stream_find_by_publish_key("pub_key_123") {
                DbLookup::Ok(s) => s.id,
                _ => panic!("publish key lookup failed"),
            },
            "stream1"
        );
        assert!(matches!(
            db.viewer_find_by_play_key("pl_key_456"),
            DbLookup::Ok(_)
        ));
        assert!(matches!(
            db.stream_find_by_stats_key("st_key_789"),
            DbLookup::Ok(_)
        ));
        assert!(matches!(
            db.stream_find_by_stats_key("wrong_key"),
            DbLookup::Missing
        ));

        assert_eq!(db.stream_list().len(), 1);
    }

    #[test]
    fn publishers_players_and_stats() {
        let db = Db::open(":memory:").unwrap();
        db.stream_add(&sample_stream(
            "stream1",
            "pub_key_123",
            "pl_key_456",
            "st_key_789",
        ))
        .unwrap();

        let mut p = Publisher {
            id: "pub1".to_string(),
            stream_id: "stream1".to_string(),
            app: "live".to_string(),
            stream_name: "test".to_string(),
            video_codec: "h264".to_string(),
            audio_codec: "aac".to_string(),
            video_width: 1920,
            video_height: 1080,
            fps: 60.0,
            bytes_in: 1024768,
            bitrate_kbps: 2500.0,
            connected_at: now_ts(),
            active: true,
            ..Default::default()
        };
        assert!(db.publisher_try_acquire(&p));
        assert_eq!(db.publisher_list(Some("stream1")).len(), 1);

        let DbLookup::Ok(viewer) = db.viewer_find_by_play_key("pl_key_456") else {
            panic!("viewer not found");
        };
        let player = Player {
            id: "pl1".to_string(),
            stream_id: "stream1".to_string(),
            viewer_id: viewer.id,
            app: "live".to_string(),
            stream_name: "test".to_string(),
            bytes_out: 512000,
            bitrate_kbps: 2400.0,
            connected_at: now_ts(),
            active: true,
            ..Default::default()
        };
        assert!(db.player_try_acquire(&player));
        assert_eq!(db.player_list(Some("stream1")).len(), 1);

        let stat = StatSample {
            stream_id: "stream1".to_string(),
            bitrate_in_kbps: 2500.0,
            fps: 60.0,
            width: 1920,
            height: 1080,
            video_codec: "h264".to_string(),
            audio_codec: "aac".to_string(),
            player_count: 1,
            ts: now_ts(),
        };
        assert!(db.stat_add(&stat));
        assert_eq!(db.stat_recent("stream1", 10).len(), 1);

        // Deactivate publisher via update, then it must drop out of the
        // active list.
        p.active = false;
        assert!(db.publisher_update("pub1", &p));
        assert_eq!(db.publisher_list(Some("stream1")).len(), 0);
    }

    #[test]
    fn stream_delete_cascades_active_publishers() {
        let db = Db::open(":memory:").unwrap();
        db.stream_add(&sample_stream(
            "cascade",
            "pub_cascade",
            "pl_cascade",
            "st_cascade",
        ))
        .unwrap();
        db.publisher_try_acquire(&Publisher {
            id: "pub_cascade_1".to_string(),
            stream_id: "cascade".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        });

        assert!(matches!(db.stream_delete("cascade"), Some(true)));
        assert_eq!(db.publisher_list(Some("cascade")).len(), 0);
        assert!(matches!(db.stream_get("cascade"), DbLookup::Missing));
    }

    #[test]
    fn max_length_stream_id_round_trips() {
        let db = Db::open(":memory:").unwrap();
        let long_id = "a".repeat(63);
        db.stream_add(&sample_stream(&long_id, "pub_long", "pl_long", "st_long"))
            .unwrap();

        let DbLookup::Ok(got) = db.stream_get(&long_id) else {
            panic!("long id stream not found");
        };
        assert_eq!(got.id.len(), 63);
        assert_eq!(got.id, long_id);
    }

    /// Regression test mirroring the C suite's #9: on_close-style updates
    /// must only ever deactivate the publisher whose stream_id they were
    /// looked up for, never a different stream's publisher.
    #[test]
    fn on_close_targets_correct_publisher() {
        let db = Db::open(":memory:").unwrap();
        db.stream_add(&sample_stream(
            "stream1",
            "pub_key_1",
            "pl_key_1",
            "st_key_1",
        ))
        .unwrap();
        db.stream_add(&sample_stream(
            "stream2",
            "pub_key_2",
            "pl_key_2",
            "st_key_2",
        ))
        .unwrap();

        db.publisher_try_acquire(&Publisher {
            id: "pub_1000_abc".to_string(),
            stream_id: "stream1".to_string(),
            app: "live".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        });
        db.publisher_try_acquire(&Publisher {
            id: "pub_1000_def".to_string(),
            stream_id: "stream2".to_string(),
            app: "live".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        });

        // Simulate on_close for pub1: find by publish_key -> stream_id -> list.
        let DbLookup::Ok(found) = db.stream_find_by_publish_key("pub_key_1") else {
            panic!("publish key not found");
        };
        let mut pubs = db.publisher_list(Some(&found.id));
        assert_eq!(pubs.len(), 1);
        assert_eq!(pubs[0].id, "pub_1000_abc");

        pubs[0].active = false;
        db.publisher_update(&pubs[0].id, &pubs[0]);

        let active_stream2 = db.publisher_list(Some("stream2"));
        assert_eq!(active_stream2.len(), 1);
        assert_eq!(active_stream2[0].id, "pub_1000_def");
        assert!(active_stream2[0].active);

        assert_eq!(db.publisher_list(Some("stream1")).len(), 0);
    }

    #[test]
    fn player_try_acquire_enforces_per_key_connection_cap() {
        let db = Db::open(":memory:").unwrap();
        db.stream_add(&sample_stream(
            "stream1",
            "pub_key_123",
            "pl_key_456",
            "st_key_789",
        ))
        .unwrap();
        let DbLookup::Ok(viewer) = db.viewer_find_by_play_key("pl_key_456") else {
            panic!("viewer not found");
        };

        for i in 0..MAX_CONNECTIONS_PER_PLAY_KEY {
            let player = Player {
                id: format!("pl{i}"),
                stream_id: "stream1".to_string(),
                viewer_id: viewer.id.clone(),
                connected_at: now_ts(),
                active: true,
                ..Default::default()
            };
            assert!(db.player_try_acquire(&player), "slot {i} should succeed");
        }

        let overflow = Player {
            id: "pl_overflow".to_string(),
            stream_id: "stream1".to_string(),
            viewer_id: viewer.id.clone(),
            connected_at: now_ts(),
            active: true,
            ..Default::default()
        };
        assert!(!db.player_try_acquire(&overflow));
    }
}
