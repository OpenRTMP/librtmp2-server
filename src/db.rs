//! SQLite persistence layer.
//!
//! Stores streams, publishers, players and stats samples. `Db` wraps a
//! single connection behind a mutex; SQLite itself only allows one writer
//! at a time anyway, so this matches the C version's locking model without
//! needing a connection pool.

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Db {
    conn: Mutex<Connection>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Stream {
    pub id: String,
    pub name: String,
    pub app: String,
    pub publish_key: String,
    pub play_key: String,
    pub stats_key: String,
    pub enabled: bool,
    pub allowed_codecs: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Publisher {
    pub id: String,
    pub stream_id: String,
    pub remote_addr: String,
    pub app: String,
    pub stream_name: String,
    pub video_codec: String,
    pub audio_codec: String,
    pub video_width: u32,
    pub video_height: u32,
    pub fps: f64,
    pub bytes_in: u64,
    pub bitrate_kbps: f64,
    pub connected_at: i64,
    pub active: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Player {
    pub id: String,
    pub stream_id: String,
    pub remote_addr: String,
    pub app: String,
    pub stream_name: String,
    pub bytes_out: u64,
    pub bitrate_kbps: f64,
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

#[allow(dead_code)]
pub fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS streams (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  app TEXT NOT NULL DEFAULT 'live',
  publish_key TEXT UNIQUE NOT NULL,
  play_key TEXT UNIQUE NOT NULL,
  stats_key TEXT UNIQUE NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  allowed_codecs TEXT NOT NULL DEFAULT 'avc1,hvc1,av01',
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS publishers (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  remote_addr TEXT NOT NULL DEFAULT '',
  app TEXT NOT NULL DEFAULT '',
  stream_name TEXT NOT NULL DEFAULT '',
  video_codec TEXT NOT NULL DEFAULT '',
  audio_codec TEXT NOT NULL DEFAULT '',
  video_width INTEGER NOT NULL DEFAULT 0,
  video_height INTEGER NOT NULL DEFAULT 0,
  fps REAL NOT NULL DEFAULT 0,
  bytes_in INTEGER NOT NULL DEFAULT 0,
  bitrate_kbps REAL NOT NULL DEFAULT 0,
  connected_at INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS players (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  remote_addr TEXT NOT NULL DEFAULT '',
  app TEXT NOT NULL DEFAULT '',
  stream_name TEXT NOT NULL DEFAULT '',
  bytes_out INTEGER NOT NULL DEFAULT 0,
  bitrate_kbps REAL NOT NULL DEFAULT 0,
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
";

impl Db {
    pub fn open(path: &str) -> rusqlite::Result<Db> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_millis(1000))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        conn.execute_batch(SCHEMA)?;
        crate::log_info!("Database opened: {path}");
        Ok(Db {
            conn: Mutex::new(conn),
        })
    }

    // ==================== STREAMS ====================

    pub fn stream_add(&self, s: &Stream) -> bool {
        let conn = self.conn.lock().unwrap();
        let rc = conn.execute(
            "INSERT INTO streams (id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at) \
             VALUES (?,?,?,?,?,?,?,?,?)",
            params![s.id, s.name, s.app, s.publish_key, s.play_key, s.stats_key, s.enabled, s.allowed_codecs, s.created_at],
        );
        match rc {
            Ok(_) => {
                crate::log_info!("Stream added: id={} app={}", s.id, s.app);
                true
            }
            Err(e) => {
                crate::log_error!("Failed to add stream {}: {e}", s.id);
                false
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
            allowed_codecs: row.get(7)?,
            created_at: row.get(8)?,
        })
    }

    const STREAM_COLS: &'static str =
        "id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at";

    pub fn stream_get(&self, id: &str) -> Option<Stream> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            &format!("SELECT {} FROM streams WHERE id=?", Self::STREAM_COLS),
            params![id],
            Self::load_stream_row,
        )
        .optional()
        .unwrap_or(None)
    }

    #[allow(dead_code)]
    pub fn stream_get_by_app(&self, app: &str, stream_name: &str) -> Option<Stream> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            &format!(
                "SELECT {} FROM streams WHERE app=? AND name=?",
                Self::STREAM_COLS
            ),
            params![app, stream_name],
            Self::load_stream_row,
        )
        .optional()
        .unwrap_or(None)
    }

    fn stream_find_by(&self, column: &str, key: &str) -> Option<Stream> {
        if !matches!(column, "publish_key" | "play_key" | "stats_key") {
            crate::log_error!("stream_find_by: rejected disallowed column '{column}'");
            return None;
        }
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            &format!(
                "SELECT {} FROM streams WHERE {column}=? AND enabled=1",
                Self::STREAM_COLS
            ),
            params![key],
            Self::load_stream_row,
        )
        .optional()
        .unwrap_or(None)
    }

    pub fn stream_find_by_publish_key(&self, key: &str) -> Option<Stream> {
        self.stream_find_by("publish_key", key)
    }

    pub fn stream_find_by_play_key(&self, key: &str) -> Option<Stream> {
        self.stream_find_by("play_key", key)
    }

    pub fn stream_find_by_stats_key(&self, key: &str) -> Option<Stream> {
        self.stream_find_by("stats_key", key)
    }

    #[allow(dead_code)]
    pub fn stream_update(&self, id: &str, s: &Stream) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE streams SET name=?,app=?,publish_key=?,play_key=?,stats_key=?,enabled=?,allowed_codecs=? WHERE id=?",
            params![s.name, s.app, s.publish_key, s.play_key, s.stats_key, s.enabled, s.allowed_codecs, id],
        )
        .is_ok()
    }

    /// Cascade: remove dependent rows so deleted streams cannot leave ghost
    /// active publishers/players that pollute stats after stream re-creation.
    ///
    /// Returns `Some(true)` = deleted, `Some(false)` = not found, `None` = DB error.
    pub fn stream_delete(&self, id: &str) -> Option<bool> {
        let conn = self.conn.lock().unwrap();
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
            Ok(rows) => {
                if tx.commit().is_ok() {
                    Some(rows > 0)
                } else {
                    None
                }
            }
            Err(e) => {
                crate::log_error!("DB cascade delete error for {id}: {e}");
                let _ = tx.rollback();
                None
            }
        }
    }

    pub fn stream_list(&self) -> Vec<Stream> {
        let conn = self.conn.lock().unwrap();
        let Ok(mut stmt) = conn.prepare(&format!(
            "SELECT {} FROM streams ORDER BY created_at",
            Self::STREAM_COLS
        )) else {
            return Vec::new();
        };
        stmt.query_map([], Self::load_stream_row)
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
    }

    // ==================== PUBLISHERS ====================

    pub fn publisher_add(&self, p: &Publisher) -> bool {
        let Ok(bytes_in) = i64::try_from(p.bytes_in) else {
            crate::log_error!("publisher_add: bytes_in {} overflows i64", p.bytes_in);
            return false;
        };
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO publishers \
             (id,stream_id,remote_addr,app,stream_name,video_codec,audio_codec,video_width,video_height,fps,bytes_in,bitrate_kbps,connected_at,active) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            params![p.id, p.stream_id, p.remote_addr, p.app, p.stream_name, p.video_codec, p.audio_codec,
                    p.video_width, p.video_height, p.fps, bytes_in, p.bitrate_kbps, p.connected_at],
        )
        .is_ok()
    }

    pub fn publisher_update(&self, id: &str, p: &Publisher) -> bool {
        let Ok(bytes_in) = i64::try_from(p.bytes_in) else {
            crate::log_error!("publisher_update: bytes_in {} overflows i64", p.bytes_in);
            return false;
        };
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE publishers SET stream_id=?,remote_addr=?,app=?,stream_name=?,\
             video_codec=?,audio_codec=?,video_width=?,video_height=?,fps=?,\
             bytes_in=?,bitrate_kbps=?,active=? WHERE id=?",
            params![
                p.stream_id,
                p.remote_addr,
                p.app,
                p.stream_name,
                p.video_codec,
                p.audio_codec,
                p.video_width,
                p.video_height,
                p.fps,
                bytes_in,
                p.bitrate_kbps,
                p.active,
                id
            ],
        )
        .is_ok()
    }

    #[allow(dead_code)]
    pub fn publisher_remove(&self, id: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM publishers WHERE id=?", params![id])
            .is_ok()
    }

    fn load_publisher_row(row: &rusqlite::Row) -> rusqlite::Result<Publisher> {
        Ok(Publisher {
            id: row.get(0)?,
            stream_id: row.get(1)?,
            remote_addr: row.get(2)?,
            app: row.get(3)?,
            stream_name: row.get(4)?,
            video_codec: row.get(5)?,
            audio_codec: row.get(6)?,
            video_width: row.get(7)?,
            video_height: row.get(8)?,
            fps: row.get(9)?,
            bytes_in: u64::try_from(row.get::<_, i64>(10)?).map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    10,
                    rusqlite::types::Type::Integer,
                    "negative bytes_in".into(),
                )
            })?,
            bitrate_kbps: row.get(11)?,
            connected_at: row.get(12)?,
            active: row.get(13)?,
        })
    }

    const PUBLISHER_COLS: &'static str = "id,stream_id,remote_addr,app,stream_name,video_codec,audio_codec,video_width,video_height,fps,bytes_in,bitrate_kbps,connected_at,active";

    pub fn publisher_list(&self, stream_id: Option<&str>) -> Vec<Publisher> {
        let conn = self.conn.lock().unwrap();
        match stream_id {
            Some(sid) => {
                let Ok(mut stmt) = conn.prepare(&format!(
                    "SELECT {} FROM publishers WHERE stream_id=? AND active=1",
                    Self::PUBLISHER_COLS
                )) else {
                    return Vec::new();
                };
                stmt.query_map(params![sid], Self::load_publisher_row)
                    .map(|rows| rows.filter_map(|r| r.ok()).collect())
                    .unwrap_or_default()
            }
            None => {
                let Ok(mut stmt) = conn.prepare(&format!(
                    "SELECT {} FROM publishers WHERE active=1",
                    Self::PUBLISHER_COLS
                )) else {
                    return Vec::new();
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

    // ==================== PLAYERS ====================

    pub fn player_add(&self, p: &Player) -> bool {
        let Ok(bytes_out) = i64::try_from(p.bytes_out) else {
            crate::log_error!("player_add: bytes_out {} overflows i64", p.bytes_out);
            return false;
        };
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO players \
             (id,stream_id,remote_addr,app,stream_name,bytes_out,bitrate_kbps,connected_at,active) \
             VALUES (?,?,?,?,?,?,?,?,1)",
            params![
                p.id,
                p.stream_id,
                p.remote_addr,
                p.app,
                p.stream_name,
                bytes_out,
                p.bitrate_kbps,
                p.connected_at
            ],
        )
        .is_ok()
    }

    pub fn player_update(&self, id: &str, p: &Player) -> bool {
        let Ok(bytes_out) = i64::try_from(p.bytes_out) else {
            crate::log_error!("player_update: bytes_out {} overflows i64", p.bytes_out);
            return false;
        };
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE players SET stream_id=?,remote_addr=?,app=?,stream_name=?,\
             bytes_out=?,bitrate_kbps=?,active=? WHERE id=?",
            params![
                p.stream_id,
                p.remote_addr,
                p.app,
                p.stream_name,
                bytes_out,
                p.bitrate_kbps,
                p.active,
                id
            ],
        )
        .is_ok()
    }

    #[allow(dead_code)]
    pub fn player_remove(&self, id: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM players WHERE id=?", params![id])
            .is_ok()
    }

    fn load_player_row(row: &rusqlite::Row) -> rusqlite::Result<Player> {
        Ok(Player {
            id: row.get(0)?,
            stream_id: row.get(1)?,
            remote_addr: row.get(2)?,
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
            connected_at: row.get(7)?,
            active: row.get(8)?,
        })
    }

    const PLAYER_COLS: &'static str =
        "id,stream_id,remote_addr,app,stream_name,bytes_out,bitrate_kbps,connected_at,active";

    pub fn player_list(&self, stream_id: Option<&str>) -> Vec<Player> {
        let conn = self.conn.lock().unwrap();
        match stream_id {
            Some(sid) => {
                let Ok(mut stmt) = conn.prepare(&format!(
                    "SELECT {} FROM players WHERE stream_id=? AND active=1",
                    Self::PLAYER_COLS
                )) else {
                    return Vec::new();
                };
                stmt.query_map(params![sid], Self::load_player_row)
                    .map(|rows| rows.filter_map(|r| r.ok()).collect())
                    .unwrap_or_default()
            }
            None => {
                let Ok(mut stmt) = conn.prepare(&format!(
                    "SELECT {} FROM players WHERE active=1",
                    Self::PLAYER_COLS
                )) else {
                    return Vec::new();
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
        let conn = self.conn.lock().unwrap();
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
        let conn = self.conn.lock().unwrap();
        let Ok(mut stmt) = conn.prepare(
            "SELECT stream_id,bitrate_in_kbps,fps,width,height,video_codec,audio_codec,player_count,ts \
             FROM stats_samples WHERE stream_id=? ORDER BY ts DESC LIMIT ?",
        ) else {
            return Vec::new();
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

    fn sample_stream(id: &str, pub_key: &str, play_key: &str, stats_key: &str) -> Stream {
        Stream {
            id: id.to_string(),
            name: format!("{id} name"),
            app: "live".to_string(),
            publish_key: pub_key.to_string(),
            play_key: play_key.to_string(),
            stats_key: stats_key.to_string(),
            enabled: true,
            allowed_codecs: "avc1,hvc1,av01".to_string(),
            created_at: now_ts(),
        }
    }

    #[test]
    fn stream_crud_and_keys() {
        let db = Db::open(":memory:").unwrap();

        let s = sample_stream("stream1", "pub_key_123", "pl_key_456", "st_key_789");
        assert!(db.stream_add(&s));

        let got = db.stream_get("stream1").expect("not found");
        assert_eq!(got.name, "stream1 name");

        assert_eq!(
            db.stream_find_by_publish_key("pub_key_123").unwrap().id,
            "stream1"
        );
        assert!(db.stream_find_by_play_key("pl_key_456").is_some());
        assert!(db.stream_find_by_stats_key("st_key_789").is_some());
        assert!(db.stream_find_by_stats_key("wrong_key").is_none());

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
        ));

        let mut p = Publisher {
            id: "pub1".to_string(),
            stream_id: "stream1".to_string(),
            remote_addr: "127.0.0.1:54321".to_string(),
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
        };
        assert!(db.publisher_add(&p));
        assert_eq!(db.publisher_list(Some("stream1")).len(), 1);

        let player = Player {
            id: "pl1".to_string(),
            stream_id: "stream1".to_string(),
            remote_addr: "10.0.0.1:12345".to_string(),
            app: "live".to_string(),
            stream_name: "test".to_string(),
            bytes_out: 512000,
            bitrate_kbps: 2400.0,
            connected_at: now_ts(),
            active: true,
        };
        assert!(db.player_add(&player));
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
        ));
        db.publisher_add(&Publisher {
            id: "pub_cascade_1".to_string(),
            stream_id: "cascade".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        });

        assert!(matches!(db.stream_delete("cascade"), Some(true)));
        assert_eq!(db.publisher_list(Some("cascade")).len(), 0);
        assert!(db.stream_get("cascade").is_none());
    }

    #[test]
    fn max_length_stream_id_round_trips() {
        let db = Db::open(":memory:").unwrap();
        let long_id = "a".repeat(63);
        db.stream_add(&sample_stream(&long_id, "pub_long", "pl_long", "st_long"));

        let got = db.stream_get(&long_id).expect("not found");
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
        ));
        db.stream_add(&sample_stream(
            "stream2",
            "pub_key_2",
            "pl_key_2",
            "st_key_2",
        ));

        db.publisher_add(&Publisher {
            id: "pub_1000_abc".to_string(),
            stream_id: "stream1".to_string(),
            app: "live".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        });
        db.publisher_add(&Publisher {
            id: "pub_1000_def".to_string(),
            stream_id: "stream2".to_string(),
            app: "live".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        });

        // Simulate on_close for pub1: find by publish_key -> stream_id -> list.
        let found = db.stream_find_by_publish_key("pub_key_1").unwrap();
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
}
