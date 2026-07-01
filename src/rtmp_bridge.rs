//! Integration seam between the RTMP protocol layer and the SQLite-backed
//! server state.
//!
//! `librtmp2` provides the RTMP protocol implementation. This module defines
//! the server-side callback contract — [`RtmpEventHandler`] — plus
//! [`DbRtmpBridge`], the DB-backed implementation that validates publish/play
//! keys, tracks per-connection publisher/player rows, updates publisher stats,
//! and deactivates rows on disconnect.
//!
//! `src/server.rs` drives this bridge from the integrated `librtmp2` server poll
//! loop. The current integration forwards connection lifecycle and publish/play
//! state into this bridge, and uses codec/frame metadata for policy checks and
//! stats updates. It does not imply that every incoming media frame is currently
//! forwarded as a full per-frame callback.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::db::{Db, Player, Publisher};

/// Opaque per-connection identifier assigned by the RTMP layer. The original
/// C code keyed connection state off the `lrtmp2_conn_t*` pointer; any stable,
/// unique handle works here.
pub type ConnId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Video,
    Audio,
}

#[derive(Debug, Clone)]
pub struct FrameInfo {
    pub kind: FrameKind,
    pub timestamp: u32,
    pub size: u32,
    /// Codec string, e.g. "avc1", "hvc1", "mp4a".
    pub codec: String,
}

/// Callback contract used by the RTMP server integration. Mirrors librtmp2's
/// `on_connect` / `on_publish` / `on_play` / `on_frame` / `on_close` hook shape.
pub trait RtmpEventHandler: Send + Sync {
    /// Called immediately after a new TCP connection is accepted.
    fn on_connect(&self, conn: ConnId);
    /// Return `Err` to reject the publish (invalid publish_key).
    fn on_publish(
        &self,
        conn: ConnId,
        app: &str,
        stream_key: &str,
        remote_addr: &str,
    ) -> Result<(), ()>;
    /// Return `Err` to reject the play request (invalid play_key).
    fn on_play(
        &self,
        conn: ConnId,
        app: &str,
        stream_key: &str,
        remote_addr: &str,
    ) -> Result<(), ()>;
    /// Validate frame/codec metadata. The current server integration uses this
    /// for codec enforcement and does not guarantee one call per incoming media
    /// frame.
    fn on_frame(&self, conn: ConnId, frame: &FrameInfo) -> bool;
    /// Called when the connection is closed (cleanly or by error).
    fn on_close(&self, conn: ConnId);
}

#[derive(Default)]
struct ConnState {
    publisher: Option<Publisher>,
    player: Option<Player>,
    /// DB stream id for the published stream, set in on_publish.
    pub stream_id: String,
    /// Comma-separated allowed codec list from the stream row.
    pub allowed_codecs: String,
    /// Timestamp of the last stats flush to the DB.
    last_stats_at: Option<Instant>,
    /// bytes_received snapshot at the last stats flush.
    bytes_at_last_stats: u64,
}

/// DB-backed [`RtmpEventHandler`]. Each connection's role(s) and DB row(s)
/// live in a per-connection map entry, captured at publish/play time — so
/// closing one connection can never touch another connection's row, unlike
/// state keyed only by stream id.
pub struct DbRtmpBridge {
    db: Arc<Db>,
    conns: Mutex<HashMap<ConnId, ConnState>>,
}

fn gen_id(prefix: &str) -> String {
    use rand::RngExt;
    format!("{prefix}{:016x}", rand::rng().random::<u64>())
}

impl DbRtmpBridge {
    /// Create a new bridge backed by the given database handle.
    pub fn new(db: Arc<Db>) -> Self {
        DbRtmpBridge {
            db,
            conns: Mutex::new(HashMap::new()),
        }
    }

    /// Return the DB stream id for a publishing connection, or empty string.
    pub fn stream_id_for_conn(&self, conn: ConnId) -> String {
        self.conns
            .lock()
            .unwrap()
            .get(&conn)
            .map(|s| s.stream_id.clone())
            .unwrap_or_default()
    }

    /// Return the allowed_codecs string for a publishing connection.
    pub fn allowed_codecs_for_conn(&self, conn: ConnId) -> String {
        self.conns
            .lock()
            .unwrap()
            .get(&conn)
            .map(|s| s.allowed_codecs.clone())
            .unwrap_or_default()
    }

    /// Update publisher stats (bytes_in, bitrate, codec) in the DB.
    /// Called from the server poll loop after every poll iteration.
    pub fn update_publisher_stats(
        &self,
        conn: ConnId,
        bytes_received: u64,
        video_codec: &str,
        audio_codec: &str,
    ) {
        let mut guard = self.conns.lock().unwrap();
        let Some(cs) = guard.get_mut(&conn) else {
            return;
        };
        let Some(ref mut pub_row) = cs.publisher else {
            return;
        };

        let now = Instant::now();
        let elapsed_secs = cs
            .last_stats_at
            .map(|t| now.duration_since(t).as_secs_f64())
            .unwrap_or(0.0);

        let bytes_delta = bytes_received.saturating_sub(cs.bytes_at_last_stats);

        // Only flush to DB if at least 1 second has passed (rate-limit writes).
        if elapsed_secs < 1.0 && cs.last_stats_at.is_some() {
            return;
        }

        let bitrate_kbps = if elapsed_secs > 0.0 {
            (bytes_delta as f64 * 8.0) / (elapsed_secs * 1000.0)
        } else {
            0.0
        };

        pub_row.bytes_in = bytes_received;
        pub_row.bitrate_kbps = bitrate_kbps;
        if !video_codec.is_empty() {
            pub_row.video_codec = video_codec.to_string();
        }
        if !audio_codec.is_empty() {
            pub_row.audio_codec = audio_codec.to_string();
        }

        cs.last_stats_at = Some(now);
        cs.bytes_at_last_stats = bytes_received;

        // Clone the row to release the lock before the DB call.
        let pub_id = pub_row.id.clone();
        let pub_row_clone = pub_row.clone();
        drop(guard);

        self.db.publisher_update(&pub_id, &pub_row_clone);
    }
}

impl RtmpEventHandler for DbRtmpBridge {
    fn on_connect(&self, conn: ConnId) {
        self.conns
            .lock()
            .unwrap()
            .insert(conn, ConnState::default());
        crate::log_debug!("RTMP: new connection {conn}");
    }

    fn on_publish(
        &self,
        conn: ConnId,
        app: &str,
        stream_key: &str,
        remote_addr: &str,
    ) -> Result<(), ()> {
        crate::log_info!("RTMP: publish request app='{app}' key=<redacted>");

        let Some(stream) = self.db.stream_find_by_publish_key(stream_key) else {
            crate::log_warn!("RTMP: publish rejected — invalid publish_key for app='{app}'");
            return Err(());
        };
        if stream.app != app {
            crate::log_warn!(
                "RTMP: publish rejected — key belongs to app='{}', requested app='{app}'",
                stream.app
            );
            return Err(());
        }

        let pub_row = Publisher {
            id: gen_id("pub_"),
            stream_id: stream.id.clone(),
            remote_addr: remote_addr.to_string(),
            app: app.to_string(),
            stream_name: stream.name.clone(),
            active: true,
            connected_at: crate::db::now_ts(),
            ..Default::default()
        };
        if !self.db.publisher_add(&pub_row) {
            crate::log_warn!("RTMP: publish rejected — failed to record publisher row");
            return Err(());
        }

        let pub_id = pub_row.id.clone();
        let stream_id = stream.id.clone();
        let allowed_codecs = stream.allowed_codecs.clone();

        let mut guard = self.conns.lock().unwrap();
        let cs = guard.entry(conn).or_default();
        cs.publisher = Some(pub_row);
        cs.stream_id = stream_id;
        cs.allowed_codecs = allowed_codecs;

        crate::log_info!(
            "RTMP: publish accepted stream='{}' publisher={pub_id}",
            stream.id
        );
        Ok(())
    }

    fn on_play(
        &self,
        conn: ConnId,
        app: &str,
        stream_key: &str,
        remote_addr: &str,
    ) -> Result<(), ()> {
        crate::log_info!("RTMP: play request app='{app}' key=<redacted>");

        let Some(stream) = self.db.stream_find_by_play_key(stream_key) else {
            crate::log_warn!("RTMP: play rejected — invalid play_key for app='{app}'");
            return Err(());
        };
        if stream.app != app {
            crate::log_warn!(
                "RTMP: play rejected — key belongs to app='{}', requested app='{app}'",
                stream.app
            );
            return Err(());
        }

        let player_row = Player {
            id: gen_id("pl_"),
            stream_id: stream.id.clone(),
            remote_addr: remote_addr.to_string(),
            app: app.to_string(),
            stream_name: stream.name.clone(),
            active: true,
            connected_at: crate::db::now_ts(),
            ..Default::default()
        };
        if !self.db.player_add(&player_row) {
            crate::log_warn!("RTMP: play rejected — failed to record player row");
            return Err(());
        }

        let player_id = player_row.id.clone();
        let stream_id = stream.id.clone();
        let mut guard = self.conns.lock().unwrap();
        let cs = guard.entry(conn).or_default();
        cs.player = Some(player_row);
        // Store stream_id for player connections too (used for deletion signalling).
        if cs.stream_id.is_empty() {
            cs.stream_id = stream_id;
        }

        crate::log_info!(
            "RTMP: play accepted stream='{}' player={player_id}",
            stream.id
        );
        Ok(())
    }

    fn on_frame(&self, conn: ConnId, frame: &FrameInfo) -> bool {
        match frame.kind {
            FrameKind::Video => crate::log_debug!(
                "RTMP: VIDEO frame ts={} size={} codec={}",
                frame.timestamp,
                frame.size,
                frame.codec
            ),
            FrameKind::Audio => crate::log_debug!(
                "RTMP: AUDIO frame ts={} size={} codec={}",
                frame.timestamp,
                frame.size,
                frame.codec
            ),
        }

        // Enforce allowed_codecs: reject if the detected codec is not listed.
        if !frame.codec.is_empty() {
            let guard = self.conns.lock().unwrap();
            if let Some(cs) = guard.get(&conn)
                && !cs.allowed_codecs.is_empty()
            {
                let allowed = cs
                    .allowed_codecs
                    .split(',')
                    .map(|s| s.trim())
                    .any(|c| c.eq_ignore_ascii_case(&frame.codec));
                if !allowed {
                    crate::log_warn!(
                        "RTMP: codec '{}' not in allowed list '{}' — closing conn={conn}",
                        frame.codec,
                        cs.allowed_codecs
                    );
                    return false;
                }
            }
        }

        true
    }

    fn on_close(&self, conn: ConnId) {
        let cs = self.conns.lock().unwrap().remove(&conn);
        let Some(cs) = cs else {
            crate::log_warn!("RTMP: on_close for untracked connection {conn}");
            return;
        };

        if let Some(mut pub_row) = cs.publisher {
            pub_row.active = false;
            self.db.publisher_update(&pub_row.id, &pub_row);
            crate::log_info!(
                "RTMP: publisher disconnected: stream={} id={}",
                pub_row.stream_id,
                pub_row.id
            );
        }

        if let Some(mut player_row) = cs.player {
            player_row.active = false;
            self.db.player_update(&player_row.id, &player_row);
            crate::log_info!(
                "RTMP: player disconnected: stream={} id={}",
                player_row.stream_id,
                player_row.id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_stream(id: &str, pub_key: &str, play_key: &str) -> crate::db::Stream {
        crate::db::Stream {
            id: id.to_string(),
            name: format!("{id} name"),
            app: "live".to_string(),
            publish_key: pub_key.to_string(),
            play_key: play_key.to_string(),
            stats_key: format!("st_{id}"),
            enabled: true,
            allowed_codecs: "avc1,hvc1,av01,mp4a".to_string(),
            created_at: crate::db::now_ts(),
        }
    }

    #[test]
    fn publish_rejects_unknown_key() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let bridge = DbRtmpBridge::new(db);
        bridge.on_connect(1);
        assert!(bridge.on_publish(1, "live", "bogus", "1.2.3.4:1").is_err());
    }

    #[test]
    fn publish_rejects_key_for_wrong_app() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.stream_add(&sample_stream("s1", "pub_k", "pl_k"))
            .unwrap();
        let bridge = DbRtmpBridge::new(Arc::clone(&db));

        bridge.on_connect(1);
        assert!(bridge.on_publish(1, "other", "pub_k", "1.2.3.4:1").is_err());
        assert_eq!(db.publisher_list(Some("s1")).len(), 0);
    }

    #[test]
    fn play_rejects_key_for_wrong_app() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.stream_add(&sample_stream("s1", "pub_k", "pl_k"))
            .unwrap();
        let bridge = DbRtmpBridge::new(Arc::clone(&db));

        bridge.on_connect(1);
        assert!(bridge.on_play(1, "other", "pl_k", "1.2.3.4:1").is_err());
        assert_eq!(db.player_list(Some("s1")).len(), 0);
    }

    #[test]
    fn publish_then_close_deactivates_publisher() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.stream_add(&sample_stream("s1", "pub_k", "pl_k"))
            .unwrap();
        let bridge = DbRtmpBridge::new(Arc::clone(&db));

        bridge.on_connect(1);
        assert!(bridge.on_publish(1, "live", "pub_k", "1.2.3.4:1").is_ok());
        assert_eq!(db.publisher_list(Some("s1")).len(), 1);

        bridge.on_close(1);
        assert_eq!(db.publisher_list(Some("s1")).len(), 0);
    }

    #[test]
    fn close_only_affects_its_own_connection() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.stream_add(&sample_stream("s1", "pub_k1", "pl_k1"))
            .unwrap();
        db.stream_add(&sample_stream("s2", "pub_k2", "pl_k2"))
            .unwrap();
        let bridge = DbRtmpBridge::new(Arc::clone(&db));

        bridge.on_connect(1);
        bridge.on_connect(2);
        assert!(bridge.on_publish(1, "live", "pub_k1", "1.1.1.1:1").is_ok());
        assert!(bridge.on_publish(2, "live", "pub_k2", "2.2.2.2:2").is_ok());

        bridge.on_close(1);
        assert_eq!(db.publisher_list(Some("s1")).len(), 0);
        assert_eq!(db.publisher_list(Some("s2")).len(), 1);
    }

    #[test]
    fn on_frame_rejects_disallowed_codec() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.stream_add(&sample_stream("s1", "pub_k", "pl_k"))
            .unwrap();
        let bridge = DbRtmpBridge::new(Arc::clone(&db));

        bridge.on_connect(1);
        assert!(bridge.on_publish(1, "live", "pub_k", "1.2.3.4:1").is_ok());

        // "vp9" is not in "avc1,hvc1,av01,mp4a"
        let frame = FrameInfo {
            kind: FrameKind::Video,
            timestamp: 0,
            size: 100,
            codec: "vp9".to_string(),
        };
        assert!(!bridge.on_frame(1, &frame));
    }

    #[test]
    fn on_frame_allows_listed_codec() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.stream_add(&sample_stream("s1", "pub_k", "pl_k"))
            .unwrap();
        let bridge = DbRtmpBridge::new(Arc::clone(&db));

        bridge.on_connect(1);
        assert!(bridge.on_publish(1, "live", "pub_k", "1.2.3.4:1").is_ok());

        let frame = FrameInfo {
            kind: FrameKind::Video,
            timestamp: 0,
            size: 100,
            codec: "avc1".to_string(),
        };
        assert!(bridge.on_frame(1, &frame));
    }

    #[test]
    fn on_frame_allows_default_aac_audio_codec() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.stream_add(&sample_stream("s1", "pub_k", "pl_k"))
            .unwrap();
        let bridge = DbRtmpBridge::new(Arc::clone(&db));

        bridge.on_connect(1);
        assert!(bridge.on_publish(1, "live", "pub_k", "1.2.3.4:1").is_ok());

        let frame = FrameInfo {
            kind: FrameKind::Audio,
            timestamp: 0,
            size: 100,
            codec: "mp4a".to_string(),
        };
        assert!(bridge.on_frame(1, &frame));
    }
}
