//! Integration seam between the RTMP protocol layer and the SQLite-backed
//! server state.
//!
//! `librtmp2` (the RTMP protocol implementation) is being rewritten in Rust
//! separately. This module defines the callback contract that crate is
//! expected to drive — [`RtmpEventHandler`] — plus [`DbRtmpBridge`], the
//! concrete implementation that mirrors the original C `rtmp_callbacks.c`:
//! validating publish/play keys against the database, tracking per-connection
//! publisher/player rows, and deactivating them on disconnect.
//!
//! Once the Rust `librtmp2` crate exists, its server type should accept an
//! `Arc<dyn RtmpEventHandler>` (or call these methods directly) instead of
//! C function-pointer callbacks with a shared `userdata` slot.

// This whole module is a seam: nothing in this repo drives `RtmpEventHandler`
// yet, since that's the future `librtmp2` crate's job. Exercised only by
// this module's own tests until that crate exists and wires it in.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    pub codec: String,
}

/// Callback contract the RTMP protocol layer drives. Mirrors librtmp2's
/// `on_connect` / `on_publish` / `on_play` / `on_frame` / `on_close` hooks.
pub trait RtmpEventHandler: Send + Sync {
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
    fn on_frame(&self, conn: ConnId, frame: &FrameInfo);
    fn on_close(&self, conn: ConnId);
}

#[derive(Default)]
struct ConnState {
    publisher: Option<Publisher>,
    player: Option<Player>,
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
    pub fn new(db: Arc<Db>) -> Self {
        DbRtmpBridge {
            db,
            conns: Mutex::new(HashMap::new()),
        }
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
        self.conns
            .lock()
            .unwrap()
            .entry(conn)
            .or_default()
            .publisher = Some(pub_row);

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
        self.conns.lock().unwrap().entry(conn).or_default().player = Some(player_row);

        crate::log_info!(
            "RTMP: play accepted stream='{}' player={player_id}",
            stream.id
        );
        Ok(())
    }

    fn on_frame(&self, _conn: ConnId, frame: &FrameInfo) {
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
        // TODO: update publisher stats in DB (bitrate, codec, fps) once the
        // RTMP layer exposes per-frame size/timing through this seam.
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
            allowed_codecs: "avc1,hvc1,av01".to_string(),
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
    fn publish_then_close_deactivates_publisher() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        db.stream_add(&sample_stream("s1", "pub_k", "pl_k"));
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
        db.stream_add(&sample_stream("s1", "pub_k1", "pl_k1"));
        db.stream_add(&sample_stream("s2", "pub_k2", "pl_k2"));
        let bridge = DbRtmpBridge::new(Arc::clone(&db));

        bridge.on_connect(1);
        bridge.on_connect(2);
        assert!(bridge.on_publish(1, "live", "pub_k1", "1.1.1.1:1").is_ok());
        assert!(bridge.on_publish(2, "live", "pub_k2", "2.2.2.2:2").is_ok());

        bridge.on_close(1);
        assert_eq!(db.publisher_list(Some("s1")).len(), 0);
        assert_eq!(db.publisher_list(Some("s2")).len(), 1);
    }
}
