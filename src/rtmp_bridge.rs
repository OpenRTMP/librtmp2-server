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
//! state into this bridge, and uses frame metadata for stats updates.

#![allow(dead_code)]

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::db::{Db, DbLookup, Player, Publisher};
use crate::keygen::{self, PREFIX_PLAY_KEY, PREFIX_PUBLISH_KEY};

const RTMP_AUTH_FAILURE_WINDOW: Duration = Duration::from_secs(60);
const RTMP_AUTH_MAX_FAILURES: usize = 10;
/// Cap tracked auth-failure buckets so a scan from many distinct IPs cannot
/// grow `auth_failures` without bound, mirroring `rate_limit::MAX_TRACKED_KEYS`.
const MAX_TRACKED_AUTH_FAILURE_KEYS: usize = 10_000;

/// Distinguishes a brute-forceable credential/app mismatch from a rejection
/// that has nothing to do with guessing a valid key (deleted/disabled
/// stream, connection-limit, DB or keygen error). Only the former should
/// count against the auth-failure rate limit — counting the latter lets
/// unrelated operational failures throttle a legitimate client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthFailureKind {
    Credential,
    Operational,
}

/// Opaque per-connection identifier assigned by the RTMP layer. The original
/// C code keyed connection state off the `lrtmp2_conn_t*` pointer; any stable,
/// unique handle works here.
pub type ConnId = u64;

/// Stream metadata polled from a publisher `Conn` (onMetaData + codec detection).
#[derive(Debug, Clone, Copy, Default)]
pub struct PublisherStreamMetadata {
    pub video_width: Option<u32>,
    pub video_height: Option<u32>,
    pub framerate: Option<f64>,
    pub audio_sample_rate: Option<u32>,
    pub audio_channels: Option<u32>,
}

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
/// `on_connect` / `on_publish` / `authorize_play` / `on_frame` / `on_close` hook shape.
pub trait RtmpEventHandler: Send + Sync {
    /// Called immediately after a new TCP connection is accepted.
    fn on_connect(&self, conn: ConnId, remote_addr: &str);
    /// Atomically authorize a publish (DB slot + per-connection state).
    /// Called from the RTMP publish callback before media relay is enabled.
    #[allow(clippy::result_unit_err)]
    fn authorize_publish(&self, conn: ConnId, app: &str, stream_key: &str) -> Result<(), ()>;
    /// Atomically authorize a play (DB slot + per-connection state).
    /// Called from the RTMP play callback before `Play.Start` is sent.
    #[allow(clippy::result_unit_err)]
    fn authorize_play(&self, conn: ConnId, app: &str, stream_key: &str) -> Result<(), ()>;
    /// Optional per-frame hook for debug logging; always accepts media.
    fn on_frame(&self, conn: ConnId, frame: &FrameInfo) -> bool;
    /// Called when the connection is closed (cleanly or by error).
    fn on_close(&self, conn: ConnId);
}

#[derive(Default)]
struct ConnState {
    /// Client IP (no port) this connection was accepted from, used to key
    /// auth-failure rate limiting by a stable identity instead of `ConnId`.
    remote_ip: String,
    publisher: Option<Publisher>,
    player: Option<Player>,
    /// Configured play-key row id for the active viewer session.
    viewer_id: String,
    /// DB stream id for the published stream, set in on_publish.
    pub stream_id: String,
    /// Timestamp of the last publisher stats flush to the DB.
    publisher_last_stats_at: Option<Instant>,
    /// Raw connection byte counter at the start of the current publisher session.
    publisher_bytes_base: u64,
    /// Publisher session-local bytes snapshot at the last stats flush.
    publisher_bytes_at_last_stats: u64,
    /// Rebase the next publisher stats update after replacing publisher state.
    publisher_stats_reset_pending: bool,
    /// Timestamp of the last player stats flush to the DB.
    player_last_stats_at: Option<Instant>,
    /// Raw connection byte counter at the start of the current player session.
    player_bytes_base: u64,
    /// Player session-local bytes snapshot at the last stats flush.
    player_bytes_at_last_stats: u64,
    /// Rebase the next player stats update after replacing player state.
    player_stats_reset_pending: bool,
    /// Timestamp of the last RTT flush to the DB.
    last_rtt_at: Option<Instant>,
}

/// DB-backed [`RtmpEventHandler`]. Each connection's role(s) and DB row(s)
/// live in a per-connection map entry, captured at publish/play time — so
/// closing one connection can never touch another connection's row, unlike
/// state keyed only by stream id.
pub struct DbRtmpBridge {
    db: Arc<Db>,
    conns: Mutex<HashMap<ConnId, ConnState>>,
    deleted_streams: Arc<Mutex<HashSet<String>>>,
    /// Failed publish/play auth attempts keyed by client IP (not `ConnId`) so
    /// reconnecting on a fresh TCP connection does not reset the window —
    /// see `is_auth_rate_limited`.
    auth_failures: Mutex<HashMap<String, Vec<Instant>>>,
}

/// Strip the port from a `host:port` / `[host]:port` remote address string,
/// leaving a stable per-client identity to key auth-failure tracking by.
fn remote_ip_of(remote_addr: &str) -> String {
    if let Some(rest) = remote_addr.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        return rest[..end].to_string();
    }
    match remote_addr.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host.to_string(),
        _ => remote_addr.to_string(),
    }
}

fn apply_publisher_codecs(pub_row: &mut Publisher, video_codec: &str, audio_codec: &str) {
    if !video_codec.is_empty() {
        pub_row.video_codec = video_codec.to_string();
    }
    if !audio_codec.is_empty() {
        pub_row.audio_codec = audio_codec.to_string();
    }
}

fn apply_publisher_metadata(pub_row: &mut Publisher, metadata: PublisherStreamMetadata) {
    if let Some(w) = metadata.video_width.filter(|v| *v > 0) {
        pub_row.video_width = w;
    }
    if let Some(h) = metadata.video_height.filter(|v| *v > 0) {
        pub_row.video_height = h;
    }
    if let Some(fps) = metadata.framerate.filter(|v| *v > 0.0 && v.is_finite()) {
        pub_row.fps = fps;
    }
    if let Some(sr) = metadata.audio_sample_rate.filter(|v| *v > 0) {
        pub_row.audio_sample_rate = sr;
    }
    if let Some(ch) = metadata.audio_channels.filter(|v| *v > 0) {
        pub_row.audio_channels = ch;
    }
}

impl DbRtmpBridge {
    /// Create a new bridge backed by the given database handle.
    pub fn new(db: Arc<Db>, deleted_streams: Arc<Mutex<HashSet<String>>>) -> Self {
        DbRtmpBridge {
            db,
            conns: Mutex::new(HashMap::new()),
            deleted_streams,
            auth_failures: Mutex::new(HashMap::new()),
        }
    }

    /// True once `on_connect` has recorded a remote IP for `conn`. Lets
    /// callers that may need to register a connection mid-poll (before the
    /// normal `on_connect` pass runs) skip the redundant re-registration and
    /// its log line on every subsequent publish/play attempt.
    pub(crate) fn is_registered(&self, conn: ConnId) -> bool {
        self.conns
            .lock()
            .get(&conn)
            .is_some_and(|cs| !cs.remote_ip.is_empty())
    }

    fn is_auth_rate_limited(&self, remote_ip: &str) -> bool {
        let mut guard = self.auth_failures.lock();
        let now = Instant::now();
        let Some(entries) = guard.get_mut(remote_ip) else {
            return false;
        };
        entries.retain(|t| {
            now.checked_duration_since(*t)
                .is_none_or(|age| age < RTMP_AUTH_FAILURE_WINDOW)
        });
        if entries.is_empty() {
            guard.remove(remote_ip);
            return false;
        }
        entries.len() >= RTMP_AUTH_MAX_FAILURES
    }

    /// Drop every tracked IP whose failure window has fully expired, so
    /// one-off failures from many distinct IPs don't accumulate forever.
    fn purge_expired_auth_failures(guard: &mut HashMap<String, Vec<Instant>>, now: Instant) {
        guard.retain(|_, entries| {
            entries.retain(|t| {
                now.checked_duration_since(*t)
                    .is_none_or(|age| age < RTMP_AUTH_FAILURE_WINDOW)
            });
            !entries.is_empty()
        });
    }

    /// Remove the least-recently-active IP bucket when still at capacity
    /// after purging expired entries.
    fn evict_oldest_auth_failure_bucket(guard: &mut HashMap<String, Vec<Instant>>) {
        let Some(oldest_key) = guard
            .iter()
            .min_by_key(|(_, entries)| entries.last().copied().unwrap_or_else(Instant::now))
            .map(|(key, _)| key.clone())
        else {
            return;
        };
        guard.remove(&oldest_key);
    }

    fn record_auth_failure(&self, remote_ip: &str) {
        let mut guard = self.auth_failures.lock();
        let now = Instant::now();
        if !guard.contains_key(remote_ip) && guard.len() >= MAX_TRACKED_AUTH_FAILURE_KEYS {
            Self::purge_expired_auth_failures(&mut guard, now);
            if guard.len() >= MAX_TRACKED_AUTH_FAILURE_KEYS {
                Self::evict_oldest_auth_failure_bucket(&mut guard);
            }
        }
        let entries = guard.entry(remote_ip.to_string()).or_default();
        entries.retain(|t| {
            now.checked_duration_since(*t)
                .is_none_or(|age| age < RTMP_AUTH_FAILURE_WINDOW)
        });
        entries.push(now);
    }

    fn clear_auth_failures(&self, remote_ip: &str) {
        self.auth_failures.lock().remove(remote_ip);
    }

    /// Active RTMP connections still tied to a stream (publisher and/or player).
    pub fn live_conn_count_for_stream(&self, stream_id: &str) -> usize {
        self.conns
            .lock()
            .values()
            .filter(|cs| {
                cs.stream_id == stream_id && (cs.publisher.is_some() || cs.player.is_some())
            })
            .count()
    }

    fn restore_publisher_row(&self, old_pub: &Publisher) -> bool {
        let mut restored = old_pub.clone();
        restored.active = true;
        let old_id = restored.id.clone();
        if self.db.publisher_update(&old_id, &restored) {
            return true;
        }
        self.db.publisher_try_acquire(&restored)
    }

    fn restore_player_row(&self, old_player: &Player) -> bool {
        let mut restored = old_player.clone();
        restored.active = true;
        let prior_id = restored.id.clone();
        if self.db.player_update(&prior_id, &restored) {
            return true;
        }
        self.db.player_try_acquire(&restored)
    }

    /// Return the DB stream id for a publishing connection, or empty string.
    pub fn stream_id_for_conn(&self, conn: ConnId) -> String {
        self.conns
            .lock()
            .get(&conn)
            .map(|s| s.stream_id.clone())
            .unwrap_or_default()
    }

    /// Configured viewer slot id for an active player connection.
    pub fn viewer_id_for_conn(&self, conn: ConnId) -> String {
        self.conns
            .lock()
            .get(&conn)
            .map(|s| s.viewer_id.clone())
            .unwrap_or_default()
    }

    /// Whether this connection already owns an authorized player slot.
    pub fn has_player(&self, conn: ConnId) -> bool {
        self.conns
            .lock()
            .get(&conn)
            .map(|s| s.player.is_some())
            .unwrap_or(false)
    }

    /// Whether this connection already owns an authorized publisher slot.
    pub fn has_publisher(&self, conn: ConnId) -> bool {
        self.conns
            .lock()
            .get(&conn)
            .map(|s| s.publisher.is_some())
            .unwrap_or(false)
    }

    /// Update publisher stats (media bytes_in, bitrate, codec) in the DB.
    /// Called from the server poll loop after every poll iteration.
    pub fn update_publisher_stats(
        &self,
        conn: ConnId,
        media_bytes_received: u64,
        video_codec: &str,
        audio_codec: &str,
        metadata: PublisherStreamMetadata,
    ) {
        let mut guard = self.conns.lock();
        let Some(cs) = guard.get_mut(&conn) else {
            return;
        };
        let Some(ref mut pub_row) = cs.publisher else {
            return;
        };

        let now = Instant::now();
        if cs.publisher_stats_reset_pending {
            cs.publisher_stats_reset_pending = false;
            cs.publisher_bytes_base = media_bytes_received;
            cs.publisher_bytes_at_last_stats = 0;
            cs.publisher_last_stats_at = Some(now);

            pub_row.bytes_in = 0;
            pub_row.bitrate_kbps = 0.0;
            apply_publisher_codecs(pub_row, video_codec, audio_codec);
            apply_publisher_metadata(pub_row, metadata);

            let pub_id = pub_row.id.clone();
            let pub_row_clone = pub_row.clone();
            drop(guard);
            self.db.publisher_update(&pub_id, &pub_row_clone);
            return;
        }

        let elapsed_secs = cs
            .publisher_last_stats_at
            .map(|t| now.duration_since(t).as_secs_f64())
            .unwrap_or(0.0);
        let session_bytes = media_bytes_received.saturating_sub(cs.publisher_bytes_base);
        let bytes_delta = session_bytes.saturating_sub(cs.publisher_bytes_at_last_stats);

        // Only flush to DB if at least 1 second has passed (rate-limit writes).
        if elapsed_secs < 1.0 && cs.publisher_last_stats_at.is_some() {
            return;
        }

        let bitrate_kbps = if elapsed_secs > 0.0 {
            (bytes_delta as f64 * 8.0) / (elapsed_secs * 1000.0)
        } else {
            0.0
        };

        pub_row.bytes_in = session_bytes;
        pub_row.bitrate_kbps = bitrate_kbps;
        apply_publisher_codecs(pub_row, video_codec, audio_codec);
        apply_publisher_metadata(pub_row, metadata);

        cs.publisher_last_stats_at = Some(now);
        cs.publisher_bytes_at_last_stats = session_bytes;

        // Clone the row to release the lock before the DB call.
        let pub_id = pub_row.id.clone();
        let pub_row_clone = pub_row.clone();
        drop(guard);

        self.db.publisher_update(&pub_id, &pub_row_clone);
    }

    /// Update player stats (media bytes_out, bitrate) in the DB.
    pub fn update_player_stats(&self, conn: ConnId, media_bytes_sent: u64) {
        let mut guard = self.conns.lock();
        let Some(cs) = guard.get_mut(&conn) else {
            return;
        };
        let Some(ref mut player_row) = cs.player else {
            return;
        };

        let now = Instant::now();
        if cs.player_stats_reset_pending {
            cs.player_stats_reset_pending = false;
            cs.player_bytes_base = media_bytes_sent;
            cs.player_bytes_at_last_stats = 0;
            cs.player_last_stats_at = Some(now);

            player_row.bytes_out = 0;
            player_row.bitrate_kbps = 0.0;

            let player_id = player_row.id.clone();
            let row = player_row.clone();
            drop(guard);
            self.db.player_update(&player_id, &row);
            return;
        }

        let elapsed_secs = cs
            .player_last_stats_at
            .map(|t| now.duration_since(t).as_secs_f64())
            .unwrap_or(0.0);
        let session_bytes = media_bytes_sent.saturating_sub(cs.player_bytes_base);
        let bytes_delta = session_bytes.saturating_sub(cs.player_bytes_at_last_stats);

        if elapsed_secs < 1.0 && cs.player_last_stats_at.is_some() {
            return;
        }

        let bitrate_kbps = if elapsed_secs > 0.0 {
            (bytes_delta as f64 * 8.0) / (elapsed_secs * 1000.0)
        } else {
            0.0
        };

        player_row.bytes_out = session_bytes;
        player_row.bitrate_kbps = bitrate_kbps;
        cs.player_last_stats_at = Some(now);
        cs.player_bytes_at_last_stats = session_bytes;

        let player_id = player_row.id.clone();
        let row = player_row.clone();
        drop(guard);

        self.db.player_update(&player_id, &row);
    }

    /// Persist the latest measured client↔server RTT for this connection.
    pub fn update_rtt(&self, conn: ConnId, rtt_ms: f64) {
        if !rtt_ms.is_finite() || rtt_ms <= 0.0 {
            return;
        }

        let mut guard = self.conns.lock();
        let Some(cs) = guard.get_mut(&conn) else {
            return;
        };

        let now = Instant::now();
        let elapsed_secs = cs
            .last_rtt_at
            .map(|t| now.duration_since(t).as_secs_f64())
            .unwrap_or(f64::INFINITY);
        if elapsed_secs < 1.0 && cs.last_rtt_at.is_some() {
            return;
        }

        if let Some(ref mut pub_row) = cs.publisher {
            pub_row.rtt_ms = rtt_ms;
            cs.last_rtt_at = Some(now);
            let pub_id = pub_row.id.clone();
            let row = pub_row.clone();
            drop(guard);
            self.db.publisher_update(&pub_id, &row);
            return;
        }

        if let Some(ref mut player_row) = cs.player {
            player_row.rtt_ms = rtt_ms;
            cs.last_rtt_at = Some(now);
            let player_id = player_row.id.clone();
            let row = player_row.clone();
            drop(guard);
            self.db.player_update(&player_id, &row);
        }
    }

    fn try_authorize_publish(
        &self,
        conn: ConnId,
        app: &str,
        stream_key: &str,
    ) -> Result<(), AuthFailureKind> {
        crate::log_info!("RTMP: publish request app='{app}' key=<redacted>");

        // Look up ignoring `enabled` so a valid key for a disabled/pending-delete
        // stream is classified as an operational rejection below, not a
        // credential mismatch — otherwise a publisher retrying against its own
        // just-disabled stream would burn the shared per-IP auth-failure budget.
        let DbLookup::Ok(stream) = self.db.stream_find_by_publish_key_any(stream_key) else {
            crate::log_warn!("RTMP: publish rejected — invalid publish_key for app='{app}'");
            return Err(AuthFailureKind::Credential);
        };
        if !stream.enabled {
            crate::log_warn!(
                "RTMP: publish rejected — stream '{}' is disabled",
                stream.id
            );
            return Err(AuthFailureKind::Operational);
        }
        if self.deleted_streams.lock().contains(&stream.id) {
            crate::log_warn!(
                "RTMP: publish rejected — stream '{}' is being deleted",
                stream.id
            );
            return Err(AuthFailureKind::Operational);
        }
        if stream.app != app {
            crate::log_warn!(
                "RTMP: publish rejected — key belongs to app='{}', requested app='{app}'",
                stream.app
            );
            return Err(AuthFailureKind::Credential);
        }

        let pub_id = match keygen::keygen_stream_key(PREFIX_PUBLISH_KEY) {
            Ok(id) => id,
            Err(e) => {
                crate::log_warn!("RTMP: publish rejected — session id generation failed: {e}");
                return Err(AuthFailureKind::Operational);
            }
        };

        let pub_row = Publisher {
            id: pub_id,
            stream_id: stream.id.clone(),
            app: app.to_string(),
            stream_name: stream.name.clone(),
            active: true,
            connected_at: crate::db::now_ts(),
            ..Default::default()
        };

        let old_pub = {
            let guard = self.conns.lock();
            guard.get(&conn).and_then(|cs| cs.publisher.clone())
        };
        let replacing_publisher = old_pub.is_some();
        if let Some(mut prior) = old_pub.clone() {
            prior.active = false;
            let prior_id = prior.id.clone();
            if !self.db.publisher_update(&prior_id, &prior) {
                crate::log_warn!(
                    "RTMP: publish rejected — failed to deactivate prior publisher row"
                );
                return Err(AuthFailureKind::Operational);
            }
        }

        if !self.db.publisher_try_acquire(&pub_row) {
            if let Some(ref prior) = old_pub
                && !self.restore_publisher_row(prior)
            {
                crate::log_error!(
                    "RTMP: publish rollback failed — prior publisher row remains inactive"
                );
            }
            crate::log_warn!(
                "RTMP: publish rejected — stream '{}' already has an active publisher",
                stream.id
            );
            return Err(AuthFailureKind::Operational);
        }

        let stream_id = stream.id.clone();

        let mut guard = self.conns.lock();
        let cs = guard.entry(conn).or_default();
        cs.publisher = Some(pub_row);
        cs.stream_id = stream_id;
        if replacing_publisher {
            cs.publisher_last_stats_at = None;
            cs.publisher_bytes_at_last_stats = 0;
            cs.publisher_stats_reset_pending = true;
        }

        crate::log_info!(
            "RTMP: publish authorized stream='{}' publisher session={}",
            stream.id,
            cs.publisher.as_ref().map(|p| p.id.as_str()).unwrap_or("")
        );
        Ok(())
    }

    fn try_authorize_play(
        &self,
        conn: ConnId,
        app: &str,
        stream_key: &str,
    ) -> Result<(), AuthFailureKind> {
        crate::log_info!("RTMP: play request app='{app}' key=<redacted>");

        let DbLookup::Ok(viewer) = self.db.viewer_find_by_play_key(stream_key) else {
            crate::log_warn!("RTMP: play rejected — invalid play_key for app='{app}'");
            return Err(AuthFailureKind::Credential);
        };
        let DbLookup::Ok(stream) = self.db.stream_get(&viewer.stream_id) else {
            crate::log_warn!("RTMP: play rejected — stream missing for play_key");
            return Err(AuthFailureKind::Operational);
        };
        if self.deleted_streams.lock().contains(&stream.id) {
            crate::log_warn!(
                "RTMP: play rejected — stream '{}' is being deleted",
                stream.id
            );
            return Err(AuthFailureKind::Operational);
        }
        if !stream.enabled {
            crate::log_warn!("RTMP: play rejected — stream '{}' is disabled", stream.id);
            return Err(AuthFailureKind::Operational);
        }
        if stream.app != app {
            crate::log_warn!(
                "RTMP: play rejected — key belongs to app='{}', requested app='{app}'",
                stream.app
            );
            return Err(AuthFailureKind::Credential);
        }

        let player_id = match keygen::keygen_stream_key(PREFIX_PLAY_KEY) {
            Ok(id) => id,
            Err(e) => {
                crate::log_warn!("RTMP: play rejected — session id generation failed: {e}");
                return Err(AuthFailureKind::Operational);
            }
        };

        let player_row = Player {
            id: player_id,
            stream_id: stream.id.clone(),
            viewer_id: viewer.id.clone(),
            app: app.to_string(),
            stream_name: stream.name.clone(),
            active: true,
            connected_at: crate::db::now_ts(),
            ..Default::default()
        };

        let old_player = {
            let guard = self.conns.lock();
            guard.get(&conn).and_then(|cs| cs.player.clone())
        };
        if let Some(mut prior) = old_player.clone() {
            prior.active = false;
            let prior_id = prior.id.clone();
            if !self.db.player_update(&prior_id, &prior) {
                crate::log_warn!("RTMP: play rejected — failed to deactivate prior player row");
                return Err(AuthFailureKind::Operational);
            }
        }

        if !self.db.player_try_acquire(&player_row) {
            if let Some(ref prior) = old_player
                && !self.restore_player_row(prior)
            {
                crate::log_error!("RTMP: play rollback failed — prior player row not restored");
            }
            crate::log_warn!(
                "RTMP: play rejected — connection limit ({}) reached for play key",
                crate::db::MAX_CONNECTIONS_PER_PLAY_KEY
            );
            return Err(AuthFailureKind::Operational);
        }

        let player_id = player_row.id.clone();
        let stream_id = stream.id.clone();
        let viewer_id = viewer.id.clone();
        let replacing_player = old_player.is_some();

        {
            let mut guard = self.conns.lock();
            let cs = guard.entry(conn).or_default();
            cs.player = Some(player_row);
            cs.viewer_id = viewer_id;
            if cs.publisher.is_none() || cs.stream_id.is_empty() {
                cs.stream_id = stream_id;
            }
            if replacing_player {
                cs.player_last_stats_at = None;
                cs.player_bytes_at_last_stats = 0;
                cs.player_stats_reset_pending = true;
            }
        }

        crate::log_info!(
            "RTMP: play accepted stream='{}' player session={player_id}",
            stream.id
        );
        Ok(())
    }
}

impl RtmpEventHandler for DbRtmpBridge {
    fn on_connect(&self, conn: ConnId, remote_addr: &str) {
        // Use entry(...).or_default() so a publish/play callback that already ran
        // during the same poll() tick keeps its ConnState — insert() would wipe
        // an authorized publisher/player and leave a ghost active row in the DB.
        let mut conns = self.conns.lock();
        let cs = conns.entry(conn).or_default();
        cs.remote_ip = remote_ip_of(remote_addr);
        drop(conns);
        crate::log_debug!("RTMP: new connection {conn}");
    }

    fn authorize_publish(&self, conn: ConnId, app: &str, stream_key: &str) -> Result<(), ()> {
        let remote_ip = self
            .conns
            .lock()
            .get(&conn)
            .map(|cs| cs.remote_ip.clone())
            .unwrap_or_default();
        // Auth throttling needs a real per-client key. If authorize_publish
        // runs before on_connect has populated ConnState (supported by the
        // same-tick race noted in on_connect), remote_ip is empty — skip
        // throttling entirely rather than share a "" bucket across unrelated
        // connections, which would let one attacker lock out everyone else.
        let ip_known = !remote_ip.is_empty();
        if ip_known && self.is_auth_rate_limited(&remote_ip) {
            crate::log_warn!("RTMP: publish rejected — auth rate limit exceeded conn={conn}");
            return Err(());
        }
        match self.try_authorize_publish(conn, app, stream_key) {
            Ok(()) => {
                if ip_known {
                    self.clear_auth_failures(&remote_ip);
                }
                Ok(())
            }
            Err(kind) => {
                if ip_known && kind == AuthFailureKind::Credential {
                    self.record_auth_failure(&remote_ip);
                }
                Err(())
            }
        }
    }

    fn authorize_play(&self, conn: ConnId, app: &str, stream_key: &str) -> Result<(), ()> {
        let remote_ip = self
            .conns
            .lock()
            .get(&conn)
            .map(|cs| cs.remote_ip.clone())
            .unwrap_or_default();
        let ip_known = !remote_ip.is_empty();
        if ip_known && self.is_auth_rate_limited(&remote_ip) {
            crate::log_warn!("RTMP: play rejected — auth rate limit exceeded conn={conn}");
            return Err(());
        }
        match self.try_authorize_play(conn, app, stream_key) {
            Ok(()) => {
                if ip_known {
                    self.clear_auth_failures(&remote_ip);
                }
                Ok(())
            }
            Err(kind) => {
                if ip_known && kind == AuthFailureKind::Credential {
                    self.record_auth_failure(&remote_ip);
                }
                Err(())
            }
        }
    }

    fn on_frame(&self, conn: ConnId, frame: &FrameInfo) -> bool {
        let _ = conn;
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
        true
    }

    fn on_close(&self, conn: ConnId) {
        // Deliberately do NOT clear auth_failures here: it is keyed by remote
        // IP (not ConnId) precisely so a client can't reset the brute-force
        // window by reconnecting. Entries expire naturally via the sliding
        // window in is_auth_rate_limited/record_auth_failure.
        let cs = self.conns.lock().remove(&conn);
        let Some(cs) = cs else {
            crate::log_warn!("RTMP: on_close for untracked connection {conn}");
            return;
        };

        if let Some(mut pub_row) = cs.publisher {
            pub_row.active = false;
            self.db.publisher_update(&pub_row.id, &pub_row);
            crate::log_info!(
                "RTMP: publisher disconnected: stream={} session={}",
                pub_row.stream_id,
                pub_row.id
            );
        }

        if let Some(mut player_row) = cs.player {
            player_row.active = false;
            self.db.player_update(&player_row.id, &player_row);
            crate::log_info!(
                "RTMP: player disconnected: stream={} session={}",
                player_row.stream_id,
                player_row.id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn test_bridge(db: Arc<Db>) -> DbRtmpBridge {
        DbRtmpBridge::new(db, Arc::new(Mutex::new(HashSet::new())))
    }

    fn sample_stream(id: &str, pub_key: &str, play_key: &str) -> crate::db::Stream {
        crate::db::Stream {
            id: id.to_string(),
            name: format!("{id} name"),
            app: "live".to_string(),
            publish_key: pub_key.to_string(),
            play_key: play_key.to_string(),
            stats_key: format!("st_{id}"),
            enabled: true,
            created_at: crate::db::now_ts(),
        }
    }

    fn add_stream_with_player(db: &Db, id: &str, pub_key: &str, play_key: &str) {
        db.stream_add(&sample_stream(id, pub_key, play_key))
            .unwrap();
    }

    #[test]
    fn publish_failures_count_toward_auth_rate_limit_when_remote_ip_known() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let bridge = test_bridge(db);
        let ip = "203.0.113.7:1935";

        for conn in 0..RTMP_AUTH_MAX_FAILURES as u64 {
            bridge.on_connect(conn, ip);
            assert!(bridge.authorize_publish(conn, "live", "bogus").is_err());
        }

        bridge.on_connect(RTMP_AUTH_MAX_FAILURES as u64, ip);
        assert!(bridge.is_auth_rate_limited(&remote_ip_of(ip)));
    }

    #[test]
    fn publish_before_on_connect_skips_auth_failure_tracking() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let bridge = test_bridge(db);
        let ip = "203.0.113.7:1935";

        for conn in 0..(RTMP_AUTH_MAX_FAILURES as u64 + 2) {
            assert!(bridge.authorize_publish(conn, "live", "bogus").is_err());
            bridge.on_connect(conn, ip);
        }

        assert!(
            !bridge.is_auth_rate_limited(&remote_ip_of(ip)),
            "failed publish attempts before on_connect must not consume the per-IP auth budget"
        );
    }

    #[test]
    fn auth_rate_limit_survives_reconnect_from_same_ip() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let bridge = test_bridge(db);
        let ip = "203.0.113.7:1935";

        // Exhaust the failure budget across many short-lived connections from
        // the same client IP, each closed after its failed attempt.
        for conn in 0..RTMP_AUTH_MAX_FAILURES as u64 {
            bridge.on_connect(conn, ip);
            assert!(bridge.authorize_publish(conn, "live", "bogus").is_err());
            bridge.on_close(conn);
        }

        // A fresh connection from the same IP must still be throttled — the
        // window must not reset just because each prior connection closed.
        // Assert the throttle directly (not just an Err from a bogus key,
        // which would fail either way) to actually prove rate limiting.
        let next_conn = RTMP_AUTH_MAX_FAILURES as u64;
        bridge.on_connect(next_conn, ip);
        assert!(bridge.is_auth_rate_limited(&remote_ip_of(ip)));
        assert!(
            bridge
                .authorize_publish(next_conn, "live", "bogus")
                .is_err()
        );

        // A different client IP is unaffected.
        let other_ip = "198.51.100.1:1935";
        bridge.on_connect(next_conn + 1, other_ip);
        assert!(!bridge.is_auth_rate_limited(&remote_ip_of(other_ip)));
        assert!(
            bridge
                .authorize_publish(next_conn + 1, "live", "bogus")
                .is_err()
        );
    }

    #[test]
    fn publish_with_valid_key_for_disabled_stream_does_not_burn_auth_budget() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k", "pl_k");
        assert!(db.stream_disable("s1").unwrap());
        let bridge = test_bridge(db);
        let ip = "203.0.113.8:1935";

        // A valid key for a disabled/pending-delete stream must be rejected
        // as an operational failure, not a credential mismatch — otherwise
        // it would consume the shared per-IP auth-failure budget just like
        // a brute-force guess would.
        for conn in 0..(RTMP_AUTH_MAX_FAILURES as u64 + 2) {
            bridge.on_connect(conn, ip);
            assert!(bridge.authorize_publish(conn, "live", "pub_k").is_err());
            bridge.on_close(conn);
        }

        assert!(!bridge.is_auth_rate_limited(&remote_ip_of(ip)));
    }

    #[test]
    fn publish_rejects_unknown_key() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let bridge = test_bridge(db);
        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", "bogus").is_err());
    }

    #[test]
    fn publish_rejects_key_for_wrong_app() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k", "pl_k");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "other", "pub_k").is_err());
        assert_eq!(db.publisher_list(Some("s1")).len(), 0);
    }

    #[test]
    fn play_rejects_key_for_wrong_app() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k", "pl_k");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_play(1, "other", "pl_k").is_err());
        assert_eq!(db.player_list(Some("s1")).len(), 0);
    }

    #[test]
    fn publish_then_close_deactivates_publisher() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k", "pl_k");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", "pub_k").is_ok());
        assert_eq!(db.publisher_list(Some("s1")).len(), 1);

        bridge.on_close(1);
        assert_eq!(db.publisher_list(Some("s1")).len(), 0);
    }

    #[test]
    fn close_only_affects_its_own_connection() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k1", "pl_k1");
        add_stream_with_player(&db, "s2", "pub_k2", "pl_k2");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        bridge.on_connect(2, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", "pub_k1").is_ok());
        assert!(bridge.authorize_publish(2, "live", "pub_k2").is_ok());

        bridge.on_close(1);
        assert_eq!(db.publisher_list(Some("s1")).len(), 0);
        assert_eq!(db.publisher_list(Some("s2")).len(), 1);
    }

    #[test]
    fn authorize_publish_rejects_second_publisher() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k", "pl_k");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", "pub_k").is_ok());

        bridge.on_connect(2, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(2, "live", "pub_k").is_err());
    }

    #[test]
    fn on_connect_preserves_prior_authorize_publish_state() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k", "pl_k");
        let bridge = test_bridge(Arc::clone(&db));

        // publish callback can run during poll() before the poll-loop on_connect.
        assert!(bridge.authorize_publish(1, "live", "pub_k").is_ok());
        bridge.on_connect(1, "127.0.0.1:1000");

        assert!(bridge.has_publisher(1));
        assert_eq!(db.publisher_list(Some("s1")).len(), 1);
    }

    #[test]
    fn is_registered_reflects_on_connect_state() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let bridge = test_bridge(db);

        assert!(!bridge.is_registered(7));
        bridge.on_connect(7, "127.0.0.1:1000");
        assert!(bridge.is_registered(7));
    }

    #[test]
    fn authorize_publish_switching_streams_deactivates_prior_publisher() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k1", "pl_k1");
        add_stream_with_player(&db, "s2", "pub_k2", "pl_k2");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", "pub_k1").is_ok());
        assert!(bridge.authorize_publish(1, "live", "pub_k2").is_ok());

        assert_eq!(db.publisher_list(Some("s1")).len(), 0);
        assert_eq!(db.publisher_list(Some("s2")).len(), 1);
    }

    #[test]
    fn authorize_publish_failed_switch_keeps_prior_publisher_active() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k1", "pl_k1");
        add_stream_with_player(&db, "s2", "pub_k2", "pl_k2");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        bridge.on_connect(2, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", "pub_k1").is_ok());
        assert!(bridge.authorize_publish(2, "live", "pub_k2").is_ok());

        assert!(bridge.authorize_publish(1, "live", "pub_k2").is_err());

        assert!(bridge.has_publisher(1));
        assert_eq!(db.publisher_list(Some("s1")).len(), 1);
        assert_eq!(db.publisher_list(Some("s2")).len(), 1);
    }

    #[test]
    fn on_play_switching_streams_deactivates_prior_player() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k1", "pl_k1");
        add_stream_with_player(&db, "s2", "pub_k2", "pl_k2");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_play(1, "live", "pl_k1").is_ok());
        assert!(bridge.authorize_play(1, "live", "pl_k2").is_ok());

        assert_eq!(db.player_list(Some("s1")).len(), 0);
        assert_eq!(db.player_list(Some("s2")).len(), 1);

        let guard = bridge.conns.lock();
        assert_eq!(guard.get(&1).unwrap().stream_id.as_str(), "s2");
    }

    #[test]
    fn player_replacement_stats_reset_is_not_consumed_by_publisher_stats() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k1", "pl_k1");
        add_stream_with_player(&db, "s2", "pub_k2", "pl_k2");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", "pub_k1").is_ok());
        assert!(bridge.authorize_play(1, "live", "pl_k1").is_ok());
        bridge.update_publisher_stats(1, 1_000, "avc1", "mp4a", PublisherStreamMetadata::default());
        bridge.update_player_stats(1, 2_000);

        assert!(bridge.authorize_play(1, "live", "pl_k2").is_ok());
        bridge.update_publisher_stats(1, 1_500, "avc1", "mp4a", PublisherStreamMetadata::default());

        {
            let guard = bridge.conns.lock();
            let cs = guard.get(&1).unwrap();
            assert!(!cs.publisher_stats_reset_pending);
            assert!(cs.player_stats_reset_pending);
        }

        bridge.update_player_stats(1, 2_500);
        let players = db.player_list(Some("s2"));
        assert_eq!(players.len(), 1);
        assert_eq!(players[0].bytes_out, 0);
    }

    #[test]
    fn update_player_stats_persists_bytes_out() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k", "pl_k");
        let bridge = test_bridge(Arc::clone(&db));

        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_play(1, "live", "pl_k").is_ok());
        bridge.update_player_stats(1, 4096);

        let players = db.player_list(Some("s1"));
        assert_eq!(players.len(), 1);
        assert_eq!(players[0].bytes_out, 4096);
    }

    #[test]
    fn play_rejects_when_connection_cap_reached() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream_with_player(&db, "s1", "pub_k", "pl_k");
        let bridge = test_bridge(Arc::clone(&db));
        let DbLookup::Ok(viewer) = db.viewer_find_by_play_key("pl_k") else {
            panic!("viewer not found");
        };

        for conn in 1..=crate::db::MAX_CONNECTIONS_PER_PLAY_KEY as u64 {
            bridge.on_connect(conn, "127.0.0.1:1000");
            assert!(bridge.authorize_play(conn, "live", "pl_k").is_ok());
        }

        bridge.on_connect(99, "127.0.0.1:1000");
        assert!(bridge.authorize_play(99, "live", "pl_k").is_err());
        assert_eq!(
            db.player_list(Some(&viewer.stream_id))
                .iter()
                .filter(|p| p.viewer_id == viewer.id && p.active)
                .count(),
            crate::db::MAX_CONNECTIONS_PER_PLAY_KEY
        );
    }
}
