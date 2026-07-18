//! Server application lifecycle: wires together the database, the HTTP API,
//! and the RTMP listener(s), then runs until a shutdown signal arrives.

use parking_lot::Mutex;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

use crate::config::ServerConfig;
use crate::db::Db;
use crate::http::{self, AppState};
use crate::rtmp_bridge::{DbRtmpBridge, FrameInfo, FrameKind, RtmpEventHandler};

/// RTMP publish/play callbacks are plain function pointers; the bridge is
/// registered on the RTMP thread before the poll loop starts.
pub(crate) static RTMP_BRIDGE: StdMutex<Option<Arc<DbRtmpBridge>>> = StdMutex::new(None);

thread_local! {
    static RTMP_POLL_SERVER: Cell<Option<*mut librtmp2::server::Server>> = const {
        Cell::new(None)
    };
}

/// Pin the active RTMP server for the duration of `poll()` so publish/play
/// callbacks can resolve `remote_addr` before auth rate limiting.
pub(crate) fn set_rtmp_poll_server(server: *mut librtmp2::server::Server) {
    RTMP_POLL_SERVER.with(|cell| cell.set(Some(server)));
}

pub(crate) fn clear_rtmp_poll_server() {
    RTMP_POLL_SERVER.with(|cell| cell.set(None));
}

/// How often the poll loop wakes up to service the RTMP/RTMPS listener(s).
pub(crate) const POLL_INTERVAL_MS: u64 = 50;

/// Normalize a bind string so passing it to librtmp2 cannot fall back to the
/// RTMP library default port. In particular, RTMPS host-only binds such as
/// `0.0.0.0`, `::1`, or `[::1]` must be listened on 1936, not librtmp2's
/// generic RTMP default of 1935.
fn bind_with_default_port(bind: &str, default_port: u16) -> String {
    let bind = bind.trim();

    if let Some(bracket_end) = bind.rfind(']') {
        let suffix = &bind[bracket_end + 1..];
        if suffix
            .strip_prefix(':')
            .and_then(|port| port.parse::<u16>().ok())
            .is_some()
        {
            return bind.to_string();
        }
        return format!("{}:{default_port}", &bind[..=bracket_end]);
    }

    let colon_count = bind.chars().filter(|&c| c == ':').count();
    match colon_count {
        0 => format!("{bind}:{default_port}"),
        1 => match bind.rsplit_once(':') {
            Some((host, port)) if port.parse::<u16>().is_ok() => format!("{host}:{port}"),
            Some((host, _)) => format!("{host}:{default_port}"),
            None => format!("{bind}:{default_port}"),
        },
        _ => format!("[{bind}]:{default_port}"),
    }
}

fn with_rtmp_bridge<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&DbRtmpBridge) -> R,
{
    match RTMP_BRIDGE.lock() {
        Ok(guard) => guard.as_ref().map(|bridge| f(bridge.as_ref())),
        Err(e) => {
            crate::log_error!("RTMP_BRIDGE lock poisoned; rejecting RTMP callback: {e}");
            None
        }
    }
}

/// Register the client IP on the bridge before publish/play auth runs. During
/// `server.poll()` the publish/play callbacks can fire before
/// `process_server_connections` reaches `on_connect`, which would otherwise
/// skip per-IP auth-failure tracking and rate limiting.
fn ensure_conn_registered_for_auth(conn_id: u64) {
    // Skip the pointer walk entirely once the normal `on_connect` pass (or an
    // earlier call from this same function) has already registered the
    // remote IP. Without this check every publish/play attempt on an
    // already-registered connection re-ran `on_connect` and its "new
    // connection" log line, which was both misleading and needless lock
    // contention on the hot path.
    if with_rtmp_bridge(|bridge| bridge.is_registered(conn_id)).unwrap_or(true) {
        return;
    }
    RTMP_POLL_SERVER.with(|cell| {
        let Some(server_ptr) = cell.get() else {
            return;
        };
        if server_ptr.is_null() {
            return;
        }
        // SAFETY: `RTMP_POLL_SERVER` is set only on the RTMP thread for the
        // duration of `server.poll()`, which exclusively owns `server`.
        let server = unsafe { &*server_ptr };
        let Some(conn) = server
            .connections
            .iter()
            .find(|c| c.conn_id == conn_id && c.client_fd >= 0)
        else {
            return;
        };
        with_rtmp_bridge(|bridge| bridge.on_connect(conn_id, &conn.remote_addr));
    });
}

pub(crate) fn rtmp_publish_cb(conn_id: u64, app: &str, stream_key: &str) -> bool {
    ensure_conn_registered_for_auth(conn_id);
    with_rtmp_bridge(|b| b.authorize_publish(conn_id, app, stream_key).is_ok()).unwrap_or(false)
}

pub(crate) fn rtmp_play_cb(conn_id: u64, app: &str, play_key: &str) -> bool {
    ensure_conn_registered_for_auth(conn_id);
    with_rtmp_bridge(|b| b.authorize_play(conn_id, app, play_key).is_ok()).unwrap_or(false)
}

pub(crate) fn rtmp_media_cb(
    conn_id: u64,
    frame_type: librtmp2::types::FrameType,
    codec: Option<&str>,
) -> bool {
    ensure_conn_registered_for_auth(conn_id);
    with_rtmp_bridge(|bridge| {
        let kind = match frame_type {
            librtmp2::types::FrameType::Video => FrameKind::Video,
            librtmp2::types::FrameType::Audio => FrameKind::Audio,
            _ => return true,
        };
        let frame = FrameInfo {
            kind,
            timestamp: 0,
            size: 0,
            codec: codec.unwrap_or("").to_string(),
        };
        bridge.on_frame(conn_id, &frame)
    })
    .unwrap_or(false)
}

/// Per-connection bookkeeping the RTMP poll loop keeps for the lifetime of
/// each connection.
#[derive(Default)]
pub(crate) struct TrackedConn {
    connected: bool,
    publishing: bool,
    playing: bool,
    /// When this connection was first observed (used for pre-auth idle eviction).
    first_seen_at: Option<Instant>,
    /// DB stream id, set after publish/play is fully enabled.
    stream_id: String,
    /// Last detected video codec string from the protocol layer.
    video_codec: String,
    /// Last detected audio codec string from the protocol layer.
    audio_codec: String,
}

/// Returns true when a connection has no authorized publish/play session and
/// has exceeded the configured pre-auth idle window.
fn should_evict_idle_conn(
    entry: &TrackedConn,
    has_authorized_session: bool,
    now: Instant,
    idle_timeout: Duration,
) -> bool {
    if entry.publishing || entry.playing || has_authorized_session {
        return false;
    }
    entry
        .first_seen_at
        .is_some_and(|at| now.duration_since(at) >= idle_timeout)
}

/// Drive one poll cycle's worth of connection bookkeeping: authorize new
/// publish/play commands, reject connections the bridge doesn't own, kick
/// connections whose stream/play-key was revoked, and flush stats. Returns
/// the conn_ids seen this cycle so the caller can detect connections that
/// disappeared entirely (closed by the peer, rather than rejected here).
///
/// `server` is a single `librtmp2::server::Server` that may have multiple
/// listeners bound (plaintext RTMP and, when TLS is enabled, RTMPS) — they
/// share one `connections` list, so a publisher on one listener is relayed
/// to players on any other listener by the library itself; this function
/// doesn't need to know which listener a given connection came in on.
pub(crate) fn process_server_connections(
    server: &mut librtmp2::server::Server,
    tracked: &mut HashMap<u64, TrackedConn>,
    rtmp_bridge: &Arc<DbRtmpBridge>,
    deleted_now: &HashSet<String>,
    revoked_now: &HashSet<String>,
    idle_timeout: Duration,
) -> HashSet<u64> {
    let mut current_ids = HashSet::new();
    let mut reject_indices = Vec::new();

    for (idx, conn) in server.connections.iter_mut().enumerate() {
        if conn.client_fd < 0 {
            continue;
        }
        let conn_id = conn.conn_id;
        current_ids.insert(conn_id);
        let entry = tracked.entry(conn_id).or_default();
        if !entry.connected {
            // A publish/play callback may have already run `on_connect` via
            // `ensure_conn_registered_for_auth` earlier this same poll tick;
            // skip the redundant call so the connection isn't logged twice.
            if !rtmp_bridge.is_registered(conn_id) {
                rtmp_bridge.on_connect(conn_id, &conn.remote_addr);
            }
            entry.connected = true;
            entry.first_seen_at = Some(Instant::now());
        } else if entry.first_seen_at.is_none() {
            entry.first_seen_at = Some(Instant::now());
        }

        let has_authorized_session =
            rtmp_bridge.has_publisher(conn_id) || rtmp_bridge.has_player(conn_id);
        if should_evict_idle_conn(entry, has_authorized_session, Instant::now(), idle_timeout) {
            crate::log_info!(
                "RTMP: closing idle conn={conn_id} from {} (no publish/play within {}s)",
                conn.remote_addr,
                idle_timeout.as_secs()
            );
            reject_indices.push(idx);
            continue;
        }

        let Some(stream) = conn.current_stream.as_ref() else {
            continue;
        };
        let is_publishing = stream.is_publishing;
        let is_playing = stream.is_playing;

        // Tear down bridge roles when the RTMP session drops publish/play
        // without closing TCP (FCUnpublish / closeStream / role switch).
        if entry.publishing && !is_publishing {
            rtmp_bridge.release_publisher(conn_id);
            // If the DB deactivation failed, release_publisher keeps the
            // row in ConnState for a retry on close -- keep tracking this
            // connection as the active publisher too, so idle eviction
            // doesn't reclaim it while the still-active row blocks others.
            if !rtmp_bridge.has_publisher(conn_id) {
                entry.publishing = false;
                // A future publish session on this connection should start
                // codec detection fresh rather than reporting the just-ended
                // stream's codecs until new detection overwrites them.
                entry.video_codec.clear();
                entry.audio_codec.clear();
                if !is_playing {
                    entry.stream_id.clear();
                    conn.relay_key.clear();
                    conn.relay_enabled = false;
                    conn.pending_relay.clear();
                    // No role survives this teardown -- restart the idle-eviction
                    // window so a client that FCUnpublish'd intending to
                    // republish shortly isn't judged against a first_seen_at
                    // from the original (possibly long-past) TCP connect.
                    entry.first_seen_at = Some(Instant::now());
                } else {
                    let sid = rtmp_bridge.stream_id_for_conn(conn_id);
                    entry.stream_id = sid.clone();
                    conn.relay_key = sid;
                    conn.relay_enabled = true;
                }
            }
        }
        if entry.playing && !is_playing {
            rtmp_bridge.release_player(conn_id);
            if !rtmp_bridge.has_player(conn_id) {
                entry.playing = false;
                if !is_publishing {
                    entry.stream_id.clear();
                    conn.relay_key.clear();
                    conn.relay_enabled = false;
                    conn.pending_relay.clear();
                    entry.first_seen_at = Some(Instant::now());
                } else {
                    let sid = rtmp_bridge.stream_id_for_conn(conn_id);
                    entry.stream_id = sid.clone();
                    conn.relay_key = sid;
                    conn.relay_enabled = true;
                }
            }
        }

        if is_publishing && !entry.publishing {
            if !rtmp_bridge.has_publisher(conn_id) {
                crate::log_warn!(
                    "RTMP: closing unauthorized publisher conn={conn_id} from {} app='{}' key=<redacted>",
                    conn.remote_addr,
                    conn.app
                );
                reject_indices.push(idx);
                continue;
            }
            crate::log_info!(
                "RTMP: publisher connected from {} stream='{}'",
                conn.remote_addr,
                rtmp_bridge.stream_id_for_conn(conn_id)
            );
            entry.publishing = true;
            entry.stream_id = rtmp_bridge.stream_id_for_conn(conn_id);
            conn.relay_key = entry.stream_id.clone();
            conn.relay_enabled = true;
        } else if is_publishing && entry.publishing {
            // Same-connection stream switch: bridge already moved the
            // publisher row; keep relay_key / kick targets in sync.
            let sid = rtmp_bridge.stream_id_for_conn(conn_id);
            if !sid.is_empty() && sid != entry.stream_id {
                entry.stream_id = sid.clone();
                conn.relay_key = sid;
            }
        }

        if is_playing && !entry.playing {
            if !rtmp_bridge.has_player(conn_id) {
                crate::log_warn!(
                    "RTMP: closing unauthorized player conn={conn_id} from {} app='{}' key=<redacted>",
                    conn.remote_addr,
                    conn.app
                );
                reject_indices.push(idx);
                continue;
            }
            crate::log_info!(
                "RTMP: player connected from {} stream='{}'",
                conn.remote_addr,
                rtmp_bridge.stream_id_for_conn(conn_id)
            );
            entry.playing = true;
            entry.stream_id = rtmp_bridge.stream_id_for_conn(conn_id);
            conn.relay_key = entry.stream_id.clone();
            conn.relay_enabled = true;
        } else if is_playing && entry.playing {
            let sid = rtmp_bridge.stream_id_for_conn(conn_id);
            if !sid.is_empty() && sid != entry.stream_id {
                entry.stream_id = sid.clone();
                conn.relay_key = sid;
            }
        }

        // Kick connections whose stream was deleted.
        if !entry.stream_id.is_empty() && deleted_now.contains(&entry.stream_id) {
            crate::log_info!(
                "RTMP: kicking conn={conn_id} from {} — stream '{}' was deleted",
                conn.remote_addr,
                entry.stream_id
            );
            reject_indices.push(idx);
            continue;
        }

        let viewer_id = rtmp_bridge.viewer_id_for_conn(conn_id);
        if !viewer_id.is_empty() && revoked_now.contains(&viewer_id) {
            crate::log_info!(
                "RTMP: kicking conn={conn_id} from {} — play key '{viewer_id}' was revoked",
                conn.remote_addr
            );
            reject_indices.push(idx);
            continue;
        }

        // Publisher stats: media bytes only (excludes RTMP control overhead).
        if is_publishing {
            let new_video = conn
                .detected_video_codec
                .as_deref()
                .unwrap_or("")
                .to_string();
            let new_audio = conn
                .detected_audio_codec
                .as_deref()
                .unwrap_or("")
                .to_string();

            if !new_video.is_empty() && new_video != entry.video_codec {
                entry.video_codec = new_video;
            }
            if !new_audio.is_empty() && new_audio != entry.audio_codec {
                entry.audio_codec = new_audio;
            }

            rtmp_bridge.update_publisher_stats(
                conn_id,
                conn.media_bytes_received,
                &entry.video_codec,
                &entry.audio_codec,
                crate::rtmp_bridge::PublisherStreamMetadata {
                    video_width: conn.detected_video_width,
                    video_height: conn.detected_video_height,
                    framerate: conn.detected_video_framerate,
                    audio_sample_rate: conn.detected_audio_sample_rate,
                    audio_channels: conn.detected_audio_channels,
                },
            );
        }

        if is_playing {
            rtmp_bridge.update_player_stats(conn_id, conn.media_bytes_sent);
        }
    }

    for conn in server.connections.iter() {
        if conn.client_fd < 0 {
            continue;
        }
        rtmp_bridge.update_rtt(conn.conn_id, conn.rtt_ms);
    }

    reject_indices.sort_unstable();
    reject_indices.dedup();
    for idx in reject_indices.into_iter().rev() {
        if let Some(conn) = server.connections.get_mut(idx)
            && conn.client_fd >= 0
        {
            let conn_id = conn.conn_id;
            conn.relay_enabled = false;
            conn.relay_key.clear();
            conn.pending_relay.clear();
            tracked.remove(&conn_id);
            rtmp_bridge.on_close(conn_id);
        }
        server.connections.remove(idx);
    }

    current_ids
}

fn is_valid_env_api_token(token: &str) -> bool {
    let token = token.trim();
    token.len() >= 32
        && token.len() <= 256
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
}

fn mask_api_token(token: &str) -> String {
    if token.len() <= 12 {
        return "***".to_string();
    }
    format!("{}...{}", &token[..8], &token[token.len() - 4..])
}

/// Finish any stream deletes that were left half-done (`pending_delete=1`,
/// row still present) by a prior process that crashed or was redeployed
/// mid-delete — see `handle_stream_delete`'s async `202` path in `http.rs`.
/// A fresh process start has no surviving RTMP sessions from before, so it's
/// always safe to finalize these immediately rather than leave them disabled
/// forever. Deliberately keyed on `pending_delete`, not `enabled=0` — a
/// stream can be administratively disabled without being deleted.
fn recover_pending_stream_deletes(db: &Db) {
    for id in db.stream_ids_pending_delete() {
        match db.stream_delete(&id) {
            Some(true) => {
                crate::log_warn!("Recovered abandoned delete for stream '{id}' from a prior run");
            }
            Some(false) => {}
            None => {
                crate::log_error!("Failed to recover abandoned delete for stream '{id}'");
            }
        }
    }
}

/// Load the API bearer token from the database, seeding it from `LRTMP2_API_TOKEN`
/// or generating a new value on first startup.
fn resolve_api_token(db: &Db, db_path: &str) -> Result<String, String> {
    if let Some(stored) = db.token_get()? {
        if let Ok(env_token) = std::env::var("LRTMP2_API_TOKEN") {
            let env_token = env_token.trim();
            if !env_token.is_empty() && env_token != stored {
                crate::log_warn!(
                    "LRTMP2_API_TOKEN env differs from database value; using database token ({})",
                    mask_api_token(&stored)
                );
            }
        }
        return Ok(stored);
    }

    if let Ok(env_token) = std::env::var("LRTMP2_API_TOKEN") {
        let env_token = env_token.trim();
        if !env_token.is_empty() {
            if !is_valid_env_api_token(env_token) {
                return Err(
                    "LRTMP2_API_TOKEN must be 32-256 ASCII alphanumeric characters, '-' or '_'"
                        .into(),
                );
            }
            if db.token_set(env_token)? {
                crate::log_info!(
                    "API token loaded from LRTMP2_API_TOKEN (stored in database {db_path})"
                );
            }
            return db
                .token_get()?
                .ok_or_else(|| "API token missing after env seed".to_string());
        }
    }

    let candidate = crate::keygen::keygen_api_token()?;
    if db.token_set(&candidate)? {
        eprintln!(
            "============================================================\n\
             Generated API token (stored in database {db_path}):\n\
             {}\n\
             Set LRTMP2_API_TOKEN in the panel .env to this value.\n\
             ============================================================",
            candidate
        );
        Ok(candidate)
    } else {
        db.token_get()?
            .ok_or_else(|| "API token missing after concurrent insert".to_string())
    }
}

pub struct ServerApp {
    config: ServerConfig,
    db: Arc<Db>,
    rtmp_bridge: Arc<DbRtmpBridge>,
    /// Stream IDs deleted via HTTP while connections are live. The RTMP poll
    /// loop reads this set and kicks any connection whose stream_id appears.
    deleted_streams: Arc<Mutex<HashSet<String>>>,
    /// Viewer slot IDs revoked via HTTP while player connections are live.
    revoked_viewers: Arc<Mutex<HashSet<String>>>,
}

impl ServerApp {
    /// Opens the database, loads or auto-generates the API token, and wires
    /// together all server components. Returns an error if the database cannot
    /// be opened or the token cannot be persisted.
    pub fn create(config: ServerConfig) -> Result<ServerApp, String> {
        let db_path = std::env::var("LRTMP2_DB")
            .or_else(|_| std::env::var("LRTMP2_DB_PATH"))
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or("LRTMP2_DB or LRTMP2_DB_PATH environment variable must be set to the SQLite database path")?;

        Self::bootstrap(config, &db_path)
    }

    pub(crate) fn bootstrap(mut config: ServerConfig, db_path: &str) -> Result<ServerApp, String> {
        let db = Arc::new(
            Db::open(db_path).map_err(|e| format!("Failed to open database {db_path}: {e}"))?,
        );

        config.api_token = resolve_api_token(&db, db_path)?;
        recover_pending_stream_deletes(&db);

        let deleted_streams = Arc::new(Mutex::new(HashSet::new()));
        let revoked_viewers = Arc::new(Mutex::new(HashSet::new()));

        let rtmp_bridge = Arc::new(DbRtmpBridge::new(
            Arc::clone(&db),
            Arc::clone(&deleted_streams),
        ));

        Ok(ServerApp {
            config,
            db,
            rtmp_bridge,
            deleted_streams,
            revoked_viewers,
        })
    }

    /// Runs until SIGINT/SIGTERM. Blocks the calling task.
    pub async fn run(&self) -> Result<(), String> {
        crate::log_info!("OpenRTMP librtmp2-server alpha starting...");

        if self.config.tls_enabled {
            if self.config.tls_cert_file.is_empty() || self.config.tls_key_file.is_empty() {
                return Err("TLS enabled but tls.cert_file / tls.key_file not configured".into());
            }
            crate::log_info!(
                "RTMPS enabled (cert={}) — RTMP and RTMPS will both accept connections",
                self.config.tls_cert_file
            );
        } else {
            crate::log_info!("RTMPS disabled (plaintext RTMP only)");
        }

        let state = Arc::new(AppState {
            db: Arc::clone(&self.db),
            config: self.config.clone(),
            rtmp_bridge: Arc::clone(&self.rtmp_bridge),
            deleted_streams: Arc::clone(&self.deleted_streams),
            revoked_viewers: Arc::clone(&self.revoked_viewers),
        });
        let app = http::router(state);

        let http_listener = TcpListener::bind(&self.config.http_bind)
            .await
            .map_err(|e| format!("Failed to bind HTTP on {}: {e}", self.config.http_bind))?;
        crate::log_info!("HTTP listening on {}", self.config.http_bind);

        // Start the RTMP listener(s) in a background thread. librtmp2's Server
        // uses a blocking poll loop, so it lives outside the Tokio runtime.
        // One Server binds both the always-on plaintext listener and, when TLS
        // is enabled, an additional RTMPS listener — they share one
        // connections list, so publish/play work across either listener
        // interchangeably (a publisher on RTMP can be watched over RTMPS and
        // vice versa) and RTMP_MAX_CONNECTIONS / the memory limits apply once,
        // across both listeners combined, rather than doubling per listener.
        let rtmp_bind = self.config.rtmp_bind.clone();
        let rtmps_bind = bind_with_default_port(&self.config.rtmps_bind, self.config.rtmps_port());
        let rtmps_log_bind = rtmps_bind.clone();
        let rtmp_max_conn = self.config.rtmp_max_conn;
        let idle_timeout_secs = self.config.rtmp_idle_timeout_secs.clamp(5, 600);
        let rtmp_idle_timeout = Duration::from_secs(idle_timeout_secs);
        let rtmp_resource_limits = self.config.rtmp_resource_limits();
        let rtmp_tls_enabled = self.config.tls_enabled;
        let rtmp_tls_cert = self.config.tls_cert_file.clone();
        let rtmp_tls_key = self.config.tls_key_file.clone();
        let rtmp_bridge = Arc::clone(&self.rtmp_bridge);
        let deleted_streams = Arc::clone(&self.deleted_streams);
        let revoked_viewers = Arc::clone(&self.revoked_viewers);
        let rtmp_stop = Arc::new(AtomicBool::new(false));
        let rtmp_stop_clone = Arc::clone(&rtmp_stop);

        let (rtmp_ready_tx, rtmp_ready_rx) = tokio::sync::oneshot::channel();
        let rtmp_thread = std::thread::spawn(move || {
            use librtmp2::server::Server as RtmpServer;
            use librtmp2::types::ServerConfig as RtmpConfig;

            let cfg = RtmpConfig {
                max_connections: rtmp_max_conn,
                chunk_size: 4096,
                tls_enabled: 0,
                tls_cert_file: std::ptr::null(),
                tls_key_file: std::ptr::null(),
                tls_ca_file: std::ptr::null(),
                tls_insecure: 0,
            };
            let mut server = match RtmpServer::new(cfg) {
                Ok(s) => s,
                Err(e) => {
                    let msg = format!("RTMP server init failed: {e}");
                    crate::log_warn!("{msg}");
                    let _ = rtmp_ready_tx.send(Err(msg));
                    return;
                }
            };
            server.resource_limits = rtmp_resource_limits;
            server.defer_media_relay = true;
            server.on_media_cb = Some(rtmp_media_cb);
            server.on_publish_cb = Some(rtmp_publish_cb);
            server.on_play_cb = Some(rtmp_play_cb);
            if let Err(e) = server.listen(&rtmp_bind) {
                let msg = format!("RTMP bind on {rtmp_bind} failed: {e}");
                crate::log_warn!("{msg}");
                let _ = rtmp_ready_tx.send(Err(msg));
                return;
            }
            crate::log_info!("RTMP listening on {rtmp_bind}");

            if rtmp_tls_enabled {
                if let Err(e) = server.listen_tls(&rtmps_bind, &rtmp_tls_cert, &rtmp_tls_key) {
                    let msg = format!("RTMPS bind on {rtmps_bind} failed: {e}");
                    crate::log_warn!("{msg}");
                    let _ = rtmp_ready_tx.send(Err(msg));
                    return;
                }
                crate::log_info!("RTMPS listening on {rtmps_bind}");
            }

            let _ = rtmp_ready_tx.send(Ok(()));
            if let Ok(mut guard) = RTMP_BRIDGE.lock() {
                *guard = Some(Arc::clone(&rtmp_bridge));
            }

            let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();

            loop {
                if rtmp_stop_clone.load(Ordering::Relaxed) {
                    server.stop();
                    break;
                }

                set_rtmp_poll_server(&mut server);
                let poll_result = server.poll(0);
                clear_rtmp_poll_server();
                if let Err(e) = poll_result {
                    crate::log_warn!("RTMP polling stopped: {e}");
                    break;
                }

                let deleted_now: HashSet<String> = deleted_streams.lock().iter().cloned().collect();
                let revoked_now: HashSet<String> = revoked_viewers.lock().iter().cloned().collect();

                let current_ids = process_server_connections(
                    &mut server,
                    &mut tracked,
                    &rtmp_bridge,
                    &deleted_now,
                    &revoked_now,
                    rtmp_idle_timeout,
                );

                // A conn_id still in `tracked` but absent this cycle was
                // closed by the peer (rather than rejected above) — notify
                // the bridge.
                let closed_ids: Vec<u64> = tracked
                    .keys()
                    .copied()
                    .filter(|id| !current_ids.contains(id))
                    .collect();
                for conn_id in closed_ids {
                    tracked.remove(&conn_id);
                    rtmp_bridge.on_close(conn_id);
                }

                // Drain deletion/revocation markers no live connection still references.
                let live_stream_ids: HashSet<String> = tracked
                    .values()
                    .filter(|c| !c.stream_id.is_empty())
                    .map(|c| c.stream_id.clone())
                    .collect();
                deleted_streams
                    .lock()
                    .retain(|id| live_stream_ids.contains(id));

                let live_viewer_ids: HashSet<String> = tracked
                    .keys()
                    .copied()
                    .map(|conn_id| rtmp_bridge.viewer_id_for_conn(conn_id))
                    .filter(|viewer_id| !viewer_id.is_empty())
                    .collect();
                revoked_viewers
                    .lock()
                    .retain(|viewer_id| live_viewer_ids.contains(viewer_id));

                std::thread::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS));
            }

            // Notify the bridge about connections that never got an explicit close event.
            for conn_id in tracked.keys().copied().collect::<Vec<_>>() {
                rtmp_bridge.on_close(conn_id);
            }
        });

        rtmp_ready_rx
            .await
            .map_err(|_| "RTMP startup thread exited before reporting readiness".to_string())??;

        crate::log_info!(
            "Server ready — HTTP: {}, RTMP: {}{}",
            self.config.http_bind,
            self.config.rtmp_bind,
            if self.config.tls_enabled {
                format!(", RTMPS: {rtmps_log_bind}")
            } else {
                String::new()
            }
        );

        let http_result = axum::serve(
            http_listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| format!("HTTP server error: {e}"));

        crate::log_info!("Shutting down...");
        rtmp_stop.store(true, Ordering::Relaxed);
        let _ = rtmp_thread.join();
        crate::log_info!("RTMP thread joined.");
        http_result?;
        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install SIGINT handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::{ServerApp, TrackedConn, bind_with_default_port, should_evict_idle_conn};
    use crate::config::ServerConfig;
    use std::time::{Duration, Instant};

    fn stale_first_seen(now: Instant) -> Option<Instant> {
        now.checked_sub(Duration::from_secs(120))
    }

    #[test]
    fn idle_eviction_skips_authorized_sessions() {
        let now = Instant::now();
        let entry = TrackedConn {
            first_seen_at: stale_first_seen(now),
            ..Default::default()
        };
        assert!(!should_evict_idle_conn(
            &entry,
            true,
            now,
            Duration::from_secs(30)
        ));

        let publishing_entry = TrackedConn {
            publishing: true,
            first_seen_at: stale_first_seen(now),
            ..Default::default()
        };
        assert!(!should_evict_idle_conn(
            &publishing_entry,
            false,
            now,
            Duration::from_secs(30)
        ));
    }

    #[test]
    fn idle_eviction_targets_stale_pre_auth_connections() {
        let now = Instant::now();
        let entry = TrackedConn {
            first_seen_at: stale_first_seen(now),
            ..Default::default()
        };
        assert!(should_evict_idle_conn(
            &entry,
            false,
            now,
            Duration::from_secs(30)
        ));
    }

    #[test]
    fn bind_with_default_port_leaves_explicit_ports() {
        assert_eq!(bind_with_default_port("0.0.0.0:1936", 1936), "0.0.0.0:1936");
        assert_eq!(bind_with_default_port("[::1]:1936", 1936), "[::1]:1936");
    }

    #[test]
    fn bind_with_default_port_normalizes_host_only_binds() {
        assert_eq!(bind_with_default_port("0.0.0.0", 1936), "0.0.0.0:1936");
        assert_eq!(bind_with_default_port("::1", 1936), "[::1]:1936");
        assert_eq!(bind_with_default_port("[::1]", 1936), "[::1]:1936");
    }

    #[test]
    fn create_generates_api_token_on_first_start() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("token.db");
        let db_path_str = db_path.to_str().unwrap();

        let config = ServerConfig {
            config_file: String::new(),
            ..Default::default()
        };
        let app = ServerApp::bootstrap(config, db_path_str).expect("ServerApp::bootstrap");

        let db = crate::db::Db::open(db_path_str).expect("reopen db");
        let stored = db.token_get().unwrap().expect("token should be stored");
        assert_eq!(stored.len(), 64, "generated token must be 64 hex chars");
        assert!(
            stored.chars().all(|c| c.is_ascii_hexdigit()),
            "token must be hex"
        );
        assert_eq!(app.config.api_token, stored);
    }

    #[test]
    fn bootstrap_seeds_api_token_from_env_on_first_start() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("env-token.db");
        let db_path_str = db_path.to_str().unwrap();
        let env_token = "c10123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd";

        // SAFETY: test runs single-threaded and restores the env var immediately.
        unsafe {
            std::env::set_var("LRTMP2_API_TOKEN", env_token);
        }
        let app = ServerApp::bootstrap(ServerConfig::default(), db_path_str).expect("bootstrap");
        unsafe {
            std::env::remove_var("LRTMP2_API_TOKEN");
        }

        assert_eq!(app.config.api_token, env_token);
        let db = crate::db::Db::open(db_path_str).expect("reopen db");
        assert_eq!(db.token_get().unwrap().as_deref(), Some(env_token));
    }
}
