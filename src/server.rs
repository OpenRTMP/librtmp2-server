//! Server application lifecycle: wires together the database, the HTTP API,
//! and the RTMP listener(s), then runs until a shutdown signal arrives.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::net::TcpListener;

use crate::config::ServerConfig;
use crate::db::Db;
use crate::http::{self, AppState};
use crate::rtmp_bridge::{DbRtmpBridge, FrameInfo, FrameKind, RtmpEventHandler};

/// RTMP publish/play callbacks are plain function pointers; the bridge is
/// registered on the RTMP thread before the poll loop starts.
pub(crate) static RTMP_BRIDGE: StdMutex<Option<Arc<DbRtmpBridge>>> = StdMutex::new(None);

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

pub(crate) fn rtmp_publish_cb(conn_id: u64, app: &str, stream_key: &str) -> bool {
    RTMP_BRIDGE
        .lock()
        .ok()
        .and_then(|guard| {
            guard
                .as_ref()
                .map(|b| b.authorize_publish(conn_id, app, stream_key).is_ok())
        })
        .unwrap_or(false)
}

pub(crate) fn rtmp_play_cb(conn_id: u64, app: &str, play_key: &str) -> bool {
    RTMP_BRIDGE
        .lock()
        .ok()
        .and_then(|guard| {
            guard
                .as_ref()
                .map(|b| b.authorize_play(conn_id, app, play_key).is_ok())
        })
        .unwrap_or(false)
}

pub(crate) fn rtmp_media_cb(
    conn_id: u64,
    frame_type: librtmp2::types::FrameType,
    codec: Option<&str>,
) -> bool {
    let Some(bridge) = RTMP_BRIDGE.lock().ok().and_then(|g| g.clone()) else {
        return true;
    };
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
}

/// Per-connection bookkeeping the RTMP poll loop keeps for the lifetime of
/// each connection.
#[derive(Default)]
pub(crate) struct TrackedConn {
    connected: bool,
    publishing: bool,
    playing: bool,
    /// DB stream id, set after publish/play is fully enabled.
    stream_id: String,
    /// Last detected video codec string from the protocol layer.
    video_codec: String,
    /// Last detected audio codec string from the protocol layer.
    audio_codec: String,
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
            rtmp_bridge.on_connect(conn_id);
            entry.connected = true;
        }

        let Some(stream) = conn.current_stream.as_ref() else {
            continue;
        };

        if stream.is_publishing && !entry.publishing {
            if !rtmp_bridge.has_publisher(conn_id) {
                crate::log_warn!(
                    "RTMP: closing unauthorized publisher conn={conn_id} app='{}' key=<redacted>",
                    conn.app
                );
                reject_indices.push(idx);
                continue;
            }
            crate::log_info!("RTMP: publisher connected from {}", conn.remote_addr);
            entry.publishing = true;
            entry.stream_id = rtmp_bridge.stream_id_for_conn(conn_id);
            conn.relay_key = entry.stream_id.clone();
            conn.relay_enabled = true;
        }

        if stream.is_playing && !entry.playing {
            if !rtmp_bridge.has_player(conn_id) {
                crate::log_warn!(
                    "RTMP: closing unauthorized player conn={conn_id} app='{}' key=<redacted>",
                    conn.app
                );
                reject_indices.push(idx);
                continue;
            }
            crate::log_info!("RTMP: player connected from {}", conn.remote_addr);
            entry.playing = true;
            if entry.stream_id.is_empty() {
                entry.stream_id = rtmp_bridge.stream_id_for_conn(conn_id);
            }
            conn.relay_key = entry.stream_id.clone();
            conn.relay_enabled = true;
        }

        // Kick connections whose stream was deleted.
        if !entry.stream_id.is_empty() && deleted_now.contains(&entry.stream_id) {
            crate::log_info!(
                "RTMP: kicking conn={conn_id} — stream '{}' was deleted",
                entry.stream_id
            );
            reject_indices.push(idx);
            continue;
        }

        let viewer_id = rtmp_bridge.viewer_id_for_conn(conn_id);
        if !viewer_id.is_empty() && revoked_now.contains(&viewer_id) {
            crate::log_info!("RTMP: kicking conn={conn_id} — play key '{viewer_id}' was revoked");
            reject_indices.push(idx);
            continue;
        }

        // Publisher stats: media bytes only (excludes RTMP control overhead).
        if stream.is_publishing {
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
            );
        }

        if stream.is_playing {
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

        Self::bootstrap(config, &db_path, None)
    }

    pub(crate) fn bootstrap(
        mut config: ServerConfig,
        db_path: &str,
        api_token_override: Option<&str>,
    ) -> Result<ServerApp, String> {
        let db = Arc::new(
            Db::open(db_path).map_err(|e| format!("Failed to open database {db_path}: {e}"))?,
        );

        // The API token lives exclusively in the database. On first startup it
        // is taken from LRTMP2_API_TOKEN when set, otherwise generated here;
        // afterwards it is loaded from the settings table.
        config.api_token = match db.token_get()? {
            Some(t) => t,
            None => {
                let from_env = api_token_override
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .or_else(|| {
                        std::env::var("LRTMP2_API_TOKEN")
                            .ok()
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                    });
                let candidate = match from_env {
                    Some(t) => t,
                    None => crate::keygen::keygen_api_token()?,
                };
                if db.token_set(&candidate)? {
                    if api_token_override.is_none()
                        && std::env::var("LRTMP2_API_TOKEN")
                            .ok()
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .is_none()
                    {
                        // We inserted the token — print it once so the operator
                        // can use the API.
                        eprintln!(
                            "============================================================\n\
                             Generated API token (stored in database {db_path}):\n\
                             {candidate}\n\
                             ============================================================"
                        );
                    }
                    candidate
                } else {
                    // Another process inserted first; read back the winner's token.
                    db.token_get()?
                        .ok_or("API token missing after concurrent insert")?
                }
            }
        };

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

                if let Err(e) = server.poll(0) {
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
    use super::{ServerApp, bind_with_default_port};
    use crate::config::ServerConfig;

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
    fn create_persists_env_api_token_on_first_start() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("token.db");
        let db_path_str = db_path.to_str().unwrap();
        let token = "env_api_token_for_first_start_tests_only_value_0123456789ab";

        let config = ServerConfig {
            config_file: String::new(),
            ..Default::default()
        };
        let app =
            ServerApp::bootstrap(config, db_path_str, Some(token)).expect("ServerApp::bootstrap");

        let db = crate::db::Db::open(db_path_str).expect("reopen db");
        assert_eq!(db.token_get().unwrap().as_deref(), Some(token));
        assert_eq!(app.config.api_token, token);
    }
}
