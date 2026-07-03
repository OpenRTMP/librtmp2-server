//! Server application lifecycle: wires together the database, the HTTP API,
//! and the RTMP listener(s), then runs until a shutdown signal arrives.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::net::TcpListener;

use crate::config::ServerConfig;
use crate::db::Db;
use crate::http::{self, AppState};
use crate::rtmp_bridge::{DbRtmpBridge, FrameInfo, FrameKind, RtmpEventHandler};

/// RTMP publish/play callbacks are plain function pointers; the bridge is
/// registered on the RTMP thread before the poll loop starts.
static RTMP_BRIDGE: OnceLock<Arc<DbRtmpBridge>> = OnceLock::new();

/// Conn IDs on the RTMPS listener start here so they never collide with the
/// plaintext RTMP listener's IDs (which start at 1) — both listeners run in
/// the same process and share the same bridge/tracking state keyed by
/// conn_id. No realistic deployment gets anywhere near this many connections
/// on one listener.
const TLS_CONN_ID_BASE: u64 = 1 << 40;

/// How often the combined poll loop wakes up to service both listeners.
const POLL_INTERVAL_MS: u64 = 50;

fn rtmp_publish_cb(conn_id: u64, app: &str, stream_key: &str) -> bool {
    RTMP_BRIDGE
        .get()
        .is_some_and(|b| b.authorize_publish(conn_id, app, stream_key).is_ok())
}

fn rtmp_play_cb(conn_id: u64, app: &str, play_key: &str) -> bool {
    RTMP_BRIDGE
        .get()
        .is_some_and(|b| b.authorize_play(conn_id, app, play_key).is_ok())
}

fn rtmp_media_cb(
    conn_id: u64,
    frame_type: librtmp2::types::FrameType,
    codec: Option<&str>,
) -> bool {
    let Some(bridge) = RTMP_BRIDGE.get() else {
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
/// each connection, on either listener.
#[derive(Default)]
struct TrackedConn {
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

/// Drive one poll cycle's worth of connection bookkeeping for a single
/// `librtmp2` server instance (either the plaintext or the TLS listener):
/// authorize new publish/play commands, reject connections the bridge
/// doesn't own, kick connections whose stream/play-key was revoked, and
/// flush stats. Every conn_id processed here is recorded into `current_ids`
/// so the caller can detect connections that disappeared entirely (closed by
/// the peer) once every listener has been polled this iteration.
///
/// `tracked` and the bridge are shared across every listener in the process,
/// so this only ever touches entries for conn_ids that belong to `server`
/// (conn_id ranges are disjoint per listener — see [`TLS_CONN_ID_BASE`]).
fn process_server_connections(
    server: &mut librtmp2::server::Server,
    tracked: &mut HashMap<u64, TrackedConn>,
    rtmp_bridge: &Arc<DbRtmpBridge>,
    deleted_now: &HashSet<String>,
    revoked_now: &HashSet<String>,
    current_ids: &mut HashSet<u64>,
) {
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
    pub fn create(mut config: ServerConfig) -> Result<ServerApp, String> {
        let db_path = std::env::var("LRTMP2_DB")
            .or_else(|_| std::env::var("LRTMP2_DB_PATH"))
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or("LRTMP2_DB or LRTMP2_DB_PATH environment variable must be set to the SQLite database path")?;

        let db = Arc::new(
            Db::open(&db_path).map_err(|e| format!("Failed to open database {db_path}: {e}"))?,
        );

        // The API token lives exclusively in the database. On first startup it
        // is generated here; afterwards it is loaded from the settings table.
        config.api_token = match db.token_get()? {
            Some(t) => t,
            None => {
                let candidate = crate::keygen::keygen_api_token()?;
                if db.token_set(&candidate)? {
                    // We inserted the token — print it once so the operator
                    // can use the API.
                    eprintln!(
                        "============================================================\n\
                         Generated API token (stored in database {db_path}):\n\
                         {candidate}\n\
                         ============================================================"
                    );
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
        // The plaintext listener always runs; the RTMPS listener additionally
        // runs alongside it when TLS is enabled — both accept connections at
        // the same time on their own ports.
        let rtmp_bind = self.config.rtmp_bind.clone();
        let rtmps_bind = self.config.rtmps_bind.clone();
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

            let plain_cfg = RtmpConfig {
                max_connections: rtmp_max_conn,
                chunk_size: 4096,
                tls_enabled: 0,
                tls_cert_file: std::ptr::null(),
                tls_key_file: std::ptr::null(),
                tls_ca_file: std::ptr::null(),
                tls_insecure: 0,
            };
            let mut plain_server = match RtmpServer::new(plain_cfg) {
                Ok(s) => s,
                Err(e) => {
                    let msg = format!("RTMP server init failed: {e}");
                    crate::log_warn!("{msg}");
                    let _ = rtmp_ready_tx.send(Err(msg));
                    return;
                }
            };
            plain_server.resource_limits = rtmp_resource_limits;
            plain_server.defer_media_relay = true;
            plain_server.on_media_cb = Some(rtmp_media_cb);
            plain_server.on_publish_cb = Some(rtmp_publish_cb);
            plain_server.on_play_cb = Some(rtmp_play_cb);
            if let Err(e) = plain_server.listen(&rtmp_bind) {
                let msg = format!("RTMP bind on {rtmp_bind} failed: {e}");
                crate::log_warn!("{msg}");
                let _ = rtmp_ready_tx.send(Err(msg));
                return;
            }
            crate::log_info!("RTMP listening on {rtmp_bind}");

            // Build CStrings inside the thread so the pointers remain valid
            // for the duration of RtmpServer::new().
            let cert_cstr = std::ffi::CString::new(rtmp_tls_cert).ok();
            let key_cstr = std::ffi::CString::new(rtmp_tls_key).ok();

            let mut tls_server = if rtmp_tls_enabled {
                let tls_cfg = RtmpConfig {
                    max_connections: rtmp_max_conn,
                    chunk_size: 4096,
                    tls_enabled: 1,
                    tls_cert_file: cert_cstr
                        .as_ref()
                        .map(|s| s.as_ptr() as *const u8)
                        .unwrap_or(std::ptr::null()),
                    tls_key_file: key_cstr
                        .as_ref()
                        .map(|s| s.as_ptr() as *const u8)
                        .unwrap_or(std::ptr::null()),
                    tls_ca_file: std::ptr::null(),
                    tls_insecure: 0,
                };
                let mut s = match RtmpServer::new(tls_cfg) {
                    Ok(s) => s,
                    Err(e) => {
                        let msg = format!("RTMPS server init failed: {e}");
                        crate::log_warn!("{msg}");
                        let _ = rtmp_ready_tx.send(Err(msg));
                        return;
                    }
                };
                s.set_conn_id_base(TLS_CONN_ID_BASE);
                s.resource_limits = rtmp_resource_limits;
                s.defer_media_relay = true;
                s.on_media_cb = Some(rtmp_media_cb);
                s.on_publish_cb = Some(rtmp_publish_cb);
                s.on_play_cb = Some(rtmp_play_cb);
                if let Err(e) = s.listen(&rtmps_bind) {
                    let msg = format!("RTMPS bind on {rtmps_bind} failed: {e}");
                    crate::log_warn!("{msg}");
                    let _ = rtmp_ready_tx.send(Err(msg));
                    return;
                }
                crate::log_info!("RTMPS listening on {rtmps_bind}");
                Some(s)
            } else {
                None
            };

            let _ = rtmp_ready_tx.send(Ok(()));
            let _ = RTMP_BRIDGE.set(Arc::clone(&rtmp_bridge));

            let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();

            loop {
                if rtmp_stop_clone.load(Ordering::Relaxed) {
                    plain_server.stop();
                    if let Some(ref mut s) = tls_server {
                        s.stop();
                    }
                    break;
                }

                if let Err(e) = plain_server.poll(0) {
                    crate::log_warn!("RTMP polling stopped: {e}");
                    break;
                }
                // A failure here must not take down the plaintext listener —
                // RTMP is meant to stay available even if RTMPS breaks.
                let mut tls_poll_failed = false;
                if let Some(ref mut s) = tls_server
                    && let Err(e) = s.poll(0)
                {
                    crate::log_warn!("RTMPS polling stopped: {e}; RTMP keeps running");
                    tls_poll_failed = true;
                }
                if tls_poll_failed && let Some(mut s) = tls_server.take() {
                    s.stop();
                }

                // Snapshot deleted stream IDs / revoked viewer IDs once per
                // poll cycle, shared across both listeners this iteration.
                let deleted_now: HashSet<String> = deleted_streams.lock().iter().cloned().collect();
                let revoked_now: HashSet<String> = revoked_viewers.lock().iter().cloned().collect();
                let mut current_ids: HashSet<u64> = HashSet::new();

                process_server_connections(
                    &mut plain_server,
                    &mut tracked,
                    &rtmp_bridge,
                    &deleted_now,
                    &revoked_now,
                    &mut current_ids,
                );
                if let Some(ref mut s) = tls_server {
                    process_server_connections(
                        s,
                        &mut tracked,
                        &rtmp_bridge,
                        &deleted_now,
                        &revoked_now,
                        &mut current_ids,
                    );
                }

                // A conn_id still in `tracked` but absent from every
                // listener's connections this iteration was closed by the
                // peer (rather than rejected above) — notify the bridge.
                let closed_ids: Vec<u64> = tracked
                    .keys()
                    .copied()
                    .filter(|id| !current_ids.contains(id))
                    .collect();
                for conn_id in closed_ids {
                    tracked.remove(&conn_id);
                    rtmp_bridge.on_close(conn_id);
                }

                // Drain deletion/revocation markers no live connection on
                // either listener still references.
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
                format!(", RTMPS: {}", self.config.rtmps_bind)
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
