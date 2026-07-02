//! Server application lifecycle: wires together the database, the HTTP API,
//! and the RTMP listener, then runs until a shutdown signal arrives.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use parking_lot::Mutex;
use tokio::net::TcpListener;

use crate::config::ServerConfig;
use crate::db::Db;
use crate::http::{self, AppState};
use crate::rtmp_bridge::{DbRtmpBridge, FrameInfo, FrameKind, RtmpEventHandler};

/// RTMP publish/play callbacks are plain function pointers; the bridge is
/// registered on the RTMP thread before the poll loop starts.
static RTMP_BRIDGE: OnceLock<Arc<DbRtmpBridge>> = OnceLock::new();

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

        let rtmp_bridge = Arc::new(DbRtmpBridge::new(Arc::clone(&db)));
        let deleted_streams = Arc::new(Mutex::new(HashSet::new()));
        let revoked_viewers = Arc::new(Mutex::new(HashSet::new()));

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
            crate::log_info!("RTMPS enabled (cert={})", self.config.tls_cert_file);
        } else {
            crate::log_info!("RTMPS disabled (plaintext RTMP only)");
        }

        let state = Arc::new(AppState {
            db: Arc::clone(&self.db),
            config: self.config.clone(),
            deleted_streams: Arc::clone(&self.deleted_streams),
            revoked_viewers: Arc::clone(&self.revoked_viewers),
        });
        let app = http::router(state);

        let http_listener = TcpListener::bind(&self.config.http_bind)
            .await
            .map_err(|e| format!("Failed to bind HTTP on {}: {e}", self.config.http_bind))?;
        crate::log_info!("HTTP listening on {}", self.config.http_bind);

        // Start the RTMP listener in a background thread. librtmp2's Server
        // uses a blocking poll loop, so it lives outside the Tokio runtime.
        let rtmp_bind = self.config.rtmp_bind.clone();
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
            use std::collections::HashMap;

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

            // Build CStrings inside the thread so the pointers remain valid
            // for the duration of RtmpServer::new().
            let cert_cstr = std::ffi::CString::new(rtmp_tls_cert).ok();
            let key_cstr = std::ffi::CString::new(rtmp_tls_key).ok();

            let cfg = RtmpConfig {
                max_connections: rtmp_max_conn,
                chunk_size: 4096,
                tls_enabled: if rtmp_tls_enabled { 1 } else { 0 },
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
            if let Err(e) = server.listen(&rtmp_bind) {
                let msg = format!("RTMP bind on {rtmp_bind} failed: {e}");
                crate::log_warn!("{msg}");
                let _ = rtmp_ready_tx.send(Err(msg));
                return;
            }
            crate::log_info!("RTMP listening on {rtmp_bind}");
            let _ = rtmp_ready_tx.send(Ok(()));

            let _ = RTMP_BRIDGE.set(Arc::clone(&rtmp_bridge));
            server.on_publish_cb = Some(rtmp_publish_cb);
            server.on_play_cb = Some(rtmp_play_cb);

            let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();

            loop {
                if rtmp_stop_clone.load(Ordering::Relaxed) {
                    server.stop();
                    break;
                }

                if let Err(e) = server.poll(50) {
                    crate::log_warn!("RTMP polling stopped: {e}");
                    break;
                }

                let mut current_ids = std::collections::HashSet::new();
                let mut reject_indices = Vec::new();

                // Snapshot deleted stream IDs once per poll cycle.
                let deleted_now: std::collections::HashSet<String> =
                    deleted_streams.lock().iter().cloned().collect();
                let revoked_now: std::collections::HashSet<String> =
                    revoked_viewers.lock().iter().cloned().collect();

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
                        crate::log_info!(
                            "RTMP: kicking conn={conn_id} — play key '{viewer_id}' was revoked"
                        );
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

                let closed_ids: Vec<u64> = tracked
                    .keys()
                    .copied()
                    .filter(|id| !current_ids.contains(id))
                    .collect();
                for conn_id in closed_ids {
                    tracked.remove(&conn_id);
                    rtmp_bridge.on_close(conn_id);
                }

                // Drain deletion markers that no live connection still references.
                let live_stream_ids: std::collections::HashSet<String> = tracked
                    .values()
                    .filter(|c| !c.stream_id.is_empty())
                    .map(|c| c.stream_id.clone())
                    .collect();
                deleted_streams
                    .lock()
                    .retain(|id| live_stream_ids.contains(id));

                let live_viewer_ids: std::collections::HashSet<String> = tracked
                    .keys()
                    .copied()
                    .map(|conn_id| rtmp_bridge.viewer_id_for_conn(conn_id))
                    .filter(|viewer_id| !viewer_id.is_empty())
                    .collect();
                revoked_viewers
                    .lock()
                    .retain(|viewer_id| live_viewer_ids.contains(viewer_id));
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
            "Server ready — HTTP: {}, RTMP: {}",
            self.config.http_bind,
            self.config.rtmp_bind
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
