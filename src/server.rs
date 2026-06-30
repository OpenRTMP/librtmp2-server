//! Server application lifecycle: wires together the database, the HTTP API,
//! and the RTMP listener, then runs until a shutdown signal arrives.

use std::sync::Arc;
use tokio::net::TcpListener;

use crate::config::ServerConfig;
use crate::db::Db;
use crate::http::{self, AppState};
use crate::rtmp_bridge::{DbRtmpBridge, RtmpEventHandler};

pub struct ServerApp {
    config: ServerConfig,
    db: Arc<Db>,
    rtmp_bridge: Arc<DbRtmpBridge>,
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
                let candidate = crate::keygen::keygen_secret("")?;
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

        Ok(ServerApp {
            config,
            db,
            rtmp_bridge,
        })
    }

    /// Runs until SIGINT/SIGTERM. Blocks the calling task.
    pub async fn run(&self) -> Result<(), String> {
        crate::log_info!("librtmp2-server v0.1.0 starting...");

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
        let rtmp_tls_enabled = self.config.tls_enabled;
        let rtmp_tls_cert = self.config.tls_cert_file.clone();
        let rtmp_tls_key = self.config.tls_key_file.clone();
        let rtmp_bridge = Arc::clone(&self.rtmp_bridge);

        let (rtmp_ready_tx, rtmp_ready_rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            use librtmp2::server::Server as RtmpServer;
            use librtmp2::types::ServerConfig as RtmpConfig;
            use std::collections::{HashMap, HashSet};

            #[derive(Default)]
            struct TrackedConn {
                connected: bool,
                publishing: bool,
                playing: bool,
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
            if let Err(e) = server.listen(&rtmp_bind) {
                let msg = format!("RTMP bind on {rtmp_bind} failed: {e}");
                crate::log_warn!("{msg}");
                let _ = rtmp_ready_tx.send(Err(msg));
                return;
            }
            crate::log_info!("RTMP listening on {rtmp_bind}");
            let _ = rtmp_ready_tx.send(Ok(()));

            let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();

            loop {
                if let Err(e) = server.poll(50) {
                    crate::log_warn!("RTMP polling stopped: {e}");
                    break;
                }

                let mut current_ids = HashSet::new();
                let mut reject_indices = Vec::new();

                for (idx, conn) in server.connections.iter().enumerate() {
                    if conn.client_fd < 0 {
                        continue;
                    }
                    let conn_id = conn.client_fd as u64;
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
                        match rtmp_bridge.on_publish(conn_id, &conn.app, &stream.name, "") {
                            Ok(()) => entry.publishing = true,
                            Err(()) => {
                                crate::log_warn!(
                                    "RTMP: closing unauthorized publisher conn={conn_id} app='{}' key=<redacted>",
                                    conn.app
                                );
                                reject_indices.push(idx);
                            }
                        }
                    }

                    if stream.is_playing && !entry.playing {
                        match rtmp_bridge.on_play(conn_id, &conn.app, &stream.name, "") {
                            Ok(()) => entry.playing = true,
                            Err(()) => {
                                crate::log_warn!(
                                    "RTMP: closing unauthorized player conn={conn_id} app='{}' key=<redacted>",
                                    conn.app
                                );
                                reject_indices.push(idx);
                            }
                        }
                    }
                }

                reject_indices.sort_unstable();
                reject_indices.dedup();
                for idx in reject_indices.into_iter().rev() {
                    if let Some(conn) = server.connections.get(idx) {
                        if conn.client_fd >= 0 {
                            let conn_id = conn.client_fd as u64;
                            tracked.remove(&conn_id);
                            rtmp_bridge.on_close(conn_id);
                        }
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

        axum::serve(http_listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| format!("HTTP server error: {e}"))?;

        crate::log_info!("Shutting down...");
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
