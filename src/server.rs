//! Server application lifecycle: wires together the database, the HTTP API,
//! and the RTMP listener, then runs until a shutdown signal arrives.

use std::sync::Arc;
use tokio::net::TcpListener;

use crate::config::ServerConfig;
use crate::db::Db;
use crate::http::{self, AppState};
use crate::rtmp_bridge::DbRtmpBridge;

pub struct ServerApp {
    config: ServerConfig,
    db: Arc<Db>,
    #[allow(dead_code)] // wired in once the Rust librtmp2 crate exists
    rtmp_bridge: Arc<DbRtmpBridge>,
}

impl ServerApp {
    /// Opens the database, loads or auto-generates the API token, and wires
    /// together all server components. Returns an error if the database cannot
    /// be opened or the token cannot be persisted.
    pub fn create(mut config: ServerConfig) -> Result<ServerApp, String> {
        let db_path = std::env::var("LRTMP2_DB")
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or("LRTMP2_DB environment variable must be set to the SQLite database path")?;

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
        std::thread::spawn(move || {
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
                    crate::log_warn!("RTMP server init failed: {e}");
                    return;
                }
            };
            if let Err(e) = server.listen(&rtmp_bind) {
                crate::log_warn!("RTMP bind on {rtmp_bind} failed: {e}");
                return;
            }
            crate::log_info!("RTMP listening on {rtmp_bind}");
            loop {
                if server.poll(50).is_err() {
                    break;
                }
            }
        });

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
