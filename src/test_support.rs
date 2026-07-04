//! In-process test harness: spins up HTTP + RTMP with an in-memory DB.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle as TokioJoinHandle;

use crate::config::ServerConfig;
use crate::db::Db;
use crate::http::{self, AppState};
use crate::logger;
use crate::rtmp_bridge::DbRtmpBridge;
use crate::server::{
    process_server_connections, rtmp_media_cb, rtmp_play_cb, rtmp_publish_cb, TrackedConn,
    POLL_INTERVAL_MS, RTMP_BRIDGE,
};

static TEST_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> &'static Runtime {
    TEST_RUNTIME.get_or_init(|| {
        Runtime::new().expect("failed to create tokio runtime for integration tests")
    })
}

pub struct TestServer {
    pub http_base: String,
    pub rtmp_port: u16,
    pub api_token: String,
    pub db: Arc<Db>,
    rtmp_stop: Arc<AtomicBool>,
    rtmp_thread: Option<JoinHandle<()>>,
    http_shutdown: Option<oneshot::Sender<()>>,
    http_task: Option<TokioJoinHandle<()>>,
}

impl TestServer {
    /// Start HTTP + RTMP on loopback using an in-memory database.
    pub fn start(rtmp_port: u16, api_token: &str) -> Self {
        logger::init(0, "");

        let db = Arc::new(Db::open(":memory:").unwrap());
        let deleted_streams = Arc::new(Mutex::new(HashSet::new()));
        let revoked_viewers = Arc::new(Mutex::new(HashSet::new()));
        let rtmp_bridge = Arc::new(DbRtmpBridge::new(
            Arc::clone(&db),
            Arc::clone(&deleted_streams),
        ));

        let config = ServerConfig {
            api_token: api_token.to_string(),
            rtmp_bind: format!("127.0.0.1:{rtmp_port}"),
            ..Default::default()
        };

        let state = Arc::new(AppState {
            db: Arc::clone(&db),
            config,
            rtmp_bridge: Arc::clone(&rtmp_bridge),
            deleted_streams,
            revoked_viewers,
        });

        let app = http::router(state);

        let rt = runtime();
        let http_listener = rt
            .block_on(TcpListener::bind("127.0.0.1:0"))
            .expect("bind HTTP test port");
        let http_addr = http_listener.local_addr().unwrap();
        let http_base = format!("http://{http_addr}");

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let http_task = rt.spawn(async move {
            axum::serve(
                http_listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("HTTP test server failed");
        });

        let rtmp_bind = format!("127.0.0.1:{rtmp_port}");
        let rtmp_stop = Arc::new(AtomicBool::new(false));
        let rtmp_stop_clone = Arc::clone(&rtmp_stop);
        let (rtmp_ready_tx, rtmp_ready_rx) = std::sync::mpsc::channel();
        let deleted_for_rtmp = Arc::clone(&deleted_streams);
        let revoked_for_rtmp = Arc::clone(&revoked_viewers);

        let rtmp_thread = thread::spawn(move || {
            use librtmp2::server::Server as RtmpServer;
            use librtmp2::types::ServerConfig as RtmpConfig;

            let cfg = RtmpConfig {
                max_connections: 32,
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
                    let _ = rtmp_ready_tx.send(Err(format!("RTMP init failed: {e}")));
                    return;
                }
            };
            server.defer_media_relay = true;
            server.on_media_cb = Some(rtmp_media_cb);
            server.on_publish_cb = Some(rtmp_publish_cb);
            server.on_play_cb = Some(rtmp_play_cb);

            if let Err(e) = server.listen(&rtmp_bind) {
                let _ = rtmp_ready_tx.send(Err(format!("RTMP bind failed: {e}")));
                return;
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

                if server.poll(0).is_err() {
                    break;
                }

                let deleted_now: HashSet<String> =
                    deleted_for_rtmp.lock().iter().cloned().collect();
                let revoked_now: HashSet<String> =
                    revoked_for_rtmp.lock().iter().cloned().collect();

                let current_ids = process_server_connections(
                    &mut server,
                    &mut tracked,
                    &rtmp_bridge,
                    &deleted_now,
                    &revoked_now,
                );

                let closed_ids: Vec<u64> = tracked
                    .keys()
                    .copied()
                    .filter(|id| !current_ids.contains(id))
                    .collect();
                for conn_id in closed_ids {
                    tracked.remove(&conn_id);
                    rtmp_bridge.on_close(conn_id);
                }

                thread::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS));
            }

            for conn_id in tracked.keys().copied().collect::<Vec<_>>() {
                rtmp_bridge.on_close(conn_id);
            }
        });

        match rtmp_ready_rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => panic!("RTMP test server failed: {msg}"),
            Err(_) => panic!("RTMP test server startup timed out"),
        }

        thread::sleep(std::time::Duration::from_millis(100));

        Self {
            http_base,
            rtmp_port,
            api_token: api_token.to_string(),
            db,
            rtmp_stop,
            rtmp_thread: Some(rtmp_thread),
            http_shutdown: Some(shutdown_tx),
            http_task: Some(http_task),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.rtmp_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.rtmp_thread.take() {
            let _ = handle.join();
        }
        if let Ok(mut guard) = RTMP_BRIDGE.lock() {
            *guard = None;
        }
        if let Some(tx) = self.http_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.http_task.take() {
            runtime().block_on(async {
                let _ = task.await;
            });
        }
    }
}
