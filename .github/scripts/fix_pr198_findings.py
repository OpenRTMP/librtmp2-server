from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise RuntimeError(f"missing expected block: {label}")
    return text.replace(old, new, 1)


path = Path("src/media_output.rs")
text = path.read_text()

text = replace_once(
    text,
    "use std::time::{Duration, SystemTime, UNIX_EPOCH};",
    "use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};",
    "Instant import",
)

text = replace_once(
    text,
    """pub struct MediaOutputManager {
    config: MediaOutputConfig,
    db: Arc<Db>,
    sessions: HashMap<u64, MediaSession>,
}""",
    """pub struct MediaOutputManager {
    config: MediaOutputConfig,
    db: Arc<Db>,
    sessions: HashMap<u64, MediaSession>,
    failed_sessions: HashSet<u64>,
}""",
    "failed session field",
)

text = replace_once(
    text,
    """        Self {
            config,
            db,
            sessions: HashMap::new(),
        }""",
    """        Self {
            config,
            db,
            sessions: HashMap::new(),
            failed_sessions: HashSet::new(),
        }""",
    "failed session init",
)

text = replace_once(
    text,
    """        if !self.config.enabled() || stream_id.is_empty() {
            return;
        }
        if self
            .sessions""",
    """        if !self.config.enabled() || stream_id.is_empty() {
            return;
        }
        if self.failed_sessions.contains(&conn_id) {
            return;
        }
        if self
            .sessions""",
    "failed session guard",
)

text = replace_once(
    text,
    """            Err(e) => {
                crate::log_error!(\"Media outputs: failed to start stream '{}': {e}\", stream.id)
            }""",
    """            Err(e) => {
                self.failed_sessions.insert(conn_id);
                crate::log_error!(
                    \"Media outputs: failed to start stream '{}': {e}; retries disabled for conn={conn_id}\",
                    stream.id
                );
            }""",
    "failed session handling",
)

text = replace_once(
    text,
    """        for id in stale {
            if let Some(session) = self.sessions.remove(&id) {
                session.stop(&self.config);
            }
        }
    }

    pub fn stop_all(&mut self) {
        let sessions = std::mem::take(&mut self.sessions);""",
    """        for id in stale {
            if let Some(session) = self.sessions.remove(&id) {
                session.stop(&self.config);
            }
        }
        self.failed_sessions.retain(|id| live.contains(id));
    }

    pub fn stop_all(&mut self) {
        self.failed_sessions.clear();
        let sessions = std::mem::take(&mut self.sessions);""",
    "failed session cleanup",
)

text = replace_once(
    text,
    """    fn stop(mut self, config: &MediaOutputConfig) {
        self.sinks.clear();
        if let Some(mut child) = self.publish_exec.take() {""",
    """    fn stop(mut self, config: &MediaOutputConfig) {
        let sinks = std::mem::take(&mut self.sinks);
        for sink in sinks {
            sink.stop();
        }
        if let Some(mut child) = self.publish_exec.take() {""",
    "session sink drain",
)

text = replace_once(
    text,
    """struct SinkSender {
    label: String,
    tx: Option<mpsc::SyncSender<Arc<Vec<u8>>>>,
    queued_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    failed: Arc<AtomicBool>,
}""",
    """struct SinkSender {
    label: String,
    tx: Option<mpsc::SyncSender<Arc<Vec<u8>>>>,
    queued_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    failed: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}""",
    "worker handle field",
)

text = replace_once(
    text,
    """    fn disable(&mut self, reason: &str) {
        if !self.failed.swap(true, Ordering::AcqRel) {
            crate::log_warn!(\"Media output '{}' disabled: {reason}\", self.label);
        }
        self.tx = None;
    }
}""",
    """    fn disable(&mut self, reason: &str) {
        if !self.failed.swap(true, Ordering::AcqRel) {
            crate::log_warn!(\"Media output '{}' disabled: {reason}\", self.label);
        }
        self.tx = None;
    }

    fn stop(mut self) {
        self.tx = None;
        let Some(worker) = self.worker.take() else {
            return;
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !worker.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if worker.is_finished() {
            if worker.join().is_err() {
                crate::log_warn!(\"Media output '{}' worker panicked during shutdown\", self.label);
            }
        } else {
            crate::log_warn!(
                \"Media output '{}' worker did not stop within 5s; continuing shutdown\",
                self.label
            );
        }
    }
}""",
    "sink stop method",
)

text = replace_once(
    text,
    """    if thread::Builder::new()
        .name(thread_name)
        .spawn(move || worker(rx, worker_bytes, worker_failed))
        .is_err()
    {
        failed.store(true, Ordering::Relaxed);
    }
    SinkSender {
        label,
        tx: Some(tx),
        queued_bytes,
        max_bytes,
        failed,
    }""",
    """    let worker = match thread::Builder::new()
        .name(thread_name)
        .spawn(move || worker(rx, worker_bytes, worker_failed))
    {
        Ok(handle) => Some(handle),
        Err(e) => {
            failed.store(true, Ordering::Relaxed);
            crate::log_error!(\"Failed to start media output worker '{label}': {e}\");
            None
        }
    };
    SinkSender {
        label,
        tx: Some(tx),
        queued_bytes,
        max_bytes,
        failed,
        worker,
    }""",
    "worker handle capture",
)

text = replace_once(
    text,
    """    loop {
        let Some(rel) = out[search_from..].find(\"URI=\\\"\") else {
            break;
        };
        let start = search_from + rel + 5;""",
    """    while let Some(rel) = out[search_from..].find(\"URI=\\\"\") {
        let start = search_from + rel + 5;""",
    "while-let clippy fix",
)

path.write_text(text)

path = Path("src/server.rs")
text = path.read_text()
text = replace_once(
    text,
    """        let mut app = http::router(state);
        if media_output_config.hls_enabled {
            app = app.merge(crate::media_output::hls_router(
                media_output_config.hls_path.clone(),
                Arc::clone(&self.db),
                media_output_config.hls_require_key,
            ));""",
    """        let mut app = http::router(Arc::clone(&state));
        if media_output_config.hls_enabled {
            let hls_limiter = crate::rate_limit::RateLimiter::new(
                self.config.http_rate_limit_config(),
                self.config.http_trusted_proxies.clone(),
                Arc::clone(&state.api_token),
            );
            let hls_app = crate::media_output::hls_router(
                media_output_config.hls_path.clone(),
                Arc::clone(&self.db),
                media_output_config.hls_require_key,
            )
            .layer(axum::middleware::from_fn_with_state(
                hls_limiter,
                crate::rate_limit::middleware,
            ));
            app = app.merge(hls_app);""",
    "HLS rate limiter",
)
path.write_text(text)
