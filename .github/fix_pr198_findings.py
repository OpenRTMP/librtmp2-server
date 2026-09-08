from pathlib import Path


def read(path: str) -> str:
    return Path(path).read_text()


def write(path: str, text: str) -> None:
    Path(path).write_text(text)


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly 1 target, found {count}")
    return text.replace(old, new, 1)


def replace_between(text: str, start: str, end: str, new: str, label: str) -> str:
    start_pos = text.find(start)
    if start_pos < 0:
        raise SystemExit(f"{label}: start marker not found")
    end_pos = text.find(end, start_pos)
    if end_pos < 0:
        raise SystemExit(f"{label}: end marker not found")
    return text[:start_pos] + new + text[end_pos:]


# src/media_output.rs
path = "src/media_output.rs"
text = read(path)
text = replace_once(
    text,
    "use std::io::{self, Write};",
    "use std::io::{self, Read, Write};",
    "Read import",
)
text = replace_once(
    text,
    "const MAX_FLV_PAYLOAD: usize = 0x00ff_ffff;\n",
    "const MAX_FLV_PAYLOAD: usize = 0x00ff_ffff;\n"
    "const MAX_HLS_FILE_BYTES: u64 = 32 * 1024 * 1024;\n"
    "const HLS_STREAM_SUBDIR: &str = \".openrtmp-hls\";\n"
    "const RETIRED_SESSION_QUEUE: usize = 1024;\n",
    "media constants",
)
text = replace_once(
    text,
    """            if let Ok(value) = std::env::var(env_key)
                && !value.is_empty()
            {
                config.apply(config_key, &value);
            }
""",
    """            if let Ok(value) = std::env::var(env_key) {
                let clearable = matches!(
                    config_key,
                    "MEDIA_PUSH_TARGETS" | "MEDIA_EXEC_PUBLISH" | "MEDIA_EXEC_PUBLISH_DONE"
                );
                if clearable || !value.is_empty() {
                    config.apply(config_key, &value);
                }
            }
""",
    "empty env overrides",
)

manager = r'''struct RetiredSession {
    session: MediaSession,
    config: MediaOutputConfig,
}

/// One publisher's output workers. Sessions are keyed by the real local
/// publisher connection id, so cluster-injected remote frames are never
/// recorded/pushed a second time on subscriber nodes.
pub struct MediaOutputManager {
    config: MediaOutputConfig,
    db: Arc<Db>,
    sessions: HashMap<u64, MediaSession>,
    retire_tx: Option<mpsc::SyncSender<RetiredSession>>,
    retire_worker: Option<thread::JoinHandle<()>>,
}

impl MediaOutputManager {
    pub fn new(config: MediaOutputConfig, db: Arc<Db>) -> Self {
        let (retire_tx, retire_rx) = mpsc::sync_channel(RETIRED_SESSION_QUEUE);
        let retire_worker = match thread::Builder::new()
            .name("media-session-reaper".to_string())
            .spawn(move || {
                while let Ok(task) = retire_rx.recv() {
                    task.session.stop(&task.config);
                }
            })
        {
            Ok(handle) => Some(handle),
            Err(e) => {
                crate::log_error!("Failed to start media session reaper: {e}");
                None
            }
        };
        let retire_tx = retire_worker.as_ref().map(|_| retire_tx);

        Self {
            config,
            db,
            sessions: HashMap::new(),
            retire_tx,
            retire_worker,
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled()
    }

    fn retire_session(&mut self, session: MediaSession) {
        let task = RetiredSession {
            session,
            config: self.config.clone(),
        };
        let result = self
            .retire_tx
            .as_ref()
            .map_or(Err(mpsc::TrySendError::Disconnected(task)), |tx| {
                tx.try_send(task)
            });
        match result {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(task))
            | Err(mpsc::TrySendError::Disconnected(task)) => {
                crate::log_warn!(
                    "Media output reaper queue unavailable; using one-off teardown worker"
                );
                let _ = thread::Builder::new()
                    .name("media-session-teardown".to_string())
                    .spawn(move || task.session.stop(&task.config));
            }
        }
    }

    pub fn ensure_publisher(&mut self, conn_id: u64, stream_id: &str) {
        if !self.config.enabled() || stream_id.is_empty() {
            return;
        }
        if self
            .sessions
            .get(&conn_id)
            .is_some_and(|s| s.stream_id == stream_id)
        {
            return;
        }
        if let Some(old) = self.sessions.remove(&conn_id) {
            self.retire_session(old);
        }
        let DbLookup::Ok(stream) = self.db.stream_get(stream_id) else {
            return;
        };
        let session = MediaSession::start(
            conn_id,
            &stream.id,
            &stream.name,
            &stream.app,
            &stream.publish_key,
            &self.config,
        );
        self.sessions.insert(conn_id, session);
    }

    pub fn handle_frame(&mut self, frame: &RelayFrame) {
        let Some(session) = self.sessions.get_mut(&frame.publisher_conn_id) else {
            return;
        };
        if session.app != frame.app || session.publish_route != frame.stream_name {
            return;
        }
        let Some(tag) = flv_tag(frame.frame_type, frame.timestamp, &frame.payload) else {
            return;
        };
        let tag = Arc::new(tag);
        for sink in &mut session.sinks {
            sink.try_send(Arc::clone(&tag));
        }
    }

    pub fn retain_publishers(&mut self, live: &HashSet<u64>) {
        let stale: Vec<u64> = self
            .sessions
            .keys()
            .copied()
            .filter(|id| !live.contains(id))
            .collect();
        for id in stale {
            if let Some(session) = self.sessions.remove(&id) {
                self.retire_session(session);
            }
        }
    }

    pub fn stop_all(&mut self) {
        let sessions = std::mem::take(&mut self.sessions);
        for (_, session) in sessions {
            session.stop(&self.config);
        }

        self.retire_tx.take();
        if let Some(worker) = self.retire_worker.take()
            && worker.join().is_err()
        {
            crate::log_warn!("Media output session reaper panicked during shutdown");
        }
    }
}

'''
text = replace_between(
    text,
    "/// One publisher's output workers.",
    "struct MediaSession {",
    manager,
    "media manager",
)

session = r'''struct MediaSession {
    conn_id: u64,
    stream_id: String,
    stream_name: String,
    app: String,
    publish_route: String,
    sinks: Vec<SinkSender>,
    publish_exec: Option<Child>,
    recording_file: Option<PathBuf>,
    hls_playlist: Option<PathBuf>,
}

impl MediaSession {
    fn start(
        conn_id: u64,
        stream_id: &str,
        stream_name: &str,
        app: &str,
        publish_route: &str,
        config: &MediaOutputConfig,
    ) -> Self {
        let safe_id = safe_component(stream_id);
        let max_queue_bytes = config.export_buffer_bytes();
        let mut sinks = Vec::new();
        let mut recording_file = None;
        let mut hls_playlist = None;

        if config.recording_enabled {
            let dir = config.recording_path.join(&safe_id);
            let path = dir.join(format!("{}.flv", unix_millis()));
            sinks.push(spawn_recording_sink(path.clone(), max_queue_bytes));
            recording_file = Some(path);
        }

        if config.hls_enabled {
            let dir = config
                .hls_path
                .join(&safe_id)
                .join(HLS_STREAM_SUBDIR);
            let playlist = dir.join("index.m3u8");
            sinks.push(spawn_hls_sink(
                config,
                dir,
                playlist.clone(),
                max_queue_bytes,
                format!("hls:{stream_id}"),
            ));
            hls_playlist = Some(playlist);
        }

        for (index, target) in config.push_targets.iter().enumerate() {
            if !target.matches(stream_id, stream_name) {
                continue;
            }
            let url = target.render_url(stream_id, stream_name, app);
            if !(url.starts_with("rtmp://") || url.starts_with("rtmps://")) {
                crate::log_warn!(
                    "Media outputs: rendered push target #{index} is not an RTMP(S) URL"
                );
                continue;
            }
            sinks.push(spawn_push_sink(
                config,
                url,
                max_queue_bytes,
                format!("push#{index}:{stream_id}"),
            ));
        }

        let env = ExecEnv {
            conn_id,
            stream_id,
            stream_name,
            app,
            recording_file: recording_file.as_deref(),
            hls_playlist: hls_playlist.as_deref(),
        };
        let publish_exec = if config.exec_publish.trim().is_empty() {
            None
        } else {
            match spawn_hook(&config.exec_publish, "publish", &env) {
                Ok(child) => Some(child),
                Err(e) => {
                    crate::log_error!("Media outputs: publish exec failed for '{stream_id}': {e}");
                    None
                }
            }
        };

        crate::log_info!(
            "Media outputs: publisher session started stream='{stream_id}' recording={} hls={} push_targets={}",
            recording_file.is_some(),
            hls_playlist.is_some(),
            sinks.len().saturating_sub(
                recording_file.is_some() as usize + hls_playlist.is_some() as usize
            )
        );

        Self {
            conn_id,
            stream_id: stream_id.to_string(),
            stream_name: stream_name.to_string(),
            app: app.to_string(),
            publish_route: publish_route.to_string(),
            sinks,
            publish_exec,
            recording_file,
            hls_playlist,
        }
    }

    fn stop(mut self, config: &MediaOutputConfig) {
        let sinks = std::mem::take(&mut self.sinks);
        for sink in sinks {
            sink.stop();
        }
        if let Some(mut child) = self.publish_exec.take() {
            terminate_child(&mut child);
        }
        if !config.exec_publish_done.trim().is_empty() {
            let env = ExecEnv {
                conn_id: self.conn_id,
                stream_id: &self.stream_id,
                stream_name: &self.stream_name,
                app: &self.app,
                recording_file: self.recording_file.as_deref(),
                hls_playlist: self.hls_playlist.as_deref(),
            };
            match spawn_hook(&config.exec_publish_done, "publish_done", &env) {
                Ok(child) => reap_child_async(
                    child,
                    format!("publish-done-{}", safe_component(&self.stream_id)),
                ),
                Err(e) => crate::log_error!(
                    "Media outputs: publish_done exec failed for '{}': {e}",
                    self.stream_id
                ),
            }
        }
        crate::log_info!(
            "Media outputs: publisher session stopped stream='{}'",
            self.stream_id
        );
    }
}

'''
text = replace_between(
    text,
    "struct MediaSession {",
    "struct SinkSender {",
    session,
    "media session",
)

old_stop = r'''    fn stop(mut self) {
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
                crate::log_warn!(
                    "Media output '{}' worker panicked during shutdown",
                    self.label
                );
            }
        } else {
            crate::log_warn!(
                "Media output '{}' worker did not stop within 5s; continuing shutdown",
                self.label
            );
        }
    }
'''
new_stop = r'''    fn stop(mut self) {
        self.tx = None;
        let Some(worker) = self.worker.take() else {
            return;
        };

        let drain_deadline = Instant::now() + Duration::from_secs(5);
        while !worker.is_finished() && Instant::now() < drain_deadline {
            thread::sleep(Duration::from_millis(10));
        }

        if !worker.is_finished() {
            self.failed.store(true, Ordering::Release);
            let cancel_deadline = Instant::now() + Duration::from_secs(1);
            while !worker.is_finished() && Instant::now() < cancel_deadline {
                thread::sleep(Duration::from_millis(10));
            }
        }

        if worker.is_finished() {
            if worker.join().is_err() {
                crate::log_warn!(
                    "Media output '{}' worker panicked during shutdown",
                    self.label
                );
            }
        } else {
            crate::log_warn!(
                "Media output '{}' worker did not stop after bounded drain/cancel; detaching",
                self.label
            );
        }
    }
'''
text = replace_once(text, old_stop, new_stop, "sink stop")

old_record = r'''fn spawn_recording_sink(path: PathBuf, max_bytes: usize) -> SinkSender {
    let label = format!("record:{}", path.display());
    make_sink(label, max_bytes, move |rx, queued, failed| {
        let result = (|| -> io::Result<()> {
            let mut file = File::create(&path)?;
            file.write_all(flv_header())?;
            consume_queue(rx, queued, |tag| file.write_all(tag))?;
            file.flush()
        })();
        if let Err(e) = result {
            failed.store(true, Ordering::Relaxed);
            crate::log_error!("Recording worker failed for {}: {e}", path.display());
        }
    })
}
'''
new_record = r'''fn spawn_recording_sink(path: PathBuf, max_bytes: usize) -> SinkSender {
    let label = format!("record:{}", path.display());
    make_sink(label, max_bytes, move |rx, queued, failed| {
        let result = (|| -> io::Result<()> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = File::create(&path)?;
            file.write_all(flv_header())?;
            consume_queue(rx, queued, &failed, |tag| file.write_all(tag))?;
            file.flush()
        })();
        if let Err(e) = result {
            failed.store(true, Ordering::Relaxed);
            crate::log_error!("Recording worker failed for {}: {e}", path.display());
        }
    })
}
'''
text = replace_once(text, old_record, new_record, "recording worker")

ffmpeg_call = "            run_ffmpeg_worker(cmd, rx, queued)\n"
if text.count(ffmpeg_call) != 2:
    raise SystemExit(f"ffmpeg worker calls: expected 2, found {text.count(ffmpeg_call)}")
text = text.replace(
    ffmpeg_call,
    "            run_ffmpeg_worker(cmd, rx, queued, Arc::clone(&failed))\n",
)

ffmpeg_block = r'''fn run_ffmpeg_worker(
    mut cmd: Command,
    rx: mpsc::Receiver<Arc<Vec<u8>>>,
    queued: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
) -> io::Result<()> {
    let mut child = cmd.spawn()?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("FFmpeg stdin unavailable"))?;
    let writer_failed = Arc::clone(&failed);
    let writer = thread::Builder::new()
        .name("media-ffmpeg-writer".to_string())
        .spawn(move || -> io::Result<()> {
            stdin.write_all(flv_header())?;
            let result = consume_queue(rx, queued, &writer_failed, |tag| stdin.write_all(tag));
            drop(stdin);
            result
        })?;
    let mut writer = Some(writer);

    loop {
        if failed.load(Ordering::Acquire) {
            terminate_child(&mut child);
            return join_io_worker_bounded(
                writer.take().expect("FFmpeg writer handle present"),
                Duration::from_secs(1),
            );
        }

        if writer.as_ref().is_some_and(|handle| handle.is_finished()) {
            let write_result = writer
                .take()
                .expect("FFmpeg writer handle present")
                .join()
                .map_err(|_| io::Error::other("FFmpeg writer thread panicked"))?;
            if let Err(e) = write_result {
                terminate_child(&mut child);
                return Err(e);
            }
            wait_child_bounded(&mut child, Duration::from_secs(3));
            return Ok(());
        }

        match child.try_wait()? {
            Some(status) => {
                failed.store(true, Ordering::Release);
                let write_result = join_io_worker_bounded(
                    writer.take().expect("FFmpeg writer handle present"),
                    Duration::from_secs(1),
                );
                if !status.success() {
                    return Err(io::Error::other(format!(
                        "FFmpeg exited with status {status}"
                    )));
                }
                return write_result;
            }
            None => thread::sleep(Duration::from_millis(25)),
        }
    }
}

fn join_io_worker_bounded(
    worker: thread::JoinHandle<io::Result<()>>,
    timeout: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    while !worker.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if !worker.is_finished() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "media writer did not stop after cancellation",
        ));
    }
    worker
        .join()
        .map_err(|_| io::Error::other("media writer thread panicked"))?
}

fn consume_queue<F>(
    rx: mpsc::Receiver<Arc<Vec<u8>>>,
    queued: Arc<AtomicUsize>,
    failed: &AtomicBool,
    mut write: F,
) -> io::Result<()>
where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    while !failed.load(Ordering::Acquire) {
        let Ok(tag) = rx.recv() else {
            break;
        };
        if failed.load(Ordering::Acquire) {
            break;
        }
        let size = tag.len();
        let result = write(tag.as_slice());
        queued.fetch_sub(size, Ordering::AcqRel);
        result?;
    }
    queued.store(0, Ordering::Release);
    Ok(())
}

'''
text = replace_between(
    text,
    "fn run_ffmpeg_worker(",
    "fn terminate_child(",
    ffmpeg_block,
    "FFmpeg supervisor",
)

hook_marker = """        .spawn()\n}\n\nfn unix_millis() -> u128 {\n"""
hook_reaper = """        .spawn()\n}\n\nfn reap_child_async(mut child: Child, label: String) {\n    let _ = thread::Builder::new()\n        .name(format!(\"media-hook-{}\", safe_component(&label)))\n        .spawn(move || wait_child_bounded(&mut child, Duration::from_secs(30)));\n}\n\nfn unix_millis() -> u128 {\n"""
text = replace_once(text, hook_marker, hook_reaper, "hook reaper")

text = replace_once(
    text,
    "    let stream_root = hls_root.join(&stream_id);\n",
    "    let stream_root = hls_root.join(&stream_id).join(HLS_STREAM_SUBDIR);\n",
    "HLS serving subdir",
)
text = replace_once(
    text,
    """        let full = full.canonicalize()?;
        if !full.starts_with(&stream_root) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "HLS file path escaped stream root",
            ));
        }
        fs::read(full)
""",
    """        let full = full.canonicalize()?;
        if !full.starts_with(&stream_root) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "HLS file path escaped stream root",
            ));
        }
        let metadata = fs::metadata(&full)?;
        if !metadata.is_file() || metadata.len() > MAX_HLS_FILE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HLS file is not a regular file or exceeds the size limit",
            ));
        }
        let file = File::open(&full)?;
        let mut body = Vec::with_capacity(metadata.len() as usize);
        let mut limited = file.take(MAX_HLS_FILE_BYTES + 1);
        limited.read_to_end(&mut body)?;
        if body.len() as u64 > MAX_HLS_FILE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HLS file grew beyond the size limit while reading",
            ));
        }
        Ok(body)
""",
    "bounded HLS read",
)
write(path, text)

# src/rate_limit.rs
path = "src/rate_limit.rs"
text = read(path)
text = replace_once(
    text,
    """        } else if matches!(path, "/stats" | "/stats-nginx") {
            (self.config.stats_max, path.to_string())
        } else {
""",
    """        } else if let Some(hls_path) = path.strip_prefix("/hls/") {
            let stream_id = hls_path
                .split('/')
                .next()
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown");
            (self.config.default_max, format!("hls:{stream_id}"))
        } else if matches!(path, "/stats" | "/stats-nginx") {
            (self.config.stats_max, path.to_string())
        } else {
""",
    "HLS rate bucket",
)
write(path, text)

# src/server.rs
path = "src/server.rs"
text = read(path)
text = replace_once(
    text,
    """            let hls_limiter = crate::rate_limit::RateLimiter::new(
                self.config.http_rate_limit_config(),
                self.config.http_trusted_proxies.clone(),
                Arc::clone(&state.api_token),
            );
""",
    """            let mut hls_rate_config = self.config.http_rate_limit_config();
            let window_secs = usize::try_from(hls_rate_config.window.as_secs().max(1))
                .unwrap_or(usize::MAX);
            let playback_minimum = 600usize.saturating_mul(window_secs).div_ceil(60);
            hls_rate_config.default_max = hls_rate_config.default_max.max(playback_minimum);
            let hls_limiter = crate::rate_limit::RateLimiter::new(
                hls_rate_config,
                self.config.http_trusted_proxies.clone(),
                Arc::clone(&state.api_token),
            );
""",
    "HLS playback rate limit",
)
write(path, text)

# docs/media-outputs.md
path = "docs/media-outputs.md"
text = read(path)
text = replace_once(
    text,
    """HLS files are deliberately limited to `.m3u8`, `.m4s`, `.mp4`, and `.ts`, and
request paths are checked for traversal before touching the filesystem.
""",
    """HLS files are deliberately limited to `.m3u8`, `.m4s`, `.mp4`, and `.ts`.
Resolved paths are canonicalized and must stay inside the configured HLS root
and stream directory; individual files are capped at 32 MiB before they are
buffered for an HTTP response. HLS-owned files live in an internal per-stream
subdirectory, so using the same top-level path for recording and HLS cannot
cause HLS cleanup to delete FLV recordings.

HLS HTTP requests use a playback-sized rate-limit bucket scoped per client IP
and stream, with a floor equivalent to 600 requests per minute. This avoids the
general HTTP default starving normal segmented playback behind NAT.
""",
    "HLS docs",
)
text = replace_once(
    text,
    """Every file-config key above has an `LRTMP2_` process-environment form which
takes precedence. Examples:
""",
    """Every file-config key above has an `LRTMP2_` process-environment form which
takes precedence. Empty process-environment values explicitly clear
`MEDIA_PUSH_TARGETS`, `MEDIA_EXEC_PUBLISH`, and `MEDIA_EXEC_PUBLISH_DONE`, so a
container deployment can disable values inherited from the file. Examples:
""",
    "environment docs",
)
write(path, text)
