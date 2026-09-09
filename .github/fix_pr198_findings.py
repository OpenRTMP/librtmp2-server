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
        raise SystemExit(f"{label}: start marker missing")
    end_pos = text.find(end, start_pos)
    if end_pos < 0:
        raise SystemExit(f"{label}: end marker missing")
    return text[:start_pos] + new + text[end_pos:]


# Cargo dependencies needed for streaming HLS bodies and Unix hook process groups.
cargo = read("Cargo.toml")
cargo = replace_once(
    cargo,
    'tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "signal", "time", "sync", "io-util"] }',
    'tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "signal", "time", "sync", "io-util", "fs"] }\ntokio-util = { version = "0.7", features = ["io"] }',
    "tokio fs/tokio-util",
)
cargo = replace_once(
    cargo,
    'parking_lot = "0.12"',
    'parking_lot = "0.12"\nlibc = "0.2"',
    "libc dependency",
)
write("Cargo.toml", cargo)


media = read("src/media_output.rs")
media = replace_once(media, "use axum::Router;", "use axum::{Router, body::Body};", "axum Body import")
media = replace_once(
    media,
    "use serde::Deserialize;",
    "use parking_lot::Mutex as ParkingMutex;\nuse serde::Deserialize;",
    "parking mutex import",
)
media = replace_once(
    media,
    "use librtmp2::types::FrameType;",
    "use librtmp2::types::FrameType;\nuse tokio_util::io::ReaderStream;",
    "ReaderStream import",
)
media = replace_once(
    media,
    "const MAX_FLV_PAYLOAD: usize = 0x00ff_ffff;",
    "const MAX_FLV_PAYLOAD: usize = 0x00ff_ffff;\nconst MAX_HLS_PLAYLIST_BYTES: u64 = 1024 * 1024;",
    "playlist cap",
)

old_render = '''    fn render_url(&self, stream_id: &str, stream_name: &str, app: &str) -> String {
        self.url_template
            .replace("{stream_id}", stream_id)
            .replace("{stream_name}", stream_name)
            .replace("{app}", app)
    }
}'''
new_render = '''    fn render_url(&self, stream_id: &str, stream_name: &str, app: &str) -> String {
        self.url_template
            .replace("{stream_id}", &url_component(stream_id))
            .replace("{stream_name}", &url_component(stream_name))
            .replace("{app}", &url_component(app))
    }
}

fn url_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.' | b'~') {
            out.push(*byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{byte:02X}");
        }
    }
    out
}'''
media = replace_once(media, old_render, new_render, "push URL encoding")

old_env = '''            if let Ok(value) = std::env::var(env_key)
                && !value.is_empty()
            {
                config.apply(config_key, &value);
            }'''
new_env = '''            if let Ok(value) = std::env::var(env_key) {
                let clearable = matches!(
                    config_key,
                    "MEDIA_PUSH_TARGETS" | "MEDIA_EXEC_PUBLISH" | "MEDIA_EXEC_PUBLISH_DONE"
                );
                if clearable || !value.is_empty() {
                    config.apply(config_key, &value);
                }
            }'''
media = replace_once(media, old_env, new_env, "empty env overrides")

manager_and_session = r'''pub struct MediaOutputManager {
    config: MediaOutputConfig,
    db: Arc<Db>,
    sessions: HashMap<u64, MediaSession>,
    retire_tx: Option<mpsc::Sender<MediaSession>>,
    reaper: Option<thread::JoinHandle<()>>,
}

impl MediaOutputManager {
    pub fn new(config: MediaOutputConfig, db: Arc<Db>) -> Self {
        let (retire_tx, retire_rx) = mpsc::channel::<MediaSession>();
        let reaper_config = config.clone();
        let reaper = thread::Builder::new()
            .name("media-session-reaper".to_string())
            .spawn(move || {
                while let Ok(session) = retire_rx.recv() {
                    session.stop(&reaper_config);
                }
            });
        let (retire_tx, reaper) = match reaper {
            Ok(handle) => (Some(retire_tx), Some(handle)),
            Err(e) => {
                crate::log_error!("Failed to start media session reaper: {e}");
                (None, None)
            }
        };
        Self {
            config,
            db,
            sessions: HashMap::new(),
            retire_tx,
            reaper,
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled()
    }

    fn retire(&mut self, session: MediaSession) {
        if let Some(tx) = self.retire_tx.as_ref() {
            if let Err(e) = tx.send(session) {
                e.0.stop(&self.config);
            }
        } else {
            session.stop(&self.config);
        }
    }

    fn start_session(&mut self, conn_id: u64, stream_id: &str, generation: u64) {
        let DbLookup::Ok(stream) = self.db.stream_get(stream_id) else {
            return;
        };
        let session = MediaSession::start(
            conn_id,
            generation,
            &stream.id,
            &stream.name,
            &stream.app,
            &self.config,
        );
        self.sessions.insert(conn_id, session);
    }

    pub fn ensure_publisher(&mut self, conn_id: u64, stream_id: &str, generation: u64) {
        if !self.config.enabled() || stream_id.is_empty() {
            return;
        }
        if self.sessions.get(&conn_id).is_some_and(|session| {
            session.stream_id == stream_id && session.generation == generation
        }) {
            return;
        }
        if let Some(old) = self.sessions.remove(&conn_id) {
            self.retire(old);
        }
        self.start_session(conn_id, stream_id, generation);
    }

    pub fn handle_frame(&mut self, frame: &RelayFrame, stream_id: &str, generation: u64) {
        if !self.config.enabled() || stream_id.is_empty() {
            return;
        }

        let needs_switch = self
            .sessions
            .get(&frame.publisher_conn_id)
            .is_none_or(|session| session.stream_id != stream_id);
        if needs_switch {
            if let Some(old) = self.sessions.remove(&frame.publisher_conn_id) {
                self.retire(old);
            }
            self.start_session(frame.publisher_conn_id, stream_id, generation);
        }

        // RTMP timestamps normally move forward. A significant backwards jump
        // on the same route marks a same-connection republish boundary; restart
        // outputs before writing the new session so recordings/HLS/hooks do not
        // merge two logical publish sessions.
        let timestamp_reset = self
            .sessions
            .get(&frame.publisher_conn_id)
            .and_then(|session| session.last_timestamp)
            .is_some_and(|last| frame.timestamp.saturating_add(1000) < last);
        if timestamp_reset {
            if let Some(old) = self.sessions.remove(&frame.publisher_conn_id) {
                self.retire(old);
            }
            self.start_session(frame.publisher_conn_id, stream_id, generation);
        }

        let Some(session) = self.sessions.get_mut(&frame.publisher_conn_id) else {
            return;
        };
        if session.stream_id != stream_id {
            return;
        }
        session.last_timestamp = Some(frame.timestamp);
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
                self.retire(session);
            }
        }
    }

    pub fn stop_all(&mut self) {
        let sessions = std::mem::take(&mut self.sessions);
        for (_, session) in sessions {
            session.stop(&self.config);
        }
        self.retire_tx.take();
        if let Some(reaper) = self.reaper.take()
            && reaper.join().is_err()
        {
            crate::log_warn!("Media session reaper panicked during shutdown");
        }
    }
}

struct MediaSession {
    conn_id: u64,
    generation: u64,
    stream_id: String,
    stream_name: String,
    app: String,
    sinks: Vec<SinkSender>,
    publish_exec: Option<Child>,
    recording_file: Option<PathBuf>,
    hls_playlist: Option<PathBuf>,
    last_timestamp: Option<u32>,
}

impl MediaSession {
    fn start(
        conn_id: u64,
        generation: u64,
        stream_id: &str,
        stream_name: &str,
        app: &str,
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
            let dir = config.hls_path.join(&safe_id);
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
            generation,
            stream_id: stream_id.to_string(),
            stream_name: stream_name.to_string(),
            app: app.to_string(),
            sinks,
            publish_exec,
            recording_file,
            hls_playlist,
            last_timestamp: None,
        }
    }

    fn stop(mut self, config: &MediaOutputConfig) {
        let sinks = std::mem::take(&mut self.sinks);
        for sink in sinks {
            sink.stop();
        }
        if let Some(mut child) = self.publish_exec.take() {
            terminate_hook_child(&mut child);
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
                Ok(child) => reap_child_async(child, format!("publish_done:{}", self.stream_id)),
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
media = replace_between(
    media,
    "pub struct MediaOutputManager {",
    "struct SinkSender {",
    manager_and_session,
    "manager/session replacement",
)

worker_block = r'''fn spawn_recording_sink(path: PathBuf, max_bytes: usize) -> SinkSender {
    let label = format!("record:{}", path.display());
    make_sink(label, max_bytes, move |rx, queued, failed| {
        let result = (|| -> io::Result<()> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = File::create(&path)?;
            file.write_all(flv_header())?;
            consume_queue(rx, queued, Arc::clone(&failed), |tag| file.write_all(tag))?;
            file.flush()
        })();
        if let Err(e) = result {
            failed.store(true, Ordering::Relaxed);
            crate::log_error!("Recording worker failed for {}: {e}", path.display());
        }
    })
}

fn clean_hls_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let owned = matches!(name.as_ref(), "index.m3u8" | "index.m3u8.tmp" | "init.mp4")
            || (name.starts_with("segment_")
                && (name.ends_with(".m4s") || name.ends_with(".ts")));
        if owned {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn spawn_hls_sink(
    config: &MediaOutputConfig,
    dir: PathBuf,
    playlist: PathBuf,
    max_bytes: usize,
    label: String,
) -> SinkSender {
    let ffmpeg = config.ffmpeg_bin.clone();
    let segment_type = config.hls_segment_type.clone();
    let hls_time = config.hls_time_secs;
    let list_size = config.hls_list_size;
    let transcode = config.hls_transcode;
    make_sink(label, max_bytes, move |rx, queued, failed| {
        let result = (|| -> io::Result<()> {
            clean_hls_dir(&dir)?;
            let mut cmd = Command::new(&ffmpeg);
            add_ffmpeg_input(&mut cmd);
            add_codec_args(&mut cmd, transcode);
            cmd.args([
                "-f",
                "hls",
                "-hls_time",
                &hls_time.to_string(),
                "-hls_list_size",
                &list_size.to_string(),
                "-hls_flags",
                "delete_segments+independent_segments+omit_endlist",
            ]);
            if segment_type == "fmp4" {
                let segments = dir.join("segment_%06d.m4s");
                cmd.args([
                    "-hls_segment_type",
                    "fmp4",
                    "-hls_fmp4_init_filename",
                    "init.mp4",
                    "-hls_segment_filename",
                ])
                .arg(segments);
            } else {
                let segments = dir.join("segment_%06d.ts");
                cmd.arg("-hls_segment_filename").arg(segments);
            }
            cmd.arg(&playlist);
            run_ffmpeg_worker(cmd, rx, queued, Arc::clone(&failed))
        })();
        if let Err(e) = result {
            failed.store(true, Ordering::Relaxed);
            crate::log_error!("HLS worker failed for {}: {e}", playlist.display());
        }
    })
}

fn spawn_push_sink(
    config: &MediaOutputConfig,
    url: String,
    max_bytes: usize,
    label: String,
) -> SinkSender {
    let ffmpeg = config.ffmpeg_bin.clone();
    let transcode = config.push_transcode;
    make_sink(label.clone(), max_bytes, move |rx, queued, failed| {
        let result = {
            let mut cmd = Command::new(&ffmpeg);
            add_ffmpeg_input(&mut cmd);
            // FFmpeg diagnostics can echo the full destination URL, including
            // upstream stream keys. Keep push stderr out of server logs.
            cmd.stderr(Stdio::null());
            add_codec_args(&mut cmd, transcode);
            cmd.args(["-f", "flv"]).arg(&url);
            run_ffmpeg_worker(cmd, rx, queued, Arc::clone(&failed))
        };
        if let Err(e) = result {
            failed.store(true, Ordering::Relaxed);
            crate::log_error!("Push worker '{label}' failed: {e}");
        }
    })
}

fn add_ffmpeg_input(cmd: &mut Command) {
    cmd.args([
        "-hide_banner",
        "-loglevel",
        "warning",
        "-f",
        "flv",
        "-i",
        "pipe:0",
        "-map",
        "0:v?",
        "-map",
        "0:a?",
    ])
    .stdin(Stdio::piped())
    .stdout(Stdio::null())
    .stderr(Stdio::inherit());
}

fn add_codec_args(cmd: &mut Command, transcode: bool) {
    if transcode {
        cmd.args([
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-tune",
            "zerolatency",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
        ]);
    } else {
        cmd.args(["-c", "copy"]);
    }
}

fn run_ffmpeg_worker(
    mut cmd: Command,
    rx: mpsc::Receiver<Arc<Vec<u8>>>,
    queued: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
) -> io::Result<()> {
    let child = Arc::new(ParkingMutex::new(cmd.spawn()?));
    let mut stdin = {
        let mut child = child.lock();
        child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("FFmpeg stdin unavailable"))?
    };

    let monitor_child = Arc::clone(&child);
    let monitor_failed = Arc::clone(&failed);
    let monitor_done = Arc::new(AtomicBool::new(false));
    let monitor_done_worker = Arc::clone(&monitor_done);
    let monitor = thread::spawn(move || {
        while !monitor_done_worker.load(Ordering::Acquire) {
            if monitor_failed.load(Ordering::Acquire) {
                let mut child = monitor_child.lock();
                if child.try_wait().ok().flatten().is_none() {
                    let _ = child.kill();
                }
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
    });

    stdin.write_all(flv_header())?;
    let write_result = consume_queue(rx, queued, Arc::clone(&failed), |tag| stdin.write_all(tag));
    drop(stdin);
    if write_result.is_err() {
        failed.store(true, Ordering::Release);
    }
    monitor_done.store(true, Ordering::Release);
    let _ = monitor.join();

    let mut child = child.lock();
    if write_result.is_err() {
        terminate_child(&mut child);
        return write_result;
    }
    wait_child_bounded(&mut child, Duration::from_secs(3));
    Ok(())
}

fn consume_queue<F>(
    rx: mpsc::Receiver<Arc<Vec<u8>>>,
    queued: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
    mut write: F,
) -> io::Result<()>
where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    while !failed.load(Ordering::Acquire) {
        let tag = match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(tag) => tag,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let size = tag.len();
        if failed.load(Ordering::Acquire) {
            queued.store(0, Ordering::Release);
            break;
        }
        let result = write(tag.as_slice());
        queued.fetch_sub(size, Ordering::AcqRel);
        result?;
    }
    if failed.load(Ordering::Acquire) {
        queued.store(0, Ordering::Release);
    }
    Ok(())
}

fn terminate_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill();
    }
    let _ = child.wait();
}

fn wait_child_bounded(child: &mut Child, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            _ => {
                terminate_child(child);
                return;
            }
        }
    }
}

'''
media = replace_between(
    media,
    "fn spawn_recording_sink(",
    "struct ExecEnv<'a> {",
    worker_block,
    "worker block",
)

hook_block = r'''fn spawn_hook(command: &str, event: &str, env: &ExecEnv<'_>) -> io::Result<Child> {
    #[cfg(unix)]
    let mut cmd = {
        use std::os::unix::process::CommandExt;
        let mut c = Command::new("/bin/sh");
        c.arg("-c").arg(command);
        // Give the hook its own process group so teardown can terminate shell
        // pipelines and descendants, not just the top-level /bin/sh process.
        c.process_group(0);
        c
    };
    #[cfg(windows)]
    let mut cmd = {
        let mut c = Command::new("cmd");
        c.arg("/C").arg(command);
        c
    };
    #[cfg(not(any(unix, windows)))]
    return Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "exec hooks unsupported on this platform",
    ));

    cmd.env("OPENRTMP_EVENT", event)
        .env("OPENRTMP_STREAM_ID", env.stream_id)
        .env("OPENRTMP_STREAM_NAME", env.stream_name)
        .env("OPENRTMP_APP", env.app)
        .env("OPENRTMP_PUBLISHER_CONN_ID", env.conn_id.to_string())
        .env(
            "OPENRTMP_RECORDING_FILE",
            env.recording_file
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
        )
        .env(
            "OPENRTMP_HLS_PLAYLIST",
            env.hls_playlist
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
}

fn reap_child_async(mut child: Child, label: String) {
    thread::spawn(move || {
        if let Err(e) = child.wait() {
            crate::log_warn!("Media hook '{label}' reap failed: {e}");
        }
    });
}

#[cfg(unix)]
fn terminate_hook_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let pgid = child.id() as i32;
        // SAFETY: the hook is spawned in its own process group with PGID equal
        // to the child PID; a negative PID targets that group only.
        unsafe {
            libc::kill(-pgid, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_millis(500);
        while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        if child.try_wait().ok().flatten().is_none() {
            // SAFETY: same dedicated process group as above.
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
            }
        }
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn terminate_hook_child(child: &mut Child) {
    terminate_child(child);
}

'''
media = replace_between(media, "fn spawn_hook(", "fn unix_millis()", hook_block, "hook block")

hls_handler = r'''async fn handle_hls(
    State(state): State<HlsState>,
    AxumPath((stream_id, raw_path)): AxumPath<(String, String)>,
    Query(query): Query<HlsQuery>,
) -> Response {
    if safe_component(&stream_id) != stream_id {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if state.require_key {
        let Some(key) = query.key.as_deref() else {
            return StatusCode::UNAUTHORIZED.into_response();
        };
        let authorized = matches!(
            state.db.viewer_find_by_play_key(key),
            DbLookup::Ok(ref viewer) if viewer.stream_id == stream_id
        ) && matches!(state.db.stream_get(&stream_id), DbLookup::Ok(ref stream) if stream.enabled);
        if !authorized {
            return StatusCode::FORBIDDEN.into_response();
        }
    }

    let Some(relative) = safe_hls_path(&raw_path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let extension = Path::new(&raw_path)
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("");
    let hls_root = state.root.clone();
    let stream_root = hls_root.join(&stream_id);
    let full = stream_root.join(relative);
    let result = tokio::task::spawn_blocking(move || -> io::Result<PathBuf> {
        let hls_root = hls_root.canonicalize()?;
        let stream_root = stream_root.canonicalize()?;
        if !stream_root.starts_with(&hls_root) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "HLS stream path escaped configured root",
            ));
        }
        let full = full.canonicalize()?;
        if !full.starts_with(&stream_root) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "HLS file path escaped stream root",
            ));
        }
        Ok(full)
    })
    .await;
    let Ok(Ok(full)) = result else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let content_type = match extension {
        "m3u8" => "application/vnd.apple.mpegurl",
        "m4s" => "video/iso.segment",
        "mp4" => "video/mp4",
        "ts" => "video/mp2t",
        _ => "application/octet-stream",
    };
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );

    if extension == "m3u8" {
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache, no-store, must-revalidate"),
        );
        let Ok(metadata) = tokio::fs::metadata(&full).await else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if metadata.len() > MAX_HLS_PLAYLIST_BYTES {
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        }
        let Ok(mut body) = tokio::fs::read(&full).await else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if state.require_key
            && let Some(key) = query.key.as_deref()
            && let Ok(text) = std::str::from_utf8(&body)
        {
            body = rewrite_playlist_key(text, key).into_bytes();
        }
        return (headers, body).into_response();
    }

    match tokio::fs::File::open(full).await {
        Ok(file) => (headers, Body::from_stream(ReaderStream::new(file))).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

'''
media = replace_between(media, "async fn handle_hls(", "fn safe_hls_path", hls_handler, "HLS streaming handler")

media = replace_once(
    media,
    '''        assert_eq!(
            targets[0].render_url("one", "Name", "live"),
            "rtmp://a/live/one"
        );''',
    '''        assert_eq!(
            targets[0].render_url("one", "Name", "live"),
            "rtmp://a/live/one"
        );
        let named = PushTarget {
            selector: "*".to_string(),
            url_template: "rtmp://a/live/{stream_name}".to_string(),
        };
        assert_eq!(
            named.render_url("one", "Cam One/West", "live"),
            "rtmp://a/live/Cam%20One%2FWest"
        );''',
    "URL encoding test",
)
write("src/media_output.rs", media)


server = read("src/server.rs")
server = replace_once(
    server,
    "use std::sync::{Arc, Mutex as StdMutex};",
    "use std::sync::{Arc, LazyLock, Mutex as StdMutex};",
    "LazyLock import",
)
server = replace_once(
    server,
    "pub(crate) static RTMP_BRIDGE: StdMutex<Option<Arc<DbRtmpBridge>>> = StdMutex::new(None);",
    '''pub(crate) static RTMP_BRIDGE: StdMutex<Option<Arc<DbRtmpBridge>>> = StdMutex::new(None);
static PUBLISH_GENERATIONS: LazyLock<StdMutex<HashMap<u64, u64>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn bump_publish_generation(conn_id: u64) -> u64 {
    let mut generations = PUBLISH_GENERATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let generation = generations.entry(conn_id).or_insert(0);
    *generation = generation.saturating_add(1);
    *generation
}

fn publisher_generation(conn_id: u64) -> u64 {
    PUBLISH_GENERATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&conn_id)
        .copied()
        .unwrap_or(0)
}

fn clear_publish_generation(conn_id: u64) {
    PUBLISH_GENERATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&conn_id);
}''',
    "publish generation state",
)
server = replace_once(
    server,
    '''pub(crate) fn rtmp_publish_cb(conn_id: u64, app: &str, stream_key: &str) -> bool {
    ensure_conn_registered_for_auth(conn_id);
    with_rtmp_bridge(|b| b.authorize_publish(conn_id, app, stream_key).is_ok()).unwrap_or(false)
}''',
    '''pub(crate) fn rtmp_publish_cb(conn_id: u64, app: &str, stream_key: &str) -> bool {
    ensure_conn_registered_for_auth(conn_id);
    let allowed =
        with_rtmp_bridge(|b| b.authorize_publish(conn_id, app, stream_key).is_ok()).unwrap_or(false);
    if allowed {
        bump_publish_generation(conn_id);
    }
    allowed
}''',
    "publish generation callback",
)
server = server.replace(
    "            rtmp_bridge.on_close(conn_id);",
    "            rtmp_bridge.on_close(conn_id);\n            clear_publish_generation(conn_id);",
)

old_ensure = '''                for (&conn_id, entry) in &tracked {
                    if entry.publishing && !entry.stream_id.is_empty() {
                        media_outputs.ensure_publisher(conn_id, &entry.stream_id);
                    }
                }

'''
server = replace_once(server, old_ensure, "", "move ensure after export drain")
old_media = '''                if media_outputs.enabled() {
                    for frame in &exported_frames {
                        if tracked
                            .get(&frame.publisher_conn_id)
                            .is_some_and(|entry| entry.publishing)
                        {
                            media_outputs.handle_frame(frame);
                        }
                    }
                }
'''
new_media = '''                if media_outputs.enabled() {
                    for frame in &exported_frames {
                        if tracked
                            .get(&frame.publisher_conn_id)
                            .is_some_and(|entry| entry.publishing)
                        {
                            // RelayFrame carries the route active when the frame
                            // was exported. Resolve publish keys before consulting
                            // the connection's current stream so queued frames from
                            // stream A cannot be written into a newly switched B.
                            let frame_stream_id = rtmp_bridge
                                .stream_id_for_publish_route(&frame.stream_name)
                                .unwrap_or_else(|| frame.stream_name.clone());
                            media_outputs.handle_frame(
                                frame,
                                &frame_stream_id,
                                publisher_generation(frame.publisher_conn_id),
                            );
                        }
                    }
                }

                // Reconcile the output session only after draining exported
                // frames. This preserves the tail of an old publish session and
                // still restarts hooks/outputs when the same TCP connection
                // republishes the same stream with a new generation.
                for (&conn_id, entry) in &tracked {
                    if entry.publishing && !entry.stream_id.is_empty() {
                        media_outputs.ensure_publisher(
                            conn_id,
                            &entry.stream_id,
                            publisher_generation(conn_id),
                        );
                    }
                }
'''
server = replace_once(server, old_media, new_media, "stream-aware frame dispatch")
write("src/server.rs", server)


rate = read("src/rate_limit.rs")
rate = replace_once(
    rate,
    '''        } else if matches!(path, "/stats" | "/stats-nginx") {
            (self.config.stats_max, path.to_string())
        } else {
            (self.config.default_max, "default".to_string())
        }''',
    '''        } else if matches!(path, "/stats" | "/stats-nginx") {
            (self.config.stats_max, path.to_string())
        } else if path.starts_with("/hls/") {
            // HLS performs recurring playlist + segment requests. Keep it in
            // its own bucket and size it for normal playback instead of the
            // generic 60-request/minute default. Preserve an explicit zero as
            // "deny all" rather than silently re-enabling the route.
            let hls_max = if self.config.default_max == 0 {
                0
            } else {
                self.config.default_max.saturating_mul(10).max(600)
            };
            (hls_max, "hls".to_string())
        } else {
            (self.config.default_max, "default".to_string())
        }''',
    "HLS rate limit bucket",
)
write("src/rate_limit.rs", rate)
