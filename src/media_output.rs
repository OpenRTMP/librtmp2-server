//! Optional server-side media outputs built from librtmp2 relay exports.
//!
//! The RTMP poll thread only performs bounded, non-blocking queue writes. File
//! I/O and FFmpeg live on worker threads so a slow disk/upstream cannot stall
//! RTMP ingest or local player relay.

use axum::Router;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::db::{Db, DbLookup};
use librtmp2::session::conn::RelayFrame;
use librtmp2::types::FrameType;

const DEFAULT_QUEUE_MB: usize = 32;
const SINK_QUEUE_MESSAGES: usize = 512;
const MAX_FLV_PAYLOAD: usize = 0x00ff_ffff;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTarget {
    pub selector: String,
    pub url_template: String,
}

impl PushTarget {
    fn matches(&self, stream_id: &str, stream_name: &str) -> bool {
        self.selector == "*" || self.selector == stream_id || self.selector == stream_name
    }

    fn render_url(&self, stream_id: &str, stream_name: &str, app: &str) -> String {
        self.url_template
            .replace("{stream_id}", stream_id)
            .replace("{stream_name}", stream_name)
            .replace("{app}", app)
    }
}

#[derive(Debug, Clone)]
pub struct MediaOutputConfig {
    pub recording_enabled: bool,
    pub recording_path: PathBuf,
    pub hls_enabled: bool,
    pub hls_path: PathBuf,
    pub hls_time_secs: u32,
    pub hls_list_size: u32,
    pub hls_segment_type: String,
    pub hls_transcode: bool,
    pub hls_require_key: bool,
    pub push_targets: Vec<PushTarget>,
    pub push_transcode: bool,
    pub exec_publish: String,
    pub exec_publish_done: String,
    pub ffmpeg_bin: String,
    pub queue_mb: usize,
}

impl Default for MediaOutputConfig {
    fn default() -> Self {
        Self {
            recording_enabled: false,
            recording_path: PathBuf::from("/data/recordings"),
            hls_enabled: false,
            hls_path: PathBuf::from("/data/hls"),
            hls_time_secs: 4,
            hls_list_size: 6,
            hls_segment_type: "fmp4".to_string(),
            hls_transcode: false,
            hls_require_key: true,
            push_targets: Vec::new(),
            push_transcode: false,
            exec_publish: String::new(),
            exec_publish_done: String::new(),
            ffmpeg_bin: "ffmpeg".to_string(),
            queue_mb: DEFAULT_QUEUE_MB,
        }
    }
}

impl MediaOutputConfig {
    /// Load optional `MEDIA_*` keys from the same .env file as the server and
    /// then apply `LRTMP2_MEDIA_*` process-environment overrides.
    pub fn load(config_file: &str) -> Self {
        let mut config = Self::default();
        if !config_file.is_empty()
            && let Ok(text) = fs::read_to_string(config_file)
        {
            for line in text.lines() {
                if let Some((key, value)) = parse_env_line(line) {
                    config.apply(&key, &value);
                }
            }
        }

        for (env_key, config_key) in [
            ("LRTMP2_MEDIA_RECORDING_ENABLED", "MEDIA_RECORDING_ENABLED"),
            ("LRTMP2_MEDIA_RECORDING_PATH", "MEDIA_RECORDING_PATH"),
            ("LRTMP2_MEDIA_HLS_ENABLED", "MEDIA_HLS_ENABLED"),
            ("LRTMP2_MEDIA_HLS_PATH", "MEDIA_HLS_PATH"),
            ("LRTMP2_MEDIA_HLS_TIME_SECS", "MEDIA_HLS_TIME_SECS"),
            ("LRTMP2_MEDIA_HLS_LIST_SIZE", "MEDIA_HLS_LIST_SIZE"),
            ("LRTMP2_MEDIA_HLS_SEGMENT_TYPE", "MEDIA_HLS_SEGMENT_TYPE"),
            ("LRTMP2_MEDIA_HLS_TRANSCODE", "MEDIA_HLS_TRANSCODE"),
            ("LRTMP2_MEDIA_HLS_REQUIRE_KEY", "MEDIA_HLS_REQUIRE_KEY"),
            ("LRTMP2_MEDIA_PUSH_TARGETS", "MEDIA_PUSH_TARGETS"),
            ("LRTMP2_MEDIA_PUSH_TRANSCODE", "MEDIA_PUSH_TRANSCODE"),
            ("LRTMP2_MEDIA_EXEC_PUBLISH", "MEDIA_EXEC_PUBLISH"),
            ("LRTMP2_MEDIA_EXEC_PUBLISH_DONE", "MEDIA_EXEC_PUBLISH_DONE"),
            ("LRTMP2_MEDIA_FFMPEG_BIN", "MEDIA_FFMPEG_BIN"),
            ("LRTMP2_MEDIA_QUEUE_MB", "MEDIA_QUEUE_MB"),
        ] {
            if let Ok(value) = std::env::var(env_key)
                && !value.is_empty()
            {
                config.apply(config_key, &value);
            }
        }
        config
    }

    fn apply(&mut self, key: &str, value: &str) {
        match key {
            "MEDIA_RECORDING_ENABLED" => set_bool(&mut self.recording_enabled, key, value),
            "MEDIA_RECORDING_PATH" if !value.trim().is_empty() => {
                self.recording_path = PathBuf::from(value.trim())
            }
            "MEDIA_HLS_ENABLED" => set_bool(&mut self.hls_enabled, key, value),
            "MEDIA_HLS_PATH" if !value.trim().is_empty() => {
                self.hls_path = PathBuf::from(value.trim())
            }
            "MEDIA_HLS_TIME_SECS" => {
                self.hls_time_secs = parse_u32(value, 1, 60, self.hls_time_secs, key)
            }
            "MEDIA_HLS_LIST_SIZE" => {
                self.hls_list_size = parse_u32(value, 1, 100, self.hls_list_size, key)
            }
            "MEDIA_HLS_SEGMENT_TYPE" => match value.trim().to_ascii_lowercase().as_str() {
                "fmp4" | "mpegts" => self.hls_segment_type = value.trim().to_ascii_lowercase(),
                _ => crate::log_warn!("Ignoring invalid {key}='{value}' (expected fmp4 or mpegts)"),
            },
            "MEDIA_HLS_TRANSCODE" => set_bool(&mut self.hls_transcode, key, value),
            "MEDIA_HLS_REQUIRE_KEY" => set_bool(&mut self.hls_require_key, key, value),
            "MEDIA_PUSH_TARGETS" => self.push_targets = parse_push_targets(value),
            "MEDIA_PUSH_TRANSCODE" => set_bool(&mut self.push_transcode, key, value),
            "MEDIA_EXEC_PUBLISH" => self.exec_publish = value.to_string(),
            "MEDIA_EXEC_PUBLISH_DONE" => self.exec_publish_done = value.to_string(),
            "MEDIA_FFMPEG_BIN" if !value.trim().is_empty() => {
                self.ffmpeg_bin = value.trim().to_string()
            }
            "MEDIA_QUEUE_MB" => self.queue_mb = parse_usize(value, 1, 512, self.queue_mb, key),
            _ => {}
        }
    }

    pub fn enabled(&self) -> bool {
        self.recording_enabled
            || self.hls_enabled
            || !self.push_targets.is_empty()
            || !self.exec_publish.is_empty()
            || !self.exec_publish_done.is_empty()
    }

    pub fn needs_relay_export(&self) -> bool {
        self.recording_enabled || self.hls_enabled || !self.push_targets.is_empty()
    }

    pub fn export_buffer_bytes(&self) -> usize {
        self.queue_mb.saturating_mul(1024 * 1024)
    }
}

fn parse_env_line(line: &str) -> Option<(String, String)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (key, raw) = line.split_once('=')?;
    let raw = raw.trim();
    let quoted = raw.len() >= 2
        && ((raw.starts_with('"') && raw.ends_with('"'))
            || (raw.starts_with('\'') && raw.ends_with('\'')));
    let value = if quoted { &raw[1..raw.len() - 1] } else { raw };
    Some((key.trim().to_string(), value.to_string()))
}

fn set_bool(target: &mut bool, key: &str, value: &str) {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => *target = true,
        "0" | "false" | "no" | "off" => *target = false,
        _ => crate::log_warn!("Ignoring invalid {key}='{value}' (expected true/false)"),
    }
}

fn parse_u32(value: &str, min: u32, max: u32, default: u32, key: &str) -> u32 {
    match value.trim().parse::<u32>() {
        Ok(v) => v.clamp(min, max),
        Err(_) => {
            crate::log_warn!("Ignoring invalid {key}='{value}'");
            default
        }
    }
}

fn parse_usize(value: &str, min: usize, max: usize, default: usize, key: &str) -> usize {
    match value.trim().parse::<usize>() {
        Ok(v) => v.clamp(min, max),
        Err(_) => {
            crate::log_warn!("Ignoring invalid {key}='{value}'");
            default
        }
    }
}

fn parse_push_targets(value: &str) -> Vec<PushTarget> {
    value
        .split(';')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            let (selector, url) = entry
                .split_once('|')
                .map(|(s, u)| (s.trim(), u.trim()))
                .unwrap_or(("*", entry));
            if selector.is_empty() || !(url.starts_with("rtmp://") || url.starts_with("rtmps://")) {
                crate::log_warn!(
                    "Ignoring invalid MEDIA_PUSH_TARGETS entry (RTMP(S) URL required)"
                );
                return None;
            }
            Some(PushTarget {
                selector: selector.to_string(),
                url_template: url.to_string(),
            })
        })
        .collect()
}

/// One publisher's output workers. Sessions are keyed by the real local
/// publisher connection id, so cluster-injected remote frames are never
/// recorded/pushed a second time on subscriber nodes.
pub struct MediaOutputManager {
    config: MediaOutputConfig,
    db: Arc<Db>,
    sessions: HashMap<u64, MediaSession>,
}

impl MediaOutputManager {
    pub fn new(config: MediaOutputConfig, db: Arc<Db>) -> Self {
        Self {
            config,
            db,
            sessions: HashMap::new(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled()
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
            old.stop(&self.config);
        }
        let DbLookup::Ok(stream) = self.db.stream_get(stream_id) else {
            return;
        };
        match MediaSession::start(conn_id, &stream.id, &stream.name, &stream.app, &self.config) {
            Ok(session) => {
                self.sessions.insert(conn_id, session);
            }
            Err(e) => {
                crate::log_error!("Media outputs: failed to start stream '{}': {e}", stream.id)
            }
        }
    }

    pub fn handle_frame(&mut self, frame: &RelayFrame) {
        let Some(session) = self.sessions.get_mut(&frame.publisher_conn_id) else {
            return;
        };
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
                session.stop(&self.config);
            }
        }
    }

    pub fn stop_all(&mut self) {
        let sessions = std::mem::take(&mut self.sessions);
        for (_, session) in sessions {
            session.stop(&self.config);
        }
    }
}

struct MediaSession {
    conn_id: u64,
    stream_id: String,
    stream_name: String,
    app: String,
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
        config: &MediaOutputConfig,
    ) -> io::Result<Self> {
        let safe_id = safe_component(stream_id);
        let max_queue_bytes = config.export_buffer_bytes();
        let mut sinks = Vec::new();
        let mut recording_file = None;
        let mut hls_playlist = None;

        if config.recording_enabled {
            let dir = config.recording_path.join(&safe_id);
            fs::create_dir_all(&dir)?;
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

        Ok(Self {
            conn_id,
            stream_id: stream_id.to_string(),
            stream_name: stream_name.to_string(),
            app: app.to_string(),
            sinks,
            publish_exec,
            recording_file,
            hls_playlist,
        })
    }

    fn stop(mut self, config: &MediaOutputConfig) {
        self.sinks.clear();
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
            if let Err(e) = spawn_hook(&config.exec_publish_done, "publish_done", &env) {
                crate::log_error!(
                    "Media outputs: publish_done exec failed for '{}': {e}",
                    self.stream_id
                );
            }
        }
        crate::log_info!(
            "Media outputs: publisher session stopped stream='{}'",
            self.stream_id
        );
    }
}

struct SinkSender {
    label: String,
    tx: Option<mpsc::SyncSender<Arc<Vec<u8>>>>,
    queued_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    failed: Arc<AtomicBool>,
}

impl SinkSender {
    fn try_send(&mut self, payload: Arc<Vec<u8>>) {
        if self.tx.is_none() || self.failed.load(Ordering::Relaxed) {
            self.tx = None;
            return;
        }
        let size = payload.len();
        let prior = self.queued_bytes.fetch_add(size, Ordering::AcqRel);
        if prior.saturating_add(size) > self.max_bytes {
            self.queued_bytes.fetch_sub(size, Ordering::AcqRel);
            self.disable("byte queue limit exceeded");
            return;
        }
        let result = self.tx.as_ref().unwrap().try_send(payload);
        if let Err(e) = result {
            self.queued_bytes.fetch_sub(size, Ordering::AcqRel);
            match e {
                mpsc::TrySendError::Full(_) => self.disable("message queue full"),
                mpsc::TrySendError::Disconnected(_) => self.disable("worker exited"),
            }
        }
    }

    fn disable(&mut self, reason: &str) {
        if !self.failed.swap(true, Ordering::AcqRel) {
            crate::log_warn!("Media output '{}' disabled: {reason}", self.label);
        }
        self.tx = None;
    }
}

fn make_sink<F>(label: String, max_bytes: usize, worker: F) -> SinkSender
where
    F: FnOnce(mpsc::Receiver<Arc<Vec<u8>>>, Arc<AtomicUsize>, Arc<AtomicBool>) + Send + 'static,
{
    let (tx, rx) = mpsc::sync_channel(SINK_QUEUE_MESSAGES);
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicBool::new(false));
    let worker_bytes = Arc::clone(&queued_bytes);
    let worker_failed = Arc::clone(&failed);
    let thread_name = format!("media-{}", safe_component(&label));
    if thread::Builder::new()
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
    }
}

fn spawn_recording_sink(path: PathBuf, max_bytes: usize) -> SinkSender {
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
            if dir.exists() {
                fs::remove_dir_all(&dir)?;
            }
            fs::create_dir_all(&dir)?;
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
            run_ffmpeg_worker(cmd, rx, queued)
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
        let result = (|| -> io::Result<()> {
            let mut cmd = Command::new(&ffmpeg);
            add_ffmpeg_input(&mut cmd);
            add_codec_args(&mut cmd, transcode);
            cmd.args(["-f", "flv"]).arg(&url);
            run_ffmpeg_worker(cmd, rx, queued)
        })();
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
) -> io::Result<()> {
    let mut child = cmd.spawn()?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("FFmpeg stdin unavailable"))?;
    stdin.write_all(flv_header())?;
    let write_result = consume_queue(rx, queued, |tag| stdin.write_all(tag));
    drop(stdin);
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
    mut write: F,
) -> io::Result<()>
where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    while let Ok(tag) = rx.recv() {
        let size = tag.len();
        let result = write(tag.as_slice());
        queued.fetch_sub(size, Ordering::AcqRel);
        result?;
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
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if std::time::Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(25))
            }
            _ => {
                terminate_child(child);
                return;
            }
        }
    }
}

struct ExecEnv<'a> {
    conn_id: u64,
    stream_id: &'a str,
    stream_name: &'a str,
    app: &'a str,
    recording_file: Option<&'a Path>,
    hls_playlist: Option<&'a Path>,
}

fn spawn_hook(command: &str, event: &str, env: &ExecEnv<'_>) -> io::Result<Child> {
    #[cfg(unix)]
    let mut cmd = {
        let mut c = Command::new("/bin/sh");
        c.arg("-c").arg(command);
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

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn safe_component(input: &str) -> String {
    let mut out: String = input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() || out == "." || out == ".." {
        out = "stream".to_string();
    }
    out
}

fn flv_header() -> &'static [u8] {
    b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00"
}

fn flv_tag(frame_type: FrameType, timestamp: u32, payload: &[u8]) -> Option<Vec<u8>> {
    if payload.len() > MAX_FLV_PAYLOAD {
        return None;
    }
    let tag_type = match frame_type {
        FrameType::Audio => 8u8,
        FrameType::Video => 9u8,
        FrameType::Script | FrameType::Metadata => 18u8,
    };
    let size = payload.len() as u32;
    let mut out = Vec::with_capacity(15 + payload.len());
    out.push(tag_type);
    out.extend_from_slice(&[(size >> 16) as u8, (size >> 8) as u8, size as u8]);
    out.extend_from_slice(&[
        (timestamp >> 16) as u8,
        (timestamp >> 8) as u8,
        timestamp as u8,
        (timestamp >> 24) as u8,
    ]);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(payload);
    out.extend_from_slice(&(11u32.saturating_add(size)).to_be_bytes());
    Some(out)
}

#[derive(Clone)]
struct HlsState {
    root: PathBuf,
    db: Arc<Db>,
    require_key: bool,
}

#[derive(Deserialize)]
struct HlsQuery {
    key: Option<String>,
}

/// Serve generated HLS from the existing HTTP listener. When key protection
/// is enabled, the same enabled play/viewer keys accepted by RTMP are used.
pub fn hls_router(root: PathBuf, db: Arc<Db>, require_key: bool) -> Router {
    let state = HlsState {
        root,
        db,
        require_key,
    };
    Router::new()
        .route("/hls/{stream_id}/{*path}", get(handle_hls))
        .with_state(state)
}

async fn handle_hls(
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
    let full = state.root.join(&stream_id).join(relative);
    let result = tokio::task::spawn_blocking(move || fs::read(full)).await;
    let Ok(Ok(mut body)) = result else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let extension = Path::new(&raw_path)
        .extension()
        .and_then(|v| v.to_str())
        .unwrap_or("");
    if extension == "m3u8"
        && state.require_key
        && let Some(key) = query.key.as_deref()
        && let Ok(text) = std::str::from_utf8(&body)
    {
        body = rewrite_playlist_key(&text, key).into_bytes();
    }

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
    }
    (headers, body).into_response()
}

fn safe_hls_path(raw: &str) -> Option<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return None;
    }
    for component in path.components() {
        match component {
            Component::Normal(part) if !part.is_empty() => {}
            _ => return None,
        }
    }
    match path.extension().and_then(|v| v.to_str()) {
        Some("m3u8" | "m4s" | "mp4" | "ts") => Some(path.to_path_buf()),
        _ => None,
    }
}

fn rewrite_playlist_key(text: &str, key: &str) -> String {
    let mut out = String::with_capacity(text.len() + 64);
    for line in text.lines() {
        if line.starts_with('#') {
            out.push_str(&rewrite_uri_attributes(line, key));
        } else if line.trim().is_empty() {
            out.push_str(line);
        } else {
            out.push_str(&append_key(line, key));
        }
        out.push('\n');
    }
    out
}

fn rewrite_uri_attributes(line: &str, key: &str) -> String {
    let mut out = line.to_string();
    let mut search_from = 0usize;
    loop {
        let Some(rel) = out[search_from..].find("URI=\"") else {
            break;
        };
        let start = search_from + rel + 5;
        let Some(end_rel) = out[start..].find('"') else {
            break;
        };
        let end = start + end_rel;
        let uri = append_key(&out[start..end], key);
        out.replace_range(start..end, &uri);
        search_from = start + uri.len() + 1;
    }
    out
}

fn append_key(uri: &str, key: &str) -> String {
    if uri.starts_with("http://") || uri.starts_with("https://") || uri.contains("key=") {
        return uri.to_string();
    }
    let separator = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{separator}key={key}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flv_tag_layout_preserves_timestamp_and_payload() {
        let tag = flv_tag(FrameType::Video, 0x12_345678, &[1, 2, 3]).unwrap();
        assert_eq!(tag[0], 9);
        assert_eq!(&tag[1..4], &[0, 0, 3]);
        assert_eq!(&tag[4..8], &[0x34, 0x56, 0x78, 0x12]);
        assert_eq!(&tag[11..14], &[1, 2, 3]);
        assert_eq!(u32::from_be_bytes(tag[14..18].try_into().unwrap()), 14);
    }

    #[test]
    fn push_targets_support_selector_and_templates() {
        let targets = parse_push_targets("*|rtmp://a/live/{stream_id};cam|rtmps://b/live/key");
        assert_eq!(targets.len(), 2);
        assert!(targets[0].matches("one", "name"));
        assert!(targets[1].matches("id", "cam"));
        assert_eq!(
            targets[0].render_url("one", "Name", "live"),
            "rtmp://a/live/one"
        );
    }

    #[test]
    fn hls_paths_reject_traversal_and_unknown_files() {
        assert!(safe_hls_path("segment_000001.m4s").is_some());
        assert!(safe_hls_path("../server.db").is_none());
        assert!(safe_hls_path("index.html").is_none());
    }

    #[test]
    fn playlist_rewriter_protects_segments_and_init_map() {
        let input = "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4,\nsegment_1.m4s\n";
        let out = rewrite_playlist_key(input, "play_abc");
        assert!(out.contains("URI=\"init.mp4?key=play_abc\""));
        assert!(out.contains("segment_1.m4s?key=play_abc"));
    }

    #[test]
    fn safe_component_never_allows_parent_components() {
        assert_eq!(safe_component("../../x"), ".._.._x");
        assert_eq!(safe_component(".."), "stream");
    }
}
