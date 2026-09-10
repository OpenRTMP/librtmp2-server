//! Optional server-side media outputs built from librtmp2 relay exports.
//!
//! The RTMP poll thread only performs bounded, non-blocking queue writes. File
//! I/O and FFmpeg live on worker threads so a slow disk/upstream cannot stall
//! RTMP ingest or local player relay.

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, body::Body};
use parking_lot::Mutex as ParkingMutex;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::db::{Db, DbLookup};
use librtmp2::session::conn::RelayFrame;
use librtmp2::types::FrameType;
use tokio_util::io::ReaderStream;

const DEFAULT_QUEUE_MB: usize = 32;
const SINK_QUEUE_MESSAGES: usize = 512;
const RETIRED_SESSION_QUEUE: usize = 16;
const MAX_FLV_PAYLOAD: usize = 0x00ff_ffff;
static HLS_SESSION_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const MAX_HLS_PLAYLIST_BYTES: u64 = 1024 * 1024;

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
            if let Ok(value) = std::env::var(env_key) {
                let clearable = matches!(
                    config_key,
                    "MEDIA_PUSH_TARGETS" | "MEDIA_EXEC_PUBLISH" | "MEDIA_EXEC_PUBLISH_DONE"
                );
                if clearable || !value.is_empty() {
                    config.apply(config_key, &value);
                }
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
    retire_tx: Option<mpsc::SyncSender<MediaSession>>,
    reaper: Option<thread::JoinHandle<()>>,
}

impl MediaOutputManager {
    pub fn new(config: MediaOutputConfig, db: Arc<Db>) -> Self {
        let (retire_tx, retire_rx) = mpsc::sync_channel::<MediaSession>(RETIRED_SESSION_QUEUE);
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
        let Some(tx) = self.retire_tx.as_ref() else {
            session.abort("session reaper unavailable");
            return;
        };
        match tx.try_send(session) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(session)) => {
                crate::log_warn!(
                    "Media session retirement queue full; cancelling excess session immediately"
                );
                session.abort("session retirement queue full");
            }
            Err(mpsc::TrySendError::Disconnected(session)) => {
                crate::log_warn!("Media session reaper disconnected; cancelling session");
                session.abort("session reaper disconnected");
            }
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
            if let Err(e) = fs::create_dir_all(&dir) {
                crate::log_warn!(
                    "Media outputs: unable to create recording directory '{}': {e}",
                    dir.display()
                );
            } else {
                restrict_media_path_permissions(&dir, true);
            }
            let path = dir.join(format!("{}.flv", unix_millis()));
            sinks.push(spawn_recording_sink(path.clone(), max_queue_bytes));
            recording_file = Some(path);
        }

        if config.hls_enabled {
            let stream_dir = config.hls_path.join(&safe_id);
            if let Err(e) = fs::create_dir_all(&stream_dir) {
                crate::log_warn!(
                    "Media outputs: unable to create HLS stream directory '{}': {e}",
                    stream_dir.display()
                );
            } else {
                restrict_media_path_permissions(&stream_dir, true);
            }
            let dir = stream_dir.join(hls_session_dir_name(conn_id, generation));
            let playlist = dir.join("index.m3u8");
            sinks.push(spawn_hls_sink(
                config,
                stream_dir,
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

    fn abort(mut self, reason: &str) {
        for mut sink in std::mem::take(&mut self.sinks) {
            sink.disable(reason);
        }
        if let Some(mut child) = self.publish_exec.take() {
            terminate_hook_child(&mut child);
        }
        crate::log_warn!(
            "Media outputs: publisher session cancelled stream='{}' reason='{reason}'",
            self.stream_id
        );
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

struct SinkSender {
    label: String,
    tx: Option<mpsc::SyncSender<Arc<Vec<u8>>>>,
    queued_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    failed: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
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
    let worker = match thread::Builder::new()
        .name(thread_name)
        .spawn(move || worker(rx, worker_bytes, worker_failed))
    {
        Ok(handle) => Some(handle),
        Err(e) => {
            failed.store(true, Ordering::Relaxed);
            crate::log_error!("Failed to start media output worker '{label}': {e}");
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
    }
}

fn spawn_recording_sink(path: PathBuf, max_bytes: usize) -> SinkSender {
    let label = format!("record:{}", path.display());
    make_sink(label, max_bytes, move |rx, queued, failed| {
        private_media_umask();
        let result = (|| -> io::Result<()> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
                restrict_media_path_permissions(parent, true);
            }
            let mut file = File::create(&path)?;
            restrict_media_path_permissions(&path, false);
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

fn hls_session_dir_name(conn_id: u64, generation: u64) -> String {
    let sequence = HLS_SESSION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "session-{:020}-{sequence:020}-{conn_id:020}-{generation:020}",
        unix_millis()
    )
}

fn clean_older_hls_sessions(stream_dir: &Path, current_dir: &Path) -> io::Result<()> {
    let Some(current_name) = current_dir.file_name().and_then(std::ffi::OsStr::to_str) else {
        return Ok(());
    };
    for entry in fs::read_dir(stream_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("session-")
            && name.as_ref() < current_name
            && let Err(e) = fs::remove_dir_all(entry.path())
        {
            crate::log_warn!(
                "Unable to remove stale HLS session directory '{}': {e}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

fn clean_hls_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    restrict_media_path_permissions(dir, true);
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let owned = matches!(name.as_ref(), "index.m3u8" | "index.m3u8.tmp" | "init.mp4")
            || (name.starts_with("segment_") && (name.ends_with(".m4s") || name.ends_with(".ts")));
        if owned {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn spawn_hls_sink(
    config: &MediaOutputConfig,
    stream_dir: PathBuf,
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
        private_media_umask();
        let result = (|| -> io::Result<()> {
            clean_hls_dir(&dir)?;
            restrict_media_path_permissions(&dir, true);
            clean_older_hls_sessions(&stream_dir, &dir)?;
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

#[cfg(windows)]
fn terminate_hook_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let pid = child.id().to_string();
        let killed_tree = Command::new("taskkill")
            .args(["/PID", &pid, "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        if !killed_tree {
            let _ = child.kill();
        }
    }
    let _ = child.wait();
}

#[cfg(not(any(unix, windows)))]
fn terminate_hook_child(child: &mut Child) {
    terminate_child(child);
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Recording/HLS workers and FFmpeg children inherit the process umask, which
/// often leaves new files world-readable on multi-user hosts. Tighten created
/// media paths the same way `db::restrict_db_file_permissions` does for SQLite.
#[cfg(unix)]
fn restrict_media_path_permissions(path: &Path, is_dir: bool) {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let mode = if is_dir { 0o700 } else { 0o600 };
    if let Err(e) = fs::set_permissions(path, fs::Permissions::from_mode(mode)) {
        crate::log_warn!("Could not restrict permissions on {}: {e}", path.display());
    }
}

#[cfg(not(unix))]
fn restrict_media_path_permissions(_path: &Path, _is_dir: bool) {}

/// Apply a private umask for worker threads that spawn FFmpeg so segment files
/// are not created world-readable before we can chmod parent directories.
#[cfg(unix)]
fn private_media_umask() {
    // SAFETY: umask is process-global but these workers run in dedicated
    // threads that do not share filesystem creation with unrelated tasks.
    unsafe {
        libc::umask(0o177);
    }
}

#[cfg(not(unix))]
fn private_media_umask() {}

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

fn authorize_hls_request(
    state: &HlsState,
    stream_id: &str,
    key: Option<&str>,
) -> Result<(), StatusCode> {
    if !state.require_key {
        return Ok(());
    }
    let key = key.ok_or(StatusCode::UNAUTHORIZED)?;
    let viewer_allowed = matches!(
        state.db.viewer_find_by_play_key(key),
        DbLookup::Ok(ref viewer) if viewer.stream_id == stream_id
    );
    let stream_enabled =
        matches!(state.db.stream_get(stream_id), DbLookup::Ok(ref stream) if stream.enabled);
    if viewer_allowed && stream_enabled {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

fn latest_hls_session(stream_root: &Path) -> io::Result<String> {
    let mut latest: Option<String> = None;
    for entry in fs::read_dir(stream_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("session-") || safe_component(&name) != name {
            continue;
        }
        if latest
            .as_ref()
            .is_none_or(|current| name.as_str() > current.as_str())
        {
            latest = Some(name);
        }
    }
    latest.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no active HLS session"))
}

async fn active_hls_redirect(state: &HlsState, stream_id: &str, key: Option<&str>) -> Response {
    let root = match fs::canonicalize(&state.root) {
        Ok(root) => root,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let stream_root = state.root.join(stream_id);
    let stream_root = match fs::canonicalize(&stream_root) {
        Ok(path) => path,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    if !stream_root.starts_with(&root) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let result = tokio::task::spawn_blocking(move || latest_hls_session(&stream_root)).await;
    let Ok(Ok(session)) = result else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut location = format!("/hls/{}/{session}/index.m3u8", url_component(stream_id));
    if let Some(key) = key {
        location.push_str("?key=");
        location.push_str(&url_component(key));
    }
    let Ok(location) = HeaderValue::from_str(&location) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let mut headers = HeaderMap::new();
    headers.insert(header::LOCATION, location);
    (StatusCode::TEMPORARY_REDIRECT, headers).into_response()
}

fn canonical_hls_file(root: &Path, stream_id: &str, relative: &Path) -> io::Result<PathBuf> {
    let hls_root = root.canonicalize()?;
    let stream_root = hls_root.join(stream_id).canonicalize()?;
    if !stream_root.starts_with(&hls_root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "HLS stream path escaped configured root",
        ));
    }
    let full = stream_root.join(relative).canonicalize()?;
    if !full.starts_with(&stream_root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "HLS file path escaped stream root",
        ));
    }
    Ok(full)
}

fn hls_headers(extension: &str) -> HeaderMap {
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
    headers
}

async fn serve_hls_playlist(
    full: PathBuf,
    mut headers: HeaderMap,
    require_key: bool,
    key: Option<String>,
) -> Response {
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
    if require_key
        && let Some(key) = key.as_deref()
        && let Ok(text) = std::str::from_utf8(&body)
    {
        body = rewrite_playlist_key(text, key).into_bytes();
    }
    (headers, body).into_response()
}

async fn handle_hls(
    State(state): State<HlsState>,
    AxumPath((stream_id, raw_path)): AxumPath<(String, String)>,
    Query(query): Query<HlsQuery>,
) -> Response {
    if safe_component(&stream_id) != stream_id {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if let Err(status) = authorize_hls_request(&state, &stream_id, query.key.as_deref()) {
        return status.into_response();
    }
    if raw_path == "index.m3u8" {
        return active_hls_redirect(&state, &stream_id, query.key.as_deref()).await;
    }

    let Some(relative) = safe_hls_path(&raw_path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let extension = Path::new(&raw_path)
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("")
        .to_string();
    let root = state.root.clone();
    let path_stream_id = stream_id.clone();
    let result =
        tokio::task::spawn_blocking(move || canonical_hls_file(&root, &path_stream_id, &relative))
            .await;
    let Ok(Ok(full)) = result else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let headers = hls_headers(&extension);
    if extension == "m3u8" {
        return serve_hls_playlist(full, headers, state.require_key, query.key).await;
    }
    match tokio::fs::File::open(full).await {
        Ok(file) => (headers, Body::from_stream(ReaderStream::new(file))).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
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
    match path.extension().and_then(std::ffi::OsStr::to_str) {
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
    while let Some(rel) = out[search_from..].find("URI=\"") {
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
        let named = PushTarget {
            selector: "*".to_string(),
            url_template: "rtmp://a/live/{stream_name}".to_string(),
        };
        assert_eq!(
            named.render_url("one", "Cam One/West", "live"),
            "rtmp://a/live/Cam%20One%2FWest"
        );
    }

    #[test]
    fn hls_paths_reject_traversal_and_unknown_files() {
        assert!(safe_hls_path("segment_000001.m4s").is_some());
        assert!(safe_hls_path("session-0001/segment_000001.m4s").is_some());
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

    #[test]
    #[cfg(unix)]
    fn media_output_files_are_not_world_readable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!(
            "lrtmp2-media-perms-{}",
            std::process::id()
        ));
        let file = dir.join("sample.flv");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        restrict_media_path_permissions(&dir, true);
        File::create(&file).unwrap();
        restrict_media_path_permissions(&file, false);

        let dir_mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        let file_mode = fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "media directories must not be world-accessible");
        assert_eq!(file_mode, 0o600, "media files must not be world-readable");

        let _ = fs::remove_dir_all(&dir);
    }
}
