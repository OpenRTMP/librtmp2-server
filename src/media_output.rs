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
use std::net::{IpAddr, SocketAddr};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::db::{Db, DbLookup, Player, Stream, StreamViewer};
use librtmp2::session::conn::RelayFrame;
use librtmp2::types::FrameType;
use tokio_util::io::ReaderStream;

const DEFAULT_QUEUE_MB: usize = 32;
const SINK_QUEUE_MESSAGES: usize = 512;
const RETIRED_SESSION_QUEUE: usize = 16;
/// Minimum gap between two same-connection republish restarts on one publisher
/// connection. Each restart rebuilds every media sink (an FFmpeg child plus
/// sink/monitor threads), so a publisher that walks its RTMP timestamps
/// backwards by more than 1s on every frame must not be able to force one
/// restart per frame.
const REPUBLISH_RESTART_MIN_INTERVAL: Duration = Duration::from_secs(2);
const MAX_FLV_PAYLOAD: usize = 0x00ff_ffff;
static HLS_SESSION_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static RECORDING_SESSION_SEQUENCE: AtomicU64 = AtomicU64::new(0);
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
        if config.recording_enabled {
            harden_existing_media_root(&config.recording_path, "recording");
        }
        if config.hls_enabled {
            harden_existing_media_root(&config.hls_path, "HLS");
        }

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
            .is_some_and(|session| {
                // Debounce: a republish restart is rate-limited per publisher
                // connection (see REPUBLISH_RESTART_MIN_INTERVAL). The publisher
                // controls `frame.timestamp`, so a strictly-decreasing sequence
                // would otherwise retire and re-create the session — FFmpeg
                // child, sink worker and monitor thread — on every frame.
                if session
                    .restarted_at
                    .is_some_and(|at| at.elapsed() < REPUBLISH_RESTART_MIN_INTERVAL)
                {
                    return false;
                }
                let Some(last) = session.last_timestamp else {
                    return false;
                };
                // A backward jump of more than 1s marks a same-connection
                // republish boundary. A "backward" delta that is really a u32
                // millisecond wraparound (~49.7 days) is a continuation, not a
                // republish — only treat plausible (sub-half-range) backward
                // deltas as a reset.
                let backward = last.wrapping_sub(frame.timestamp);
                backward > 1000 && backward < u32::MAX / 2
            });
        if timestamp_reset {
            if let Some(old) = self.sessions.remove(&frame.publisher_conn_id) {
                self.retire(old);
            }
            self.start_session(frame.publisher_conn_id, stream_id, generation);
            if let Some(session) = self.sessions.get_mut(&frame.publisher_conn_id) {
                session.restarted_at = Some(Instant::now());
            }
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
    /// When this session was created by a republish restart, so the next one
    /// can be rate-limited per publisher connection.
    restarted_at: Option<Instant>,
}

fn setup_recording_sink(
    config: &MediaOutputConfig,
    safe_id: &str,
    max_queue_bytes: usize,
    sinks: &mut Vec<SinkSender>,
) -> Option<PathBuf> {
    if !config.recording_enabled {
        return None;
    }

    let dir = config.recording_path.join(safe_id);
    // The `<millis>` stem alone is not unique: a same-connection republish
    // restart retires the old session and starts a new one in the same call,
    // and retirement is asynchronous, so two sessions can resolve to the same
    // path — the new worker then truncates the retired worker's still-open
    // recording and both tag streams interleave into one file. The monotonic
    // sequence keeps every session on its own file, exactly as
    // `hls_session_dir_name` does for HLS.
    let sequence = RECORDING_SESSION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = dir.join(format!("{}-{sequence:020}.flv", unix_millis()));
    sinks.push(spawn_recording_sink(path.clone(), max_queue_bytes));
    Some(path)
}

fn setup_hls_sink(
    config: &MediaOutputConfig,
    safe_id: &str,
    conn_id: u64,
    generation: u64,
    stream_id: &str,
    max_queue_bytes: usize,
    sinks: &mut Vec<SinkSender>,
) -> Option<PathBuf> {
    if !config.hls_enabled {
        return None;
    }

    let stream_dir = config.hls_path.join(safe_id);
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
    Some(playlist)
}

fn add_push_sinks(
    config: &MediaOutputConfig,
    stream_id: &str,
    stream_name: &str,
    app: &str,
    max_queue_bytes: usize,
    sinks: &mut Vec<SinkSender>,
) {
    for (index, target) in config.push_targets.iter().enumerate() {
        if !target.matches(stream_id, stream_name) {
            continue;
        }
        let url = target.render_url(stream_id, stream_name, app);
        if !(url.starts_with("rtmp://") || url.starts_with("rtmps://")) {
            crate::log_warn!("Media outputs: rendered push target #{index} is not an RTMP(S) URL");
            continue;
        }
        sinks.push(spawn_push_sink(
            config,
            url,
            max_queue_bytes,
            format!("push#{index}:{stream_id}"),
        ));
    }
}

fn spawn_publish_exec(config: &MediaOutputConfig, env: &ExecEnv<'_>) -> Option<Child> {
    if config.exec_publish.trim().is_empty() {
        return None;
    }

    match spawn_hook(&config.exec_publish, "publish", env) {
        Ok(child) => Some(child),
        Err(e) => {
            crate::log_error!(
                "Media outputs: publish exec failed for '{}': {e}",
                env.stream_id
            );
            None
        }
    }
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

        let recording_file = setup_recording_sink(config, &safe_id, max_queue_bytes, &mut sinks);
        let hls_playlist = setup_hls_sink(
            config,
            &safe_id,
            conn_id,
            generation,
            stream_id,
            max_queue_bytes,
            &mut sinks,
        );
        add_push_sinks(
            config,
            stream_id,
            stream_name,
            app,
            max_queue_bytes,
            &mut sinks,
        );

        let env = ExecEnv {
            conn_id,
            stream_id,
            stream_name,
            app,
            recording_file: recording_file.as_deref(),
            hls_playlist: hls_playlist.as_deref(),
        };
        let publish_exec = spawn_publish_exec(config, &env);

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
            restarted_at: None,
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
        let result = (|| -> io::Result<()> {
            if let Some(parent) = path.parent() {
                ensure_private_directory(parent)?;
            }
            let mut file = create_private_media_file(&path)?;
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
    ensure_private_directory(dir)?;
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
        let result = (|| -> io::Result<()> {
            ensure_private_directory(&stream_dir)?;
            clean_hls_dir(&dir)?;
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
            configure_private_child_umask(&mut cmd);
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

fn symlink_error(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("refusing symlinked media path '{}'", path.display()),
    )
}

fn ensure_private_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(symlink_error(path));
            }
            if !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("media path '{}' is not a directory", path.display()),
                ));
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => fs::create_dir_all(path)?,
        Err(e) => return Err(e),
    }
    restrict_media_path_permissions(path, true)
}

fn harden_existing_media_root(root: &Path, label: &str) {
    if let Err(e) = harden_media_tree(root) {
        crate::log_warn!(
            "Media outputs: unable to harden existing {label} path '{}': {e}",
            root.display()
        );
    }
}

fn harden_media_tree(root: &Path) -> io::Result<()> {
    ensure_private_directory(root)?;
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir)? {
            harden_media_entry(entry?, &mut pending)?;
        }
    }
    Ok(())
}

fn harden_media_entry(entry: fs::DirEntry, pending: &mut Vec<PathBuf>) -> io::Result<()> {
    let path = entry.path();
    let file_type = entry.file_type()?;
    if file_type.is_symlink() {
        crate::log_warn!(
            "Media outputs: skipping symlink while hardening existing media '{}': target is not followed",
            path.display()
        );
        return Ok(());
    }
    if file_type.is_dir() {
        restrict_media_path_permissions(&path, true)?;
        pending.push(path);
    } else if file_type.is_file() {
        restrict_media_path_permissions(&path, false)?;
    }
    Ok(())
}

#[cfg(unix)]
fn restrict_media_path_permissions(path: &Path, is_dir: bool) -> io::Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(symlink_error(path));
    }

    let mut options = fs::OpenOptions::new();
    options.read(true).custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if metadata.is_dir() != is_dir {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("media path '{}' has unexpected type", path.display()),
        ));
    }
    let mode = if is_dir { 0o700 } else { 0o600 };
    file.set_permissions(fs::Permissions::from_mode(mode))
}

#[cfg(windows)]
fn restrict_media_path_permissions(path: &Path, is_dir: bool) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(symlink_error(path));
    }
    if metadata.is_dir() != is_dir {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("media path '{}' has unexpected type", path.display()),
        ));
    }

    let username = std::env::var("USERNAME").unwrap_or_default();
    if username.is_empty() {
        return Err(io::Error::other(
            "USERNAME is unavailable; cannot restrict Windows media ACL",
        ));
    }
    let grant = if is_dir {
        format!("{username}:(OI)(CI)F")
    } else {
        format!("{username}:(F)")
    };
    let status = Command::new("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r", &grant])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "icacls exited with {status} for '{}'",
            path.display()
        )))
    }
}

#[cfg(not(any(unix, windows)))]
fn restrict_media_path_permissions(_path: &Path, _is_dir: bool) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_private_media_file(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

#[cfg(not(unix))]
fn create_private_media_file(path: &Path) -> io::Result<File> {
    let file = File::create(path)?;
    restrict_media_path_permissions(path, false)?;
    Ok(file)
}

#[cfg(unix)]
fn configure_private_child_umask(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;

    // SAFETY: pre_exec runs after fork in the FFmpeg child. umask is changed
    // only there, so the server process and unrelated hooks keep their umask.
    unsafe {
        cmd.pre_exec(|| {
            libc::umask(0o177);
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_private_child_umask(_cmd: &mut Command) {}

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

fn hls_session_ttl(segment_secs: u32) -> Duration {
    let scaled = (segment_secs as u64).saturating_mul(2).saturating_add(5);
    Duration::from_secs(scaled.max(30))
}

#[derive(Debug, Clone)]
struct HlsViewerSession {
    last_seen: Instant,
    player: Player,
}

/// Tracks distinct HLS clients while reserving their slots through the same
/// `players` table used by RTMP. This makes HLS↔RTMP admission atomic at the
/// DB layer and also causes cluster heartbeats to include HLS sessions.
#[derive(Default)]
struct HlsSessionRegistry {
    inner: ParkingMutex<HashMap<String, HashMap<IpAddr, HlsViewerSession>>>,
}

impl HlsSessionRegistry {
    fn deactivate(db: &Db, session: &HlsViewerSession) -> bool {
        let mut player = session.player.clone();
        player.active = false;
        if db.player_update(&player.id, &player) {
            true
        } else {
            crate::log_warn!(
                "HLS: failed to release viewer slot {} for viewer {}",
                player.id,
                player.viewer_id
            );
            false
        }
    }

    fn release_expired(db: &Db, session: &HlsViewerSession) -> bool {
        match db.stream_get(&session.player.stream_id) {
            DbLookup::Missing => true,
            DbLookup::Failed => false,
            DbLookup::Ok(_) => Self::deactivate(db, session),
        }
    }

    fn purge_stale_locked(
        guard: &mut HashMap<String, HashMap<IpAddr, HlsViewerSession>>,
        db: &Db,
        now: Instant,
        ttl: Duration,
    ) {
        guard.retain(|_, entries| {
            entries.retain(|_, session| {
                let expired = now
                    .checked_duration_since(session.last_seen)
                    .is_some_and(|age| age >= ttl);
                !expired || !Self::release_expired(db, session)
            });
            !entries.is_empty()
        });
    }

    fn purge_stale(&self, db: &Db, ttl: Duration) {
        let mut guard = self.inner.lock();
        Self::purge_stale_locked(&mut guard, db, Instant::now(), ttl);
    }

    fn reserve_or_renew(
        &self,
        db: &Db,
        viewer: &StreamViewer,
        stream: &Stream,
        client: IpAddr,
        ttl: Duration,
        remote_sessions: u64,
    ) -> bool {
        let mut guard = self.inner.lock();
        let now = Instant::now();
        Self::purge_stale_locked(&mut guard, db, now, ttl);

        if let Some(existing) = guard
            .get_mut(&viewer.id)
            .and_then(|entries| entries.get_mut(&client))
        {
            existing.last_seen = now;
            return true;
        }

        let local = db.player_active_count_for_viewer(&viewer.id);
        if local.saturating_add(remote_sessions) >= crate::db::MAX_CONNECTIONS_PER_PLAY_KEY as u64 {
            return false;
        }

        let player_id = match crate::keygen::keygen_stream_key("hls_") {
            Ok(id) => id,
            Err(e) => {
                crate::log_error!("HLS: viewer session id generation failed: {e}");
                return false;
            }
        };
        let player = Player {
            id: player_id,
            stream_id: stream.id.clone(),
            viewer_id: viewer.id.clone(),
            app: stream.app.clone(),
            stream_name: stream.name.clone(),
            active: true,
            connected_at: crate::db::now_ts(),
            ..Default::default()
        };

        // `player_try_acquire` performs the local count+insert in one SQLite
        // transaction. RTMP uses the same method, so an HLS/RTMP race cannot
        // over-admit the play key on this node.
        if !db.player_try_acquire(&player) {
            return false;
        }

        guard.entry(viewer.id.clone()).or_default().insert(
            client,
            HlsViewerSession {
                last_seen: now,
                player,
            },
        );
        true
    }
}

pub type ViewerRemoteSessionCountFn = Arc<dyn Fn(&str) -> u64 + Send + Sync>;

#[derive(Clone)]
struct HlsState {
    root: PathBuf,
    db: Arc<Db>,
    require_key: bool,
    sessions: Arc<HlsSessionRegistry>,
    session_ttl: Duration,
    trusted_proxies: Arc<Vec<IpAddr>>,
    remote_viewer_sessions: Option<ViewerRemoteSessionCountFn>,
}

#[derive(Deserialize)]
struct HlsQuery {
    key: Option<String>,
}

/// Serve generated HLS from the existing HTTP listener. When key protection
/// is enabled, the same enabled play/viewer keys accepted by RTMP are used.
pub fn hls_router(
    root: PathBuf,
    db: Arc<Db>,
    require_key: bool,
    hls_time_secs: u32,
    trusted_proxies: Vec<IpAddr>,
    remote_viewer_sessions: Option<ViewerRemoteSessionCountFn>,
) -> Router {
    let session_ttl = hls_session_ttl(hls_time_secs);
    let sessions = Arc::new(HlsSessionRegistry::default());

    // Expired HLS reservations must be released even if that viewer never
    // sends another request; otherwise stale DB rows could block later RTMP
    // viewers indefinitely. Sweep all viewer buckets in the background.
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let cleanup_sessions = Arc::clone(&sessions);
        let cleanup_db = Arc::clone(&db);
        let interval = Duration::from_secs((session_ttl.as_secs() / 2).clamp(1, 30));
        handle.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                cleanup_sessions.purge_stale(&cleanup_db, session_ttl);
            }
        });
    }

    let state = HlsState {
        root,
        db,
        require_key,
        sessions,
        session_ttl,
        trusted_proxies: Arc::new(trusted_proxies),
        remote_viewer_sessions,
    };
    Router::new()
        .route("/hls/{stream_id}/{*path}", get(handle_hls))
        .with_state(state)
}

fn authorize_hls_request(
    state: &HlsState,
    stream_id: &str,
    key: Option<&str>,
    client: IpAddr,
) -> Result<(), StatusCode> {
    if !state.require_key {
        return Ok(());
    }
    let key = key.ok_or(StatusCode::UNAUTHORIZED)?;
    let DbLookup::Ok(viewer) = state.db.viewer_find_by_play_key(key) else {
        return Err(StatusCode::FORBIDDEN);
    };
    if viewer.stream_id != stream_id {
        return Err(StatusCode::FORBIDDEN);
    }
    let DbLookup::Ok(stream) = state.db.stream_get(stream_id) else {
        return Err(StatusCode::FORBIDDEN);
    };
    if !stream.enabled {
        return Err(StatusCode::FORBIDDEN);
    }
    let remote = state
        .remote_viewer_sessions
        .as_ref()
        .map(|f| f(&viewer.id))
        .unwrap_or(0);
    if !state.sessions.reserve_or_renew(
        &state.db,
        &viewer,
        &stream,
        client,
        state.session_ttl,
        remote,
    ) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
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

fn resolve_active_hls_session(root: &Path, stream_id: &str) -> Result<String, StatusCode> {
    let hls_root = fs::canonicalize(root).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let stream_root = root.join(stream_id);
    let stream_root = fs::canonicalize(&stream_root).map_err(|_| StatusCode::NOT_FOUND)?;
    if !stream_root.starts_with(&hls_root) {
        return Err(StatusCode::BAD_REQUEST);
    }
    latest_hls_session(&stream_root).map_err(|_| StatusCode::NOT_FOUND)
}

async fn active_hls_redirect(state: &HlsState, stream_id: &str, key: Option<&str>) -> Response {
    let root = state.root.clone();
    let path_stream_id = stream_id.to_owned();
    let result =
        tokio::task::spawn_blocking(move || resolve_active_hls_session(&root, &path_stream_id))
            .await;
    let session = match result {
        Ok(Ok(session)) => session,
        Ok(Err(status)) => return status.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    AxumPath((stream_id, raw_path)): AxumPath<(String, String)>,
    Query(query): Query<HlsQuery>,
) -> Response {
    if safe_component(&stream_id) != stream_id {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let client = crate::rate_limit::resolve_client_ip(
        addr.ip(),
        headers.get("X-Forwarded-For"),
        state.trusted_proxies.as_slice(),
    );
    if let Err(status) = authorize_hls_request(&state, &stream_id, query.key.as_deref(), client) {
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
    fn hls_session_ttl_scales_with_segment_duration() {
        assert_eq!(hls_session_ttl(4), Duration::from_secs(30));
        assert!(hls_session_ttl(60) >= Duration::from_secs(120));
    }

    #[test]
    fn hls_connection_cap_counts_rtmp_and_hls_clients() {
        use crate::db::{Db, Stream};
        use crate::keygen::{PREFIX_PLAY_KEY, PREFIX_PUBLISH_KEY, PREFIX_STATS_KEY};

        let db = Arc::new(Db::open(":memory:").unwrap());
        let stream = Stream {
            id: "s1".to_string(),
            name: "cam".to_string(),
            app: "live".to_string(),
            publish_key: format!("{PREFIX_PUBLISH_KEY}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            play_key: format!("{PREFIX_PLAY_KEY}bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            stats_key: format!("{PREFIX_STATS_KEY}cccccccccccccccccccccccccccccccc"),
            enabled: true,
            created_at: 0,
        };
        let viewer = db.stream_add(&stream).unwrap();
        let sessions = HlsSessionRegistry::default();
        let ttl = Duration::from_secs(30);
        let cap = crate::db::MAX_CONNECTIONS_PER_PLAY_KEY as u64;
        let client = IpAddr::from([203, 0, 113, 9]);

        for i in 0..cap {
            let player = crate::db::Player {
                id: format!("pl{i}"),
                stream_id: stream.id.clone(),
                viewer_id: viewer.id.clone(),
                active: true,
                connected_at: 0,
                ..Default::default()
            };
            assert!(db.player_try_acquire(&player));
        }
        assert!(!sessions.reserve_or_renew(&db, &viewer, &stream, client, ttl, 0,));

        db.players_deactivate_for_viewer(&viewer.id);
        for i in 0..cap {
            assert!(sessions.reserve_or_renew(
                &db,
                &viewer,
                &stream,
                IpAddr::from([198, 51, 100, i as u8]),
                ttl,
                0,
            ));
        }
        assert!(!sessions.reserve_or_renew(&db, &viewer, &stream, client, ttl, 0,));
        assert!(sessions.reserve_or_renew(
            &db,
            &viewer,
            &stream,
            IpAddr::from([198, 51, 100, 0]),
            ttl,
            0,
        ));
    }

    #[test]
    #[cfg(unix)]
    fn media_output_files_are_not_world_readable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("lrtmp2-media-perms-{}", std::process::id()));
        let file = dir.join("sample.flv");
        let _ = fs::remove_dir_all(&dir);
        ensure_private_directory(&dir).unwrap();
        let _created = create_private_media_file(&file).unwrap();

        let dir_mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        let file_mode = fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            dir_mode, 0o700,
            "media directories must not be world-accessible"
        );
        assert_eq!(file_mode, 0o600, "media files must not be world-readable");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(unix)]
    fn media_directory_setup_refuses_symlinks() {
        use std::os::unix::fs::symlink;

        let base = std::env::temp_dir().join(format!(
            "lrtmp2-media-symlink-{}-{}",
            std::process::id(),
            unix_millis()
        ));
        let target = base.join("target");
        let link = base.join("stream");
        fs::create_dir_all(&target).unwrap();
        symlink(&target, &link).unwrap();

        let result = ensure_private_directory(&link);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);

        let _ = fs::remove_dir_all(&base);
    }

    // ---- shared helpers -------------------------------------------------

    use crate::db::Stream;
    use crate::keygen::{PREFIX_PLAY_KEY, PREFIX_PUBLISH_KEY, PREFIX_STATS_KEY};
    use serial_test::serial;

    fn key_body(seed: char) -> String {
        std::iter::repeat_n(seed, 32).collect()
    }

    fn add_stream(db: &Db, id: &str, name: &str, seed: char, enabled: bool) -> StreamViewer {
        let stream = Stream {
            id: id.to_string(),
            name: name.to_string(),
            app: "live".to_string(),
            publish_key: format!("{PREFIX_PUBLISH_KEY}{}", key_body(seed)),
            play_key: format!("{PREFIX_PLAY_KEY}{}", key_body(seed)),
            stats_key: format!("{PREFIX_STATS_KEY}{}", key_body(seed)),
            enabled,
            created_at: 0,
        };
        db.stream_add(&stream).unwrap()
    }

    fn test_db() -> Arc<Db> {
        let db = Arc::new(Db::open(":memory:").unwrap());
        add_stream(&db, "s1", "Cam One", 'a', true);
        add_stream(&db, "s2", "cam2", 'b', true);
        db
    }

    fn frame(conn_id: u64, frame_type: FrameType, timestamp: u32, payload: &[u8]) -> RelayFrame {
        RelayFrame {
            frame_type,
            timestamp,
            payload: payload.to_vec(),
            cache_payload: None,
            app: "live".to_string(),
            stream_name: "cam".to_string(),
            publisher_conn_id: conn_id,
        }
    }

    /// Poll `check` until it returns true or `timeout` elapses.
    fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if check() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn files_with_extension(dir: &Path, extension: &str) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .map(|e| e.path())
                    .filter(|p| p.extension().and_then(std::ffi::OsStr::to_str) == Some(extension))
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }

    /// Write an executable fake FFmpeg that records its argv to `<script>.args`,
    /// creates the HLS playlist named by its last argument and then runs `body`
    /// (which decides how stdin is handled).
    #[cfg(unix)]
    fn fake_ffmpeg(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let script = dir.join(name);
        let args_file = dir.join(format!("{name}.args"));
        let text = format!(
            "#!/bin/sh\n\
             printf '%s\\n' \"$*\" > '{args}.tmp' && mv '{args}.tmp' '{args}'\n\
             for last; do :; done\n\
             case \"$last\" in *.m3u8) printf '#EXTM3U\\n' > \"$last\";; esac\n\
             {body}\n",
            args = args_file.display()
        );
        {
            let mut file = File::create(&script).unwrap();
            file.write_all(text.as_bytes()).unwrap();
            file.sync_all().unwrap();
        }
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        script
    }

    // ---- configuration --------------------------------------------------

    #[test]
    fn config_defaults_are_disabled() {
        let config = MediaOutputConfig::default();
        assert!(!config.recording_enabled);
        assert_eq!(config.recording_path, PathBuf::from("/data/recordings"));
        assert_eq!(config.hls_path, PathBuf::from("/data/hls"));
        assert_eq!(config.hls_time_secs, 4);
        assert_eq!(config.hls_list_size, 6);
        assert_eq!(config.hls_segment_type, "fmp4");
        assert!(config.hls_require_key);
        assert_eq!(config.ffmpeg_bin, "ffmpeg");
        assert!(!config.enabled());
        assert!(!config.needs_relay_export());
        assert_eq!(config.export_buffer_bytes(), DEFAULT_QUEUE_MB * 1024 * 1024);
    }

    #[test]
    fn config_enabled_tracks_each_output() {
        let mut config = MediaOutputConfig {
            exec_publish: "true".to_string(),
            ..Default::default()
        };
        assert!(config.enabled());
        assert!(!config.needs_relay_export(), "hooks alone need no relay");
        config.exec_publish.clear();
        config.exec_publish_done = "true".to_string();
        assert!(config.enabled());
        config.exec_publish_done.clear();
        config.push_targets = parse_push_targets("rtmp://example/live/x");
        assert!(config.enabled());
        assert!(config.needs_relay_export());
        config.push_targets.clear();
        config.hls_enabled = true;
        assert!(config.needs_relay_export());
        config.hls_enabled = false;
        config.recording_enabled = true;
        assert!(config.needs_relay_export());
    }

    #[test]
    fn config_apply_parses_valid_values() {
        let mut config = MediaOutputConfig::default();
        config.apply("MEDIA_RECORDING_ENABLED", "yes");
        config.apply("MEDIA_RECORDING_PATH", " /tmp/rec ");
        config.apply("MEDIA_HLS_ENABLED", "ON");
        config.apply("MEDIA_HLS_PATH", "/tmp/hls");
        config.apply("MEDIA_HLS_TIME_SECS", "2");
        config.apply("MEDIA_HLS_LIST_SIZE", "10");
        config.apply("MEDIA_HLS_SEGMENT_TYPE", " MPEGTS ");
        config.apply("MEDIA_HLS_TRANSCODE", "1");
        config.apply("MEDIA_HLS_REQUIRE_KEY", "false");
        config.apply("MEDIA_PUSH_TARGETS", "cam|rtmp://a/live/{stream_id}");
        config.apply("MEDIA_PUSH_TRANSCODE", "true");
        config.apply("MEDIA_EXEC_PUBLISH", "echo start");
        config.apply("MEDIA_EXEC_PUBLISH_DONE", "echo done");
        config.apply("MEDIA_FFMPEG_BIN", " /opt/ffmpeg ");
        config.apply("MEDIA_QUEUE_MB", "64");
        config.apply("MEDIA_UNKNOWN", "ignored");

        assert!(config.recording_enabled);
        assert_eq!(config.recording_path, PathBuf::from("/tmp/rec"));
        assert!(config.hls_enabled);
        assert_eq!(config.hls_path, PathBuf::from("/tmp/hls"));
        assert_eq!(config.hls_time_secs, 2);
        assert_eq!(config.hls_list_size, 10);
        assert_eq!(config.hls_segment_type, "mpegts");
        assert!(config.hls_transcode);
        assert!(!config.hls_require_key);
        assert_eq!(
            config.push_targets,
            vec![PushTarget {
                selector: "cam".to_string(),
                url_template: "rtmp://a/live/{stream_id}".to_string(),
            }]
        );
        assert!(config.push_transcode);
        assert_eq!(config.exec_publish, "echo start");
        assert_eq!(config.exec_publish_done, "echo done");
        assert_eq!(config.ffmpeg_bin, "/opt/ffmpeg");
        assert_eq!(config.queue_mb, 64);
        assert_eq!(config.export_buffer_bytes(), 64 * 1024 * 1024);
    }

    #[test]
    fn config_apply_ignores_or_clamps_invalid_values() {
        let mut config = MediaOutputConfig::default();
        config.apply("MEDIA_RECORDING_ENABLED", "maybe");
        config.apply("MEDIA_RECORDING_PATH", "   ");
        config.apply("MEDIA_HLS_PATH", "");
        config.apply("MEDIA_HLS_TIME_SECS", "abc");
        config.apply("MEDIA_HLS_LIST_SIZE", "-3");
        config.apply("MEDIA_HLS_SEGMENT_TYPE", "webm");
        config.apply("MEDIA_FFMPEG_BIN", " ");
        config.apply("MEDIA_QUEUE_MB", "lots");
        assert!(!config.recording_enabled);
        assert_eq!(config.recording_path, PathBuf::from("/data/recordings"));
        assert_eq!(config.hls_path, PathBuf::from("/data/hls"));
        assert_eq!(config.hls_time_secs, 4);
        assert_eq!(config.hls_list_size, 6);
        assert_eq!(config.hls_segment_type, "fmp4");
        assert_eq!(config.ffmpeg_bin, "ffmpeg");
        assert_eq!(config.queue_mb, DEFAULT_QUEUE_MB);

        config.apply("MEDIA_HLS_TIME_SECS", "0");
        config.apply("MEDIA_HLS_LIST_SIZE", "1000");
        config.apply("MEDIA_QUEUE_MB", "100000");
        assert_eq!(config.hls_time_secs, 1);
        assert_eq!(config.hls_list_size, 100);
        assert_eq!(config.queue_mb, 512);
        config.apply("MEDIA_QUEUE_MB", "0");
        assert_eq!(config.queue_mb, 1);

        config.apply("MEDIA_RECORDING_ENABLED", "true");
        config.apply("MEDIA_RECORDING_ENABLED", "off");
        assert!(!config.recording_enabled);
    }

    #[test]
    fn push_target_parser_rejects_invalid_entries() {
        let targets = parse_push_targets(
            " ; rtmp://default/live ; |rtmp://empty/selector ; cam|http://not/rtmp ; cam|rtmps://ok",
        );
        assert_eq!(
            targets,
            vec![
                PushTarget {
                    selector: "*".to_string(),
                    url_template: "rtmp://default/live".to_string(),
                },
                PushTarget {
                    selector: "cam".to_string(),
                    url_template: "rtmps://ok".to_string(),
                },
            ]
        );
        assert!(!targets[1].matches("id", "other"));
        assert!(targets[1].matches("cam", "other"));
    }

    #[test]
    fn env_lines_skip_comments_and_strip_quotes() {
        assert_eq!(parse_env_line(""), None);
        assert_eq!(parse_env_line("   # comment"), None);
        assert_eq!(parse_env_line("NO_EQUALS"), None);
        assert_eq!(
            parse_env_line(" KEY = value "),
            Some(("KEY".to_string(), "value".to_string()))
        );
        assert_eq!(
            parse_env_line("KEY=\"quoted value\""),
            Some(("KEY".to_string(), "quoted value".to_string()))
        );
        assert_eq!(
            parse_env_line("KEY='single'"),
            Some(("KEY".to_string(), "single".to_string()))
        );
        assert_eq!(
            parse_env_line("KEY=\""),
            Some(("KEY".to_string(), "\"".to_string()))
        );
    }

    #[test]
    #[serial]
    fn config_load_reads_file_then_env_overrides() {
        let dir = tempfile::tempdir().unwrap();
        let env_file = dir.path().join("server.env");
        fs::write(
            &env_file,
            "# media outputs\n\
             MEDIA_RECORDING_ENABLED=true\n\
             MEDIA_RECORDING_PATH=\"/srv/rec\"\n\
             MEDIA_HLS_PATH=/srv/hls\n\
             MEDIA_EXEC_PUBLISH='echo hi'\n\
             MEDIA_FFMPEG_BIN=/usr/bin/ffmpeg\n\
             LRTMP2_API_TOKEN=ignored\n",
        )
        .unwrap();

        // SAFETY: #[serial] keeps env-mutating tests from overlapping, and the
        // variables are removed before the assertions.
        unsafe {
            std::env::set_var("LRTMP2_MEDIA_HLS_PATH", "/env/hls");
            std::env::set_var("LRTMP2_MEDIA_EXEC_PUBLISH", "");
            std::env::set_var("LRTMP2_MEDIA_FFMPEG_BIN", "");
            std::env::set_var("LRTMP2_MEDIA_QUEUE_MB", "8");
        }
        let config = MediaOutputConfig::load(env_file.to_str().unwrap());
        unsafe {
            std::env::remove_var("LRTMP2_MEDIA_HLS_PATH");
            std::env::remove_var("LRTMP2_MEDIA_EXEC_PUBLISH");
            std::env::remove_var("LRTMP2_MEDIA_FFMPEG_BIN");
            std::env::remove_var("LRTMP2_MEDIA_QUEUE_MB");
        }

        assert!(config.recording_enabled);
        assert_eq!(config.recording_path, PathBuf::from("/srv/rec"));
        assert_eq!(config.hls_path, PathBuf::from("/env/hls"));
        assert_eq!(config.exec_publish, "", "empty env clears hooks");
        assert_eq!(
            config.ffmpeg_bin, "/usr/bin/ffmpeg",
            "empty env must not clear non-clearable keys"
        );
        assert_eq!(config.queue_mb, 8);

        let missing = MediaOutputConfig::load(dir.path().join("absent.env").to_str().unwrap());
        assert!(!missing.recording_enabled);
        let empty = MediaOutputConfig::load("");
        assert_eq!(empty.hls_path, PathBuf::from("/data/hls"));
    }

    // ---- sinks and queues -----------------------------------------------

    #[test]
    fn sink_sender_disables_when_byte_limit_exceeded() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let mut sink = make_sink("limit".to_string(), 8, move |_rx, _queued, _failed| {
            let _ = release_rx.recv();
        });
        sink.try_send(Arc::new(vec![0; 4]));
        assert!(sink.tx.is_some());
        assert_eq!(sink.queued_bytes.load(Ordering::Acquire), 4);
        sink.try_send(Arc::new(vec![0; 5]));
        assert!(sink.tx.is_none());
        assert!(sink.failed.load(Ordering::Acquire));
        assert_eq!(sink.queued_bytes.load(Ordering::Acquire), 4);
        // Further sends are dropped silently once disabled.
        sink.try_send(Arc::new(vec![0; 1]));
        sink.disable("again");
        drop(release_tx);
        sink.stop();
    }

    #[test]
    fn sink_sender_disables_when_message_queue_full() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let mut sink = make_sink(
            "full".to_string(),
            usize::MAX,
            move |rx, _queued, _failed| {
                let _ = release_rx.recv();
                drop(rx);
            },
        );
        for _ in 0..SINK_QUEUE_MESSAGES {
            sink.try_send(Arc::new(vec![1]));
        }
        assert!(sink.tx.is_some());
        sink.try_send(Arc::new(vec![1]));
        assert!(sink.tx.is_none());
        assert!(sink.failed.load(Ordering::Acquire));
        assert_eq!(
            sink.queued_bytes.load(Ordering::Acquire),
            SINK_QUEUE_MESSAGES
        );
        drop(release_tx);
        sink.stop();
    }

    #[test]
    fn sink_sender_disables_when_worker_exited() {
        let mut sink = make_sink("exit".to_string(), usize::MAX, |rx, _queued, _failed| {
            drop(rx);
        });
        let worker_done = wait_until(Duration::from_secs(5), || {
            sink.worker.as_ref().is_some_and(|w| w.is_finished())
        });
        assert!(worker_done);
        sink.try_send(Arc::new(vec![1, 2, 3]));
        assert!(sink.tx.is_none());
        assert!(sink.failed.load(Ordering::Acquire));
        assert_eq!(sink.queued_bytes.load(Ordering::Acquire), 0);
        sink.stop();
    }

    #[test]
    fn sink_stop_tolerates_panicked_and_missing_workers() {
        let sink = make_sink("panic".to_string(), 16, |_rx, _queued, _failed| {
            panic!("worker panic for coverage");
        });
        sink.stop();

        let mut sink = make_sink("none".to_string(), 16, |_rx, _queued, _failed| {});
        if let Some(worker) = sink.worker.take() {
            worker.join().unwrap();
        }
        sink.stop();
    }

    #[test]
    fn consume_queue_writes_until_disconnected() {
        let (tx, rx) = mpsc::sync_channel(4);
        let queued = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicBool::new(false));
        tx.send(Arc::new(vec![1, 2])).unwrap();
        queued.fetch_add(2, Ordering::AcqRel);
        let sender = thread::spawn(move || {
            // Arrives after at least one receive timeout.
            thread::sleep(Duration::from_millis(150));
            tx.send(Arc::new(vec![3])).unwrap();
        });
        queued.fetch_add(1, Ordering::AcqRel);
        let mut written = Vec::new();
        consume_queue(rx, Arc::clone(&queued), failed, |tag| {
            written.extend_from_slice(tag);
            Ok(())
        })
        .unwrap();
        sender.join().unwrap();
        assert_eq!(written, vec![1, 2, 3]);
        assert_eq!(queued.load(Ordering::Acquire), 0);
    }

    #[test]
    fn consume_queue_propagates_write_errors() {
        let (tx, rx) = mpsc::sync_channel(4);
        let queued = Arc::new(AtomicUsize::new(3));
        tx.send(Arc::new(vec![1, 2, 3])).unwrap();
        let err = consume_queue(rx, Arc::clone(&queued), Arc::default(), |_| {
            Err(io::Error::other("disk full"))
        })
        .unwrap_err();
        assert_eq!(err.to_string(), "disk full");
        assert_eq!(queued.load(Ordering::Acquire), 0);
    }

    #[test]
    fn consume_queue_stops_and_resets_when_failed() {
        // Failure raised by the writer stops after the current tag.
        let (tx, rx) = mpsc::sync_channel(4);
        let queued = Arc::new(AtomicUsize::new(2));
        let failed = Arc::new(AtomicBool::new(false));
        tx.send(Arc::new(vec![1])).unwrap();
        tx.send(Arc::new(vec![2])).unwrap();
        let writer_failed = Arc::clone(&failed);
        let mut writes = 0;
        consume_queue(rx, Arc::clone(&queued), Arc::clone(&failed), |_| {
            writes += 1;
            writer_failed.store(true, Ordering::Release);
            Ok(())
        })
        .unwrap();
        assert_eq!(writes, 1);
        assert_eq!(queued.load(Ordering::Acquire), 0);

        // Already failed: nothing is written and the byte counter is cleared.
        let (tx, rx) = mpsc::sync_channel(4);
        tx.send(Arc::new(vec![9])).unwrap();
        let queued = Arc::new(AtomicUsize::new(1));
        let failed = Arc::new(AtomicBool::new(true));
        consume_queue(rx, Arc::clone(&queued), failed, |_| {
            panic!("must not write after failure")
        })
        .unwrap();
        assert_eq!(queued.load(Ordering::Acquire), 0);
    }

    #[test]
    fn consume_queue_drops_tag_received_after_failure() {
        let (tx, rx) = mpsc::sync_channel(4);
        let queued = Arc::new(AtomicUsize::new(1));
        let failed = Arc::new(AtomicBool::new(false));
        let setter_failed = Arc::clone(&failed);
        let sender = thread::spawn(move || {
            // Let the consumer block in recv_timeout, then fail and deliver.
            thread::sleep(Duration::from_millis(30));
            setter_failed.store(true, Ordering::Release);
            let _ = tx.send(Arc::new(vec![7]));
        });
        consume_queue(rx, Arc::clone(&queued), failed, |_| {
            panic!("must not write after failure")
        })
        .unwrap();
        sender.join().unwrap();
        assert_eq!(queued.load(Ordering::Acquire), 0);
    }

    // ---- HLS directory housekeeping --------------------------------------

    #[test]
    fn clean_hls_dir_removes_only_owned_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("session");
        fs::create_dir_all(dir.join("nested")).unwrap();
        for name in [
            "index.m3u8",
            "index.m3u8.tmp",
            "init.mp4",
            "segment_000001.m4s",
            "segment_000002.ts",
            "segment_000003.txt",
            "notes.txt",
        ] {
            fs::write(dir.join(name), b"x").unwrap();
        }
        clean_hls_dir(&dir).unwrap();
        let mut left: Vec<String> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, vec!["nested", "notes.txt", "segment_000003.txt"]);

        // A missing directory is created instead.
        let fresh = tmp.path().join("fresh");
        clean_hls_dir(&fresh).unwrap();
        assert!(fresh.is_dir());
    }

    #[test]
    fn clean_older_hls_sessions_keeps_current_and_newer() {
        let tmp = tempfile::tempdir().unwrap();
        let stream_dir = tmp.path();
        for name in ["session-001", "session-002", "session-003", "other"] {
            fs::create_dir_all(stream_dir.join(name)).unwrap();
        }
        fs::write(stream_dir.join("session-000-file"), b"x").unwrap();
        clean_older_hls_sessions(stream_dir, &stream_dir.join("session-002")).unwrap();
        assert!(!stream_dir.join("session-001").exists());
        assert!(stream_dir.join("session-002").is_dir());
        assert!(stream_dir.join("session-003").is_dir());
        assert!(stream_dir.join("other").is_dir());
        assert!(stream_dir.join("session-000-file").is_file());

        // A current path without a file name is a no-op.
        clean_older_hls_sessions(stream_dir, Path::new("/")).unwrap();
        assert!(stream_dir.join("session-003").is_dir());
        assert!(clean_older_hls_sessions(&stream_dir.join("absent"), Path::new("x")).is_err());
    }

    #[test]
    fn hls_session_dir_names_sort_by_creation() {
        let first = hls_session_dir_name(1, 2);
        let second = hls_session_dir_name(1, 2);
        assert!(first.starts_with("session-"));
        assert!(first < second);
        assert_eq!(safe_component(&first), first);
    }

    #[test]
    fn ffmpeg_codec_args_follow_transcode_flag() {
        let args = |transcode| {
            let mut cmd = Command::new("ffmpeg");
            add_ffmpeg_input(&mut cmd);
            add_codec_args(&mut cmd, transcode);
            cmd.get_args()
                .map(|a| a.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(" ")
        };
        let copy = args(false);
        assert!(copy.starts_with("-hide_banner -loglevel warning -f flv -i pipe:0"));
        assert!(copy.ends_with("-c copy"));
        let transcode = args(true);
        assert!(transcode.contains("-c:v libx264"));
        assert!(transcode.contains("-c:a aac"));
    }

    #[test]
    fn flv_tag_maps_frame_types_and_rejects_oversized_payloads() {
        assert_eq!(flv_tag(FrameType::Audio, 0, &[]).unwrap()[0], 8);
        assert_eq!(flv_tag(FrameType::Script, 0, &[]).unwrap()[0], 18);
        assert_eq!(flv_tag(FrameType::Metadata, 0, &[]).unwrap()[0], 18);
        assert!(flv_tag(FrameType::Video, 0, &vec![0; MAX_FLV_PAYLOAD + 1]).is_none());
    }

    // ---- manager / recording --------------------------------------------

    #[test]
    fn disabled_manager_ignores_publishers_and_frames() {
        let mut manager = MediaOutputManager::new(MediaOutputConfig::default(), test_db());
        assert!(!manager.enabled());
        manager.ensure_publisher(1, "s1", 1);
        manager.handle_frame(&frame(1, FrameType::Video, 0, &[1]), "s1", 1);
        assert!(manager.sessions.is_empty());
        manager.stop_all();
    }

    #[test]
    fn recording_sink_writes_flv_end_to_end() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("recordings");
        let config = MediaOutputConfig {
            recording_enabled: true,
            recording_path: root.clone(),
            ..Default::default()
        };
        let mut manager = MediaOutputManager::new(config, test_db());
        assert!(manager.enabled());
        assert!(root.is_dir(), "recording root is created and hardened");

        // Empty or unknown stream ids never create sessions.
        manager.ensure_publisher(7, "", 1);
        manager.handle_frame(&frame(7, FrameType::Video, 0, &[1]), "", 1);
        manager.ensure_publisher(7, "missing", 1);
        manager.handle_frame(&frame(7, FrameType::Video, 0, &[1]), "missing", 1);
        assert!(manager.sessions.is_empty());

        manager.ensure_publisher(7, "s1", 1);
        manager.ensure_publisher(7, "s1", 1); // same route and generation: no-op
        assert_eq!(manager.sessions.len(), 1);
        let frames = [
            frame(7, FrameType::Script, 0, b"meta"),
            frame(7, FrameType::Video, 0, &[0x17, 0, 0, 0, 0]),
            frame(7, FrameType::Audio, 20, &[0xaf, 0x01, 0x21]),
            // Oversized payloads are skipped rather than written.
            frame(7, FrameType::Video, 40, &vec![0; MAX_FLV_PAYLOAD + 1]),
            frame(7, FrameType::Video, 40, &[0x27, 1, 0, 0, 0, 9]),
        ];
        for f in &frames {
            manager.handle_frame(f, "s1", 1);
        }
        assert_eq!(manager.sessions[&7].last_timestamp, Some(40));
        manager.stop_all();
        assert!(manager.sessions.is_empty());

        let files = files_with_extension(&root.join("s1"), "flv");
        assert_eq!(files.len(), 1, "one recording per publish session");
        let data = fs::read(&files[0]).unwrap();
        assert!(data.starts_with(flv_header()));
        let mut expected = flv_header().to_vec();
        for f in frames.iter().filter(|f| f.payload.len() <= MAX_FLV_PAYLOAD) {
            expected.extend(flv_tag(f.frame_type, f.timestamp, &f.payload).unwrap());
        }
        assert_eq!(data, expected);
    }

    #[test]
    fn manager_restarts_sessions_on_route_generation_and_timestamp_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("recordings");
        let config = MediaOutputConfig {
            recording_enabled: true,
            recording_path: root.clone(),
            ..Default::default()
        };
        let mut manager = MediaOutputManager::new(config, test_db());
        let video = |ts| frame(3, FrameType::Video, ts, &[0x17, 0]);
        // Recording file names are millisecond timestamps; keep sessions apart.
        let next_ms = || thread::sleep(Duration::from_millis(3));

        let current_file = |m: &MediaOutputManager| m.sessions[&3].recording_file.clone();

        // handle_frame starts a session on its own.
        manager.handle_frame(&video(u32::MAX - 5), "s1", 1);
        assert_eq!(manager.sessions[&3].stream_id, "s1");
        let first = current_file(&manager);

        // u32 wraparound and small backwards jitter are continuations.
        manager.handle_frame(&video(10), "s1", 1);
        manager.handle_frame(&video(5), "s1", 1);
        manager.handle_frame(&video(5_000), "s1", 1);
        assert_eq!(current_file(&manager), first);

        // A large backwards jump is a same-connection republish.
        next_ms();
        manager.handle_frame(&video(0), "s1", 1);
        assert_ne!(current_file(&manager), first);

        // A new generation replaces the session too.
        next_ms();
        manager.ensure_publisher(3, "s1", 2);
        assert_eq!(manager.sessions[&3].generation, 2);

        // Switching the route retires the old stream's session.
        manager.handle_frame(&video(100), "s2", 2);
        assert_eq!(manager.sessions[&3].stream_id, "s2");

        // Publishers that are no longer live are retired.
        manager.retain_publishers(&HashSet::from([3]));
        assert_eq!(manager.sessions.len(), 1);
        manager.retain_publishers(&HashSet::new());
        assert!(manager.sessions.is_empty());
        manager.stop_all();

        assert_eq!(files_with_extension(&root.join("s1"), "flv").len(), 3);
        let s2 = files_with_extension(&root.join("s2"), "flv");
        assert_eq!(s2.len(), 1);
        assert!(fs::read(&s2[0]).unwrap().starts_with(flv_header()));
    }

    #[test]
    fn manager_aborts_sessions_when_reaper_unavailable() {
        let tmp = tempfile::tempdir().unwrap();
        let config = MediaOutputConfig {
            recording_enabled: true,
            recording_path: tmp.path().join("rec"),
            ..Default::default()
        };
        let mut manager = MediaOutputManager::new(config, test_db());

        // No reaper: sessions are cancelled inline.
        let saved_tx = manager.retire_tx.take();
        manager.ensure_publisher(1, "s1", 1);
        manager.ensure_publisher(1, "s1", 2);
        assert_eq!(manager.sessions[&1].generation, 2);

        // Retirement queue full (rendezvous channel nobody receives on).
        let (full_tx, _full_rx) = mpsc::sync_channel(0);
        manager.retire_tx = Some(full_tx);
        manager.ensure_publisher(1, "s1", 3);
        assert_eq!(manager.sessions[&1].generation, 3);

        // Reaper disconnected.
        let (gone_tx, gone_rx) = mpsc::sync_channel(1);
        drop(gone_rx);
        manager.retire_tx = Some(gone_tx);
        manager.ensure_publisher(1, "s1", 4);
        assert_eq!(manager.sessions[&1].generation, 4);

        drop(saved_tx);
        manager.stop_all();
    }

    #[test]
    fn manager_warns_when_media_roots_cannot_be_hardened() {
        let tmp = tempfile::tempdir().unwrap();
        let not_a_dir = tmp.path().join("file");
        fs::write(&not_a_dir, b"x").unwrap();
        let config = MediaOutputConfig {
            recording_enabled: true,
            recording_path: not_a_dir.clone(),
            hls_enabled: true,
            hls_path: not_a_dir.clone(),
            ffmpeg_bin: tmp.path().join("no-ffmpeg").display().to_string(),
            ..Default::default()
        };
        let mut manager = MediaOutputManager::new(config, test_db());
        manager.ensure_publisher(1, "s1", 1);
        // Both workers fail to create their directories under a regular file.
        let failed = wait_until(Duration::from_secs(5), || {
            manager.sessions[&1]
                .sinks
                .iter()
                .all(|s| s.failed.load(Ordering::Acquire))
        });
        assert!(failed);
        manager.handle_frame(&frame(1, FrameType::Video, 0, &[1]), "s1", 1);
        assert!(manager.sessions[&1].sinks.iter().all(|s| s.tx.is_none()));
        manager.stop_all();
        assert!(not_a_dir.is_file());
    }

    #[test]
    #[cfg(unix)]
    fn harden_media_tree_restricts_existing_entries_and_skips_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        fs::create_dir_all(root.join("a/b")).unwrap();
        fs::write(root.join("a/b/file.flv"), b"x").unwrap();
        fs::set_permissions(root.join("a"), fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(root.join("a/b/file.flv"), fs::Permissions::from_mode(0o644)).unwrap();
        symlink(tmp.path(), root.join("link")).unwrap();
        harden_media_tree(&root).unwrap();
        let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&root.join("a")), 0o700);
        assert_eq!(mode(&root.join("a/b")), 0o700);
        assert_eq!(mode(&root.join("a/b/file.flv")), 0o600);
        assert_eq!(mode(tmp.path()) & 0o700, 0o700);
    }

    // ---- exec hooks -------------------------------------------------------

    #[cfg(unix)]
    fn read_env_dump(path: &Path) -> HashMap<String, String> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[cfg(unix)]
    fn dump_env_command(target: &Path) -> String {
        format!(
            "env | grep '^OPENRTMP_' > '{p}.tmp' && mv '{p}.tmp' '{p}'",
            p = target.display()
        )
    }

    #[test]
    #[cfg(unix)]
    fn exec_hooks_receive_publish_environment() {
        let tmp = tempfile::tempdir().unwrap();
        let publish_env = tmp.path().join("publish.env");
        let done_env = tmp.path().join("done.env");
        let rec_root = tmp.path().join("rec");
        let config = MediaOutputConfig {
            recording_enabled: true,
            recording_path: rec_root.clone(),
            // The publish hook keeps running until the session stops it.
            exec_publish: format!("{}; exec sleep 30", dump_env_command(&publish_env)),
            exec_publish_done: dump_env_command(&done_env),
            ..Default::default()
        };
        let mut manager = MediaOutputManager::new(config, test_db());
        manager.ensure_publisher(42, "s1", 1);
        assert!(manager.sessions[&42].publish_exec.is_some());
        assert!(wait_until(Duration::from_secs(5), || publish_env.exists()));

        let started = Instant::now();
        manager.stop_all();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "long-running publish hook is terminated on stop"
        );

        let env = read_env_dump(&publish_env);
        assert_eq!(env["OPENRTMP_EVENT"], "publish");
        assert_eq!(env["OPENRTMP_STREAM_ID"], "s1");
        assert_eq!(env["OPENRTMP_STREAM_NAME"], "Cam One");
        assert_eq!(env["OPENRTMP_APP"], "live");
        assert_eq!(env["OPENRTMP_PUBLISHER_CONN_ID"], "42");
        let recording = PathBuf::from(&env["OPENRTMP_RECORDING_FILE"]);
        assert!(recording.starts_with(rec_root.join("s1")));
        assert_eq!(
            env.get("OPENRTMP_HLS_PLAYLIST").map(String::as_str),
            Some("")
        );

        assert!(wait_until(Duration::from_secs(5), || done_env.exists()));
        let done = read_env_dump(&done_env);
        assert_eq!(done["OPENRTMP_EVENT"], "publish_done");
        assert_eq!(done["OPENRTMP_STREAM_ID"], "s1");
        assert_eq!(done["OPENRTMP_RECORDING_FILE"], recording.to_string_lossy());
    }

    #[test]
    #[cfg(unix)]
    fn hook_only_sessions_start_without_relay_sinks() {
        let tmp = tempfile::tempdir().unwrap();
        let marker = tmp.path().join("hook.env");
        let config = MediaOutputConfig {
            exec_publish: dump_env_command(&marker),
            ..Default::default()
        };
        let mut manager = MediaOutputManager::new(config, test_db());
        manager.ensure_publisher(9, "s2", 1);
        assert!(manager.sessions[&9].sinks.is_empty());
        assert!(wait_until(Duration::from_secs(5), || marker.exists()));
        let env = read_env_dump(&marker);
        assert_eq!(env["OPENRTMP_STREAM_NAME"], "cam2");
        assert_eq!(env["OPENRTMP_RECORDING_FILE"], "");
        // Frames still flow through without sinks.
        manager.handle_frame(&frame(9, FrameType::Audio, 0, &[1]), "s2", 1);
        manager.stop_all();
    }

    #[test]
    #[cfg(unix)]
    fn terminate_hook_child_escalates_to_sigkill() {
        let env = ExecEnv {
            conn_id: 1,
            stream_id: "s1",
            stream_name: "cam",
            app: "live",
            recording_file: None,
            hls_playlist: None,
        };
        // Ignoring SIGTERM (inherited by `sleep`) forces the SIGKILL path.
        let mut child = spawn_hook("trap '' TERM; sleep 30", "publish", &env).unwrap();
        thread::sleep(Duration::from_millis(50));
        let started = Instant::now();
        terminate_hook_child(&mut child);
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(child.try_wait().unwrap().is_some());

        // An already-exited hook is simply reaped.
        let mut done = spawn_hook("exit 0", "publish", &env).unwrap();
        assert!(wait_until(Duration::from_secs(5), || {
            done.try_wait().ok().flatten().is_some()
        }));
        terminate_hook_child(&mut done);
    }

    #[test]
    #[cfg(unix)]
    fn aborting_a_session_terminates_its_publish_hook() {
        let tmp = tempfile::tempdir().unwrap();
        let config = MediaOutputConfig {
            exec_publish: "exec sleep 30".to_string(),
            recording_enabled: true,
            recording_path: tmp.path().join("rec"),
            ..Default::default()
        };
        let session = MediaSession::start(5, 1, "s1", "cam", "live", &config);
        assert!(session.publish_exec.is_some());
        assert_eq!(session.sinks.len(), 1);
        let started = Instant::now();
        session.abort("test");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    // ---- FFmpeg-backed sinks ---------------------------------------------

    #[test]
    #[cfg(unix)]
    fn hls_sink_runs_ffmpeg_and_replaces_older_sessions() {
        let tmp = tempfile::tempdir().unwrap();
        let hls_root = tmp.path().join("hls");
        let stream_dir = hls_root.join("s1");
        fs::create_dir_all(stream_dir.join("session-0")).unwrap();
        fs::create_dir_all(stream_dir.join("keep")).unwrap();
        let ffmpeg = fake_ffmpeg(tmp.path(), "ffmpeg-hls", "exec cat > /dev/null");
        let config = MediaOutputConfig {
            hls_enabled: true,
            hls_path: hls_root.clone(),
            ffmpeg_bin: ffmpeg.display().to_string(),
            hls_time_secs: 2,
            hls_list_size: 3,
            ..Default::default()
        };
        let mut manager = MediaOutputManager::new(config, test_db());
        manager.ensure_publisher(11, "s1", 4);
        let playlist = manager.sessions[&11].hls_playlist.clone().unwrap();
        assert!(playlist.starts_with(&stream_dir));
        assert!(wait_until(Duration::from_secs(5), || playlist.exists()));
        for ts in [0, 40, 80] {
            manager.handle_frame(&frame(11, FrameType::Video, ts, &[0x17, 0, 1]), "s1", 4);
        }
        manager.stop_all();

        assert!(!stream_dir.join("session-0").exists());
        assert!(stream_dir.join("keep").is_dir());
        let args = fs::read_to_string(tmp.path().join("ffmpeg-hls.args")).unwrap();
        assert!(args.contains("-f flv -i pipe:0"), "{args}");
        assert!(args.contains("-c copy"), "{args}");
        assert!(args.contains("-hls_time 2 -hls_list_size 3"), "{args}");
        assert!(args.contains("-hls_segment_type fmp4"), "{args}");
        assert!(args.contains("segment_%06d.m4s"), "{args}");
        assert!(args.trim_end().ends_with("index.m3u8"), "{args}");
    }

    #[test]
    #[cfg(unix)]
    fn hls_sink_supports_mpegts_and_transcoding() {
        let tmp = tempfile::tempdir().unwrap();
        let ffmpeg = fake_ffmpeg(tmp.path(), "ffmpeg-ts", "exec cat > /dev/null");
        let config = MediaOutputConfig {
            hls_enabled: true,
            hls_path: tmp.path().join("hls"),
            hls_segment_type: "mpegts".to_string(),
            hls_transcode: true,
            ffmpeg_bin: ffmpeg.display().to_string(),
            ..Default::default()
        };
        let mut sinks = Vec::new();
        let playlist = setup_hls_sink(&config, "s1", 1, 1, "s1", 1024 * 1024, &mut sinks).unwrap();
        assert_eq!(sinks.len(), 1);
        assert!(wait_until(Duration::from_secs(5), || playlist.exists()));
        for sink in sinks {
            sink.stop();
        }
        let args = fs::read_to_string(tmp.path().join("ffmpeg-ts.args")).unwrap();
        assert!(args.contains("-c:v libx264"), "{args}");
        assert!(args.contains("segment_%06d.ts"), "{args}");
        assert!(!args.contains("-hls_segment_type"), "{args}");

        let disabled = MediaOutputConfig::default();
        let mut none = Vec::new();
        assert!(setup_hls_sink(&disabled, "s1", 1, 1, "s1", 1, &mut none).is_none());
        assert!(setup_recording_sink(&disabled, "s1", 1, &mut none).is_none());
        assert!(none.is_empty());
    }

    #[test]
    #[cfg(unix)]
    fn push_sinks_render_matching_targets() {
        let tmp = tempfile::tempdir().unwrap();
        let ffmpeg = fake_ffmpeg(tmp.path(), "ffmpeg-push", "exec cat > /dev/null");
        let config = MediaOutputConfig {
            push_targets: vec![
                PushTarget {
                    selector: "s1".to_string(),
                    url_template: "rtmp://example/{app}/{stream_name}".to_string(),
                },
                PushTarget {
                    selector: "other".to_string(),
                    url_template: "rtmp://unused/live".to_string(),
                },
                PushTarget {
                    selector: "*".to_string(),
                    url_template: "{app}/not-rtmp".to_string(),
                },
            ],
            push_transcode: true,
            ffmpeg_bin: ffmpeg.display().to_string(),
            ..Default::default()
        };
        let mut manager = MediaOutputManager::new(config, test_db());
        manager.ensure_publisher(2, "s1", 1);
        assert_eq!(manager.sessions[&2].sinks.len(), 1);
        assert_eq!(manager.sessions[&2].sinks[0].label, "push#0:s1");
        let args_file = tmp.path().join("ffmpeg-push.args");
        assert!(wait_until(Duration::from_secs(5), || args_file.exists()));
        manager.handle_frame(&frame(2, FrameType::Audio, 0, &[0xaf, 0]), "s1", 1);
        manager.stop_all();
        let args = fs::read_to_string(&args_file).unwrap();
        assert!(args.contains("-c:v libx264"), "{args}");
        assert!(
            args.trim_end()
                .ends_with("-f flv rtmp://example/live/Cam%20One"),
            "{args}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn push_sink_fails_when_ffmpeg_cannot_start() {
        let tmp = tempfile::tempdir().unwrap();
        let config = MediaOutputConfig {
            ffmpeg_bin: tmp.path().join("missing-ffmpeg").display().to_string(),
            ..Default::default()
        };
        let mut sink = spawn_push_sink(&config, "rtmp://x/live".to_string(), 1024, "p".into());
        assert!(wait_until(Duration::from_secs(5), || {
            sink.failed.load(Ordering::Acquire)
        }));
        sink.try_send(Arc::new(vec![1]));
        assert!(sink.tx.is_none());
        sink.stop();
    }

    #[test]
    #[cfg(unix)]
    fn ffmpeg_worker_fails_when_encoder_exits_early() {
        let tmp = tempfile::tempdir().unwrap();
        let ffmpeg = fake_ffmpeg(tmp.path(), "ffmpeg-exit", "exit 1");
        let config = MediaOutputConfig {
            ffmpeg_bin: ffmpeg.display().to_string(),
            ..Default::default()
        };
        let mut sink =
            spawn_push_sink(&config, "rtmp://x/live".to_string(), usize::MAX, "e".into());
        // Keep feeding until the broken pipe is noticed by the worker.
        let failed = wait_until(Duration::from_secs(10), || {
            sink.try_send(Arc::new(vec![0; 4096]));
            sink.failed.load(Ordering::Acquire)
        });
        assert!(failed);
        sink.stop();
    }

    #[test]
    #[cfg(unix)]
    fn ffmpeg_worker_kills_stalled_encoder_when_sink_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        // Never reads stdin: the worker blocks once the pipe buffer fills.
        let ffmpeg = fake_ffmpeg(tmp.path(), "ffmpeg-stall", "exec sleep 30");
        let config = MediaOutputConfig {
            ffmpeg_bin: ffmpeg.display().to_string(),
            ..Default::default()
        };
        let mut sink = spawn_push_sink(&config, "rtmp://x/live".to_string(), 1 << 20, "s".into());
        let chunk = Arc::new(vec![0u8; 64 * 1024]);
        let disabled = wait_until(Duration::from_secs(10), || {
            sink.try_send(Arc::clone(&chunk));
            sink.tx.is_none()
        });
        assert!(disabled, "byte limit disables the stalled sink");
        let started = Instant::now();
        sink.stop();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "monitor kills the stalled encoder"
        );
    }

    #[test]
    fn wait_child_bounded_terminates_overrunning_child() {
        let mut child = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        wait_child_bounded(&mut child, Duration::from_millis(50));
        assert!(child.try_wait().unwrap().is_some());
    }

    // ---- HLS HTTP serving -------------------------------------------------

    use axum::http::Request;
    use tower::ServiceExt;

    const SESSION_OLD: &str = "session-00000000000000000001-a";
    const SESSION_NEW: &str = "session-00000000000000000002-b";
    const PLAYLIST: &str = "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4,\nsegment_000001.m4s\n";

    struct HlsFixture {
        _tmp: tempfile::TempDir,
        root: PathBuf,
        db: Arc<Db>,
        key_s1: String,
        key_s2: String,
        key_disabled: String,
    }

    fn hls_fixture() -> HlsFixture {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("hls");
        let s1 = root.join("s1");
        fs::create_dir_all(s1.join(SESSION_OLD)).unwrap();
        fs::create_dir_all(s1.join(SESSION_NEW)).unwrap();
        // Not a safe component: must never be chosen as the active session.
        fs::create_dir_all(s1.join("session-zz bad")).unwrap();
        fs::write(s1.join("session-zzz-file"), b"x").unwrap();
        let session = s1.join(SESSION_NEW);
        fs::write(session.join("index.m3u8"), PLAYLIST).unwrap();
        fs::write(session.join("segment_000001.m4s"), b"SEGMENT").unwrap();
        fs::write(session.join("init.mp4"), b"INIT").unwrap();
        fs::create_dir_all(root.join("s2")).unwrap();

        let db = Arc::new(Db::open(":memory:").unwrap());
        let key_s1 = add_stream(&db, "s1", "cam", 'a', true).play_key;
        let key_s2 = add_stream(&db, "s2", "cam2", 'b', true).play_key;
        let key_disabled = add_stream(&db, "s3", "cam3", 'c', false).play_key;
        HlsFixture {
            _tmp: tmp,
            root,
            db,
            key_s1,
            key_s2,
            key_disabled,
        }
    }

    fn hls_app(fx: &HlsFixture, require_key: bool) -> Router {
        hls_router(
            fx.root.clone(),
            Arc::clone(&fx.db),
            require_key,
            4,
            Vec::new(),
            None,
        )
    }

    async fn hls_get(app: &Router, uri: &str) -> (StatusCode, HeaderMap, Vec<u8>) {
        let mut request = Request::builder().uri(uri).body(Body::empty()).unwrap();
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo(SocketAddr::from((
                [192, 0, 2, 10],
                40000,
            ))));
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, headers, body)
    }

    #[tokio::test]
    async fn hls_requires_valid_enabled_play_key_for_stream() {
        let fx = hls_fixture();
        let app = hls_app(&fx, true);
        let (status, _, _) = hls_get(&app, "/hls/s1/index.m3u8").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _, _) = hls_get(&app, "/hls/s1/index.m3u8?key=nope").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let wrong = format!("/hls/s1/index.m3u8?key={}", fx.key_s2);
        assert_eq!(hls_get(&app, &wrong).await.0, StatusCode::FORBIDDEN);
        let disabled = format!("/hls/s3/index.m3u8?key={}", fx.key_disabled);
        assert_eq!(hls_get(&app, &disabled).await.0, StatusCode::FORBIDDEN);
        let (status, _, _) = hls_get(&app, "/hls/bad%20id/index.m3u8").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn hls_redirects_to_latest_session_and_rewrites_playlist() {
        let fx = hls_fixture();
        let app = hls_app(&fx, true);
        let key = &fx.key_s1;

        let (status, headers, _) = hls_get(&app, &format!("/hls/s1/index.m3u8?key={key}")).await;
        assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
        let location = headers[header::LOCATION].to_str().unwrap();
        assert_eq!(
            location,
            format!("/hls/s1/{SESSION_NEW}/index.m3u8?key={key}")
        );

        let (status, headers, body) = hls_get(&app, location).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers[header::CONTENT_TYPE],
            "application/vnd.apple.mpegurl"
        );
        assert_eq!(
            headers[header::CACHE_CONTROL],
            "no-cache, no-store, must-revalidate"
        );
        assert_eq!(headers[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        let text = String::from_utf8(body).unwrap();
        assert!(
            text.contains(&format!("URI=\"init.mp4?key={key}\"")),
            "{text}"
        );
        assert!(
            text.contains(&format!("segment_000001.m4s?key={key}")),
            "{text}"
        );

        let segment = format!("/hls/s1/{SESSION_NEW}/segment_000001.m4s?key={key}");
        let (status, headers, body) = hls_get(&app, &segment).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], "video/iso.segment");
        assert_eq!(body, b"SEGMENT");

        let init = format!("/hls/s1/{SESSION_NEW}/init.mp4?key={key}");
        let (status, headers, body) = hls_get(&app, &init).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], "video/mp4");
        assert_eq!(body, b"INIT");

        // The same client renews its slot instead of consuming another.
        assert_eq!(
            hls_get(&app, &segment).await.0,
            StatusCode::OK,
            "renewal keeps the viewer admitted"
        );
    }

    #[tokio::test]
    async fn hls_without_key_requirement_serves_unmodified_playlists() {
        let fx = hls_fixture();
        let app = hls_app(&fx, false);
        let (status, headers, _) = hls_get(&app, "/hls/s1/index.m3u8").await;
        assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            headers[header::LOCATION],
            format!("/hls/s1/{SESSION_NEW}/index.m3u8").as_str()
        );
        let (status, _, body) = hls_get(
            &app,
            &format!("/hls/s1/{SESSION_NEW}/index.m3u8?key=anything"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, PLAYLIST.as_bytes());
    }

    #[tokio::test]
    async fn hls_rejects_traversal_and_reports_missing_files() {
        let fx = hls_fixture();
        let app = hls_app(&fx, false);
        for uri in [
            "/hls/s1/../s2/index.m3u8",
            "/hls/s1/%2E%2E/secret.m3u8",
            &format!("/hls/s1/{SESSION_NEW}/index.html"),
            &format!("/hls/s1/{SESSION_NEW}/.hidden/../x.ts"),
        ] {
            let status = hls_get(&app, uri).await.0;
            assert!(
                matches!(status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND),
                "{uri}: {status}"
            );
        }
        assert_eq!(
            hls_get(&app, "/hls/s1/x/../../s2/a.ts").await.0,
            StatusCode::BAD_REQUEST
        );
        let missing = format!("/hls/s1/{SESSION_NEW}/segment_999999.m4s");
        assert_eq!(hls_get(&app, &missing).await.0, StatusCode::NOT_FOUND);
        let missing_playlist = format!("/hls/s1/{SESSION_OLD}/index.m3u8");
        assert_eq!(
            hls_get(&app, &missing_playlist).await.0,
            StatusCode::NOT_FOUND
        );
        // Stream directory without sessions, and no directory at all.
        assert_eq!(
            hls_get(&app, "/hls/s2/index.m3u8").await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            hls_get(&app, "/hls/s9/index.m3u8").await.0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn hls_refuses_symlinks_that_escape_the_root() {
        use std::os::unix::fs::symlink;

        let fx = hls_fixture();
        let outside = fx._tmp.path().join("outside");
        fs::create_dir_all(outside.join(SESSION_NEW)).unwrap();
        fs::write(outside.join("leak.m3u8"), "#EXTM3U\nsecret\n").unwrap();
        symlink(&outside, fx.root.join("s4")).unwrap();
        symlink(&outside, fx.root.join("s1").join("escape")).unwrap();

        let app = hls_app(&fx, false);
        assert_eq!(
            hls_get(&app, "/hls/s4/index.m3u8").await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            hls_get(&app, "/hls/s4/leak.m3u8").await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            hls_get(&app, "/hls/s1/escape/leak.m3u8").await.0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn hls_rejects_oversized_playlists_and_missing_root() {
        let fx = hls_fixture();
        let big = fx.root.join("s1").join(SESSION_NEW).join("big.m3u8");
        fs::write(&big, vec![b'#'; MAX_HLS_PLAYLIST_BYTES as usize + 1]).unwrap();
        let app = hls_app(&fx, false);
        let uri = format!("/hls/s1/{SESSION_NEW}/big.m3u8");
        assert_eq!(hls_get(&app, &uri).await.0, StatusCode::PAYLOAD_TOO_LARGE);

        let missing_root = hls_router(
            fx.root.join("absent"),
            Arc::clone(&fx.db),
            false,
            4,
            Vec::new(),
            None,
        );
        assert_eq!(
            hls_get(&missing_root, "/hls/s1/index.m3u8").await.0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn hls_counts_remote_cluster_sessions_toward_the_cap() {
        let fx = hls_fixture();
        let remote: ViewerRemoteSessionCountFn =
            Arc::new(|_viewer| crate::db::MAX_CONNECTIONS_PER_PLAY_KEY as u64);
        let app = hls_router(
            fx.root.clone(),
            Arc::clone(&fx.db),
            true,
            4,
            Vec::new(),
            Some(remote),
        );
        let uri = format!("/hls/s1/index.m3u8?key={}", fx.key_s1);
        assert_eq!(hls_get(&app, &uri).await.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn hls_headers_map_extensions_to_content_types() {
        for (ext, expected) in [
            ("m3u8", "application/vnd.apple.mpegurl"),
            ("m4s", "video/iso.segment"),
            ("mp4", "video/mp4"),
            ("ts", "video/mp2t"),
            ("bin", "application/octet-stream"),
        ] {
            assert_eq!(hls_headers(ext)[header::CONTENT_TYPE], expected);
        }
    }

    #[test]
    fn playlist_rewriter_keeps_absolute_and_keyed_uris() {
        let input = "#EXTM3U\n\n#EXT-X-KEY:METHOD=NONE,URI=\"k?x=1\"\nhttps://cdn/seg.ts\nseg.ts?key=old\nseg2.ts?a=b\n#BROKEN:URI=\"open\n";
        let out = rewrite_playlist_key(input, "k1");
        assert!(out.contains("URI=\"k?x=1&key=k1\""), "{out}");
        assert!(out.contains("\nhttps://cdn/seg.ts\n"), "{out}");
        assert!(out.contains("\nseg.ts?key=old\n"), "{out}");
        assert!(out.contains("\nseg2.ts?a=b&key=k1\n"), "{out}");
        assert!(out.contains("#BROKEN:URI=\"open\n"), "{out}");
        assert!(out.contains("#EXTM3U\n\n"), "{out}");
        assert!(safe_hls_path("/abs/index.m3u8").is_none());
    }

    #[test]
    fn hls_registry_releases_expired_viewer_slots() {
        let db = Db::open(":memory:").unwrap();
        let viewer = add_stream(&db, "s1", "cam", 'a', true);
        let DbLookup::Ok(stream) = db.stream_get("s1") else {
            panic!("stream missing");
        };
        let sessions = HlsSessionRegistry::default();
        let client = IpAddr::from([192, 0, 2, 1]);
        let ttl = Duration::from_secs(30);
        assert!(sessions.reserve_or_renew(&db, &viewer, &stream, client, ttl, 0));
        assert_eq!(db.player_active_count_for_viewer(&viewer.id), 1);

        // Still fresh: the slot is kept.
        sessions.purge_stale(&db, ttl);
        assert_eq!(db.player_active_count_for_viewer(&viewer.id), 1);

        // Expired while the stream exists: the DB slot is released.
        sessions.purge_stale(&db, Duration::ZERO);
        assert_eq!(db.player_active_count_for_viewer(&viewer.id), 0);
        assert!(sessions.inner.lock().is_empty());

        // Expired after the stream was deleted: the entry is simply dropped.
        assert!(sessions.reserve_or_renew(&db, &viewer, &stream, client, ttl, 0));
        assert_eq!(db.stream_delete("s1"), Some(true));
        sessions.purge_stale(&db, Duration::ZERO);
        assert!(sessions.inner.lock().is_empty());
    }
}
