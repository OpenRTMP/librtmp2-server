//! Live-media-aware outbound queue for cluster media connections.
//!
//! A cluster media connection multiplexes live streams over one socket. A
//! plain bounded FIFO behaves badly there: when the peer stalls, new frames
//! are rejected while an ever older backlog is kept, and once the socket
//! drains the peer receives seconds-old video and stays behind live. This
//! queue prefers freshness instead, using only the codec-neutral
//! [`DeliveryHint`] that librtmp2 attached when the frame was exported (the
//! relay never inspects codec payloads):
//!
//! * `Critical` frames (codec headers, metadata) and non-media control
//!   messages are protected: they are never dropped by the age or resync
//!   rules, only by the absolute bounds as a last resort.
//! * When a stream's queued media gets older than
//!   [`LiveQueueConfig::max_media_age`], or the queue hits its message/byte
//!   bound, the stream's queued non-critical frames are discarded and the
//!   stream enters **AwaitingResync**: further `Droppable` frames of that
//!   stream are discarded on arrival until a `ResyncPoint` (a keyframe, or
//!   any audio frame on an audio-only stream) arrives. That frame is queued,
//!   the stream is `Normal` again and the peer continues near live.
//!   A stream that sees no `ResyncPoint` for [`LiveQueueConfig::resync_wait_max`]
//!   stops waiting, so a publisher without keyframes cannot be starved.
//! * Eviction under the absolute bounds is per stream: the stream holding
//!   the most evictable bytes loses its frames first (ties: smaller
//!   `(app, stream)` first), so one overloaded stream does not destroy the
//!   frames of the others. Only when no non-critical media is left, the
//!   oldest `Critical` media frame is evicted, and the stream is flagged so
//!   the hub can re-send its init cache ahead of the next resync point.
//! * A single frame larger than half the byte bound is refused instead of
//!   emptying the queue for it.
//!
//! Drop accounting (every dropped media frame is counted once in
//! `dropped_frames_total` and in exactly one category): `oversized`,
//! `critical` (class Critical), `stale` (older than the age bound or
//! discarded when the stream is purged for a stall/reconnect), otherwise
//! `droppable` (discarded under queue pressure or while awaiting a resync).
//!
//! The age bound default is **experimental**: a conservative value derived
//! from typical 2 s GOPs, not from field data.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use librtmp2::DeliveryHint;
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::Notify;

use crate::cluster::media::protocol::MediaMessage;

/// Default for [`LiveQueueConfig::max_media_age`] (experimental).
pub const DEFAULT_MAX_MEDIA_AGE: Duration = Duration::from_millis(3_000);
/// Default for [`LiveQueueConfig::resync_wait_max`].
pub const DEFAULT_RESYNC_WAIT_MAX: Duration = Duration::from_secs(10);
/// Default for [`LiveQueueConfig::max_messages`].
pub const DEFAULT_MAX_MESSAGES: usize = 1024;

#[derive(Debug, Clone)]
pub struct LiveQueueConfig {
    /// Absolute bound on queued messages.
    pub max_messages: usize,
    /// Absolute bound on queued bytes (see [`approx_size`]).
    pub max_bytes: usize,
    /// Queued media older than this triggers a resync of its stream.
    pub max_media_age: Duration,
    /// Longest a stream waits for a resync point before accepting frames
    /// again.
    pub resync_wait_max: Duration,
}

impl LiveQueueConfig {
    /// Bounds from the configured queue size in MiB (at least 1 MiB) and
    /// the age bound in milliseconds (`0` selects the default).
    pub fn new(max_queue_mb: u32, max_media_age_ms: u32) -> Self {
        Self {
            max_messages: DEFAULT_MAX_MESSAGES,
            max_bytes: (max_queue_mb as usize)
                .saturating_mul(1024 * 1024)
                .max(1024 * 1024),
            max_media_age: if max_media_age_ms == 0 {
                DEFAULT_MAX_MEDIA_AGE
            } else {
                Duration::from_millis(u64::from(max_media_age_ms))
            },
            resync_wait_max: DEFAULT_RESYNC_WAIT_MAX,
        }
    }
}

impl From<u32> for LiveQueueConfig {
    /// Queue size in MiB with the default age bound.
    fn from(max_queue_mb: u32) -> Self {
        Self::new(max_queue_mb, 0)
    }
}

/// Approximate queue footprint of a message.
pub(crate) fn approx_size(msg: &MediaMessage) -> usize {
    match msg {
        MediaMessage::MediaFrame { payload, .. } => payload.len() + 64,
        MediaMessage::InitCache {
            metadata,
            avc_header,
            aac_header,
            keyframe,
            ..
        } => {
            metadata.as_ref().map(|v| v.len()).unwrap_or(0)
                + avc_header.as_ref().map(|v| v.len()).unwrap_or(0)
                + aac_header.as_ref().map(|v| v.len()).unwrap_or(0)
                + keyframe.as_ref().map(|(_, p)| p.len()).unwrap_or(0)
                + 64
        }
        _ => 128,
    }
}

/// Why a message was not queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropCause {
    /// Larger than half the byte bound.
    Oversized,
    /// Droppable frame of a stream waiting for its next resync point.
    AwaitingResync,
    /// No room even after evicting everything evictable.
    Full,
    /// The queue was closed.
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    Queued,
    Dropped(DropCause),
}

impl PushResult {
    pub fn is_queued(self) -> bool {
        self == Self::Queued
    }
}

#[derive(Default)]
pub struct LiveQueueStats {
    dropped_total: AtomicU64,
    dropped_droppable: AtomicU64,
    dropped_stale: AtomicU64,
    dropped_critical: AtomicU64,
    oversized_dropped: AtomicU64,
    resync_count: AtomicU64,
    resync_timeouts: AtomicU64,
    max_bytes_seen: AtomicU64,
    max_messages_seen: AtomicU64,
    max_age_ms_seen: AtomicU64,
}

/// Point-in-time view of one queue, for status endpoints and tests.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct LiveQueueSnapshot {
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub oldest_queue_age_ms: u64,
    pub streams_awaiting_resync: usize,
    pub dropped_frames_total: u64,
    pub dropped_droppable_frames: u64,
    pub dropped_stale_frames: u64,
    pub dropped_critical_frames: u64,
    pub oversized_frames_dropped: u64,
    pub resync_count: u64,
    pub resync_timeouts: u64,
    pub max_queue_bytes_seen: u64,
    pub max_queue_messages_seen: u64,
    pub max_oldest_queue_age_ms_seen: u64,
}

impl LiveQueueSnapshot {
    /// Sum of several queues (counters add; high-water marks take the max).
    pub fn merge(&mut self, other: &Self) {
        self.queue_messages += other.queue_messages;
        self.queue_bytes += other.queue_bytes;
        self.oldest_queue_age_ms = self.oldest_queue_age_ms.max(other.oldest_queue_age_ms);
        self.streams_awaiting_resync += other.streams_awaiting_resync;
        self.dropped_frames_total += other.dropped_frames_total;
        self.dropped_droppable_frames += other.dropped_droppable_frames;
        self.dropped_stale_frames += other.dropped_stale_frames;
        self.dropped_critical_frames += other.dropped_critical_frames;
        self.oversized_frames_dropped += other.oversized_frames_dropped;
        self.resync_count += other.resync_count;
        self.resync_timeouts += other.resync_timeouts;
        self.max_queue_bytes_seen = self.max_queue_bytes_seen.max(other.max_queue_bytes_seen);
        self.max_queue_messages_seen = self
            .max_queue_messages_seen
            .max(other.max_queue_messages_seen);
        self.max_oldest_queue_age_ms_seen = self
            .max_oldest_queue_age_ms_seen
            .max(other.max_oldest_queue_age_ms_seen);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Everything that is not a `MediaFrame`.
    Control,
    Media(DeliveryHint),
}

impl Kind {
    fn of(msg: &MediaMessage) -> Self {
        match msg {
            MediaMessage::MediaFrame { hint, .. } => Self::Media(*hint),
            _ => Self::Control,
        }
    }

    fn evictable(self) -> bool {
        matches!(self, Self::Media(h) if h != DeliveryHint::Critical)
    }
}

struct Entry {
    msg: MediaMessage,
    size: usize,
    at: Instant,
    kind: Kind,
}

impl Entry {
    fn names(&self) -> Option<(&str, &str)> {
        match &self.msg {
            MediaMessage::MediaFrame { app, stream, .. } => Some((app, stream)),
            _ => None,
        }
    }
}

#[derive(Default)]
struct StreamState {
    /// Set while the stream waits for a resync point.
    awaiting_since: Option<Instant>,
    /// Queued non-critical media bytes / frames of this stream.
    evictable_bytes: usize,
    queued_frames: usize,
    /// A `Critical` frame was evicted: re-send the init cache before the
    /// next resync point.
    needs_reinit: bool,
}

impl StreamState {
    fn idle(&self) -> bool {
        self.awaiting_since.is_none() && self.queued_frames == 0 && !self.needs_reinit
    }
}

#[derive(Clone, Copy)]
enum Reason {
    /// Older than the age bound / reconnect: counted as `stale`.
    Stale,
    /// Queue pressure or awaiting resync: counted as `droppable`.
    Pressure,
}

struct Inner {
    entries: VecDeque<Entry>,
    bytes: usize,
    streams: HashMap<String, HashMap<String, StreamState>>,
    closed: bool,
    /// A critical frame of some stream was evicted since the owner last
    /// published `reinit_pending`.
    reinit_dirty: bool,
}

impl Inner {
    fn state_mut(&mut self, app: &str, stream: &str) -> &mut StreamState {
        if !self.streams.contains_key(app) {
            self.streams.insert(app.to_string(), HashMap::new());
        }
        let per_app = self.streams.get_mut(app).expect("inserted above");
        if !per_app.contains_key(stream) {
            per_app.insert(stream.to_string(), StreamState::default());
        }
        per_app.get_mut(stream).expect("inserted above")
    }

    fn state(&self, app: &str, stream: &str) -> Option<&StreamState> {
        self.streams.get(app)?.get(stream)
    }

    fn prune_idle(&mut self, app: &str, stream: &str) {
        let Some(per_app) = self.streams.get_mut(app) else {
            return;
        };
        if per_app.get(stream).is_some_and(StreamState::idle) {
            per_app.remove(stream);
        }
        if per_app.is_empty() {
            self.streams.remove(app);
        }
    }

    fn is_awaiting(&self, app: &str, stream: &str) -> bool {
        self.state(app, stream)
            .is_some_and(|s| s.awaiting_since.is_some())
    }

    /// Book-keeping for an entry that just left `entries` (not for pops
    /// that go to the writer, which use the same path).
    fn account_removed(&mut self, entry: &Entry) {
        self.bytes = self.bytes.saturating_sub(entry.size);
        if let Some((app, stream)) = entry.names() {
            if let Some(st) = self
                .streams
                .get_mut(app)
                .and_then(|per_app| per_app.get_mut(stream))
            {
                st.queued_frames = st.queued_frames.saturating_sub(1);
                if entry.kind.evictable() {
                    st.evictable_bytes = st.evictable_bytes.saturating_sub(entry.size);
                }
            }
            let (app, stream) = (app.to_string(), stream.to_string());
            self.prune_idle(&app, &stream);
        }
    }

    fn count_drop(stats: &LiveQueueStats, entry_kind: Kind, reason: Reason) {
        stats.dropped_total.fetch_add(1, Ordering::Relaxed);
        let counter = match (entry_kind, reason) {
            (Kind::Media(DeliveryHint::Critical), _) => &stats.dropped_critical,
            (_, Reason::Stale) => &stats.dropped_stale,
            (_, Reason::Pressure) => &stats.dropped_droppable,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove every queued non-critical frame of the stream. Returns how
    /// many were removed.
    fn purge_stream(
        &mut self,
        app: &str,
        stream: &str,
        reason: Reason,
        stats: &LiveQueueStats,
    ) -> usize {
        let mut removed = Vec::new();
        let mut kept = VecDeque::with_capacity(self.entries.len());
        for entry in std::mem::take(&mut self.entries) {
            let hit = entry.kind.evictable() && entry.names() == Some((app, stream));
            if hit {
                removed.push(entry);
            } else {
                kept.push_back(entry);
            }
        }
        self.entries = kept;
        let n = removed.len();
        for entry in &removed {
            Self::count_drop(stats, entry.kind, reason);
            self.account_removed(entry);
        }
        n
    }

    /// Like [`Self::purge_stream`] for the age trigger: also keep a fresh
    /// suffix of the stream that starts at a fresh `ResyncPoint`, since
    /// those frames are still decodable and near live. Returns `(fresh,
    /// skipped)`: whether such a suffix exists (the stream is then already
    /// resynced) and whether a dependent (`Droppable`) frame was discarded,
    /// i.e. the receiver will see a jump forward rather than plain aging out
    /// of old resync points (audio-only streams).
    fn purge_stale_stream(
        &mut self,
        app: &str,
        stream: &str,
        now: Instant,
        max_age: Duration,
        stats: &LiveQueueStats,
    ) -> (bool, bool) {
        let fresh_resync = self.entries.iter().position(|e| {
            e.names() == Some((app, stream))
                && e.kind == Kind::Media(DeliveryHint::ResyncPoint)
                && now.saturating_duration_since(e.at) <= max_age
        });
        let mut removed = Vec::new();
        let mut kept = VecDeque::with_capacity(self.entries.len());
        for (i, entry) in std::mem::take(&mut self.entries).into_iter().enumerate() {
            let before_fresh = fresh_resync.is_none_or(|k| i < k);
            let hit =
                before_fresh && entry.kind.evictable() && entry.names() == Some((app, stream));
            if hit {
                removed.push(entry);
            } else {
                kept.push_back(entry);
            }
        }
        self.entries = kept;
        let skipped = removed
            .iter()
            .any(|e| e.kind == Kind::Media(DeliveryHint::Droppable));
        for entry in &removed {
            Self::count_drop(stats, entry.kind, Reason::Stale);
            self.account_removed(entry);
        }
        (fresh_resync.is_some(), skipped)
    }

    /// The stream with the most evictable bytes (ties: smaller names).
    fn heaviest_evictable(&self) -> Option<(String, String)> {
        let mut best: Option<(usize, &str, &str)> = None;
        for (app, per_app) in &self.streams {
            for (stream, st) in per_app {
                if st.evictable_bytes == 0 {
                    continue;
                }
                let better = match best {
                    None => true,
                    Some((bytes, a, s)) => {
                        st.evictable_bytes > bytes
                            || (st.evictable_bytes == bytes
                                && (app.as_str(), stream.as_str()) < (a, s))
                    }
                };
                if better {
                    best = Some((st.evictable_bytes, app, stream));
                }
            }
        }
        best.map(|(_, a, s)| (a.to_string(), s.to_string()))
    }

    /// Age rule: discard stale frames of every stream whose queued media is
    /// older than `max_age`. Cheap when the head is fresh.
    fn enforce_age(&mut self, now: Instant, cfg: &LiveQueueConfig, stats: &LiveQueueStats) {
        let head_old = self
            .entries
            .front()
            .is_some_and(|e| now.saturating_duration_since(e.at) > cfg.max_media_age);
        if !head_old {
            return;
        }
        let mut stale: Vec<(String, String)> = Vec::new();
        for entry in &self.entries {
            if now.saturating_duration_since(entry.at) <= cfg.max_media_age {
                break;
            }
            if let (true, Some((app, stream))) = (entry.kind.evictable(), entry.names())
                && !stale.iter().any(|(a, s)| a == app && s == stream)
            {
                stale.push((app.to_string(), stream.to_string()));
            }
        }
        for (app, stream) in stale {
            let (resynced, skipped) =
                self.purge_stale_stream(&app, &stream, now, cfg.max_media_age, stats);
            if resynced {
                if skipped {
                    stats.resync_count.fetch_add(1, Ordering::Relaxed);
                }
                if let Some(st) = self.streams.get_mut(&app).and_then(|m| m.get_mut(&stream)) {
                    st.awaiting_since = None;
                }
            } else {
                let st = self.state_mut(&app, &stream);
                st.awaiting_since.get_or_insert(now);
            }
            self.prune_idle(&app, &stream);
        }
    }

    /// Evict until one more message of `size` bytes fits. Returns `false`
    /// when nothing evictable is left and it still does not fit.
    fn make_room(
        &mut self,
        size: usize,
        now: Instant,
        cfg: &LiveQueueConfig,
        stats: &LiveQueueStats,
    ) -> bool {
        loop {
            if self.entries.len() < cfg.max_messages
                && self.bytes.saturating_add(size) <= cfg.max_bytes
            {
                return true;
            }
            if let Some((app, stream)) = self.heaviest_evictable() {
                self.purge_stream(&app, &stream, Reason::Pressure, stats);
                self.state_mut(&app, &stream)
                    .awaiting_since
                    .get_or_insert(now);
                continue;
            }
            // Only protected frames left: evict the oldest critical media
            // frame. Control messages are never evicted.
            let Some(idx) = self
                .entries
                .iter()
                .position(|e| e.kind == Kind::Media(DeliveryHint::Critical))
            else {
                return false;
            };
            if let Some(entry) = self.entries.remove(idx) {
                Self::count_drop(stats, entry.kind, Reason::Pressure);
                if let Some((app, stream)) = entry.names() {
                    let (app, stream) = (app.to_string(), stream.to_string());
                    self.state_mut(&app, &stream).needs_reinit = true;
                    self.reinit_dirty = true;
                }
                self.account_removed(&entry);
            }
        }
    }

    /// Streams that stopped waiting for a resync point are released.
    fn release_expired_waits(
        &mut self,
        now: Instant,
        cfg: &LiveQueueConfig,
        stats: &LiveQueueStats,
    ) {
        let mut released = Vec::new();
        for (app, per_app) in &self.streams {
            for (stream, st) in per_app {
                if st
                    .awaiting_since
                    .is_some_and(|t| now.saturating_duration_since(t) >= cfg.resync_wait_max)
                {
                    released.push((app.clone(), stream.clone()));
                }
            }
        }
        for (app, stream) in released {
            stats.resync_timeouts.fetch_add(1, Ordering::Relaxed);
            if let Some(st) = self.streams.get_mut(&app).and_then(|m| m.get_mut(&stream)) {
                st.awaiting_since = None;
            }
            self.prune_idle(&app, &stream);
        }
    }
}

pub struct LiveMediaQueue {
    inner: Mutex<Inner>,
    notify: Notify,
    cfg: LiveQueueConfig,
    stats: LiveQueueStats,
    /// Fast path for [`Self::take_reinit`].
    reinit_pending: AtomicBool,
}

impl LiveMediaQueue {
    pub fn new(cfg: LiveQueueConfig) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: VecDeque::new(),
                bytes: 0,
                streams: HashMap::new(),
                closed: false,
                reinit_dirty: false,
            }),
            notify: Notify::new(),
            cfg,
            stats: LiveQueueStats::default(),
            reinit_pending: AtomicBool::new(false),
        }
    }

    pub fn config(&self) -> &LiveQueueConfig {
        &self.cfg
    }

    pub fn push(&self, msg: MediaMessage) -> PushResult {
        self.push_at(msg, Instant::now())
    }

    pub(crate) fn push_at(&self, msg: MediaMessage, now: Instant) -> PushResult {
        let size = approx_size(&msg);
        let kind = Kind::of(&msg);
        let mut g = self.inner.lock();
        if g.closed {
            return PushResult::Dropped(DropCause::Closed);
        }
        // Media frames above half the bound are refused so one frame cannot
        // empty the queue; control messages (e.g. an `InitCache` carrying a
        // keyframe) may use the whole bound.
        let limit = match kind {
            Kind::Media(_) => self.cfg.max_bytes / 2,
            Kind::Control => self.cfg.max_bytes,
        };
        if size > limit {
            self.stats.dropped_total.fetch_add(1, Ordering::Relaxed);
            self.stats.oversized_dropped.fetch_add(1, Ordering::Relaxed);
            return PushResult::Dropped(DropCause::Oversized);
        }
        g.enforce_age(now, &self.cfg, &self.stats);
        g.release_expired_waits(now, &self.cfg, &self.stats);
        let names = match &msg {
            MediaMessage::MediaFrame { app, stream, .. } => Some((app.clone(), stream.clone())),
            _ => None,
        };
        if let (Kind::Media(hint), Some((app, stream))) = (kind, &names)
            && g.is_awaiting(app, stream)
        {
            match hint {
                DeliveryHint::Droppable => {
                    Inner::count_drop(&self.stats, kind, Reason::Pressure);
                    return PushResult::Dropped(DropCause::AwaitingResync);
                }
                DeliveryHint::ResyncPoint => {
                    // Whatever is still queued for the stream predates
                    // the gap; the resync point supersedes it.
                    g.purge_stream(app, stream, Reason::Stale, &self.stats);
                    if let Some(st) = g.streams.get_mut(app).and_then(|m| m.get_mut(stream)) {
                        st.awaiting_since = None;
                    }
                    self.stats.resync_count.fetch_add(1, Ordering::Relaxed);
                }
                DeliveryHint::Critical => {}
            }
        }
        if !g.make_room(size, now, &self.cfg, &self.stats) {
            if let Kind::Media(_) = kind {
                Inner::count_drop(&self.stats, kind, Reason::Pressure);
            }
            // Critical frames evicted on the way to a failed attempt are
            // gone all the same: publish the reinit request.
            if std::mem::take(&mut g.reinit_dirty) {
                self.reinit_pending.store(true, Ordering::Release);
            }
            return PushResult::Dropped(DropCause::Full);
        }
        // A critical frame of any stream may have been evicted to make room.
        if std::mem::take(&mut g.reinit_dirty) {
            self.reinit_pending.store(true, Ordering::Release);
        }
        // Making room may have put the incoming stream itself into
        // AwaitingResync (it was the heaviest).
        if let (Kind::Media(hint), Some((app, stream))) = (kind, &names)
            && g.is_awaiting(app, stream)
        {
            match hint {
                DeliveryHint::Droppable => {
                    Inner::count_drop(&self.stats, kind, Reason::Pressure);
                    return PushResult::Dropped(DropCause::AwaitingResync);
                }
                DeliveryHint::ResyncPoint => {
                    if let Some(st) = g.streams.get_mut(app).and_then(|m| m.get_mut(stream)) {
                        st.awaiting_since = None;
                    }
                    self.stats.resync_count.fetch_add(1, Ordering::Relaxed);
                }
                DeliveryHint::Critical => {}
            }
        }
        if let Some((app, stream)) = &names {
            let evictable = kind.evictable();
            let st = g.state_mut(app, stream);
            st.queued_frames += 1;
            if evictable {
                st.evictable_bytes += size;
            }
            if st.needs_reinit {
                self.reinit_pending.store(true, Ordering::Release);
            }
        }
        g.bytes += size;
        g.entries.push_back(Entry {
            msg,
            size,
            at: now,
            kind,
        });
        self.note_high_water(&g, now);
        drop(g);
        self.notify.notify_one();
        PushResult::Queued
    }

    fn note_high_water(&self, g: &Inner, now: Instant) {
        self.stats
            .max_bytes_seen
            .fetch_max(g.bytes as u64, Ordering::Relaxed);
        self.stats
            .max_messages_seen
            .fetch_max(g.entries.len() as u64, Ordering::Relaxed);
        if let Some(head) = g.entries.front() {
            let age = now.saturating_duration_since(head.at).as_millis() as u64;
            self.stats.max_age_ms_seen.fetch_max(age, Ordering::Relaxed);
        }
    }

    /// Take the next message for the writer without waiting.
    pub(crate) fn pop_at(&self, now: Instant) -> Option<MediaMessage> {
        let mut g = self.inner.lock();
        if g.closed {
            return None;
        }
        // Account the head's age before the age rule discards it, so the
        // high-water mark shows how far behind the writer got.
        self.note_high_water(&g, now);
        g.enforce_age(now, &self.cfg, &self.stats);
        g.release_expired_waits(now, &self.cfg, &self.stats);
        let entry = g.entries.pop_front()?;
        g.account_removed(&entry);
        Some(entry.msg)
    }

    /// Wait for the next message. `None` once the queue is closed.
    pub async fn pop(&self) -> Option<MediaMessage> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Registers interest first so a push between the check and the
            // await cannot be missed.
            notified.as_mut().enable();
            if let Some(msg) = self.pop_at(Instant::now()) {
                return Some(msg);
            }
            if self.inner.lock().closed {
                return None;
            }
            notified.await;
        }
    }

    pub fn close(&self) {
        let mut g = self.inner.lock();
        g.closed = true;
        g.entries.clear();
        g.bytes = 0;
        g.streams.clear();
        drop(g);
        self.notify.notify_waiters();
        self.notify.notify_one();
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }

    /// The connection was lost: everything queued for the old connection
    /// is stale. Non-critical media is discarded and its streams wait for a
    /// resync point; control messages and critical frames stay queued for
    /// the next connection.
    pub fn on_connection_reset(&self) {
        let now = Instant::now();
        let mut g = self.inner.lock();
        let streams: Vec<(String, String)> = g
            .streams
            .iter()
            .flat_map(|(a, m)| {
                m.iter()
                    .filter(|(_, s)| s.evictable_bytes > 0)
                    .map(|(s, _)| (a.clone(), s.clone()))
            })
            .collect();
        for (app, stream) in streams {
            g.purge_stream(&app, &stream, Reason::Stale, &self.stats);
            g.state_mut(&app, &stream).awaiting_since.get_or_insert(now);
        }
    }

    /// Whether the init cache of `stream` has to be re-sent because a
    /// critical frame was evicted. Clears the flag.
    pub fn take_reinit(&self, app: &str, stream: &str) -> bool {
        if !self.reinit_pending.load(Ordering::Acquire) {
            return false;
        }
        let mut g = self.inner.lock();
        let taken = g
            .streams
            .get_mut(app)
            .and_then(|m| m.get_mut(stream))
            .is_some_and(|s| std::mem::take(&mut s.needs_reinit));
        let any = g
            .streams
            .values()
            .any(|m| m.values().any(|s| s.needs_reinit));
        self.reinit_pending.store(any, Ordering::Release);
        g.prune_idle(app, stream);
        taken
    }

    /// Flag `stream` for an init-cache re-send (a re-send could not be
    /// queued).
    pub fn mark_reinit(&self, app: &str, stream: &str) {
        let mut g = self.inner.lock();
        g.state_mut(app, stream).needs_reinit = true;
        self.reinit_pending.store(true, Ordering::Release);
    }

    pub fn awaiting_resync(&self, app: &str, stream: &str) -> bool {
        self.inner.lock().is_awaiting(app, stream)
    }

    pub fn snapshot(&self) -> LiveQueueSnapshot {
        self.snapshot_at(Instant::now())
    }

    pub(crate) fn snapshot_at(&self, now: Instant) -> LiveQueueSnapshot {
        let g = self.inner.lock();
        let s = &self.stats;
        LiveQueueSnapshot {
            queue_messages: g.entries.len(),
            queue_bytes: g.bytes,
            oldest_queue_age_ms: g.entries.front().map_or(0, |e| {
                now.saturating_duration_since(e.at).as_millis() as u64
            }),
            streams_awaiting_resync: g
                .streams
                .values()
                .flat_map(|m| m.values())
                .filter(|st| st.awaiting_since.is_some())
                .count(),
            dropped_frames_total: s.dropped_total.load(Ordering::Relaxed),
            dropped_droppable_frames: s.dropped_droppable.load(Ordering::Relaxed),
            dropped_stale_frames: s.dropped_stale.load(Ordering::Relaxed),
            dropped_critical_frames: s.dropped_critical.load(Ordering::Relaxed),
            oversized_frames_dropped: s.oversized_dropped.load(Ordering::Relaxed),
            resync_count: s.resync_count.load(Ordering::Relaxed),
            resync_timeouts: s.resync_timeouts.load(Ordering::Relaxed),
            max_queue_bytes_seen: s.max_bytes_seen.load(Ordering::Relaxed),
            max_queue_messages_seen: s.max_messages_seen.load(Ordering::Relaxed),
            max_oldest_queue_age_ms_seen: s.max_age_ms_seen.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KB: usize = 1024;

    fn cfg(max_bytes: usize, max_messages: usize, age_ms: u64) -> LiveQueueConfig {
        LiveQueueConfig {
            max_messages,
            max_bytes,
            max_media_age: Duration::from_millis(age_ms),
            resync_wait_max: Duration::from_secs(10),
        }
    }

    fn frame(stream: &str, hint: DeliveryHint, tag: u8, len: usize) -> MediaMessage {
        MediaMessage::MediaFrame {
            app: "live".into(),
            stream: stream.into(),
            epoch: 1,
            frame_type: 1,
            timestamp: u32::from(tag),
            timeline_ts: u32::from(tag),
            hint,
            payload: vec![tag; len],
        }
    }

    fn tag(msg: &MediaMessage) -> u8 {
        match msg {
            MediaMessage::MediaFrame { payload, .. } => payload[0],
            _ => 0xFF,
        }
    }

    fn drain(q: &LiveMediaQueue, now: Instant) -> Vec<MediaMessage> {
        std::iter::from_fn(|| q.pop_at(now)).collect()
    }

    use DeliveryHint::{Critical, Droppable, ResyncPoint};

    #[test]
    fn fifo_order_without_pressure() {
        let q = LiveMediaQueue::new(cfg(MB, 100, 3000));
        let t = Instant::now();
        q.push_at(frame("a", Critical, 1, 10), t);
        q.push_at(frame("a", ResyncPoint, 2, 10), t);
        q.push_at(frame("a", Droppable, 3, 10), t);
        q.push_at(MediaMessage::AuthOk, t);
        let tags: Vec<u8> = drain(&q, t).iter().map(tag).collect();
        assert_eq!(tags, vec![1, 2, 3, 0xFF]);
        assert_eq!(q.snapshot_at(t).queue_bytes, 0);
    }

    const MB: usize = 1024 * 1024;

    /// Test A: a writer slower than the producer keeps the queue bounded.
    #[test]
    fn slow_downstream_keeps_the_queue_bounded() {
        let q = LiveMediaQueue::new(cfg(256 * KB, 10_000, 3000));
        let start = Instant::now();
        let mut now = start;
        // 30 s of 30 fps video, 2 s GOP, 10 KiB frames; the writer drains
        // one frame per three produced.
        for i in 0..900u32 {
            now = start + Duration::from_millis(u64::from(i) * 33);
            let hint = if i % 60 == 0 { ResyncPoint } else { Droppable };
            q.push_at(frame("a", hint, (i % 250) as u8, 10 * KB), now);
            if i % 3 == 0 {
                q.pop_at(now);
            }
            let snap = q.snapshot_at(now);
            assert!(snap.queue_bytes <= 256 * KB, "bytes {}", snap.queue_bytes);
        }
        let snap = q.snapshot_at(now);
        assert!(snap.max_queue_bytes_seen <= (256 * KB) as u64);
        assert!(snap.dropped_frames_total > 0);
        assert!(snap.resync_count > 0, "must have resynced on keyframes");
        // Age is bounded too: the head never got much older than the bound.
        assert!(snap.max_oldest_queue_age_ms_seen <= 3000 + 1000, "{snap:?}");
    }

    /// Test B + C: a stall discards the stale GOP and recovery continues at
    /// the next keyframe instead of replaying the backlog.
    #[test]
    fn stall_then_recovery_jumps_to_the_next_keyframe() {
        let q = LiveMediaQueue::new(cfg(8 * MB, 10_000, 3000));
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        // Keyframe at t=0, then inter frames every 33 ms, writer stalled.
        q.push_at(frame("a", Critical, 200, 100), at(0)); // sequence header
        q.push_at(frame("a", ResyncPoint, 0, 5000), at(0));
        let mut i = 1u8;
        let mut t = 33;
        while t < 5000 {
            q.push_at(frame("a", Droppable, i, 1000), at(t));
            i = i.wrapping_add(1).max(1);
            t += 33;
        }
        // Stall over at t=5 s with no keyframe since t=0: the age rule
        // empties the stale frames, the stream waits for a keyframe.
        let first = q.pop_at(at(5000)).expect("critical header survives");
        assert_eq!(tag(&first), 200);
        assert!(
            q.pop_at(at(5000)).is_none(),
            "stale backlog must not be replayed"
        );
        assert!(q.awaiting_resync("live", "a"));
        let snap = q.snapshot_at(at(5000));
        assert!(snap.dropped_stale_frames > 50, "{snap:?}");
        assert!(snap.dropped_frames_total > 140, "{snap:?}");
        assert_eq!(snap.dropped_critical_frames, 0);
        // Inter frames are discarded until the next keyframe...
        assert_eq!(
            q.push_at(frame("a", Droppable, 7, 1000), at(5033)),
            PushResult::Dropped(DropCause::AwaitingResync)
        );
        // ...which resumes delivery.
        assert!(
            q.push_at(frame("a", ResyncPoint, 50, 5000), at(5066))
                .is_queued()
        );
        assert!(!q.awaiting_resync("live", "a"));
        assert!(
            q.push_at(frame("a", Droppable, 51, 1000), at(5100))
                .is_queued()
        );
        let tags: Vec<u8> = drain(&q, at(5100)).iter().map(tag).collect();
        assert_eq!(tags, vec![50, 51]);
        let snap = q.snapshot_at(at(5100));
        assert_eq!(snap.resync_count, 1);
        assert_eq!(snap.queue_messages, 0);
    }

    /// A fresh keyframe already in the queue when the age rule fires keeps
    /// its GOP: the peer is near live at once.
    #[test]
    fn age_rule_keeps_a_fresh_keyframe_suffix() {
        let q = LiveMediaQueue::new(cfg(8 * MB, 10_000, 3000));
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        q.push_at(frame("a", ResyncPoint, 1, 100), at(0));
        q.push_at(frame("a", Droppable, 2, 100), at(33));
        q.push_at(frame("a", ResyncPoint, 10, 100), at(2500));
        q.push_at(frame("a", Droppable, 11, 100), at(2533));
        // t = 3.4 s: the first GOP is older than 3 s, the second is not.
        let tags: Vec<u8> = drain(&q, at(3400)).iter().map(tag).collect();
        assert_eq!(tags, vec![10, 11]);
        assert!(!q.awaiting_resync("live", "a"));
        assert_eq!(q.snapshot_at(at(3400)).dropped_stale_frames, 2);
        assert_eq!(q.snapshot_at(at(3400)).resync_count, 1);
    }

    /// Test D: an overloaded stream A does not destroy stream B.
    #[test]
    fn overloaded_stream_does_not_evict_other_streams() {
        let q = LiveMediaQueue::new(cfg(100 * KB, 10_000, 3000));
        let t = Instant::now();
        // B is small and healthy: a header, a keyframe and a few frames.
        q.push_at(frame("b", Critical, 100, 200), t);
        q.push_at(frame("b", ResyncPoint, 101, 2000), t);
        for i in 0..5 {
            q.push_at(frame("b", Droppable, 110 + i, 500), t);
        }
        // A floods far beyond the bound.
        for i in 0..200u32 {
            let hint = if i % 30 == 0 { ResyncPoint } else { Droppable };
            q.push_at(frame("a", hint, (i % 90) as u8, 4 * KB), t);
        }
        let snap = q.snapshot_at(t);
        assert!(snap.queue_bytes <= 100 * KB);
        let out = drain(&q, t);
        let b_tags: Vec<u8> = out
            .iter()
            .filter(|m| matches!(m, MediaMessage::MediaFrame { stream, .. } if stream == "b"))
            .map(tag)
            .collect();
        assert_eq!(
            b_tags,
            vec![100, 101, 110, 111, 112, 113, 114],
            "B untouched"
        );
        assert!(!q.awaiting_resync("live", "b"));
        assert_eq!(snap.dropped_critical_frames, 0);
    }

    /// Test E: audio-only streams resync on audio, never on a keyframe.
    #[test]
    fn audio_only_stream_resyncs_on_audio() {
        let q = LiveMediaQueue::new(cfg(8 * MB, 10_000, 1000));
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        // librtmp2 classifies audio on an audio-only route as ResyncPoint.
        for i in 0..100u64 {
            q.push_at(
                frame("radio", ResyncPoint, (i % 200) as u8, 400),
                at(i * 20),
            );
        }
        // Stalled for 5 s: everything is stale.
        assert!(q.pop_at(at(5000)).is_none());
        assert!(q.awaiting_resync("live", "radio"));
        // The very next audio frame resumes delivery, no keyframe needed.
        assert!(
            q.push_at(frame("radio", ResyncPoint, 9, 400), at(5020))
                .is_queued()
        );
        assert!(!q.awaiting_resync("live", "radio"));
        assert_eq!(tag(&q.pop_at(at(5020)).unwrap()), 9);
    }

    /// Test F: metadata / headers survive purges and resyncs.
    #[test]
    fn critical_and_control_messages_survive_stall_and_resync() {
        let q = LiveMediaQueue::new(cfg(8 * MB, 10_000, 1000));
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        q.push_at(MediaMessage::AuthOk, at(0));
        q.push_at(frame("a", Critical, 201, 50), at(0)); // metadata
        q.push_at(frame("a", Critical, 202, 50), at(1)); // avc header
        q.push_at(frame("a", Critical, 203, 50), at(2)); // aac header
        for i in 0..50 {
            q.push_at(frame("a", Droppable, i, 500), at(10 + u64::from(i)));
        }
        let out = drain(&q, at(4000));
        let tags: Vec<u8> = out.iter().map(tag).collect();
        assert_eq!(tags, vec![0xFF, 201, 202, 203], "protected, in order");
        assert_eq!(q.snapshot_at(at(4000)).dropped_critical_frames, 0);
        assert!(!q.take_reinit("live", "a"));
    }

    /// Test G: a huge frame cannot bypass limits or wipe other streams.
    #[test]
    fn oversized_frame_is_refused_without_touching_the_queue() {
        let q = LiveMediaQueue::new(cfg(100 * KB, 10_000, 3000));
        let t = Instant::now();
        for i in 0..5 {
            q.push_at(frame("b", Droppable, i, KB), t);
        }
        let before = q.snapshot_at(t);
        // Half the bound plus one byte is refused.
        let r = q.push_at(frame("a", ResyncPoint, 9, 50 * KB), t);
        assert_eq!(r, PushResult::Dropped(DropCause::Oversized));
        let r = q.push_at(frame("a", ResyncPoint, 9, 10 * MB), t);
        assert_eq!(r, PushResult::Dropped(DropCause::Oversized));
        let after = q.snapshot_at(t);
        assert_eq!(after.queue_messages, before.queue_messages);
        assert_eq!(after.queue_bytes, before.queue_bytes);
        assert_eq!(after.oversized_frames_dropped, 2);
        assert_eq!(after.dropped_frames_total, 2);
        // A frame that fits only by evicting evicts just what it needs.
        let q = LiveMediaQueue::new(cfg(100 * KB, 10_000, 3000));
        for i in 0..4 {
            q.push_at(frame("b", Droppable, i, 10 * KB), t);
        }
        for i in 0..4 {
            q.push_at(frame("a", Droppable, 10 + i, 10 * KB), t);
        }
        assert!(
            q.push_at(frame("a", ResyncPoint, 99, 45 * KB), t)
                .is_queued()
        );
        assert!(q.snapshot_at(t).queue_bytes <= 100 * KB);
    }

    #[test]
    fn critical_media_is_evicted_last_and_flags_a_reinit() {
        let q = LiveMediaQueue::new(cfg(100 * KB, 10_000, 3000));
        let t = Instant::now();
        for i in 0..3 {
            q.push_at(frame("a", Critical, 200 + i, 30 * KB), t);
        }
        // Nothing evictable: the fourth critical frame evicts the oldest.
        assert!(q.push_at(frame("a", Critical, 210, 30 * KB), t).is_queued());
        let snap = q.snapshot_at(t);
        assert_eq!(snap.dropped_critical_frames, 1);
        assert!(snap.queue_bytes <= 100 * KB);
        assert!(q.take_reinit("live", "a"));
        assert!(!q.take_reinit("live", "a"), "flag is consumed");
        let tags: Vec<u8> = drain(&q, t).iter().map(tag).collect();
        assert_eq!(tags, vec![201, 202, 210]);
    }

    /// Evicting stream A's critical frame for a message of stream B must
    /// still flag A for an init-cache resend.
    #[test]
    fn critical_eviction_for_another_stream_flags_the_evicted_stream() {
        let q = LiveMediaQueue::new(cfg(100 * KB, 10_000, 3000));
        let t = Instant::now();
        for i in 0..3 {
            q.push_at(frame("a", Critical, 200 + i, 30 * KB), t);
        }
        assert!(q.push_at(frame("b", Critical, 9, 30 * KB), t).is_queued());
        assert_eq!(q.snapshot_at(t).dropped_critical_frames, 1);
        assert!(q.take_reinit("live", "a"));
        assert!(!q.take_reinit("live", "b"));
    }

    /// An init cache above half the bound but within it must be accepted;
    /// only media frames are limited to half.
    #[test]
    fn large_control_messages_may_use_the_whole_bound() {
        let q = LiveMediaQueue::new(cfg(100 * KB, 10_000, 3000));
        let t = Instant::now();
        let init = MediaMessage::InitCache {
            app: "live".into(),
            stream: "a".into(),
            epoch: 1,
            metadata: None,
            avc_header: None,
            aac_header: None,
            keyframe: Some((0, vec![0; 60 * KB])),
        };
        assert!(q.push_at(init, t).is_queued());
        assert_eq!(
            q.push_at(frame("a", ResyncPoint, 1, 60 * KB), t),
            PushResult::Dropped(DropCause::Oversized)
        );
        let too_big = MediaMessage::InitCache {
            app: "live".into(),
            stream: "a".into(),
            epoch: 1,
            metadata: None,
            avc_header: None,
            aac_header: None,
            keyframe: Some((0, vec![0; 200 * KB])),
        };
        assert_eq!(
            q.push_at(too_big, t),
            PushResult::Dropped(DropCause::Oversized)
        );
    }

    #[test]
    fn control_messages_are_never_evicted_and_a_full_queue_rejects_new_ones() {
        let q = LiveMediaQueue::new(cfg(MB, 3, 3000));
        let t = Instant::now();
        for _ in 0..3 {
            assert!(q.push_at(MediaMessage::AuthOk, t).is_queued());
        }
        assert_eq!(
            q.push_at(MediaMessage::AuthOk, t),
            PushResult::Dropped(DropCause::Full)
        );
        assert_eq!(
            q.push_at(frame("a", ResyncPoint, 1, 10), t),
            PushResult::Dropped(DropCause::Full)
        );
        assert_eq!(q.snapshot_at(t).queue_messages, 3);
    }

    #[test]
    fn message_bound_applies_like_the_byte_bound() {
        let q = LiveMediaQueue::new(cfg(MB, 10, 3000));
        let t = Instant::now();
        for i in 0..100u8 {
            q.push_at(
                frame(
                    "a",
                    if i % 20 == 0 { ResyncPoint } else { Droppable },
                    i,
                    10,
                ),
                t,
            );
            assert!(q.snapshot_at(t).queue_messages <= 10);
        }
    }

    #[test]
    fn waiting_for_a_resync_point_has_a_time_limit() {
        let mut c = cfg(MB, 100, 1000);
        c.resync_wait_max = Duration::from_secs(2);
        let q = LiveMediaQueue::new(c);
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        q.push_at(frame("a", Droppable, 1, 10), at(0));
        assert!(q.pop_at(at(1500)).is_none());
        assert!(q.awaiting_resync("live", "a"));
        assert!(
            !q.push_at(frame("a", Droppable, 2, 10), at(2000))
                .is_queued()
        );
        // A publisher that never sends keyframes is not starved forever.
        assert!(
            q.push_at(frame("a", Droppable, 3, 10), at(3600))
                .is_queued()
        );
        assert_eq!(q.snapshot_at(at(3600)).resync_timeouts, 1);
    }

    /// Test H (queue part): a reconnect discards stale media and keeps
    /// control traffic and critical frames.
    #[test]
    fn connection_reset_discards_stale_media_but_keeps_control() {
        let q = LiveMediaQueue::new(cfg(8 * MB, 100, 3000));
        let t = Instant::now();
        q.push_at(MediaMessage::AuthOk, t);
        q.push_at(frame("a", Critical, 201, 10), t);
        q.push_at(frame("a", ResyncPoint, 1, 10), t);
        q.push_at(frame("a", Droppable, 2, 10), t);
        q.on_connection_reset();
        assert!(q.awaiting_resync("live", "a"));
        let tags: Vec<u8> = drain(&q, t).iter().map(tag).collect();
        assert_eq!(tags, vec![0xFF, 201]);
        assert_eq!(q.snapshot_at(t).dropped_stale_frames, 2);
    }

    #[test]
    fn idle_stream_state_is_released() {
        let q = LiveMediaQueue::new(cfg(8 * MB, 100, 3000));
        let t = Instant::now();
        q.push_at(frame("a", Droppable, 1, 10), t);
        q.pop_at(t);
        assert!(q.inner.lock().streams.is_empty());
    }

    #[test]
    fn closed_queue_rejects_and_wakes_the_writer() {
        let q = LiveMediaQueue::new(cfg(MB, 10, 3000));
        q.close();
        assert_eq!(
            q.push(frame("a", Droppable, 1, 1)),
            PushResult::Dropped(DropCause::Closed)
        );
        assert!(q.pop_at(Instant::now()).is_none());
    }

    #[tokio::test]
    async fn pop_waits_for_a_push_and_returns_none_after_close() {
        let q = std::sync::Arc::new(LiveMediaQueue::new(cfg(MB, 10, 3000)));
        let q2 = std::sync::Arc::clone(&q);
        let waiter = tokio::spawn(async move { q2.pop().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.push(frame("a", ResyncPoint, 5, 1));
        let msg = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(tag(&msg), 5);
        let q3 = std::sync::Arc::clone(&q);
        let waiter = tokio::spawn(async move { q3.pop().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.close();
        let end = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(end.is_none());
    }

    /// Two-stream stall simulation: stream "a" is 30 fps video with a 2 s
    /// GOP plus audio, "b" a small audio-only stream, compared against a
    /// plain FIFO with the same input.
    struct Sim {
        start: Instant,
        q: LiveMediaQueue,
        fifo: VecDeque<(u64, usize)>,
        seq: u32,
        stall: std::ops::Range<u64>,
        first_after_stall: Option<(u64, bool)>,
        delivered_after_stall: u32,
        fifo_delivered_after_stall: u32,
        fifo_max_lag_after_stall: u64,
    }

    impl Sim {
        fn new(stall: std::ops::Range<u64>) -> Self {
            Self {
                start: Instant::now(),
                q: LiveMediaQueue::new(cfg(64 * MB, 100_000, 3000)),
                fifo: VecDeque::new(),
                seq: 0,
                stall,
                first_after_stall: None,
                delivered_after_stall: 0,
                fifo_delivered_after_stall: 0,
                fifo_max_lag_after_stall: 0,
            }
        }

        fn at(&self, ms: u64) -> Instant {
            self.start + Duration::from_millis(ms)
        }

        fn produce(&mut self, stream: &str, ms: u64, hint: DeliveryHint, tag: u8, len: usize) {
            self.q.push_at(frame(stream, hint, tag, len), self.at(ms));
            self.fifo.push_back((ms, len));
        }

        fn produce_tick(&mut self, ms: u64) {
            if ms.is_multiple_of(33) {
                let key = (ms / 33).is_multiple_of(60);
                let (hint, len) = if key {
                    (ResyncPoint, 60 * KB)
                } else {
                    (Droppable, 8 * KB)
                };
                self.produce("a", ms, hint, (self.seq % 250) as u8, len);
                self.seq += 1;
            }
            if ms.is_multiple_of(23) {
                self.produce("a", ms, Droppable, 1, KB);
            }
            if ms.is_multiple_of(20) {
                self.produce("b", ms, ResyncPoint, 2, 300);
            }
        }

        fn note_delivery(&mut self, ms: u64, msg: &MediaMessage) {
            let MediaMessage::MediaFrame { stream, hint, .. } = msg else {
                return;
            };
            if stream != "a" {
                return;
            }
            self.delivered_after_stall += 1;
            if self.first_after_stall.is_none() {
                self.first_after_stall = Some((ms, *hint == ResyncPoint));
            }
        }

        /// Writer on a fast link outside the stall.
        fn write_tick(&mut self, ms: u64) {
            if self.stall.contains(&ms) {
                return;
            }
            let after_stall = ms >= self.stall.end;
            while let Some(msg) = self.q.pop_at(self.at(ms)) {
                if after_stall {
                    self.note_delivery(ms, &msg);
                }
            }
            while let Some((t, _)) = self.fifo.pop_front() {
                if after_stall {
                    self.fifo_delivered_after_stall += 1;
                    self.fifo_max_lag_after_stall = self.fifo_max_lag_after_stall.max(ms - t);
                }
            }
        }

        fn print_report(&self, snap: &LiveQueueSnapshot) {
            println!(
                "--- cluster media overload report (stall {:?} ms, GOP 2 s) ---",
                self.stall
            );
            println!(
                "max queue bytes      : {} (bound 64 MiB)",
                snap.max_queue_bytes_seen
            );
            println!(
                "max queue age (ms)   : {}",
                snap.max_oldest_queue_age_ms_seen
            );
            println!(
                "dropped total/droppable/stale/critical/oversized: {}/{}/{}/{}/{}",
                snap.dropped_frames_total,
                snap.dropped_droppable_frames,
                snap.dropped_stale_frames,
                snap.dropped_critical_frames,
                snap.oversized_frames_dropped
            );
            println!("resync count         : {}", snap.resync_count);
            println!(
                "first stream-a frame after the stall: +{} ms, resync point: {}",
                self.first_after_stall
                    .map_or(0, |(t, _)| t - self.stall.end),
                self.first_after_stall.is_some_and(|(_, k)| k)
            );
            println!(
                "stream-a frames delivered after the stall: live-queue {}, plain FIFO would \
                 replay {} frames up to {} ms old",
                self.delivered_after_stall,
                self.fifo_delivered_after_stall,
                self.fifo_max_lag_after_stall
            );
        }
    }

    /// Reproducible overload scenario behind the numbers in
    /// `docs/clustering.md` (run with `--nocapture` to print the report): the
    /// writer is blocked from t=10 s to t=18 s.
    #[test]
    fn overload_report_stall_with_two_second_gop() {
        let end = 30_000u64;
        let mut sim = Sim::new(10_000..18_000);
        for ms in 0..=end {
            sim.produce_tick(ms);
            sim.write_tick(ms);
        }
        let snap = sim.q.snapshot_at(sim.at(end));
        sim.print_report(&snap);
        // The backlog never exceeded the age bound by more than one GOP...
        assert!(snap.max_oldest_queue_age_ms_seen <= 3000 + 1000, "{snap:?}");
        // ...nothing protected was lost, and delivery restarted on a keyframe.
        assert_eq!(snap.dropped_critical_frames, 0);
        assert!(sim.first_after_stall.is_some_and(|(_, key)| key));
        // The FIFO replays the full 8 s stall; the live queue far less.
        assert!(sim.fifo_max_lag_after_stall >= 7_000);
        assert!(sim.delivered_after_stall < sim.fifo_delivered_after_stall / 2);
        assert!(snap.resync_count >= 1);
    }

    #[test]
    fn config_derives_bounds_from_mb_and_ms() {
        let c = LiveQueueConfig::new(0, 0);
        assert_eq!(c.max_bytes, MB);
        assert_eq!(c.max_media_age, DEFAULT_MAX_MEDIA_AGE);
        let c = LiveQueueConfig::new(64, 1500);
        assert_eq!(c.max_bytes, 64 * MB);
        assert_eq!(c.max_media_age, Duration::from_millis(1500));
    }

    #[test]
    fn snapshots_merge_counters_and_maxima() {
        let a = LiveQueueSnapshot {
            queue_messages: 1,
            dropped_frames_total: 2,
            oldest_queue_age_ms: 10,
            max_queue_bytes_seen: 5,
            ..Default::default()
        };
        let mut b = LiveQueueSnapshot {
            queue_messages: 2,
            dropped_frames_total: 3,
            oldest_queue_age_ms: 30,
            max_queue_bytes_seen: 4,
            ..Default::default()
        };
        b.merge(&a);
        assert_eq!(b.queue_messages, 3);
        assert_eq!(b.dropped_frames_total, 5);
        assert_eq!(b.oldest_queue_age_ms, 30);
        assert_eq!(b.max_queue_bytes_seen, 5);
    }
}
