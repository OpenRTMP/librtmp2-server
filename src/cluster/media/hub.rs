//! Media hub: peer mesh, subscribe fan-out, inject queue.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use librtmp2::DeliveryHint;
use parking_lot::Mutex;
use rustls::{ClientConfig, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::cluster::NodeId;
use crate::cluster::media::cache::{InitCacheEntry, InitCacheStore};
use crate::cluster::media::live_queue::{LiveQueueConfig, LiveQueueSnapshot};
use crate::cluster::media::ownership::OwnershipTracker;
use crate::cluster::media::peer::{self, InboundMediaSink, MediaPeer, PeerMediaStats};
use crate::cluster::media::protocol::{MEDIA_PROTOCOL_VERSION, MediaMessage, SUBSCRIBE_DENIED};
use crate::cluster::media::subscription::SubscriptionTable;
use crate::cluster::media::timeline::TimelineRemapper;
use crate::cluster::media::{InboundSubscribeGateFn, MediaMembershipFn};

const MEDIA_INBOUND_QUEUE: usize = 4096;
/// Cap concurrent authenticated inbound media connections (frame reader tasks).
const MAX_MEDIA_CONN_INFLIGHT: usize = 512;
/// Preserve a global cap on half-open TLS/auth handshakes before inflight.
const MAX_PREAUTH_MEDIA_CONN_INFLIGHT: usize = 512;
/// Cap half-open auth handshakes per source IP so one source cannot consume
/// the entire global pre-authentication budget (mirrors control-plane limits).
const MAX_PREAUTH_MEDIA_CONN_PER_IP: usize = 16;
/// A just-admitted learner may authenticate before Raft membership has propagated
/// to the node handling its first Subscribe. Keep that one wire request alive
/// briefly instead of dropping it permanently while the connection stays open.
const SUBSCRIBE_GATE_RETRY_WINDOW: Duration = Duration::from_secs(2);
const SUBSCRIBE_GATE_RETRY_DELAY: Duration = Duration::from_millis(50);
const SUBSCRIBE_NACK_RETRY: Duration = Duration::from_millis(200);
const SUBSCRIBE_NACK_MAX: u8 = 3;
const SUBSCRIBE_NACK_SEND_RETRIES: u8 = 3;
/// Bounded retries, and the delay between them, for the InitCache enqueue that
/// registers an inbound Subscribe. The wire has no Subscribe ACK, so a
/// registration dropped on momentary queue pressure is never retried by the
/// subscriber.
const SUBSCRIBE_ENQUEUE_ATTEMPTS: u8 = 3;
const SUBSCRIBE_ENQUEUE_RETRY_DELAY: Duration = Duration::from_millis(50);
/// Soft cap on retained `subscribe_gens` entries. Generations only fence
/// in-flight NACK retries, so entries whose subscription is gone are dead
/// weight; the map is otherwise insert-only and grows for the process life.
const MAX_SUB_GENS: usize = 4096;
const ACCEPT_ERROR_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Handles a failed media-plane `accept`: logs it and backs off, returning
/// `false` once shutdown has been requested so the loop can exit. A transient
/// accept error must not stop the media plane.
async fn media_accept_retry(shutdown: &AtomicBool, e: std::io::Error) -> bool {
    if shutdown.load(Ordering::Relaxed) {
        return false;
    }
    tracing::warn!(error = %e, "cluster media accept failed; retrying");
    tokio::time::sleep(ACCEPT_ERROR_RETRY_DELAY).await;
    true
}
static MEDIA_CONN_INFLIGHT: AtomicUsize = AtomicUsize::new(0);
static PREAUTH_MEDIA_CONN_INFLIGHT: AtomicUsize = AtomicUsize::new(0);
static PREAUTH_MEDIA_CONN_PER_IP: Mutex<BTreeMap<IpAddr, usize>> = Mutex::new(BTreeMap::new());

fn try_acquire_global_preauth_media_slot() -> bool {
    if PREAUTH_MEDIA_CONN_INFLIGHT.fetch_add(1, Ordering::AcqRel) >= MAX_PREAUTH_MEDIA_CONN_INFLIGHT
    {
        PREAUTH_MEDIA_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
        return false;
    }
    true
}

fn release_global_preauth_media_slot() {
    PREAUTH_MEDIA_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
}

fn try_acquire_preauth_media_slot(peer: IpAddr) -> bool {
    let mut guard = PREAUTH_MEDIA_CONN_PER_IP.lock();
    let count = guard.entry(peer).or_insert(0);
    if *count >= MAX_PREAUTH_MEDIA_CONN_PER_IP {
        return false;
    }
    *count += 1;
    true
}

fn release_preauth_media_slot(peer: IpAddr) {
    let mut guard = PREAUTH_MEDIA_CONN_PER_IP.lock();
    if let Some(count) = guard.get_mut(&peer) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            guard.remove(&peer);
        }
    }
}

struct PreauthMediaGuard(IpAddr);

impl Drop for PreauthMediaGuard {
    fn drop(&mut self) {
        release_preauth_media_slot(self.0);
        release_global_preauth_media_slot();
    }
}

struct MediaInflightGuard;

impl Drop for MediaInflightGuard {
    fn drop(&mut self) {
        MEDIA_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Frame exported from local librtmp2 for mesh fan-out.
#[derive(Debug, Clone)]
pub struct ExportedFrame {
    pub app: String,
    pub stream: String,
    pub epoch: u64,
    pub frame_type: u8,
    pub timestamp: u32,
    /// Congestion class assigned by librtmp2 at export.
    pub hint: DeliveryHint,
    pub payload: Vec<u8>,
}

/// Frame from a remote peer ready for local inject.
#[derive(Debug, Clone)]
pub struct InjectedFrame {
    pub app: String,
    pub stream: String,
    pub epoch: u64,
    pub frame_type: u8,
    pub timestamp: u32,
    /// Congestion class the sending node attached.
    pub hint: DeliveryHint,
    pub payload: Vec<u8>,
}

/// Media-plane status: aggregate and per-peer queue statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MediaPlaneStats {
    pub protocol_version: u16,
    #[serde(flatten)]
    pub queue: LiveQueueSnapshot,
    pub write_timeouts: u64,
    pub reconnects: u64,
    pub peers: Vec<PeerMediaStats>,
}

/// A queued media frame as seen by the shared eviction policy of
/// [`ExportQueue`] and [`InjectQueue`].
trait QueuedMedia {
    fn names(&self) -> (&str, &str);
    fn hint(&self) -> DeliveryHint;
    fn size(&self) -> usize;
}

impl QueuedMedia for ExportedFrame {
    fn names(&self) -> (&str, &str) {
        (&self.app, &self.stream)
    }
    fn hint(&self) -> DeliveryHint {
        self.hint
    }
    fn size(&self) -> usize {
        self.payload.len().saturating_add(64)
    }
}

impl QueuedMedia for InjectedFrame {
    fn names(&self) -> (&str, &str) {
        (&self.app, &self.stream)
    }
    fn hint(&self) -> DeliveryHint {
        self.hint
    }
    fn size(&self) -> usize {
        self.payload.len().saturating_add(64)
    }
}

/// Counters of an [`ExportQueue`] / [`InjectQueue`], by evicted class.
#[derive(Default)]
struct EvictionCounters {
    droppable: AtomicU64,
    resync_point: AtomicU64,
    critical: AtomicU64,
    oversized: AtomicU64,
}

/// Snapshot of [`EvictionCounters`].
#[derive(Debug, Clone, Default, serde::Serialize, PartialEq, Eq)]
pub struct EvictionStats {
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub evicted_droppable_frames: u64,
    pub evicted_resync_point_frames: u64,
    pub evicted_critical_frames: u64,
    pub oversized_frames_dropped: u64,
}

impl EvictionCounters {
    fn count(&self, hint: DeliveryHint) {
        match hint {
            DeliveryHint::Droppable => &self.droppable,
            DeliveryHint::ResyncPoint => &self.resync_point,
            DeliveryHint::Critical => &self.critical,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, queue_messages: usize, queue_bytes: usize) -> EvictionStats {
        EvictionStats {
            queue_messages,
            queue_bytes,
            evicted_droppable_frames: self.droppable.load(Ordering::Relaxed),
            evicted_resync_point_frames: self.resync_point.load(Ordering::Relaxed),
            evicted_critical_frames: self.critical.load(Ordering::Relaxed),
            oversized_frames_dropped: self.oversized.load(Ordering::Relaxed),
        }
    }
}

/// Make room for `need` more bytes in a byte-bounded frame queue.
///
/// Shared eviction policy of the export and inject queues (they stay
/// separate types because their consumers differ): frames are evicted from
/// the stream that currently holds the most queued bytes (ties: smaller
/// `(app, stream)`), so one overloaded stream does not eat the frames of
/// the others; within that stream the oldest `Droppable` frame goes first,
/// then the oldest `ResyncPoint`, and a `Critical` frame only when nothing
/// else of the stream is left. Evicting a single frame can leave a gap that
/// only the next resync point repairs; the per-peer [`LiveMediaQueue`]
/// does the stream-wide resync accounting, these queues only decide *what*
/// to give up first. Returns `false` when `need` cannot fit even in an
/// empty queue.
fn evict_for_room<T: QueuedMedia>(
    frames: &mut std::collections::VecDeque<T>,
    bytes: &mut usize,
    need: usize,
    max_bytes: usize,
    counters: &EvictionCounters,
) -> bool {
    if need > max_bytes {
        return false;
    }
    while bytes.saturating_add(need) > max_bytes {
        let mut per_stream: HashMap<(&str, &str), usize> = HashMap::new();
        for f in frames.iter() {
            *per_stream.entry(f.names()).or_default() += f.size();
        }
        let Some(((app, stream), _)) = per_stream
            .into_iter()
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
        else {
            return false;
        };
        let (app, stream) = (app.to_string(), stream.to_string());
        let victim = [
            DeliveryHint::Droppable,
            DeliveryHint::ResyncPoint,
            DeliveryHint::Critical,
        ]
        .into_iter()
        .find_map(|class| {
            frames
                .iter()
                .position(|f| f.names() == (app.as_str(), stream.as_str()) && f.hint() == class)
        });
        let Some(idx) = victim else {
            return false;
        };
        if let Some(old) = frames.remove(idx) {
            *bytes = bytes.saturating_sub(old.size());
            counters.count(old.hint());
            tracing::warn!(
                app = %old.names().0,
                stream = %old.names().1,
                hint = ?old.hint(),
                "media queue full — evicting frame of the heaviest stream"
            );
        }
    }
    true
}

/// Byte-bounded queue for remote→local media injection.
///
/// Overload policy: **evict, don't reject** — room for a new frame is made
/// by the shared eviction policy (see [`evict_for_room`]); only a single
/// frame larger than the whole queue is refused, without touching the
/// backlog.
pub struct InjectQueue {
    state: Mutex<(std::collections::VecDeque<InjectedFrame>, usize)>,
    max_bytes: usize,
    counters: EvictionCounters,
}

impl InjectQueue {
    pub fn new(max_mb: u32) -> Arc<Self> {
        let max_bytes = (max_mb as usize)
            .saturating_mul(1024 * 1024)
            .max(1024 * 1024);
        Arc::new(Self {
            state: Mutex::new((std::collections::VecDeque::new(), 0)),
            max_bytes,
            counters: EvictionCounters::default(),
        })
    }

    pub fn try_send(&self, frame: InjectedFrame) -> Result<(), ()> {
        let size = frame.size();
        let mut st = self.state.lock();
        let (frames, bytes) = &mut *st;
        // Refuses an oversized frame before any eviction: draining the
        // whole backlog would destroy every other stream's frames for a
        // frame that was never going to fit anyway.
        if !evict_for_room(frames, bytes, size, self.max_bytes, &self.counters) {
            self.counters.oversized.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                app = %frame.app,
                stream = %frame.stream,
                "inbound media inject queue cannot take frame — dropping it"
            );
            return Err(());
        }
        *bytes = bytes.saturating_add(size);
        frames.push_back(frame);
        Ok(())
    }

    pub fn drain(&self) -> Vec<InjectedFrame> {
        let mut st = self.state.lock();
        st.1 = 0;
        st.0.drain(..).collect()
    }

    pub fn stats(&self) -> EvictionStats {
        let st = self.state.lock();
        self.counters.snapshot(st.0.len(), st.1)
    }
}

/// Byte-bounded ordered export queue (local librtmp2 → mesh fan-out).
///
/// Overload policy: **evict by class from the heaviest stream** (see
/// [`evict_for_room`]) until the new frame fits, or refuse a frame that
/// exceeds the whole queue without touching the backlog. FIFO order is kept
/// for the frames that remain.
pub struct ExportQueue {
    state: Mutex<(std::collections::VecDeque<ExportedFrame>, usize)>,
    max_bytes: usize,
    notify: tokio::sync::Notify,
    counters: EvictionCounters,
}

impl ExportQueue {
    pub fn new(max_mb: u32) -> Arc<Self> {
        let max_bytes = (max_mb as usize)
            .saturating_mul(1024 * 1024)
            .max(1024 * 1024);
        Arc::new(Self {
            state: Mutex::new((std::collections::VecDeque::new(), 0)),
            max_bytes,
            notify: tokio::sync::Notify::new(),
            counters: EvictionCounters::default(),
        })
    }

    pub fn push(&self, frame: ExportedFrame) {
        let size = frame.size();
        let mut st = self.state.lock();
        let (frames, bytes) = &mut *st;
        if !evict_for_room(frames, bytes, size, self.max_bytes, &self.counters) {
            self.counters.oversized.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                app = %frame.app,
                stream = %frame.stream,
                "export media queue full — dropping oversized frame"
            );
            return;
        }
        *bytes = bytes.saturating_add(size);
        frames.push_back(frame);
        drop(st);
        self.notify.notify_one();
    }

    pub fn drain(&self) -> Vec<ExportedFrame> {
        let mut st = self.state.lock();
        st.1 = 0;
        st.0.drain(..).collect()
    }

    pub fn stats(&self) -> EvictionStats {
        let st = self.state.lock();
        self.counters.snapshot(st.0.len(), st.1)
    }

    /// Wait until at least one frame is queued, then drain (no lost wakeups).
    pub async fn wait_and_drain(&self) -> Vec<ExportedFrame> {
        loop {
            let notified = self.notify.notified();
            {
                let frames = self.drain();
                if !frames.is_empty() {
                    drop(notified);
                    return frames;
                }
            }
            notified.await;
        }
    }
}

pub struct MediaHub {
    local_id: NodeId,
    secret: String,
    queue_mb: u32,
    /// Age bound of the per-peer live-media queues in ms (`0` = default).
    media_max_age_ms: AtomicU32,
    /// Configured standby replica slots (cluster inventory); MediaFrame fanout
    /// is subscriber-only so this is unused for continuous media push.
    #[allow(dead_code)]
    replicas: u32,
    ownership: Arc<OwnershipTracker>,
    peers: Mutex<HashMap<NodeId, Arc<MediaPeer>>>,
    /// Write pumps for accepted inbound sessions, keyed by remote node.
    inbound_sinks: Mutex<HashMap<NodeId, Arc<InboundMediaSink>>>,
    /// Per-(owner, app, stream) NACK retries after `subscribe_denied`.
    subscribe_nacks: Mutex<HashMap<(NodeId, String, String), u8>>,
    /// Generation per owner+stream so delayed NACK retries cannot fire after
    /// unsubscribe/resubscribe and double the owner's Subscribe refcount.
    subscribe_gens: Mutex<HashMap<(NodeId, String, String), u64>>,
    /// Source for `subscribe_gens` values. One shared counter, never reused
    /// and never handing out 0, so a key pruned while a retry sleeps (reading
    /// back as the 0 sentinel) and a key re-created later (reading back as a
    /// fresh value) can never collide with the generation a retry captured.
    next_sub_gen: AtomicU64,
    /// Serializes `subs.add` + peer (re)creation + resubscribe snapshot so a
    /// concurrent `subscribe_remote` cannot slip a new stream into the
    /// snapshot and have it sent twice (once by the fresh-peer resubscribe,
    /// once by the caller's direct send).
    subscribe_lock: Mutex<()>,
    subs: SubscriptionTable,
    cache: InitCacheStore,
    timelines: Mutex<HashMap<(String, String), TimelineRemapper>>,
    inject: Arc<InjectQueue>,
    /// Inbound frames from outbound `MediaPeer` readers, stamped with the
    /// authenticated remote node id so ownership fencing matches the direct
    /// accept path (`handle_inbound_conn`).
    inbound_tx: mpsc::Sender<(NodeId, MediaMessage)>,
    shutdown: AtomicBool,
    tls_server: Option<Arc<ServerConfig>>,
    tls_client: Option<Arc<ClientConfig>>,
    peer_reconnect_tx: mpsc::UnboundedSender<NodeId>,
    is_peer_allowed: MediaMembershipFn,
    /// Optional authorization for inbound `Subscribe` (cluster owner nodes).
    inbound_subscribe_gate: parking_lot::Mutex<Option<InboundSubscribeGateFn>>,
}

impl MediaHub {
    pub fn new(
        local_id: NodeId,
        secret: String,
        queue_mb: u32,
        replicas: u32,
        ownership: Arc<OwnershipTracker>,
        inject: Arc<InjectQueue>,
        tls_server: Option<Arc<ServerConfig>>,
        tls_client: Option<Arc<ClientConfig>>,
        is_peer_allowed: MediaMembershipFn,
    ) -> Arc<Self> {
        let (inbound_tx, mut inbound_rx) =
            mpsc::channel::<(NodeId, MediaMessage)>(MEDIA_INBOUND_QUEUE);
        let (peer_reconnect_tx, mut peer_reconnect_rx) = mpsc::unbounded_channel::<NodeId>();
        let hub = Arc::new(Self {
            local_id,
            secret,
            queue_mb,
            media_max_age_ms: AtomicU32::new(0),
            replicas,
            ownership,
            peers: Mutex::new(HashMap::new()),
            inbound_sinks: Mutex::new(HashMap::new()),
            subscribe_nacks: Mutex::new(HashMap::new()),
            subscribe_gens: Mutex::new(HashMap::new()),
            // Starts at 1: 0 is the "no generation recorded" sentinel on the
            // wire and must never be handed out as a real generation.
            next_sub_gen: AtomicU64::new(1),
            subscribe_lock: Mutex::new(()),
            subs: SubscriptionTable::new(),
            cache: InitCacheStore::new(),
            timelines: Mutex::new(HashMap::new()),
            inject,
            inbound_tx,
            shutdown: AtomicBool::new(false),
            tls_server,
            tls_client,
            peer_reconnect_tx,
            is_peer_allowed,
            inbound_subscribe_gate: parking_lot::Mutex::new(None),
        });
        let hub_c = Arc::clone(&hub);
        tokio::spawn(async move {
            while let Some((peer_id, msg)) = inbound_rx.recv().await {
                hub_c.handle_inbound(peer_id, msg);
            }
        });
        let hub_r = Arc::clone(&hub);
        tokio::spawn(async move {
            while let Some(peer_id) = peer_reconnect_rx.recv().await {
                hub_r.resubscribe_peer(peer_id);
            }
        });
        hub
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        for p in self.peers.lock().values() {
            p.close();
        }
        for s in self.inbound_sinks.lock().values() {
            s.close();
        }
    }

    pub fn peer_count(&self) -> usize {
        self.peers.lock().len()
    }

    pub fn disconnect_peer(&self, peer_id: NodeId) {
        if let Some(p) = self.peers.lock().remove(&peer_id) {
            p.close();
        }
        if let Some(s) = self.inbound_sinks.lock().remove(&peer_id) {
            s.close();
        }
        let nack_keys: Vec<_> = self
            .subscribe_nacks
            .lock()
            .keys()
            .filter(|(owner, _, _)| *owner == peer_id)
            .cloned()
            .collect();
        self.subscribe_nacks
            .lock()
            .retain(|(owner, _, _), _| *owner != peer_id);
        let streams = self.subs.streams_for_peer(peer_id);
        {
            let mut gens = self.subscribe_gens.lock();
            for key in nack_keys {
                gens.entry(key).or_insert(0);
            }
            for (app, stream) in streams {
                gens.entry((peer_id, app, stream)).or_insert(0);
            }
            for ((owner, _, _), generation) in gens.iter_mut() {
                if *owner == peer_id {
                    *generation = self.alloc_sub_gen();
                }
            }
        }
        self.subs.clear_peer(peer_id);
    }

    fn sub_key(peer_id: NodeId, app: &str, stream: &str) -> (NodeId, String, String) {
        (peer_id, app.to_string(), stream.to_string())
    }

    /// Allocate a generation no other owner+stream has ever held. A per-key
    /// counter handed out the same small values again once a key was pruned
    /// and re-created, so a retry still sleeping across that prune matched the
    /// recycled value and re-sent a `Subscribe` the owner had already counted.
    fn alloc_sub_gen(&self) -> u64 {
        self.next_sub_gen.fetch_add(1, Ordering::Relaxed)
    }

    fn sub_gen(&self, peer_id: NodeId, app: &str, stream: &str) -> u64 {
        self.subscribe_gens
            .lock()
            .get(&Self::sub_key(peer_id, app, stream))
            .copied()
            .unwrap_or(0)
    }

    /// Allocate a generation no other owner+stream has ever held and record it
    /// for `peer_id`'s `app`/`stream`; returns the value stored, so a caller
    /// that fences on it never re-reads the map and can never capture the 0 an
    /// absent key reads back as.
    fn bump_sub_gen(&self, peer_id: NodeId, app: &str, stream: &str) -> u64 {
        let mut gens = self.subscribe_gens.lock();
        let generation = self.alloc_sub_gen();
        *gens.entry(Self::sub_key(peer_id, app, stream)).or_default() = generation;
        if gens.len() > MAX_SUB_GENS {
            // Keep only the generations that still fence something: a live
            // subscription, or a NACK chain whose `schedule_subscribe_retry`
            // task may still be in flight. The bumping peer's own inactive
            // entries must go too — keeping them is what let one peer that
            // churns distinct streams grow the map past the cap for good.
            // A pruned key reads back as generation 0, which no stored
            // generation can equal, so a pruned key is a dead fence.
            let nacks = self.subscribe_nacks.lock();
            gens.retain(|key, _| {
                nacks.contains_key(key)
                    || self.subs.peers_for_stream(&key.1, &key.2).contains(&key.0)
            });
        }
        generation
    }

    fn clear_subscribe_nacks(&self, peer_id: NodeId, app: &str, stream: &str) {
        let had = self
            .subscribe_nacks
            .lock()
            .remove(&Self::sub_key(peer_id, app, stream))
            .is_some();
        if had {
            self.bump_sub_gen(peer_id, app, stream);
        }
    }

    fn subscribe_message(
        &self,
        peer_id: NodeId,
        app: &str,
        stream: &str,
        epoch: u64,
    ) -> MediaMessage {
        MediaMessage::Subscribe {
            app: app.to_string(),
            stream: stream.to_string(),
            epoch,
            generation: self.sub_gen(peer_id, app, stream),
        }
    }

    fn schedule_subscribe_retry(
        self: &Arc<Self>,
        peer_id: NodeId,
        app: String,
        stream: String,
        generation: u64,
        send_retries: u8,
    ) {
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(SUBSCRIBE_NACK_RETRY).await;
            hub.run_subscribe_retry(peer_id, app, stream, generation, send_retries)
                .await;
        });
    }

    async fn run_subscribe_retry(
        self: Arc<Self>,
        peer_id: NodeId,
        app: String,
        stream: String,
        generation: u64,
        send_retries: u8,
    ) {
        if self.shutdown.load(Ordering::Relaxed) {
            return;
        }
        if self.sub_gen(peer_id, &app, &stream) != generation {
            return;
        }
        if !self.subs.peers_for_stream(&app, &stream).contains(&peer_id) {
            return;
        }
        let epoch = self.ownership.epoch_of(&app, &stream).unwrap_or(0);
        let Some(peer) = self.peers.lock().get(&peer_id).cloned() else {
            return;
        };
        let msg = self.subscribe_message(peer_id, &app, &stream, epoch);
        if peer.try_send(msg.clone()).is_ok() || peer.try_send(msg).is_ok() {
            return;
        }
        if send_retries > 0 {
            self.schedule_subscribe_retry(peer_id, app, stream, generation, send_retries - 1);
        }
    }

    pub fn set_inbound_subscribe_gate(&self, gate: InboundSubscribeGateFn) {
        *self.inbound_subscribe_gate.lock() = Some(gate);
    }

    /// Retry a transient Subscribe denial for a bounded interval. This is
    /// needed because the media protocol has no Subscribe ACK: dropping the
    /// first request while Raft membership converges would otherwise leave the
    /// caller's retained subscription refcount with no wire-level subscription.
    async fn inbound_subscribe_allowed(&self, peer_id: NodeId, app: &str, stream: &str) -> bool {
        let Some(gate) = self.inbound_subscribe_gate.lock().clone() else {
            return true;
        };
        let app = app.to_string();
        let stream = stream.to_string();
        let retry = async move {
            loop {
                if gate(peer_id, app.clone(), stream.clone()).await {
                    return true;
                }
                tokio::time::sleep(SUBSCRIBE_GATE_RETRY_DELAY).await;
            }
        };
        tokio::time::timeout(SUBSCRIBE_GATE_RETRY_WINDOW, retry)
            .await
            .unwrap_or(false)
    }

    pub async fn serve(self: Arc<Self>, bind: SocketAddr) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(bind).await?;
        tracing::info!(%bind, tls = self.tls_server.is_some(), "cluster media plane listening");
        self.accept_loop(listener).await
    }

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) -> Result<(), std::io::Error> {
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let (stream, peer_addr) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    if !media_accept_retry(&self.shutdown, e).await {
                        break;
                    }
                    continue;
                }
            };
            let peer_ip = peer_addr.ip();
            if !try_acquire_global_preauth_media_slot() {
                tracing::debug!(%peer_addr, "media connection rejected: global preauth limit");
                continue;
            }
            if !try_acquire_preauth_media_slot(peer_ip) {
                release_global_preauth_media_slot();
                tracing::debug!(%peer_addr, "media connection rejected: preauth per-ip limit");
                continue;
            }
            let hub = Arc::clone(&self);
            tokio::spawn(async move {
                let preauth = PreauthMediaGuard(peer_ip);
                let auth_result = peer::accept_tls_then_auth(
                    stream,
                    peer_ip,
                    &hub.secret,
                    hub.local_id,
                    hub.tls_server.clone(),
                    Arc::clone(&hub.is_peer_allowed),
                )
                .await;
                drop(preauth);
                match auth_result {
                    Ok((peer_id, version, io)) => {
                        if MEDIA_CONN_INFLIGHT.fetch_add(1, Ordering::AcqRel)
                            >= MAX_MEDIA_CONN_INFLIGHT
                        {
                            MEDIA_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
                            tracing::warn!(
                                peer = peer_id,
                                "media inbound connection cap reached after auth; dropping"
                            );
                            return;
                        }
                        let _inflight = MediaInflightGuard;
                        if let Err(e) = hub.run_inbound_media_session(peer_id, version, io).await {
                            tracing::debug!(error=%e, "media inbound closed");
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error=%e, "media inbound auth failed");
                    }
                }
            });
        }
        Ok(())
    }

    async fn run_inbound_media_session(
        self: Arc<Self>,
        peer_id: NodeId,
        version: u16,
        io: Box<dyn peer::MediaIo>,
    ) -> Result<(), std::io::Error> {
        // Track Subscribe messages on this connection so a drop without Unsubscribe
        // cannot leave stale SubscriptionTable entries for the peer.
        let mut conn_subs: std::collections::HashMap<(String, String), usize> =
            std::collections::HashMap::new();
        let (mut rh, wh) = tokio::io::split(io);
        let sink = Arc::new(InboundMediaSink::spawn(
            peer_id,
            self.queue_cfg(),
            version,
            wh,
        ));
        if let Some(old) = self.inbound_sinks.lock().insert(peer_id, Arc::clone(&sink)) {
            old.close();
        }
        let result = async {
            loop {
                let msg = peer::read_media_frame_v(&mut rh, version).await?;
                match msg {
                    MediaMessage::Subscribe {
                        app,
                        stream,
                        epoch: _,
                        generation,
                    } => {
                        if !self.inbound_subscribe_allowed(peer_id, &app, &stream).await {
                            tracing::warn!(
                                peer = peer_id,
                                %app,
                                %stream,
                                "media subscribe rejected: peer not authorized"
                            );
                            let _ = sink
                                .send(subscribe_denied_error(&app, &stream, generation))
                                .await;
                            continue;
                        }
                        let init_msg = if let Some(cache) = self.cache.get(&app, &stream) {
                            Some(MediaMessage::InitCache {
                                app: app.clone(),
                                stream: stream.clone(),
                                epoch: cache.epoch,
                                metadata: cache.metadata,
                                avc_header: cache.avc_header,
                                aac_header: cache.aac_header,
                                keyframe: cache.keyframe,
                            })
                        } else {
                            None
                        };
                        // Enqueue InitCache (if any) under the subscription lock
                        // so fan-out cannot observe the new ref until headers
                        // are first in this sink's FIFO. A momentarily full
                        // sink is retried a bounded number of times instead of
                        // abandoned: the peer gets no ACK and nothing tells it
                        // to resubscribe, so a registration dropped here leaves
                        // its refcount pointing at an owner that never fans out
                        // to it again.
                        let mut registered: Result<bool, ()> = Ok(false);
                        for attempt in 0..=SUBSCRIBE_ENQUEUE_ATTEMPTS {
                            if attempt > 0 {
                                tokio::time::sleep(SUBSCRIBE_ENQUEUE_RETRY_DELAY).await;
                            }
                            registered = self.subs.add_with(peer_id, &app, &stream, || {
                                if sink.is_closed() {
                                    return Err(());
                                }
                                match &init_msg {
                                    Some(msg) => sink.try_send(msg.clone()),
                                    None => Ok(()),
                                }
                            });
                            if registered.is_ok() {
                                break;
                            }
                        }
                        if registered.is_err() {
                            if sink.is_closed() {
                                return Err(std::io::Error::other("inbound media sink closed"));
                            }
                            // The InitCache enqueue is still failing for a
                            // non-fatal reason after the bounded retries: this
                            // sink's byte budget or its bounded channel stays
                            // full. SUBSCRIBE_DENIED here would read as an
                            // authorization denial — the peer retries, then
                            // calls subs.clear_entry, dropping the refcount
                            // shared by every local player on that node, turning
                            // a stall into permanent loss of a live stream. The
                            // wire has no Subscribe ACK, so silence is the same
                            // signal success gives; the peer keeps its ref and a
                            // later resubscribe re-offers this Subscribe.
                            continue;
                        }
                        *conn_subs.entry((app.clone(), stream.clone())).or_insert(0) += 1;
                    }
                    MediaMessage::Unsubscribe { app, stream } => {
                        self.subs.remove(peer_id, &app, &stream);
                        if let Some(c) = conn_subs.get_mut(&(app.clone(), stream.clone())) {
                            *c = c.saturating_sub(1);
                            if *c == 0 {
                                conn_subs.remove(&(app, stream));
                            }
                        }
                    }
                    MediaMessage::MediaFrame {
                        app,
                        stream,
                        epoch,
                        frame_type,
                        timeline_ts,
                        hint,
                        payload,
                        ..
                    } => {
                        // Require sender node + epoch — epoch alone lets any
                        // member inject under a stolen fencing token.
                        if self.ownership.accepts_owner(&stream, peer_id, epoch) {
                            // Remap on the receiving node so ownership failover cannot
                            // reset player timelines when the new owner starts at ts=0.
                            let timestamp = {
                                let mut maps = self.timelines.lock();
                                let key = (app.clone(), stream.clone());
                                maps.entry(key).or_default().map(epoch, timeline_ts)
                            };
                            let _ = self.inject.try_send(InjectedFrame {
                                app,
                                stream,
                                epoch,
                                frame_type,
                                timestamp,
                                hint,
                                payload,
                            });
                        }
                    }
                    MediaMessage::InitCache {
                        app,
                        stream,
                        epoch,
                        metadata,
                        avc_header,
                        aac_header,
                        keyframe,
                    } => {
                        if !self.ownership.accepts_owner(&stream, peer_id, epoch) {
                            continue;
                        }
                        let entry = InitCacheEntry {
                            metadata: metadata.clone(),
                            avc_header: avc_header.clone(),
                            aac_header: aac_header.clone(),
                            keyframe: keyframe.clone(),
                            epoch,
                        };
                        self.cache.put(&app, &stream, entry.clone());
                        self.inject_init_cache_live(app, stream, entry);
                    }
                    MediaMessage::StatsReq { stream_id: _ } => {}
                    MediaMessage::Error { .. } => {}
                    other => {
                        let _ = self.inbound_tx.try_send((peer_id, other));
                    }
                }
            }
        }
        .await;
        {
            let mut sinks = self.inbound_sinks.lock();
            if sinks.get(&peer_id).is_some_and(|s| Arc::ptr_eq(s, &sink)) {
                sinks.remove(&peer_id);
            }
        }
        sink.close();
        for ((app, stream), n) in conn_subs {
            // Release only this connection's Subscribe refs; outbound
            // subscribe_remote refcounts for the same peer are left intact.
            for _ in 0..n {
                let _ = self.subs.remove(peer_id, &app, &stream);
            }
        }
        result
    }

    fn handle_inbound(self: &Arc<Self>, peer_id: NodeId, msg: MediaMessage) {
        match msg {
            MediaMessage::MediaFrame {
                app,
                stream,
                epoch,
                frame_type,
                timeline_ts,
                hint,
                payload,
                ..
            } => {
                // Same owner+epoch fence as the direct inbound-connection path —
                // epoch alone would let any connected member inject under a
                // stolen fencing token via the outbound MediaPeer reader.
                if !self.ownership.accepts_owner(&stream, peer_id, epoch) {
                    return;
                }
                self.clear_subscribe_nacks(peer_id, &app, &stream);
                let timestamp = {
                    let mut maps = self.timelines.lock();
                    let key = (app.clone(), stream.clone());
                    maps.entry(key).or_default().map(epoch, timeline_ts)
                };
                let _ = self.inject.try_send(InjectedFrame {
                    app,
                    stream,
                    epoch,
                    frame_type,
                    timestamp,
                    hint,
                    payload,
                });
            }
            // A subscriber's outbound MediaPeer reader forwards the owner's
            // InitCache reply here (see handle_inbound_conn's Subscribe arm,
            // which writes it back over that same connection). Previously
            // this hit the wildcard and was discarded, so late joiners on an
            // outbound-subscriber connection never got cached codec headers
            // or the last keyframe.
            MediaMessage::InitCache {
                app,
                stream,
                epoch,
                metadata,
                avc_header,
                aac_header,
                keyframe,
            } => {
                if !self.ownership.accepts_owner(&stream, peer_id, epoch) {
                    return;
                }
                self.clear_subscribe_nacks(peer_id, &app, &stream);
                let entry = InitCacheEntry {
                    metadata: metadata.clone(),
                    avc_header: avc_header.clone(),
                    aac_header: aac_header.clone(),
                    keyframe: keyframe.clone(),
                    epoch,
                };
                self.cache.put(&app, &stream, entry.clone());
                self.inject_init_cache_live(app, stream, entry);
            }
            MediaMessage::Error {
                code,
                message,
                generation: req_gen,
            } => {
                if code != SUBSCRIBE_DENIED {
                    return;
                }
                let Some((app, stream)) = parse_subscribe_denied(&message) else {
                    return;
                };
                if !self.subs.peers_for_stream(&app, &stream).contains(&peer_id) {
                    return;
                }
                let generation = self.sub_gen(peer_id, &app, &stream);
                if req_gen != 0 && req_gen != generation {
                    return;
                }
                // Register the NACK before allocating the generation the retry
                // captures. The prune in `bump_sub_gen` keeps every NACKed key,
                // so allocation can no longer drop this key in the gap: an
                // `unsubscribe_remote` landing there would otherwise leave the
                // retry holding the 0 an absent key reads back as — which a
                // resubscribe before the retry wakes also reads as — and the
                // duplicate Subscribe would leave a phantom owner refcount.
                let n = {
                    let mut nacks = self.subscribe_nacks.lock();
                    let e = nacks
                        .entry((peer_id, app.clone(), stream.clone()))
                        .or_insert(0);
                    *e = e.saturating_add(1);
                    *e
                };
                let generation = if generation == 0 {
                    self.bump_sub_gen(peer_id, &app, &stream)
                } else {
                    generation
                };
                if n > SUBSCRIBE_NACK_MAX {
                    self.clear_subscribe_nacks(peer_id, &app, &stream);
                    self.bump_sub_gen(peer_id, &app, &stream);
                    self.subs.clear_entry(peer_id, &app, &stream);
                    tracing::warn!(
                        peer = peer_id,
                        %app,
                        %stream,
                        "media subscribe denied after retries — dropping local refs"
                    );
                    return;
                }
                self.schedule_subscribe_retry(
                    peer_id,
                    app,
                    stream,
                    generation,
                    SUBSCRIBE_NACK_SEND_RETRIES,
                );
            }
            _ => {}
        }
    }

    /// Inject InitCache payloads onto the live path using one remapped
    /// timestamp for headers and keyframe. Mid-stream InitCache must not emit
    /// timestamp 0 ahead of a high remapped keyframe (non-monotonic for players
    /// that already saw the previous epoch). Cache keyframe timestamps are in
    /// the MediaFrame.timeline_ts domain after `fanout_local_frame` stages
    /// remapped values.
    fn inject_init_cache_live(&self, app: String, stream: String, entry: InitCacheEntry) {
        let epoch = entry.epoch;
        let (inject_ts, kf_payload) = {
            let mut maps = self.timelines.lock();
            let key = (app.clone(), stream.clone());
            let remap = maps.entry(key).or_default();
            match entry.keyframe {
                Some((ts, kf)) => {
                    // Mid-stream InitCache must not run map() on a GOP-behind
                    // keyframe: the within-epoch non-monotonic guard would
                    // permanently bump offset and jump every later MediaFrame.
                    let t = if remap.last_out() == 0 {
                        remap.map(epoch, ts)
                    } else {
                        remap.last_out()
                    };
                    (t, Some(kf))
                }
                None => (remap.last_out(), None),
            }
        };
        if let Some(md) = entry.metadata {
            let _ = self.inject.try_send(InjectedFrame {
                app: app.clone(),
                stream: stream.clone(),
                epoch,
                frame_type: 2,
                timestamp: inject_ts,
                hint: DeliveryHint::Critical,
                payload: md,
            });
        }
        if let Some(h) = entry.avc_header {
            let _ = self.inject.try_send(InjectedFrame {
                app: app.clone(),
                stream: stream.clone(),
                epoch,
                frame_type: 1,
                timestamp: inject_ts,
                hint: DeliveryHint::Critical,
                payload: h,
            });
        }
        if let Some(h) = entry.aac_header {
            let _ = self.inject.try_send(InjectedFrame {
                app: app.clone(),
                stream: stream.clone(),
                epoch,
                frame_type: 0,
                timestamp: inject_ts,
                hint: DeliveryHint::Critical,
                payload: h,
            });
        }
        if let Some(kf) = kf_payload {
            let _ = self.inject.try_send(InjectedFrame {
                app,
                stream,
                epoch,
                frame_type: 1,
                timestamp: inject_ts,
                hint: DeliveryHint::ResyncPoint,
                payload: kf,
            });
        }
    }

    /// Returns the live `MediaPeer` for `peer_id`, and whether a fresh
    /// connection was just created (either none existed, or the previous one
    /// was closed / pointed at a stale address). A fresh connection has
    /// already had every subscription this hub tracks for `peer_id` resent
    /// on it — the `SubscriptionTable` refcounts survive the swap, but the
    /// wire-level `Subscribe` state on the old connection does not, and
    /// without resending it the new connection carries none.
    fn ensure_peer(&self, peer_id: NodeId, addr: &str) -> (Arc<MediaPeer>, bool) {
        let _guard = self.subscribe_lock.lock();
        self.ensure_peer_locked(peer_id, addr)
    }

    fn ensure_peer_locked(&self, peer_id: NodeId, addr: &str) -> (Arc<MediaPeer>, bool) {
        let (peer, fresh) = {
            let mut peers = self.peers.lock();
            if let Some(p) = peers.get(&peer_id) {
                if !p.is_closed() && p.addr == addr {
                    return (Arc::clone(p), false);
                }
                // Closed, or advertised address changed — drop and redial.
                p.close();
                peers.remove(&peer_id);
            }
            let peer = Arc::new(MediaPeer::spawn(
                peer_id,
                addr.to_string(),
                self.secret.clone(),
                self.local_id,
                self.queue_cfg(),
                self.inbound_tx.clone(),
                self.tls_client.clone(),
                self.peer_reconnect_tx.clone(),
            ));
            peers.insert(peer_id, Arc::clone(&peer));
            (peer, true)
        };
        if fresh {
            self.resubscribe_peer_locked(peer_id);
        }
        (peer, fresh)
    }

    fn resubscribe_peer(&self, peer_id: NodeId) {
        let _guard = self.subscribe_lock.lock();
        self.resubscribe_peer_locked(peer_id);
    }

    fn resubscribe_peer_locked(&self, peer_id: NodeId) {
        let Some(peer) = self.peers.lock().get(&peer_id).cloned() else {
            return;
        };
        for (app, stream) in self.subs.streams_for_peer(peer_id) {
            let epoch = self.ownership.epoch_of(&app, &stream).unwrap_or(0);
            // Soft control: if the queue rejects, leave the refcount in place
            // for a later reconnect/resubscribe — this path is only entered
            // after a fresh connection already exists.
            let _ = peer.try_send(self.subscribe_message(peer_id, &app, &stream, epoch));
        }
    }

    pub async fn subscribe_remote(
        &self,
        media_addr: &str,
        peer_id: NodeId,
        app: &str,
        stream: &str,
        epoch: u64,
    ) {
        // Hold `subscribe_lock` across add + peer creation + the fresh-peer
        // resubscribe snapshot so a concurrent subscribe for another stream
        // cannot be added between the peer-map insert and the snapshot (which
        // would send that stream's Subscribe twice).
        let _guard = self.subscribe_lock.lock();
        if !self.subs.add(peer_id, app, stream) {
            return; // already subscribed (refcount)
        }
        if media_addr.is_empty() {
            // Owner address not learned yet (e.g. just after a restart). Keep
            // the refcount so a later `connect_peer` resubscribes it, but do
            // not spawn a peer that would dial an empty address forever.
            return;
        }
        let (peer, fresh) = self.ensure_peer_locked(peer_id, media_addr);
        // A fresh connection already resent every tracked subscription for
        // this peer, including the one just added above — sending it again
        // here would just double the owner's per-connection subscribe tally.
        if !fresh {
            let ok = peer.try_send(self.subscribe_message(peer_id, app, stream, epoch));
            if ok.is_err() {
                // Sole first-subscriber: roll back so a later player retries.
                // Concurrent holders already saw add==false and expect wire
                // Subscribe — keep our refcount and retry the send instead of
                // leaving them with refs and no owner fan-out.
                if !self.subs.remove_if_sole(peer_id, app, stream) {
                    let _ = peer.try_send(self.subscribe_message(peer_id, app, stream, epoch));
                } else {
                    self.bump_sub_gen(peer_id, app, stream);
                }
            }
        }
    }

    pub async fn unsubscribe_remote(&self, peer_id: NodeId, app: &str, stream: &str) {
        // Hold the same lock as subscribe_remote/ensure_peer/resubscribe_peer so
        // a fresh-peer resubscribe snapshot cannot enqueue Subscribe after this
        // Unsubscribe (the owner would keep a phantom subscription). No `.await`
        // is held across the guard.
        let _guard = self.subscribe_lock.lock();
        if !self.subs.remove(peer_id, app, stream) {
            return;
        }
        self.bump_sub_gen(peer_id, app, stream);
        self.subscribe_nacks
            .lock()
            .remove(&(peer_id, app.to_string(), stream.to_string()));
        if let Some(peer) = self.peers.lock().get(&peer_id) {
            let _ = peer.try_send(MediaMessage::Unsubscribe {
                app: app.to_string(),
                stream: stream.to_string(),
            });
        }
    }

    /// Fan out a local publisher frame to subscribed peers (+ optional standby replicas).
    pub async fn fanout_local_frame(&self, frame: ExportedFrame) {
        // Remap before staging InitCache so keyframe timestamps share the same
        // domain as MediaFrame.timeline_ts (receivers map that domain again).
        let timeline_ts = {
            let mut maps = self.timelines.lock();
            let key = (frame.app.clone(), frame.stream.clone());
            let remap = maps.entry(key).or_default();
            remap.map(frame.epoch, frame.timestamp)
        };

        self.cache.update_from_frame(
            &frame.app,
            &frame.stream,
            frame.epoch,
            frame.frame_type,
            timeline_ts,
            &frame.payload,
        );

        let hint = frame.hint;
        let msg = MediaMessage::MediaFrame {
            app: frame.app.clone(),
            stream: frame.stream.clone(),
            epoch: frame.epoch,
            frame_type: frame.frame_type,
            timestamp: frame.timestamp,
            timeline_ts,
            hint,
            payload: frame.payload.clone(),
        };

        let sinks = self.inbound_sinks.lock().clone();
        let peers = self.peers.lock().clone();
        self.subs
            .for_each_peer(&frame.app, &frame.stream, |peer_id| {
                let sink = sinks.get(&peer_id).filter(|s| !s.is_closed());
                let peer = peers.get(&peer_id).filter(|p| !p.is_closed());
                // A critical frame was evicted for this peer earlier: its
                // codec headers may be missing, so send the init cache
                // ahead of the resync point that restarts the stream.
                if hint == DeliveryHint::ResyncPoint {
                    let reinit = sink
                        .map(|s| s.take_reinit(&frame.app, &frame.stream))
                        .unwrap_or(false)
                        || peer
                            .map(|p| p.take_reinit(&frame.app, &frame.stream))
                            .unwrap_or(false);
                    if reinit {
                        self.resend_init_cache(sink, peer, &frame.app, &frame.stream);
                    }
                }
                if let Some(sink) = sink
                    && sink.try_send(msg.clone()).is_ok()
                {
                    return;
                }
                if let Some(peer) = peer {
                    let _ = peer.try_send(msg.clone());
                }
            });
    }

    /// Queue the cached init data of a stream for one peer (best effort;
    /// flags the peer again when it cannot be queued).
    fn resend_init_cache(
        &self,
        sink: Option<&Arc<InboundMediaSink>>,
        peer: Option<&Arc<MediaPeer>>,
        app: &str,
        stream: &str,
    ) {
        let Some(cache) = self.cache.get(app, stream) else {
            return;
        };
        let init = MediaMessage::InitCache {
            app: app.to_string(),
            stream: stream.to_string(),
            epoch: cache.epoch,
            metadata: cache.metadata,
            avc_header: cache.avc_header,
            aac_header: cache.aac_header,
            keyframe: cache.keyframe,
        };
        if sink.is_some_and(|s| s.try_send(init.clone()).is_ok()) {
            return;
        }
        if peer.is_some_and(|p| p.try_send(init).is_ok()) {
            return;
        }
        if let Some(s) = sink {
            s.mark_reinit(app, stream);
        }
        if let Some(p) = peer {
            p.mark_reinit(app, stream);
        }
    }

    fn queue_cfg(&self) -> LiveQueueConfig {
        LiveQueueConfig::new(self.queue_mb, self.media_max_age_ms.load(Ordering::Relaxed))
    }

    /// Set the age bound of the per-peer live-media queues (`0` = default).
    /// Applies to connections created afterwards.
    pub fn set_media_max_age_ms(&self, ms: u32) {
        self.media_max_age_ms.store(ms, Ordering::Relaxed);
    }

    /// Queue depth, age, drop and resync statistics of every media peer.
    pub fn media_stats(&self) -> MediaPlaneStats {
        let mut peers: Vec<PeerMediaStats> = self
            .peers
            .lock()
            .values()
            .map(|p| p.stats())
            .chain(self.inbound_sinks.lock().values().map(|s| s.stats()))
            .collect();
        peers.sort_by_key(|p| (p.peer_id, p.direction));
        let mut total = LiveQueueSnapshot::default();
        for p in &peers {
            total.merge(&p.queue);
        }
        MediaPlaneStats {
            protocol_version: MEDIA_PROTOCOL_VERSION,
            queue: total,
            write_timeouts: peers.iter().map(|p| p.write_timeouts).sum(),
            reconnects: peers.iter().map(|p| p.reconnects).sum(),
            peers,
        }
    }

    /// Update local init cache from librtmp2 snapshot (optional).
    pub fn put_init_cache(&self, app: &str, stream: &str, epoch: u64, entry: InitCacheEntry) {
        let mut e = entry;
        e.epoch = epoch;
        self.cache.put(app, stream, e);
    }

    pub fn ownership(&self) -> &Arc<OwnershipTracker> {
        &self.ownership
    }

    pub fn subscription_count(&self) -> usize {
        self.subs.count()
    }

    pub fn subscribed_nodes_for(&self, app: &str, stream: &str) -> Vec<NodeId> {
        self.subs.peers_for_stream(app, stream)
    }

    /// Reset timeline remappers for streams whose ownership epoch changed.
    pub fn reset_timelines_for(&self, stream_ids: &[String]) {
        let mut maps = self.timelines.lock();
        maps.retain(|(_app, sid), _| !stream_ids.iter().any(|s| s == sid));
    }

    pub fn evict_init_cache(&self, app: &str, stream: &str) {
        self.cache.remove(app, stream);
    }

    /// Bind the media port before spawning accept so startup fails hard on conflict.
    pub async fn start(self: &Arc<Self>, bind: SocketAddr) -> Result<(), String> {
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|e| format!("cluster media bind {bind}: {e}"))?;
        tracing::info!(%bind, tls = self.tls_server.is_some(), "cluster media plane listening");
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = hub.accept_loop(listener).await {
                tracing::error!(error = %e, "cluster media plane stopped");
            }
        });
        Ok(())
    }

    pub async fn connect_peer(&self, peer_id: NodeId, media_addr: &str) -> Result<(), String> {
        let _ = self.ensure_peer(peer_id, media_addr);
        Ok(())
    }
}

fn subscribe_denied_error(app: &str, stream: &str, generation: u64) -> MediaMessage {
    MediaMessage::Error {
        code: SUBSCRIBE_DENIED.to_string(),
        message: subscribe_denied_payload(app, stream),
        generation,
    }
}

fn subscribe_denied_payload(app: &str, stream: &str) -> String {
    format!("{app}\t{stream}")
}

fn parse_subscribe_denied(message: &str) -> Option<(String, String)> {
    let (app, stream) = message.split_once('\t')?;
    if app.is_empty() || stream.is_empty() {
        return None;
    }
    Some((app.to_string(), stream.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::media::wire;

    fn free_local_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    fn test_hub(local_id: NodeId) -> Arc<MediaHub> {
        MediaHub::new(
            local_id,
            "test-cluster-secret-32-chars-min--".to_string(),
            8,
            1,
            Arc::new(OwnershipTracker::new()),
            InjectQueue::new(8),
            None,
            None,
            Arc::new(|_: NodeId| true),
        )
    }

    #[tokio::test]
    async fn media_accept_retry_backs_off_and_honors_shutdown() {
        let shutdown = AtomicBool::new(false);
        let err = std::io::Error::from(std::io::ErrorKind::ConnectionAborted);
        assert!(media_accept_retry(&shutdown, err).await);

        let shutdown = AtomicBool::new(true);
        let err = std::io::Error::from(std::io::ErrorKind::ConnectionAborted);
        assert!(!media_accept_retry(&shutdown, err).await);
    }

    #[tokio::test]
    async fn unlearned_owner_addr_keeps_refcount_and_replays_on_connect() {
        let hub1 = test_hub(1);

        // The subscription is registered while the owner's address is unknown:
        // the refcount is kept and no dialing peer is spawned.
        hub1.subscribe_remote("", 2, "live", "s1", 0).await;
        assert_eq!(
            hub1.subscribed_nodes_for("live", "s1"),
            vec![2],
            "a subscription registered before the owner address is learned must \
             survive so a later connect_peer resubscribes it"
        );
        assert_eq!(
            hub1.peer_count(),
            0,
            "no media peer may be spawned for an empty owner address"
        );

        // A later topology refresh learns the address and dials the owner.
        let hub2 = test_hub(2);
        let port = free_local_port();
        let bind: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        hub2.start(bind).await.expect("media plane must bind");
        hub1.connect_peer(2, &format!("127.0.0.1:{port}"))
            .await
            .expect("connect_peer must accept the learned address");

        let replayed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if hub2.subscribed_nodes_for("live", "s1").contains(&1) {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or(false);

        assert!(
            replayed,
            "connect_peer must replay a subscription registered before the \
             owner address was learned"
        );
        assert_eq!(hub1.peer_count(), 1);
    }

    // ---- helpers -------------------------------------------------------

    const SECRET: &str = "test-cluster-secret-32-chars-min--";
    const WAIT: Duration = Duration::from_secs(5);
    const AVC_SEQ: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01];
    const AVC_KEY: &[u8] = &[0x17, 0x01, 0x00, 0x00, 0x00, 0x65];

    struct Node {
        hub: Arc<MediaHub>,
        inject: Arc<InjectQueue>,
        ownership: Arc<OwnershipTracker>,
    }

    fn node_with(
        local_id: NodeId,
        tls: Option<(Arc<ServerConfig>, Arc<ClientConfig>)>,
        allowed: MediaMembershipFn,
    ) -> Node {
        let ownership = Arc::new(OwnershipTracker::new());
        let inject = InjectQueue::new(8);
        let (tls_server, tls_client) = match tls {
            Some((s, c)) => (Some(s), Some(c)),
            None => (None, None),
        };
        let hub = MediaHub::new(
            local_id,
            SECRET.to_string(),
            8,
            1,
            Arc::clone(&ownership),
            Arc::clone(&inject),
            tls_server,
            tls_client,
            allowed,
        );
        Node {
            hub,
            inject,
            ownership,
        }
    }

    fn node(local_id: NodeId) -> Node {
        node_with(local_id, None, Arc::new(|_: NodeId| true))
    }

    fn tls_for(node: u64) -> (Arc<ServerConfig>, Arc<ClientConfig>) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cluster-tls");
        let cert = dir.join(format!("node{node}.pem"));
        let key = dir.join(format!("node{node}.key"));
        let ca = dir.join("ca.pem");
        (
            crate::cluster::security::build_server_tls(&cert, &key, &ca).unwrap(),
            crate::cluster::security::build_client_tls(&cert, &key, &ca).unwrap(),
        )
    }

    /// Run the hub's accept loop on an ephemeral port and return its address.
    async fn listen(hub: &Arc<MediaHub>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(Arc::clone(hub).accept_loop(listener));
        addr
    }

    /// Dial a hub's media plane as `node_id` (client auth + Hello) speaking
    /// the newest protocol version.
    async fn dial_hub(addr: SocketAddr, node_id: NodeId) -> tokio::net::TcpStream {
        dial_hub_with_version(addr, node_id, MEDIA_PROTOCOL_VERSION).await
    }

    /// As [`dial_hub`], announcing `version` in `Hello`. Authentication and
    /// `Hello` are v1 frames whatever the version.
    async fn dial_hub_with_version(
        addr: SocketAddr,
        node_id: NodeId,
        version: u16,
    ) -> tokio::net::TcpStream {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let MediaMessage::AuthChallenge { nonce } = peer::read_media_frame(&mut s).await.unwrap()
        else {
            panic!("expected challenge");
        };
        peer::write_media_frame(
            &mut s,
            &MediaMessage::Auth {
                node_id,
                response: crate::cluster::security::auth_response(SECRET, node_id, &nonce),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            peer::read_media_frame(&mut s).await.unwrap(),
            MediaMessage::AuthOk
        ));
        peer::write_media_frame(&mut s, &MediaMessage::Hello { version, node_id })
            .await
            .unwrap();
        if (2..=MEDIA_PROTOCOL_VERSION).contains(&version) {
            // A hub that speaks the version confirms it.
            assert!(matches!(
                peer::read_media_frame(&mut s).await.unwrap(),
                MediaMessage::Hello { version: v, .. } if v == version
            ));
        }
        s
    }

    /// Accept one outbound `MediaPeer` connection as scripted owner `owner_id`.
    async fn accept_from_hub(listener: &TcpListener, owner_id: NodeId) -> tokio::net::TcpStream {
        let (mut s, a) = tokio::time::timeout(WAIT, listener.accept())
            .await
            .expect("hub never dialed")
            .unwrap();
        peer::accept_auth(&mut s, a.ip(), SECRET, owner_id, false, None)
            .await
            .unwrap();
        s
    }

    async fn next<R: tokio::io::AsyncReadExt + Unpin>(s: &mut R) -> MediaMessage {
        tokio::time::timeout(WAIT, peer::read_media_frame_v(s, MEDIA_PROTOCOL_VERSION))
            .await
            .expect("timed out waiting for a media frame")
            .expect("media read failed")
    }

    async fn send<W: tokio::io::AsyncWriteExt + Unpin>(s: &mut W, msg: MediaMessage) {
        wire::write_frame(s, &msg, MEDIA_PROTOCOL_VERSION)
            .await
            .unwrap();
    }

    async fn eventually(mut f: impl FnMut() -> bool) -> bool {
        tokio::time::timeout(WAIT, async {
            loop {
                if f() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    }

    async fn drain_at_least(q: &InjectQueue, n: usize) -> Vec<InjectedFrame> {
        let mut out = Vec::new();
        let _ = tokio::time::timeout(WAIT, async {
            while out.len() < n {
                out.extend(q.drain());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        out
    }

    fn media_frame(epoch: u64, ts: u32, payload: &[u8]) -> MediaMessage {
        MediaMessage::MediaFrame {
            app: "live".into(),
            stream: "s".into(),
            epoch,
            frame_type: 1,
            timestamp: ts,
            timeline_ts: ts,
            hint: DeliveryHint::Droppable,
            payload: payload.to_vec(),
        }
    }

    fn subscribe(generation: u64) -> MediaMessage {
        MediaMessage::Subscribe {
            app: "live".into(),
            stream: "s".into(),
            epoch: 0,
            generation,
        }
    }

    fn denied(generation: u64) -> MediaMessage {
        subscribe_denied_error("live", "s", generation)
    }

    fn injected(payload_len: usize) -> InjectedFrame {
        InjectedFrame {
            app: "live".into(),
            stream: "s".into(),
            epoch: 1,
            frame_type: 1,
            timestamp: 0,
            hint: DeliveryHint::Droppable,
            payload: vec![0; payload_len],
        }
    }

    fn exported(epoch: u64, ts: u32, payload: &[u8]) -> ExportedFrame {
        ExportedFrame {
            app: "live".into(),
            stream: "s".into(),
            epoch,
            frame_type: 1,
            timestamp: ts,
            hint: DeliveryHint::Droppable,
            payload: payload.to_vec(),
        }
    }

    const KB: usize = 1024;

    // ---- queues and slot accounting -------------------------------------

    #[test]
    fn inject_queue_drops_oldest_on_overflow_and_rejects_oversized() {
        let q = InjectQueue::new(0); // clamps to 1 MiB
        q.try_send(injected(400 * KB)).unwrap();
        q.try_send(injected(400 * KB)).unwrap();
        // Third frame does not fit: the oldest is evicted to make room.
        q.try_send(injected(400 * KB)).unwrap();
        let frames = q.drain();
        assert_eq!(frames.len(), 2);
        assert!(q.drain().is_empty());

        // A frame larger than the whole budget is refused without evicting
        // the frames already queued.
        q.try_send(injected(10)).unwrap();
        assert!(q.try_send(injected(2 * 1024 * KB)).is_err());
        let kept = q.drain();
        assert_eq!(kept.len(), 1, "an oversized frame must not flush the queue");
        assert_eq!(kept[0].payload.len(), 10);
    }

    #[tokio::test]
    async fn export_queue_drops_oldest_and_wakes_waiter() {
        let q = ExportQueue::new(0);
        let waiter = {
            let q = Arc::clone(&q);
            tokio::spawn(async move { q.wait_and_drain().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.push(exported(1, 0, &[1]));
        let got = tokio::time::timeout(WAIT, waiter).await.unwrap().unwrap();
        assert_eq!(got.len(), 1);

        q.push(exported(1, 1, &vec![0; 400 * KB]));
        q.push(exported(1, 2, &vec![0; 400 * KB]));
        q.push(exported(1, 3, &vec![0; 400 * KB]));
        let frames = q.drain();
        assert_eq!(
            frames.iter().map(|f| f.timestamp).collect::<Vec<_>>(),
            vec![2, 3],
            "drop-oldest keeps FIFO order of the survivors"
        );

        // Oversized: the new frame is dropped and the queued frames are kept.
        q.push(exported(1, 4, &[1]));
        q.push(exported(1, 5, &vec![0; 2 * 1024 * KB]));
        let kept = q.drain();
        assert_eq!(kept.len(), 1, "an oversized frame must not flush the queue");
        assert_eq!(kept[0].timestamp, 4);
        // Frames already queued are returned immediately.
        q.push(exported(1, 6, &[1]));
        assert_eq!(q.wait_and_drain().await.len(), 1);
    }

    #[test]
    fn preauth_slots_are_bounded_per_ip_and_released() {
        let ip: IpAddr = "198.51.100.77".parse().unwrap();
        for _ in 0..MAX_PREAUTH_MEDIA_CONN_PER_IP {
            assert!(try_acquire_preauth_media_slot(ip));
        }
        assert!(!try_acquire_preauth_media_slot(ip));
        for _ in 0..MAX_PREAUTH_MEDIA_CONN_PER_IP {
            release_preauth_media_slot(ip);
        }
        assert!(!PREAUTH_MEDIA_CONN_PER_IP.lock().contains_key(&ip));
        // Releasing an untracked IP is a no-op.
        release_preauth_media_slot(ip);

        // The guard releases both the per-IP and the global slot. (The global
        // counter is shared with concurrently running tests, so only the
        // per-IP side is asserted exactly.)
        assert!(try_acquire_global_preauth_media_slot());
        assert!(try_acquire_preauth_media_slot(ip));
        drop(PreauthMediaGuard(ip));
        assert!(!PREAUTH_MEDIA_CONN_PER_IP.lock().contains_key(&ip));
    }

    #[test]
    fn subscribe_denied_payload_roundtrips() {
        assert_eq!(
            parse_subscribe_denied(&subscribe_denied_payload("live", "s")),
            Some(("live".to_string(), "s".to_string()))
        );
        assert_eq!(parse_subscribe_denied("no-tab"), None);
        assert_eq!(parse_subscribe_denied("\ts"), None);
        assert_eq!(parse_subscribe_denied("live\t"), None);
    }

    // ---- lifecycle ------------------------------------------------------

    #[tokio::test]
    async fn start_reports_bind_conflicts_and_serve_accepts() {
        let n = node(1);
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let err = n
            .hub
            .start(taken.local_addr().unwrap())
            .await
            .expect_err("bind conflict must fail startup");
        assert!(err.contains("cluster media bind"), "{err}");
        assert!(
            Arc::clone(&n.hub)
                .serve(taken.local_addr().unwrap())
                .await
                .is_err()
        );

        let port = free_local_port();
        let bind: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        tokio::spawn(Arc::clone(&n.hub).serve(bind));
        let mut connected = None;
        for _ in 0..100 {
            if let Ok(s) = tokio::net::TcpStream::connect(bind).await {
                connected = Some(s);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut s = connected.expect("serve must accept connections");
        assert!(matches!(
            peer::read_media_frame(&mut s).await.unwrap(),
            MediaMessage::AuthChallenge { .. }
        ));
    }

    #[tokio::test]
    async fn shutdown_closes_peers_sinks_and_stops_accepting() {
        let owner = node(2);
        let owner_addr = listen(&owner.hub).await;
        let sub = node(1);
        sub.hub
            .connect_peer(2, &owner_addr.to_string())
            .await
            .unwrap();
        let sub_addr = listen(&sub.hub).await;
        let _inbound = dial_hub(sub_addr, 3).await;
        assert!(eventually(|| sub.hub.inbound_sinks.lock().contains_key(&3)).await);
        let peer = sub.hub.peers.lock().get(&2).cloned().unwrap();
        let sink = sub.hub.inbound_sinks.lock().get(&3).cloned().unwrap();

        sub.hub.shutdown();
        assert!(peer.is_closed());
        assert!(sink.is_closed());
        // The accept loop observes shutdown on its next wakeup and exits,
        // dropping the listener.
        let _ = tokio::net::TcpStream::connect(sub_addr).await;
        assert!(
            eventually(|| std::net::TcpStream::connect(sub_addr).is_err()).await,
            "listener must close after shutdown"
        );
        owner.hub.shutdown();
    }

    // ---- inbound (owner-side) session ------------------------------------

    #[tokio::test]
    async fn inbound_session_serves_subscribe_frames_and_init_cache() {
        let owner = node(1);
        let addr = listen(&owner.hub).await;
        // Peer 2 is the recorded owner of "s" at epoch 7 for inbound frames.
        owner.ownership.set("s", 2, 7);
        owner.hub.put_init_cache(
            "live",
            "s",
            7,
            InitCacheEntry {
                avc_header: Some(AVC_SEQ.to_vec()),
                ..InitCacheEntry::default()
            },
        );

        let mut c = dial_hub(addr, 2).await;
        send(&mut c, subscribe(1)).await;
        match next(&mut c).await {
            MediaMessage::InitCache {
                epoch, avc_header, ..
            } => {
                assert_eq!(epoch, 7);
                assert_eq!(avc_header.as_deref(), Some(AVC_SEQ));
            }
            other => panic!("expected InitCache first, got {other:?}"),
        }
        assert!(eventually(|| owner.hub.subscribed_nodes_for("live", "s") == vec![2]).await);
        assert_eq!(owner.hub.subscription_count(), 1);

        // Local publisher frames fan out over the inbound sink.
        owner.hub.fanout_local_frame(exported(7, 40, AVC_KEY)).await;
        match next(&mut c).await {
            MediaMessage::MediaFrame { payload, .. } => assert_eq!(payload, AVC_KEY),
            other => panic!("unexpected {other:?}"),
        }
        // A stream nobody subscribed to is not sent anywhere.
        owner
            .hub
            .fanout_local_frame(ExportedFrame {
                stream: "other".into(),
                ..exported(7, 40, AVC_KEY)
            })
            .await;

        // Frames from the owner at the right epoch are injected; stale ones
        // are fenced.
        send(&mut c, media_frame(6, 10, &[9])).await;
        send(&mut c, media_frame(7, 20, &[1, 2, 3])).await;
        let frames = drain_at_least(&owner.inject, 1).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload, vec![1, 2, 3]);

        // Inbound InitCache: stale epoch ignored, current epoch cached and
        // injected (metadata, avc, aac, keyframe).
        send(
            &mut c,
            MediaMessage::InitCache {
                app: "live".into(),
                stream: "s".into(),
                epoch: 3,
                metadata: Some(b"stale".to_vec()),
                avc_header: None,
                aac_header: None,
                keyframe: None,
            },
        )
        .await;
        send(
            &mut c,
            MediaMessage::InitCache {
                app: "live".into(),
                stream: "s".into(),
                epoch: 7,
                metadata: Some(b"md".to_vec()),
                avc_header: Some(AVC_SEQ.to_vec()),
                aac_header: Some(vec![0xAF, 0x00, 0x12, 0x10]),
                keyframe: Some((100, AVC_KEY.to_vec())),
            },
        )
        .await;
        let frames = drain_at_least(&owner.inject, 4).await;
        assert_eq!(
            frames.iter().map(|f| f.frame_type).collect::<Vec<_>>(),
            vec![2, 1, 0, 1]
        );
        assert_eq!(
            owner
                .hub
                .cache
                .get("live", "s")
                .unwrap()
                .metadata
                .as_deref(),
            Some(&b"md"[..])
        );

        // Ignored / forwarded control messages keep the session alive.
        send(
            &mut c,
            MediaMessage::StatsReq {
                stream_id: "s".into(),
            },
        )
        .await;
        send(
            &mut c,
            MediaMessage::Error {
                code: "x".into(),
                message: "y".into(),
                generation: 0,
            },
        )
        .await;
        send(
            &mut c,
            MediaMessage::StreamStop {
                app: "live".into(),
                stream: "s".into(),
                epoch: 7,
            },
        )
        .await;

        // Unsubscribe releases this connection's ref.
        send(
            &mut c,
            MediaMessage::Unsubscribe {
                app: "live".into(),
                stream: "s".into(),
            },
        )
        .await;
        assert!(eventually(|| owner.hub.subscribed_nodes_for("live", "s").is_empty()).await);
        // Unsubscribe for a stream this connection never subscribed is harmless.
        send(
            &mut c,
            MediaMessage::Unsubscribe {
                app: "live".into(),
                stream: "never".into(),
            },
        )
        .await;

        // Two refs, then the connection drops without Unsubscribe: teardown
        // releases exactly this connection's refs.
        send(&mut c, subscribe(2)).await;
        let _ = next(&mut c).await; // InitCache
        send(&mut c, subscribe(3)).await;
        let _ = next(&mut c).await; // InitCache
        assert!(eventually(|| owner.hub.subscribed_nodes_for("live", "s") == vec![2]).await);
        drop(c);
        assert!(eventually(|| owner.hub.subscription_count() == 0).await);
        assert!(eventually(|| owner.hub.inbound_sinks.lock().is_empty()).await);

        owner.hub.evict_init_cache("live", "s");
        assert!(owner.hub.cache.get("live", "s").is_none());
        owner.hub.shutdown();
    }

    #[tokio::test]
    async fn inbound_reconnect_replaces_previous_sink() {
        let owner = node(1);
        let addr = listen(&owner.hub).await;
        let _c1 = dial_hub(addr, 2).await;
        assert!(eventually(|| owner.hub.inbound_sinks.lock().contains_key(&2)).await);
        let first = owner.hub.inbound_sinks.lock().get(&2).cloned().unwrap();
        let _c2 = dial_hub(addr, 2).await;
        assert!(
            eventually(|| owner
                .hub
                .inbound_sinks
                .lock()
                .get(&2)
                .is_some_and(|s| !Arc::ptr_eq(s, &first)))
            .await
        );
        assert!(first.is_closed(), "superseded sink must be closed");
        owner.hub.shutdown();
    }

    #[tokio::test]
    async fn inbound_auth_failure_and_non_member_are_dropped() {
        let owner = node_with(1, None, Arc::new(|id: NodeId| id != 9));
        let addr = listen(&owner.hub).await;

        // Non-member authenticates but is rejected by the membership gate.
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let MediaMessage::AuthChallenge { nonce } = peer::read_media_frame(&mut s).await.unwrap()
        else {
            panic!()
        };
        peer::write_media_frame(
            &mut s,
            &MediaMessage::Auth {
                node_id: 9,
                response: crate::cluster::security::auth_response(SECRET, 9, &nonce),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            peer::read_media_frame(&mut s).await.unwrap(),
            MediaMessage::AuthOk
        ));
        peer::write_media_frame(
            &mut s,
            &MediaMessage::Hello {
                version: crate::cluster::media::MEDIA_PROTOCOL_VERSION,
                node_id: 9,
            },
        )
        .await
        .unwrap();
        let closed = tokio::time::timeout(
            WAIT,
            peer::read_media_frame_v(&mut s, MEDIA_PROTOCOL_VERSION),
        )
        .await
        .unwrap();
        assert!(closed.is_err(), "non-member connection must be closed");
        assert!(owner.hub.inbound_sinks.lock().is_empty());
        owner.hub.shutdown();
    }

    #[tokio::test]
    async fn inbound_subscribe_gate_denies_then_retries_until_allowed() {
        let owner = node(1);
        let addr = listen(&owner.hub).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = Arc::clone(&calls);
        owner
            .hub
            .set_inbound_subscribe_gate(Arc::new(move |_peer, _app, stream| {
                let n = calls_c.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    // "s" is allowed on the second attempt (membership
                    // converging); "deny" never is.
                    stream == "s" && n >= 1
                })
            }));
        let mut c = dial_hub(addr, 2).await;
        send(&mut c, subscribe(0)).await;
        assert!(eventually(|| owner.hub.subscribed_nodes_for("live", "s") == vec![2]).await);
        assert!(calls.load(Ordering::SeqCst) >= 2);

        send(
            &mut c,
            MediaMessage::Subscribe {
                app: "live".into(),
                stream: "deny".into(),
                epoch: 0,
                generation: 42,
            },
        )
        .await;
        let reply = tokio::time::timeout(
            Duration::from_secs(5),
            peer::read_media_frame_v(&mut c, MEDIA_PROTOCOL_VERSION),
        )
        .await
        .unwrap()
        .unwrap();
        match reply {
            MediaMessage::Error {
                code,
                message,
                generation,
            } => {
                assert_eq!(code, SUBSCRIBE_DENIED);
                assert_eq!(message, "live\tdeny");
                assert_eq!(generation, 42);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(owner.hub.subscribed_nodes_for("live", "deny").is_empty());
        owner.hub.shutdown();
    }

    // ---- outbound (subscriber-side) handling -----------------------------

    #[tokio::test]
    async fn handle_inbound_fences_and_injects_owner_traffic() {
        let n = node(1);
        n.ownership.set("s", 2, 5);

        // Wrong sender / epoch: dropped.
        n.hub.handle_inbound(3, media_frame(5, 0, &[1]));
        n.hub.handle_inbound(2, media_frame(4, 0, &[1]));
        n.hub.handle_inbound(
            3,
            MediaMessage::InitCache {
                app: "live".into(),
                stream: "s".into(),
                epoch: 5,
                metadata: Some(vec![1]),
                avc_header: None,
                aac_header: None,
                keyframe: None,
            },
        );
        assert!(n.inject.drain().is_empty());
        assert!(n.hub.cache.get("live", "s").is_none());

        // Owner frames are injected and clear pending NACK state.
        n.hub
            .subscribe_nacks
            .lock()
            .insert(MediaHub::sub_key(2, "live", "s"), 2);
        n.hub.handle_inbound(2, media_frame(5, 1000, &[7]));
        let frames = n.inject.drain();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].timestamp, 1000);
        assert!(n.hub.subscribe_nacks.lock().is_empty());
        assert_eq!(n.hub.sub_gen(2, "live", "s"), 1);

        // Owner InitCache mid-stream reuses the last remapped timestamp.
        n.hub.handle_inbound(
            2,
            MediaMessage::InitCache {
                app: "live".into(),
                stream: "s".into(),
                epoch: 5,
                metadata: None,
                avc_header: Some(AVC_SEQ.to_vec()),
                aac_header: None,
                keyframe: Some((10, AVC_KEY.to_vec())),
            },
        );
        let frames = n.inject.drain();
        assert_eq!(frames.len(), 2);
        assert!(frames.iter().all(|f| f.timestamp == 1000));
        assert!(n.hub.cache.get("live", "s").is_some());

        // Other message kinds are ignored.
        n.hub.handle_inbound(2, MediaMessage::AuthOk);
        assert!(n.inject.drain().is_empty());
    }

    #[test]
    fn inject_init_cache_live_timestamps() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let n = rt.block_on(async { node(1) });

        // Fresh timeline with a keyframe: map the keyframe timestamp.
        n.hub.inject_init_cache_live(
            "live".into(),
            "a".into(),
            InitCacheEntry {
                keyframe: Some((500, AVC_KEY.to_vec())),
                epoch: 1,
                ..InitCacheEntry::default()
            },
        );
        let f = n.inject.drain();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].timestamp, 500);

        // No keyframe: headers reuse the last output timestamp.
        n.hub.inject_init_cache_live(
            "live".into(),
            "a".into(),
            InitCacheEntry {
                metadata: Some(vec![1]),
                aac_header: Some(vec![2]),
                epoch: 1,
                ..InitCacheEntry::default()
            },
        );
        let f = n.inject.drain();
        assert_eq!(
            f.iter().map(|x| x.timestamp).collect::<Vec<_>>(),
            vec![500, 500]
        );
        assert_eq!(
            f.iter().map(|x| x.frame_type).collect::<Vec<_>>(),
            vec![2, 0]
        );

        // Resetting the timeline makes the next keyframe map afresh.
        n.hub.reset_timelines_for(&["a".to_string()]);
        n.hub.inject_init_cache_live(
            "live".into(),
            "a".into(),
            InitCacheEntry {
                keyframe: Some((7, AVC_KEY.to_vec())),
                epoch: 2,
                ..InitCacheEntry::default()
            },
        );
        assert_eq!(n.inject.drain()[0].timestamp, 7);
        rt.block_on(async { n.hub.shutdown() });
    }

    #[tokio::test]
    async fn subscribe_denied_nacks_retry_then_give_up() {
        let n = node(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner_addr = listener.local_addr().unwrap().to_string();

        // Not subscribed / wrong code / malformed payload: ignored.
        n.hub.handle_inbound(2, denied(0));
        n.hub.handle_inbound(
            2,
            MediaMessage::Error {
                code: "other".into(),
                message: "live\ts".into(),
                generation: 0,
            },
        );
        n.hub.handle_inbound(
            2,
            MediaMessage::Error {
                code: SUBSCRIBE_DENIED.into(),
                message: "garbage".into(),
                generation: 0,
            },
        );
        assert!(n.hub.subscribe_nacks.lock().is_empty());

        n.hub.subscribe_remote(&owner_addr, 2, "live", "s", 0).await;
        let mut s = accept_from_hub(&listener, 2).await;
        assert!(matches!(next(&mut s).await, MediaMessage::Subscribe { .. }));

        // A NACK for a stale generation is ignored.
        n.hub.handle_inbound(2, denied(99));
        assert!(n.hub.subscribe_nacks.lock().is_empty());

        // A matching NACK schedules a retry that re-sends Subscribe. The retry
        // captured a freshly allocated generation (never the 0 an unrecorded
        // key reads back as), so it can never match a pruned or re-created key.
        let generation = n.hub.sub_gen(2, "live", "s");
        n.hub.handle_inbound(2, denied(generation));
        match next(&mut s).await {
            MediaMessage::Subscribe { generation: g, .. } => {
                assert_ne!(g, 0, "a retry must not capture the prunable generation 0");
                assert_eq!(g, n.hub.sub_gen(2, "live", "s"));
            }
            other => panic!("unexpected {other:?}"),
        }

        // Exhausting the retry budget drops the local refs entirely.
        for _ in 0..SUBSCRIBE_NACK_MAX {
            n.hub.handle_inbound(2, denied(0));
        }
        assert!(n.hub.subscribed_nodes_for("live", "s").is_empty());
        assert!(n.hub.subscribe_nacks.lock().is_empty());
        n.hub.shutdown();
    }

    #[tokio::test]
    async fn subscribe_retry_guards() {
        let n = node(1);
        // Not subscribed at all.
        Arc::clone(&n.hub)
            .run_subscribe_retry(2, "live".into(), "s".into(), 0, 0)
            .await;
        // Subscribed but no peer connection known.
        n.hub.subscribe_remote("", 2, "live", "s", 0).await;
        Arc::clone(&n.hub)
            .run_subscribe_retry(2, "live".into(), "s".into(), 0, 0)
            .await;
        // Generation moved on.
        Arc::clone(&n.hub)
            .run_subscribe_retry(2, "live".into(), "s".into(), 5, 0)
            .await;

        // Peer whose queue is full: the retry reschedules while budget remains.
        let dead = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };
        n.hub.connect_peer(2, &dead).await.unwrap();
        let p = n.hub.peers.lock().get(&2).cloned().unwrap();
        while p.try_send(MediaMessage::AuthOk).is_ok() {}
        Arc::clone(&n.hub)
            .run_subscribe_retry(2, "live".into(), "s".into(), 0, 1)
            .await;
        Arc::clone(&n.hub)
            .run_subscribe_retry(2, "live".into(), "s".into(), 0, 0)
            .await;

        // After shutdown, retries are no-ops.
        n.hub.shutdown();
        Arc::clone(&n.hub)
            .run_subscribe_retry(2, "live".into(), "s".into(), 0, 1)
            .await;
        assert_eq!(n.hub.subscribed_nodes_for("live", "s"), vec![2]);
    }

    #[tokio::test]
    async fn subscribe_remote_refcounts_redials_and_unsubscribes() {
        let n = node(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        // First subscribe creates the peer; the fresh-peer resubscribe sends it.
        n.hub.subscribe_remote(&addr, 2, "live", "s", 0).await;
        let mut s = accept_from_hub(&listener, 2).await;
        assert!(
            matches!(next(&mut s).await, MediaMessage::Subscribe { ref stream, .. } if stream == "s")
        );
        // Same stream again only bumps the refcount (no wire message).
        n.hub.subscribe_remote(&addr, 2, "live", "s", 0).await;
        // A second stream on the existing peer is sent directly.
        n.hub.subscribe_remote(&addr, 2, "live", "t", 3).await;
        match next(&mut s).await {
            MediaMessage::Subscribe { stream, epoch, .. } => {
                assert_eq!(stream, "t");
                assert_eq!(epoch, 3);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(n.hub.peer_count(), 1);

        // Unsubscribe: first release keeps the ref, second sends Unsubscribe.
        n.hub.unsubscribe_remote(2, "live", "s").await;
        n.hub.unsubscribe_remote(2, "live", "s").await;
        assert!(
            matches!(next(&mut s).await, MediaMessage::Unsubscribe { ref stream, .. } if stream == "s")
        );
        // Unknown stream: nothing to do.
        n.hub.unsubscribe_remote(2, "live", "zzz").await;

        // Owner drops the connection: the peer reconnects and the hub
        // resubscribes the still-held stream on the new connection.
        drop(s);
        let mut s = accept_from_hub(&listener, 2).await;
        assert!(
            matches!(next(&mut s).await, MediaMessage::Subscribe { ref stream, .. } if stream == "t")
        );

        // Owner address changes: the old peer is closed and a new one dials.
        let old = n.hub.peers.lock().get(&2).cloned().unwrap();
        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap().to_string();
        n.hub.connect_peer(2, &addr2).await.unwrap();
        assert!(old.is_closed());
        let mut s2 = accept_from_hub(&listener2, 2).await;
        assert!(
            matches!(next(&mut s2).await, MediaMessage::Subscribe { ref stream, .. } if stream == "t")
        );

        // A closed peer at the same address is replaced as well.
        let cur = n.hub.peers.lock().get(&2).cloned().unwrap();
        cur.close();
        n.hub.connect_peer(2, &addr2).await.unwrap();
        let replaced = n.hub.peers.lock().get(&2).cloned().unwrap();
        assert!(!Arc::ptr_eq(&cur, &replaced));
        // Same live address: reused.
        n.hub.connect_peer(2, &addr2).await.unwrap();
        assert!(Arc::ptr_eq(&replaced, n.hub.peers.lock().get(&2).unwrap()));
        n.hub.shutdown();
    }

    #[tokio::test]
    async fn subscribe_remote_rolls_back_when_queue_is_full() {
        let n = node(1);
        let dead = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };
        n.hub.connect_peer(2, &dead).await.unwrap();
        let p = n.hub.peers.lock().get(&2).cloned().unwrap();
        while p.try_send(MediaMessage::AuthOk).is_ok() {}
        let gen_before = n.hub.sub_gen(2, "live", "s");
        n.hub.subscribe_remote(&dead, 2, "live", "s", 0).await;
        assert!(
            n.hub.subscribed_nodes_for("live", "s").is_empty(),
            "failed first Subscribe must roll back the refcount"
        );
        assert_eq!(n.hub.sub_gen(2, "live", "s"), gen_before + 1);
        n.hub.shutdown();
    }

    #[tokio::test]
    async fn disconnect_peer_clears_state_and_bumps_generations() {
        let n = node(1);
        let owner = node(2);
        let owner_addr = listen(&owner.hub).await;
        let hub_addr = listen(&n.hub).await;

        n.hub
            .subscribe_remote(&owner_addr.to_string(), 2, "live", "s", 0)
            .await;
        let _inbound = dial_hub(hub_addr, 2).await;
        assert!(eventually(|| n.hub.inbound_sinks.lock().contains_key(&2)).await);
        n.hub
            .subscribe_nacks
            .lock()
            .insert(MediaHub::sub_key(2, "live", "nacked"), 1);
        let peer = n.hub.peers.lock().get(&2).cloned().unwrap();
        let sink = n.hub.inbound_sinks.lock().get(&2).cloned().unwrap();

        n.hub.disconnect_peer(2);
        assert_eq!(n.hub.peer_count(), 0);
        assert!(peer.is_closed());
        assert!(sink.is_closed());
        assert!(n.hub.subscribe_nacks.lock().is_empty());
        assert_eq!(n.hub.subscription_count(), 0);
        // Generations come from one never-reused counter, so the two fenced
        // keys hold distinct non-zero values: a sleeping retry holding either
        // one can never match the other key or a pruned/re-created one.
        let s_gen = n.hub.sub_gen(2, "live", "s");
        let nacked_gen = n.hub.sub_gen(2, "live", "nacked");
        assert_ne!(s_gen, 0);
        assert_ne!(nacked_gen, 0);
        assert_ne!(s_gen, nacked_gen);
        // Unknown peers are a no-op.
        n.hub.disconnect_peer(42);
        n.hub.shutdown();
        owner.hub.shutdown();
    }

    #[tokio::test]
    async fn fanout_falls_back_to_outbound_peer_without_inbound_sink() {
        let n = node(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        // Hub 1 holds an outbound peer to 2 and 2 is subscribed to "s".
        n.hub.connect_peer(2, &addr).await.unwrap();
        let mut s = accept_from_hub(&listener, 2).await;
        n.hub.subs.add(2, "live", "s");
        n.hub.fanout_local_frame(exported(1, 0, AVC_SEQ)).await;
        match next(&mut s).await {
            MediaMessage::MediaFrame { payload, .. } => assert_eq!(payload, AVC_SEQ),
            other => panic!("unexpected {other:?}"),
        }
        // The frame was staged in the local init cache as well.
        assert_eq!(
            n.hub.cache.get("live", "s").unwrap().avc_header.as_deref(),
            Some(AVC_SEQ)
        );
        assert!(Arc::ptr_eq(n.hub.ownership(), &n.ownership));
        n.hub.shutdown();
    }

    fn hinted(epoch: u64, ts: u32, hint: DeliveryHint, payload: &[u8]) -> ExportedFrame {
        ExportedFrame {
            hint,
            ..exported(epoch, ts, payload)
        }
    }

    #[tokio::test]
    async fn fanout_carries_the_delivery_hint_over_a_v2_wire() {
        let n = node(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        n.hub.connect_peer(2, &addr).await.unwrap();
        let mut s = accept_from_hub(&listener, 2).await;
        n.hub.subs.add(2, "live", "s");
        n.hub
            .fanout_local_frame(hinted(1, 0, DeliveryHint::ResyncPoint, AVC_KEY))
            .await;
        match next(&mut s).await {
            MediaMessage::MediaFrame { hint, payload, .. } => {
                assert_eq!(hint, DeliveryHint::ResyncPoint);
                assert_eq!(payload, AVC_KEY);
            }
            other => panic!("unexpected {other:?}"),
        }
        let stats = n.hub.media_stats();
        assert_eq!(stats.protocol_version, MEDIA_PROTOCOL_VERSION);
        assert_eq!(stats.peers.len(), 1);
        assert_eq!(stats.peers[0].protocol_version, MEDIA_PROTOCOL_VERSION);
        n.hub.shutdown();
    }

    #[tokio::test]
    async fn v1_client_gets_v1_framing_from_a_v2_hub() {
        let owner = node(1);
        let addr = listen(&owner.hub).await;
        let mut c = dial_hub_with_version(addr, 2, 1).await;
        peer::write_media_frame(&mut c, &subscribe(0))
            .await
            .unwrap();
        assert!(eventually(|| owner.hub.subscribed_nodes_for("live", "s") == vec![2]).await);
        owner.ownership.set("s", 1, 1);
        owner
            .hub
            .fanout_local_frame(hinted(1, 5, DeliveryHint::ResyncPoint, AVC_KEY))
            .await;
        // Plain JSON frames, readable by a v1-only node.
        let msg = tokio::time::timeout(WAIT, peer::read_media_frame(&mut c))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(msg, MediaMessage::MediaFrame { ref payload, .. } if payload == AVC_KEY));
        assert_eq!(owner.hub.media_stats().peers[0].protocol_version, 1);
        owner.hub.shutdown();
    }

    #[tokio::test]
    async fn unsupported_hello_versions_get_a_version_error() {
        let owner = node(1);
        let addr = listen(&owner.hub).await;
        for version in [0u16, 3, u16::MAX] {
            let mut c = dial_hub_with_version(addr, 2, version).await;
            match tokio::time::timeout(WAIT, peer::read_media_frame(&mut c))
                .await
                .unwrap()
                .unwrap()
            {
                MediaMessage::Error { code, .. } => assert_eq!(code, "VERSION"),
                other => panic!("unexpected {other:?}"),
            }
            let eof = tokio::time::timeout(WAIT, peer::read_media_frame(&mut c))
                .await
                .unwrap();
            assert!(eof.is_err(), "connection must close after a VERSION error");
        }
        assert!(owner.hub.inbound_sinks.lock().is_empty());
        owner.hub.shutdown();
    }

    /// Rolling upgrade: a v2 node dials a node that only speaks v1, which
    /// answers the v2 `Hello` with a v1 `Error{VERSION}` and hangs up. The
    /// dialer then reconnects speaking v1 and the subscription arrives.
    #[tokio::test]
    async fn v2_peer_falls_back_to_v1_for_a_legacy_acceptor() {
        let n = node(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        n.hub.subscribe_remote(&addr, 2, "live", "s", 0).await;

        // A legacy acceptor: same auth, but only knows protocol 1.
        async fn legacy_accept(listener: &TcpListener) -> (tokio::net::TcpStream, u16) {
            let (mut s, a) = tokio::time::timeout(WAIT, listener.accept())
                .await
                .expect("hub never dialed")
                .unwrap();
            let nonce = vec![7u8; 16];
            peer::write_media_frame(
                &mut s,
                &MediaMessage::AuthChallenge {
                    nonce: nonce.clone(),
                },
            )
            .await
            .unwrap();
            let MediaMessage::Auth { node_id, response } =
                peer::read_media_frame(&mut s).await.unwrap()
            else {
                panic!("expected Auth");
            };
            assert_eq!(
                response,
                crate::cluster::security::auth_response(SECRET, node_id, &nonce)
            );
            let _ = a;
            peer::write_media_frame(&mut s, &MediaMessage::AuthOk)
                .await
                .unwrap();
            let MediaMessage::Hello { version, .. } = peer::read_media_frame(&mut s).await.unwrap()
            else {
                panic!("expected Hello");
            };
            if version != 1 {
                peer::write_media_frame(
                    &mut s,
                    &MediaMessage::Error {
                        code: "VERSION".into(),
                        message: "unsupported media protocol".into(),
                        generation: 0,
                    },
                )
                .await
                .unwrap();
            }
            (s, version)
        }

        let (s1, v1) = legacy_accept(&listener).await;
        assert_eq!(v1, 2, "first attempt announces the newest version");
        drop(s1);
        let (mut s2, v2) = legacy_accept(&listener).await;
        assert_eq!(v2, 1, "fallback announces the legacy version");
        // The resubscribe after the fallback arrives as a plain v1 frame.
        let msg = tokio::time::timeout(WAIT, peer::read_media_frame(&mut s2))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(msg, MediaMessage::Subscribe { ref stream, .. } if stream == "s"));
        let stats = n.hub.media_stats();
        assert_eq!(stats.peers[0].version_fallbacks, 1);
        assert_eq!(stats.peers[0].protocol_version, 1);
        n.hub.shutdown();
    }

    /// A critical frame lost to the byte bound must not leave the peer
    /// without codec headers: the init cache goes out ahead of the next
    /// resync point.
    #[tokio::test]
    async fn evicted_critical_frame_resends_init_cache_before_the_next_keyframe() {
        let n = node(1);
        let dead = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };
        n.hub.connect_peer(2, &dead).await.unwrap();
        n.hub.subs.add(2, "live", "s");
        let big = vec![0u8; 1024 * KB];
        // Header-class frames from a misbehaving publisher exceed the 8 MiB
        // queue: the oldest critical frame is evicted.
        for i in 0..12u32 {
            n.hub
                .fanout_local_frame(hinted(1, i, DeliveryHint::Critical, &big))
                .await;
        }
        let peer = n.hub.peers.lock().get(&2).cloned().unwrap();
        assert!(peer.stats().queue.dropped_critical_frames > 0);
        n.hub
            .fanout_local_frame(hinted(1, 100, DeliveryHint::ResyncPoint, AVC_KEY))
            .await;
        let mut kinds = Vec::new();
        for msg in peer.drain_queue_for_test() {
            kinds.push(match msg {
                MediaMessage::InitCache { .. } => "init",
                MediaMessage::MediaFrame {
                    hint: DeliveryHint::ResyncPoint,
                    ..
                } => "key",
                _ => "other",
            });
        }
        let init = kinds
            .iter()
            .position(|k| *k == "init")
            .expect("init resent");
        let key = kinds
            .iter()
            .position(|k| *k == "key")
            .expect("keyframe queued");
        assert!(
            init < key,
            "init cache must precede the keyframe: {kinds:?}"
        );
        n.hub.shutdown();
    }

    #[tokio::test]
    async fn mutual_tls_hubs_relay_owner_frames_to_subscriber() {
        let owner = node_with(2, Some(tls_for(2)), Arc::new(|_: NodeId| true));
        let sub = node_with(1, Some(tls_for(1)), Arc::new(|_: NodeId| true));
        let owner_addr = listen(&owner.hub).await;
        owner.ownership.set("s", 2, 4);
        sub.ownership.set("s", 2, 4);

        sub.hub
            .subscribe_remote(&owner_addr.to_string(), 2, "live", "s", 4)
            .await;
        assert!(eventually(|| owner.hub.subscribed_nodes_for("live", "s") == vec![1]).await);

        owner.hub.fanout_local_frame(exported(4, 33, AVC_KEY)).await;
        let frames = drain_at_least(&sub.inject, 1).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload, AVC_KEY);
        assert_eq!(frames[0].epoch, 4);
        owner.hub.shutdown();
        sub.hub.shutdown();
    }
}
