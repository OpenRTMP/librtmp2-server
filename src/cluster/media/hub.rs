//! Media hub: peer mesh, subscribe fan-out, inject queue.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use rustls::{ClientConfig, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::cluster::NodeId;
use crate::cluster::media::cache::{InitCacheEntry, InitCacheStore};
use crate::cluster::media::ownership::OwnershipTracker;
use crate::cluster::media::peer::{self, InboundMediaSink, MediaPeer};
use crate::cluster::media::protocol::{MediaMessage, SUBSCRIBE_DENIED};
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
    pub payload: Vec<u8>,
}

/// Byte-bounded queue for remote→local media injection (reject-new on overflow).
pub struct InjectQueue {
    state: Mutex<(std::collections::VecDeque<InjectedFrame>, usize)>,
    max_bytes: usize,
}

impl InjectQueue {
    pub fn new(max_mb: u32) -> Arc<Self> {
        let max_bytes = (max_mb as usize)
            .saturating_mul(1024 * 1024)
            .max(1024 * 1024);
        Arc::new(Self {
            state: Mutex::new((std::collections::VecDeque::new(), 0)),
            max_bytes,
        })
    }

    pub fn try_send(&self, frame: InjectedFrame) -> Result<(), ()> {
        let size = frame.payload.len().saturating_add(64);
        // Reject an oversized frame before the eviction loop: draining the
        // whole backlog would destroy every other stream's frames for a frame
        // that was never going to fit anyway.
        if size > self.max_bytes {
            tracing::warn!(
                app = %frame.app,
                stream = %frame.stream,
                "inbound media inject queue full — dropping frame"
            );
            return Err(());
        }
        let mut st = self.state.lock();
        while st.1.saturating_add(size) > self.max_bytes && !st.0.is_empty() {
            if let Some(dropped) = st.0.pop_front() {
                st.1 =
                    st.1.saturating_sub(dropped.payload.len().saturating_add(64));
            }
        }
        if st.1.saturating_add(size) > self.max_bytes {
            tracing::warn!(
                app = %frame.app,
                stream = %frame.stream,
                "inbound media inject queue full — dropping frame"
            );
            return Err(());
        }
        st.1 = st.1.saturating_add(size);
        st.0.push_back(frame);
        Ok(())
    }

    pub fn drain(&self) -> Vec<InjectedFrame> {
        let mut st = self.state.lock();
        st.1 = 0;
        st.0.drain(..).collect()
    }
}

/// Byte-bounded ordered export queue (local librtmp2 → mesh fan-out).
///
/// Overload policy: **drop-oldest** until the new frame fits (or drop the new
/// frame if it alone exceeds capacity). Matches live-media preference for
/// freshest frames while preserving FIFO order for frames that remain.
pub struct ExportQueue {
    state: Mutex<(std::collections::VecDeque<ExportedFrame>, usize)>,
    max_bytes: usize,
    notify: tokio::sync::Notify,
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
        })
    }

    pub fn push(&self, frame: ExportedFrame) {
        let size = frame.payload.len().saturating_add(64);
        // Drop-oldest only applies to a frame that can eventually fit; an
        // oversized frame must not empty the backlog of every other stream.
        if size > self.max_bytes {
            tracing::warn!(
                app = %frame.app,
                stream = %frame.stream,
                "export media queue full — dropping oversized frame"
            );
            return;
        }
        let mut st = self.state.lock();
        while st.1.saturating_add(size) > self.max_bytes {
            let Some(old) = st.0.pop_front() else {
                break;
            };
            st.1 = st.1.saturating_sub(old.payload.len().saturating_add(64));
            tracing::warn!(
                app = %old.app,
                stream = %old.stream,
                "export media queue full — dropping oldest frame"
            );
        }
        if st.1.saturating_add(size) > self.max_bytes {
            tracing::warn!(
                app = %frame.app,
                stream = %frame.stream,
                "export media queue full — dropping oversized frame"
            );
            return;
        }
        st.1 = st.1.saturating_add(size);
        st.0.push_back(frame);
        drop(st);
        self.notify.notify_one();
    }

    pub fn drain(&self) -> Vec<ExportedFrame> {
        let mut st = self.state.lock();
        st.1 = 0;
        st.0.drain(..).collect()
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
            replicas,
            ownership,
            peers: Mutex::new(HashMap::new()),
            inbound_sinks: Mutex::new(HashMap::new()),
            subscribe_nacks: Mutex::new(HashMap::new()),
            subscribe_gens: Mutex::new(HashMap::new()),
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
                    *generation = generation.wrapping_add(1);
                }
            }
        }
        self.subs.clear_peer(peer_id);
    }

    fn sub_key(peer_id: NodeId, app: &str, stream: &str) -> (NodeId, String, String) {
        (peer_id, app.to_string(), stream.to_string())
    }

    fn sub_gen(&self, peer_id: NodeId, app: &str, stream: &str) -> u64 {
        self.subscribe_gens
            .lock()
            .get(&Self::sub_key(peer_id, app, stream))
            .copied()
            .unwrap_or(0)
    }

    fn bump_sub_gen(&self, peer_id: NodeId, app: &str, stream: &str) {
        let mut gens = self.subscribe_gens.lock();
        let e = gens.entry(Self::sub_key(peer_id, app, stream)).or_insert(0);
        *e = e.wrapping_add(1);
        if gens.len() > MAX_SUB_GENS {
            gens.retain(|key @ (owner, _, _), _| {
                *owner == peer_id
                    || self
                        .subs
                        .peers_for_stream(&key.1, &key.2)
                        .contains(owner)
            });
        }
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
                    Ok((peer_id, io)) => {
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
                        if let Err(e) = hub.run_inbound_media_session(peer_id, io).await {
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
        io: Box<dyn peer::MediaIo>,
    ) -> Result<(), std::io::Error> {
        // Track Subscribe messages on this connection so a drop without Unsubscribe
        // cannot leave stale SubscriptionTable entries for the peer.
        let mut conn_subs: std::collections::HashMap<(String, String), usize> =
            std::collections::HashMap::new();
        let (mut rh, wh) = tokio::io::split(io);
        let sink = Arc::new(InboundMediaSink::spawn(peer_id, self.queue_mb, wh));
        if let Some(old) = self.inbound_sinks.lock().insert(peer_id, Arc::clone(&sink)) {
            old.close();
        }
        let result = async {
            loop {
                let msg = peer::read_media_frame(&mut rh).await?;
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
                        // are first in this sink's FIFO.
                        if self
                            .subs
                            .add_with(peer_id, &app, &stream, || {
                                if sink.is_closed() {
                                    return Err(());
                                }
                                if let Some(msg) = init_msg {
                                    sink.try_send(msg)
                                } else {
                                    Ok(())
                                }
                            })
                            .is_err()
                        {
                            if sink.is_closed() {
                                return Err(std::io::Error::other("inbound media sink closed"));
                            }
                            let _ =
                                sink.try_send(subscribe_denied_error(&app, &stream, generation));
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
                let n = {
                    let mut nacks = self.subscribe_nacks.lock();
                    let e = nacks
                        .entry((peer_id, app.clone(), stream.clone()))
                        .or_insert(0);
                    *e = e.saturating_add(1);
                    *e
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
                self.queue_mb,
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

        let msg = MediaMessage::MediaFrame {
            app: frame.app.clone(),
            stream: frame.stream.clone(),
            epoch: frame.epoch,
            frame_type: frame.frame_type,
            timestamp: frame.timestamp,
            timeline_ts,
            payload: frame.payload.clone(),
        };

        let sinks = self.inbound_sinks.lock().clone();
        let peers = self.peers.lock().clone();
        self.subs
            .for_each_peer(&frame.app, &frame.stream, |peer_id| {
                if let Some(sink) = sinks.get(&peer_id) {
                    if !sink.is_closed() && sink.try_send(msg.clone()).is_ok() {
                        return;
                    }
                }
                if let Some(peer) = peers.get(&peer_id) {
                    if !peer.is_closed() {
                        let _ = peer.try_send(msg.clone());
                    }
                }
            });
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

    /// Dial a hub's media plane as `node_id` (client auth + Hello).
    async fn dial_hub(addr: SocketAddr, node_id: NodeId) -> tokio::net::TcpStream {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let MediaMessage::AuthChallenge { nonce } = next(&mut s).await else {
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
        assert!(matches!(next(&mut s).await, MediaMessage::AuthOk));
        send(
            &mut s,
            MediaMessage::Hello {
                version: crate::cluster::media::MEDIA_PROTOCOL_VERSION,
                node_id,
            },
        )
        .await;
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
        tokio::time::timeout(WAIT, peer::read_media_frame(s))
            .await
            .expect("timed out waiting for a media frame")
            .expect("media read failed")
    }

    async fn send<W: tokio::io::AsyncWriteExt + Unpin>(s: &mut W, msg: MediaMessage) {
        peer::write_media_frame(s, &msg).await.unwrap();
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
            next(&mut s).await,
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
        let MediaMessage::AuthChallenge { nonce } = next(&mut s).await else {
            panic!()
        };
        send(
            &mut s,
            MediaMessage::Auth {
                node_id: 9,
                response: crate::cluster::security::auth_response(SECRET, 9, &nonce),
            },
        )
        .await;
        assert!(matches!(next(&mut s).await, MediaMessage::AuthOk));
        send(
            &mut s,
            MediaMessage::Hello {
                version: crate::cluster::media::MEDIA_PROTOCOL_VERSION,
                node_id: 9,
            },
        )
        .await;
        let closed = tokio::time::timeout(WAIT, peer::read_media_frame(&mut s))
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
        let reply = tokio::time::timeout(Duration::from_secs(5), peer::read_media_frame(&mut c))
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

        // A matching NACK schedules a retry that re-sends Subscribe.
        let generation = n.hub.sub_gen(2, "live", "s");
        n.hub.handle_inbound(2, denied(generation));
        match next(&mut s).await {
            MediaMessage::Subscribe { generation: g, .. } => assert_eq!(g, generation),
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
        assert_eq!(n.hub.sub_gen(2, "live", "s"), 1);
        assert_eq!(n.hub.sub_gen(2, "live", "nacked"), 1);
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
