//! Media hub: peer mesh, subscribe fan-out, inject queue.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::Mutex;
use rustls::{ClientConfig, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::cluster::NodeId;
use crate::cluster::media::cache::{InitCacheEntry, InitCacheStore};
use crate::cluster::media::ownership::OwnershipTracker;
use crate::cluster::media::peer::{self, MediaPeer};
use crate::cluster::media::protocol::MediaMessage;
use crate::cluster::media::subscription::SubscriptionTable;
use crate::cluster::media::timeline::TimelineRemapper;

const MEDIA_INBOUND_QUEUE: usize = 4096;
/// Cap concurrent inbound media connections (auth + frame reader tasks),
/// matching the control-plane inflight guard pattern.
const MAX_MEDIA_CONN_INFLIGHT: usize = 512;
static MEDIA_CONN_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

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
    }

    pub fn peer_count(&self) -> usize {
        self.peers.lock().len()
    }

    pub fn disconnect_peer(&self, peer_id: NodeId) {
        if let Some(p) = self.peers.lock().remove(&peer_id) {
            p.close();
        }
        self.subs.clear_peer(peer_id);
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
            let (stream, _) = listener.accept().await?;
            if MEDIA_CONN_INFLIGHT.fetch_add(1, Ordering::AcqRel) >= MAX_MEDIA_CONN_INFLIGHT {
                MEDIA_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
                tracing::warn!("media inbound connection cap reached; dropping accept");
                drop(stream);
                continue;
            }
            let hub = Arc::clone(&self);
            tokio::spawn(async move {
                let result = hub.handle_inbound_conn(stream).await;
                MEDIA_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
                if let Err(e) = result {
                    tracing::debug!(error=%e, "media inbound closed");
                }
            });
        }
        Ok(())
    }

    async fn handle_inbound_conn(
        self: Arc<Self>,
        stream: tokio::net::TcpStream,
    ) -> Result<(), std::io::Error> {
        let (peer_id, io) = peer::accept_tls_then_auth(
            stream,
            &self.secret,
            self.local_id,
            self.tls_server.clone(),
            Arc::clone(&self.is_peer_allowed),
        )
        .await?;
        // Track Subscribe messages on this connection so a drop without Unsubscribe
        // cannot leave stale SubscriptionTable entries for the peer.
        let mut conn_subs: std::collections::HashMap<(String, String), usize> =
            std::collections::HashMap::new();
        let result = async {
            let (mut rh, mut wh) = tokio::io::split(io);
            loop {
                let msg = peer::read_media_frame(&mut rh).await?;
                match msg {
                    MediaMessage::Subscribe {
                        app,
                        stream,
                        epoch: _,
                    } => {
                        self.subs.add(peer_id, &app, &stream);
                        *conn_subs.entry((app.clone(), stream.clone())).or_insert(0) += 1;
                        if let Some(cache) = self.cache.get(&app, &stream) {
                            // Send the epoch the cache was actually captured
                            // under, not the subscriber-requested one — a
                            // stale cache from a previous owner must not be
                            // relabeled as belonging to the current epoch, or
                            // receiver-side epoch fencing would accept and
                            // inject old codec state.
                            let cache_epoch = cache.epoch;
                            let _ = peer::write_media_frame(
                                &mut wh,
                                &MediaMessage::InitCache {
                                    app: app.clone(),
                                    stream: stream.clone(),
                                    epoch: cache_epoch,
                                    metadata: cache.metadata,
                                    avc_header: cache.avc_header,
                                    aac_header: cache.aac_header,
                                    keyframe: cache.keyframe,
                                },
                            )
                            .await;
                        }
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
                        self.cache.put(
                            &app,
                            &stream,
                            InitCacheEntry {
                                metadata: metadata.clone(),
                                avc_header: avc_header.clone(),
                                aac_header: aac_header.clone(),
                                keyframe: keyframe.clone(),
                                epoch,
                            },
                        );
                        if let Some(md) = metadata {
                            let _ = self.inject.try_send(InjectedFrame {
                                app: app.clone(),
                                stream: stream.clone(),
                                epoch,
                                frame_type: 2,
                                timestamp: 0,
                                payload: md,
                            });
                        }
                        if let Some(h) = avc_header {
                            let _ = self.inject.try_send(InjectedFrame {
                                app: app.clone(),
                                stream: stream.clone(),
                                epoch,
                                frame_type: 1,
                                timestamp: 0,
                                payload: h,
                            });
                        }
                        if let Some(h) = aac_header {
                            let _ = self.inject.try_send(InjectedFrame {
                                app: app.clone(),
                                stream: stream.clone(),
                                epoch,
                                frame_type: 0,
                                timestamp: 0,
                                payload: h,
                            });
                        }
                        if let Some((ts, kf)) = keyframe {
                            // Route the cached keyframe through the same
                            // per-stream timeline as subsequent MediaFrames —
                            // its raw timestamp was captured under the prior
                            // publisher's epoch and can otherwise jump
                            // backward relative to what a player already saw.
                            let timestamp = {
                                let mut maps = self.timelines.lock();
                                let key = (app.clone(), stream.clone());
                                maps.entry(key).or_default().map(epoch, ts)
                            };
                            let _ = self.inject.try_send(InjectedFrame {
                                app,
                                stream,
                                epoch,
                                frame_type: 1,
                                timestamp,
                                payload: kf,
                            });
                        }
                    }
                    MediaMessage::StatsReq { stream_id: _ } => {}
                    other => {
                        let _ = self.inbound_tx.try_send((peer_id, other));
                    }
                }
            }
        }
        .await;
        for ((app, stream), n) in conn_subs {
            // Release only this connection's Subscribe refs; outbound
            // subscribe_remote refcounts for the same peer are left intact.
            for _ in 0..n {
                let _ = self.subs.remove(peer_id, &app, &stream);
            }
        }
        result
    }

    fn handle_inbound(&self, peer_id: NodeId, msg: MediaMessage) {
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
                self.cache.put(
                    &app,
                    &stream,
                    InitCacheEntry {
                        metadata: metadata.clone(),
                        avc_header: avc_header.clone(),
                        aac_header: aac_header.clone(),
                        keyframe: keyframe.clone(),
                        epoch,
                    },
                );
                if let Some(md) = metadata {
                    let _ = self.inject.try_send(InjectedFrame {
                        app: app.clone(),
                        stream: stream.clone(),
                        epoch,
                        frame_type: 2,
                        timestamp: 0,
                        payload: md,
                    });
                }
                if let Some(h) = avc_header {
                    let _ = self.inject.try_send(InjectedFrame {
                        app: app.clone(),
                        stream: stream.clone(),
                        epoch,
                        frame_type: 1,
                        timestamp: 0,
                        payload: h,
                    });
                }
                if let Some(h) = aac_header {
                    let _ = self.inject.try_send(InjectedFrame {
                        app: app.clone(),
                        stream: stream.clone(),
                        epoch,
                        frame_type: 0,
                        timestamp: 0,
                        payload: h,
                    });
                }
                if let Some((ts, kf)) = keyframe {
                    // Same reasoning as the inbound-connection InitCache
                    // branch: remap through the per-stream timeline so a
                    // player that already saw the previous epoch doesn't get
                    // a keyframe timestamp that jumps backward.
                    let timestamp = {
                        let mut maps = self.timelines.lock();
                        let key = (app.clone(), stream.clone());
                        maps.entry(key).or_default().map(epoch, ts)
                    };
                    let _ = self.inject.try_send(InjectedFrame {
                        app,
                        stream,
                        epoch,
                        frame_type: 1,
                        timestamp,
                        payload: kf,
                    });
                }
            }
            _ => {}
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
            self.resubscribe_peer(peer_id);
        }
        (peer, fresh)
    }

    fn resubscribe_peer(&self, peer_id: NodeId) {
        let Some(peer) = self.peers.lock().get(&peer_id).cloned() else {
            return;
        };
        for (app, stream) in self.subs.streams_for_peer(peer_id) {
            let epoch = self.ownership.epoch_of(&app, &stream).unwrap_or(0);
            // Soft control: if the queue rejects, leave the refcount in place
            // for a later reconnect/resubscribe — this path is only entered
            // after a fresh connection already exists.
            let _ = peer.try_send(MediaMessage::Subscribe { app, stream, epoch });
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
        if !self.subs.add(peer_id, app, stream) {
            return; // already subscribed (refcount)
        }
        let (peer, fresh) = self.ensure_peer(peer_id, media_addr);
        // A fresh connection already resent every tracked subscription for
        // this peer, including the one just added above — sending it again
        // here would just double the owner's per-connection subscribe tally.
        if !fresh {
            let ok = peer.try_send(MediaMessage::Subscribe {
                app: app.to_string(),
                stream: stream.to_string(),
                epoch,
            });
            if ok.is_err() {
                // Sole first-subscriber: roll back so a later player retries.
                // Concurrent holders already saw add==false and expect wire
                // Subscribe — keep our refcount and retry the send instead of
                // leaving them with refs and no owner fan-out.
                if !self.subs.remove_if_sole(peer_id, app, stream) {
                    let _ = peer.try_send(MediaMessage::Subscribe {
                        app: app.to_string(),
                        stream: stream.to_string(),
                        epoch,
                    });
                }
            }
        }
    }

    pub async fn unsubscribe_remote(&self, peer_id: NodeId, app: &str, stream: &str) {
        if !self.subs.remove(peer_id, app, stream) {
            return;
        }
        if let Some(peer) = self.peers.lock().get(&peer_id) {
            let _ = peer.try_send(MediaMessage::Unsubscribe {
                app: app.to_string(),
                stream: stream.to_string(),
            });
        }
    }

    /// Fan out a local publisher frame to subscribed peers (+ optional standby replicas).
    pub async fn fanout_local_frame(&self, frame: ExportedFrame) {
        self.cache.update_from_frame(
            &frame.app,
            &frame.stream,
            frame.epoch,
            frame.frame_type,
            frame.timestamp,
            &frame.payload,
        );

        let timeline_ts = {
            let mut maps = self.timelines.lock();
            let key = (frame.app.clone(), frame.stream.clone());
            let remap = maps.entry(key).or_default();
            remap.map(frame.epoch, frame.timestamp)
        };

        let msg = MediaMessage::MediaFrame {
            app: frame.app.clone(),
            stream: frame.stream.clone(),
            epoch: frame.epoch,
            frame_type: frame.frame_type,
            timestamp: frame.timestamp,
            timeline_ts,
            payload: frame.payload.clone(),
        };

        let peers: Vec<_> = self.peers.lock().values().cloned().collect();
        for peer in peers {
            if peer.is_closed() {
                continue;
            }
            let subscribed = self
                .subs
                .peers_for_stream(&frame.app, &frame.stream)
                .contains(&peer.peer_id);
            // Only push continuous media to active subscribers. Standby
            // replicas get InitCache via Subscribe replies — flooding them
            // with MediaFrames fills remote InjectQueues with no local players.
            if subscribed {
                let _ = peer.try_send(msg.clone());
            }
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
