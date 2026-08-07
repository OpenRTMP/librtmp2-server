//! Media hub: peer mesh, subscribe fan-out, inject queue.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::cluster::media::cache::{InitCacheEntry, InitCacheStore};
use crate::cluster::media::ownership::OwnershipTracker;
use crate::cluster::media::peer::{self, MediaPeer};
use crate::cluster::media::protocol::MediaMessage;
use crate::cluster::media::subscription::SubscriptionTable;
use crate::cluster::media::timeline::TimelineRemapper;
use crate::cluster::NodeId;

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

pub struct MediaHub {
    local_id: NodeId,
    secret: String,
    queue_mb: u32,
    replicas: u32,
    ownership: Arc<OwnershipTracker>,
    peers: Mutex<HashMap<NodeId, Arc<MediaPeer>>>,
    subs: SubscriptionTable,
    cache: InitCacheStore,
    timelines: Mutex<HashMap<(String, String), TimelineRemapper>>,
    inject_tx: mpsc::UnboundedSender<InjectedFrame>,
    inbound_tx: mpsc::UnboundedSender<MediaMessage>,
    shutdown: AtomicBool,
}

impl MediaHub {
    pub fn new(
        local_id: NodeId,
        secret: String,
        queue_mb: u32,
        replicas: u32,
        ownership: Arc<OwnershipTracker>,
        inject_tx: mpsc::UnboundedSender<InjectedFrame>,
    ) -> Arc<Self> {
        let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel::<MediaMessage>();
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
            inject_tx,
            inbound_tx,
            shutdown: AtomicBool::new(false),
        });
        let hub_c = Arc::clone(&hub);
        tokio::spawn(async move {
            while let Some(msg) = inbound_rx.recv().await {
                hub_c.handle_inbound(msg);
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

    pub async fn serve(self: Arc<Self>, bind: SocketAddr) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(bind).await?;
        tracing::info!(%bind, "cluster media plane listening");
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let (mut stream, _) = listener.accept().await?;
            let hub = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = hub.handle_inbound_conn(&mut stream).await {
                    tracing::debug!(error=%e, "media inbound closed");
                }
            });
        }
        Ok(())
    }

    async fn handle_inbound_conn(self: Arc<Self>, stream: &mut TcpStream) -> Result<(), std::io::Error> {
        let peer_id = peer::accept_auth(stream, &self.secret, self.local_id).await?;
        let (mut rh, mut wh) = tokio::io::split(stream);
        // Read loop — process subscribe/media; write init cache / frames as needed.
        // For inbound server role we mainly receive SUBSCRIBE and reply with InitCache + frames
        // when we are the owner. Outbound MediaPeer handles client role.
        let subs_wanted: Arc<Mutex<Vec<(String, String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        loop {
            let msg = peer::read_media_frame(&mut rh).await?;
            match msg {
                MediaMessage::Subscribe { app, stream, epoch } => {
                    self.subs.add(peer_id, &app, &stream);
                    if let Some(cache) = self.cache.get(&app, &stream) {
                        let _ = peer::write_media_frame(
                            &mut wh,
                            &MediaMessage::InitCache {
                                app: app.clone(),
                                stream: stream.clone(),
                                epoch,
                                metadata: cache.metadata,
                                avc_header: cache.avc_header,
                                aac_header: cache.aac_header,
                                keyframe: cache.keyframe,
                            },
                        )
                        .await;
                    }
                    subs_wanted.lock().push((app, stream, epoch));
                }
                MediaMessage::Unsubscribe { app, stream } => {
                    self.subs.remove(peer_id, &app, &stream);
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
                    if self.ownership.accepts_epoch(&stream, epoch) {
                        let _ = self.inject_tx.send(InjectedFrame {
                            app,
                            stream,
                            epoch,
                            frame_type,
                            timestamp: timeline_ts,
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
                    // Inject init pieces as frames
                    if let Some(md) = metadata {
                        let _ = self.inject_tx.send(InjectedFrame {
                            app: app.clone(),
                            stream: stream.clone(),
                            epoch,
                            frame_type: 2,
                            timestamp: 0,
                            payload: md,
                        });
                    }
                    if let Some(h) = avc_header {
                        let _ = self.inject_tx.send(InjectedFrame {
                            app: app.clone(),
                            stream: stream.clone(),
                            epoch,
                            frame_type: 1,
                            timestamp: 0,
                            payload: h,
                        });
                    }
                    if let Some(h) = aac_header {
                        let _ = self.inject_tx.send(InjectedFrame {
                            app: app.clone(),
                            stream: stream.clone(),
                            epoch,
                            frame_type: 0,
                            timestamp: 0,
                            payload: h,
                        });
                    }
                    if let Some((ts, kf)) = keyframe {
                        let _ = self.inject_tx.send(InjectedFrame {
                            app,
                            stream,
                            epoch,
                            frame_type: 1,
                            timestamp: ts,
                            payload: kf,
                        });
                    }
                }
                MediaMessage::StatsReq { stream_id: _ } => {
                    // Stats proxy uses the control plane; ignore on media.
                }
                other => {
                    let _ = self.inbound_tx.send(other);
                }
            }
        }
    }

    fn handle_inbound(&self, msg: MediaMessage) {
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
                if !self.ownership.accepts_epoch(&stream, epoch) {
                    return;
                }
                let _ = self.inject_tx.send(InjectedFrame {
                    app,
                    stream,
                    epoch,
                    frame_type,
                    timestamp: timeline_ts,
                    payload,
                });
            }
            _ => {}
        }
    }

    fn ensure_peer(&self, peer_id: NodeId, addr: &str) -> Arc<MediaPeer> {
        let mut peers = self.peers.lock();
        if let Some(p) = peers.get(&peer_id) {
            if !p.is_closed() {
                return Arc::clone(p);
            }
            peers.remove(&peer_id);
        }
        let peer = Arc::new(MediaPeer::spawn(
            peer_id,
            addr.to_string(),
            self.secret.clone(),
            self.local_id,
            self.queue_mb,
            self.inbound_tx.clone(),
        ));
        peers.insert(peer_id, Arc::clone(&peer));
        peer
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
        let peer = self.ensure_peer(peer_id, media_addr);
        let _ = peer.try_send(MediaMessage::Subscribe {
            app: app.to_string(),
            stream: stream.to_string(),
            epoch,
        });
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
        let mut sent = 0u32;
        for peer in peers {
            if peer.is_closed() {
                continue;
            }
            // Prefer peers that subscribed; also push to first N as replicas.
            let subscribed = self
                .subs
                .peers_for_stream(&frame.app, &frame.stream)
                .contains(&peer.peer_id);
            if subscribed || sent < self.replicas {
                if peer.try_send(msg.clone()).is_ok() {
                    sent = sent.saturating_add(1);
                }
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

    /// Start media listener in a background task.
    pub async fn start(self: &Arc<Self>, bind: SocketAddr) -> Result<(), String> {
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = hub.serve(bind).await {
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
