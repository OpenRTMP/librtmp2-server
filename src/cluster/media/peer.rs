//! Outbound/inbound media peer connection.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering};

use crate::cluster::security::try_reserve_inflight_bytes;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use rustls::{ClientConfig, ServerConfig};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::cluster::NodeId;
use crate::cluster::media::MediaMembershipFn;
use crate::cluster::media::live_queue::{LiveMediaQueue, LiveQueueConfig, LiveQueueSnapshot};
use crate::cluster::media::protocol::{
    MEDIA_PROTOCOL_MIN_VERSION, MEDIA_PROTOCOL_VERSION, MediaMessage,
};
use crate::cluster::media::wire;
pub use crate::cluster::media::wire::MAX_FRAME;
use crate::cluster::security::{
    auth_nonce, auth_response, clear_cluster_auth_failures, cluster_auth_rate_limited,
    node_id_from_peer_certs, record_cluster_auth_failure, secrets_equal, verify_tls_node_identity,
};

/// Cap aggregate resident memory for concurrent authenticated media reads.
const MAX_MEDIA_READ_BYTES_INFLIGHT: usize = 128 * 1024 * 1024;
static MEDIA_READ_BYTES_INFLIGHT: AtomicUsize = AtomicUsize::new(0);
const MAX_AUTH_FRAME: u32 = 8 * 1024;
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound post-authentication frame reads so a peer cannot reserve the media
/// read budget indefinitely by advertising a frame and then withholding it.
const MEDIA_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on a single frame write to a peer. Without this, a peer that stops
/// reading (backpressure with no consumer) leaves the write stuck forever —
/// the task and socket never clean up, and heartbeat-driven peer replacement
/// just keeps adding more of them.
const WRITE_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(800)
} else {
    Duration::from_secs(8)
};
/// After a peer rejected the newest protocol version, dial it with
/// [`MEDIA_PROTOCOL_MIN_VERSION`] for this long before trying again (the
/// peer may have been upgraded meanwhile).
const VERSION_FALLBACK_TTL: Duration = Duration::from_secs(60);

pub(crate) trait MediaIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> MediaIo for T {}

/// Write one v1 (JSON) frame: authentication, `Hello` and `Error{VERSION}`
/// are always v1, see [`wire`].
pub async fn write_media_frame<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    msg: &MediaMessage,
) -> Result<(), std::io::Error> {
    wire::write_frame(w, msg, MEDIA_PROTOCOL_MIN_VERSION).await
}

/// Read one v1 (JSON) frame.
pub async fn read_media_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> Result<MediaMessage, std::io::Error> {
    read_media_frame_v(r, MEDIA_PROTOCOL_MIN_VERSION).await
}

/// Read one frame of the session's negotiated protocol `version`.
pub async fn read_media_frame_v<R: AsyncReadExt + Unpin>(
    r: &mut R,
    version: u16,
) -> Result<MediaMessage, std::io::Error> {
    tokio::time::timeout(
        MEDIA_READ_TIMEOUT,
        read_media_frame_max(r, version, MAX_FRAME),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "media frame read timeout"))?
}

async fn read_media_frame_max<R: AsyncReadExt + Unpin>(
    r: &mut R,
    version: u16,
    max: u32,
) -> Result<MediaMessage, std::io::Error> {
    wire::read_frame(r, version, max, |len| {
        try_reserve_inflight_bytes(
            &MEDIA_READ_BYTES_INFLIGHT,
            MAX_MEDIA_READ_BYTES_INFLIGHT,
            len,
        )
        .map_err(|_| std::io::Error::other("media read memory budget exceeded"))
    })
    .await
}

async fn read_auth_media_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> Result<MediaMessage, std::io::Error> {
    tokio::time::timeout(
        AUTH_TIMEOUT,
        read_media_frame_max(r, MEDIA_PROTOCOL_MIN_VERSION, MAX_AUTH_FRAME),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "media auth timeout"))?
}

/// Connection-level counters of one media connection endpoint.
#[derive(Default)]
pub struct PeerCounters {
    pub connects: AtomicU64,
    pub reconnects: AtomicU64,
    pub write_timeouts: AtomicU64,
    pub write_errors: AtomicU64,
    pub version_fallbacks: AtomicU64,
    /// Protocol version of the current/last session (0 = never connected).
    pub protocol_version: AtomicU16,
}

/// Per-peer status for the cluster status endpoint.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PeerMediaStats {
    pub peer_id: NodeId,
    /// `"outbound"` (we dialed) or `"inbound"` (the peer dialed us).
    pub direction: &'static str,
    pub protocol_version: u16,
    pub connects: u64,
    pub reconnects: u64,
    pub write_timeouts: u64,
    pub write_errors: u64,
    pub version_fallbacks: u64,
    #[serde(flatten)]
    pub queue: LiveQueueSnapshot,
}

/// Multiplexed long-lived peer with a bounded, live-media-aware outbound
/// queue (see [`LiveMediaQueue`]).
pub struct MediaPeer {
    pub peer_id: NodeId,
    pub addr: String,
    queue: Arc<LiveMediaQueue>,
    counters: Arc<PeerCounters>,
    closed: Arc<AtomicBool>,
}

impl MediaPeer {
    pub fn spawn(
        peer_id: NodeId,
        addr: String,
        secret: String,
        local_id: NodeId,
        queue_cfg: impl Into<LiveQueueConfig>,
        inbound: mpsc::Sender<(NodeId, MediaMessage)>,
        tls_client: Option<Arc<ClientConfig>>,
        on_reconnected: mpsc::UnboundedSender<NodeId>,
    ) -> Self {
        let queue = Arc::new(LiveMediaQueue::new(queue_cfg.into()));
        let counters = Arc::new(PeerCounters::default());
        let closed = Arc::new(AtomicBool::new(false));
        let q = Arc::clone(&queue);
        let ctr = Arc::clone(&counters);
        let cl = Arc::clone(&closed);
        let addr_c = addr.clone();
        let on_reconnected = on_reconnected.clone();

        tokio::spawn(async move {
            let mut backoff_ms = 500u64;
            let mut fallback_until: Option<Instant> = None;
            let mut first = true;
            loop {
                if cl.load(Ordering::Relaxed) {
                    break;
                }
                if !first {
                    ctr.reconnects.fetch_add(1, Ordering::Relaxed);
                    // Whatever was queued for the dead connection is stale;
                    // live media resumes at the next resync point.
                    q.on_connection_reset();
                }
                first = false;
                let version = match fallback_until {
                    Some(until) if Instant::now() < until => MEDIA_PROTOCOL_MIN_VERSION,
                    _ => MEDIA_PROTOCOL_VERSION,
                };
                let attempt_started = Instant::now();
                match connect_and_run(
                    &addr_c,
                    &secret,
                    local_id,
                    peer_id,
                    version,
                    &q,
                    &ctr,
                    &inbound,
                    &cl,
                    tls_client.clone(),
                )
                .await
                {
                    Ok(ConnectEnd::Shutdown) => break,
                    Ok(ConnectEnd::VersionRejected) => {
                        tracing::info!(
                            peer = peer_id,
                            "media peer does not speak protocol v{version}; falling back to v{MEDIA_PROTOCOL_MIN_VERSION}"
                        );
                        ctr.version_fallbacks.fetch_add(1, Ordering::Relaxed);
                        fallback_until = Some(Instant::now() + VERSION_FALLBACK_TTL);
                        // Retry at once with the older version. No resubscribe
                        // notification: the rejection came before the writer
                        // popped anything, so the queued `Subscribe`s are
                        // still there and a second copy would double the
                        // owner's per-connection refcount.
                    }
                    Ok(ConnectEnd::Transient) => {
                        tracing::debug!(peer = peer_id, "media peer reconnect after disconnect");
                        // A session that stayed up for a while is evidence of a
                        // healthy peer; reset backoff so a later transient drop
                        // reconnects promptly instead of inheriting a long delay.
                        if attempt_started.elapsed() >= Duration::from_secs(5) {
                            backoff_ms = 500;
                        }
                        let _ = on_reconnected.send(peer_id);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms.saturating_mul(2)).min(8_000);
                    }
                    Err(e) => {
                        tracing::debug!(peer = peer_id, error = %e, "media peer reconnect");
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms.saturating_mul(2)).min(8_000);
                    }
                }
            }
            cl.store(true, Ordering::Relaxed);
            q.close();
        });

        Self {
            peer_id,
            addr,
            queue,
            counters,
            closed,
        }
    }

    pub fn try_send(&self, msg: MediaMessage) -> Result<(), ()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(());
        }
        if self.queue.push(msg).is_queued() {
            Ok(())
        } else {
            Err(())
        }
    }

    /// See [`LiveMediaQueue::take_reinit`].
    pub fn take_reinit(&self, app: &str, stream: &str) -> bool {
        self.queue.take_reinit(app, stream)
    }

    pub fn mark_reinit(&self, app: &str, stream: &str) {
        self.queue.mark_reinit(app, stream);
    }

    pub fn stats(&self) -> PeerMediaStats {
        peer_stats(self.peer_id, "outbound", &self.queue, &self.counters)
    }

    /// Take the queued messages without a connection (tests).
    #[cfg(test)]
    pub(crate) fn drain_queue_for_test(&self) -> Vec<MediaMessage> {
        std::iter::from_fn(|| self.queue.pop_at(Instant::now())).collect()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.queue.close();
    }
}

impl Drop for MediaPeer {
    /// Dropping the last handle ends the dial/write task.
    fn drop(&mut self) {
        self.close();
    }
}

impl Drop for InboundMediaSink {
    fn drop(&mut self) {
        self.close();
    }
}

fn peer_stats(
    peer_id: NodeId,
    direction: &'static str,
    queue: &LiveMediaQueue,
    c: &PeerCounters,
) -> PeerMediaStats {
    PeerMediaStats {
        peer_id,
        direction,
        protocol_version: c.protocol_version.load(Ordering::Relaxed),
        connects: c.connects.load(Ordering::Relaxed),
        reconnects: c.reconnects.load(Ordering::Relaxed),
        write_timeouts: c.write_timeouts.load(Ordering::Relaxed),
        write_errors: c.write_errors.load(Ordering::Relaxed),
        version_fallbacks: c.version_fallbacks.load(Ordering::Relaxed),
        queue: queue.snapshot(),
    }
}

enum ConnectEnd {
    /// Intentional shutdown (`closed` or channel dropped).
    Shutdown,
    /// Peer disconnected; outer loop should reconnect.
    Transient,
    /// The peer answered our newer protocol version with v1 framing, i.e.
    /// it only speaks v1: reconnect with the older version.
    VersionRejected,
}

async fn connect_and_run(
    addr: &str,
    secret: &str,
    local_id: NodeId,
    peer_id: NodeId,
    version: u16,
    queue: &LiveMediaQueue,
    counters: &PeerCounters,
    inbound: &mpsc::Sender<(NodeId, MediaMessage)>,
    closed: &AtomicBool,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<ConnectEnd, std::io::Error> {
    let tcp = TcpStream::connect(addr).await?;
    let mut stream: Box<dyn MediaIo> = if let Some(cfg) = tls_client {
        let connector = TlsConnector::from(cfg);
        let host = crate::cluster::network::tls_server_name_from_addr(addr)
            .map_err(std::io::Error::other)?;
        let tls = tokio::time::timeout(AUTH_TIMEOUT, connector.connect(host, tcp))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "tls connect timeout")
            })??;
        Box::new(tls)
    } else {
        Box::new(tcp)
    };

    client_media_auth(&mut stream, secret, local_id).await?;
    // `Hello` is a v1 frame whatever version it announces; the announced
    // version frames everything after it, in both directions.
    write_media_frame(
        &mut stream,
        &MediaMessage::Hello {
            version,
            node_id: local_id,
        },
    )
    .await?;
    if version > MEDIA_PROTOCOL_MIN_VERSION && !read_hello_ack(&mut stream, version).await {
        return Ok(ConnectEnd::VersionRejected);
    }
    counters.connects.fetch_add(1, Ordering::Relaxed);
    counters.protocol_version.store(version, Ordering::Relaxed);

    let (mut rh, mut wh) = tokio::io::split(stream);
    let read_closed = Arc::new(AtomicBool::new(false));
    let rc = Arc::clone(&read_closed);
    let inbound_c = inbound.clone();
    let reader = tokio::spawn(async move {
        while !rc.load(Ordering::Relaxed) {
            match read_media_frame_v(&mut rh, version).await {
                Ok(msg) => {
                    // Stamp the authenticated remote node so the hub can apply
                    // the same accepts_owner fence as direct inbound accepts.
                    if inbound_c.try_send((peer_id, msg)).is_err() {
                        tracing::warn!("media inbound queue full — dropping frame");
                    }
                }
                Err(_) => break,
            }
        }
        rc.store(true, Ordering::Relaxed);
    });

    let mut end = ConnectEnd::Transient;
    while !closed.load(Ordering::Relaxed) {
        tokio::select! {
            msg = queue.pop() => {
                let Some(msg) = msg else {
                    end = ConnectEnd::Shutdown;
                    break;
                };
                match tokio::time::timeout(WRITE_TIMEOUT, wire::write_frame(&mut wh, &msg, version)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        counters.write_errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_) => {
                        counters.write_timeouts.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if read_closed.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }
    if closed.load(Ordering::Relaxed) {
        end = ConnectEnd::Shutdown;
    }
    // Drop the write half and abort the reader — setting `read_closed` alone
    // does not cancel a pending `read_media_frame`, which would otherwise
    // block reconnect forever when the peer has gone silent.
    read_closed.store(true, Ordering::Relaxed);
    drop(wh);
    reader.abort();
    let _ = reader.await;
    let _ = BytesMut::new(); // keep bytes dep used
    Ok(end)
}

/// Read the acceptor's answer to a `Hello` announcing a version newer than
/// [`MEDIA_PROTOCOL_MIN_VERSION`]: an acceptor that speaks it confirms with
/// its own v1-framed `Hello`; a legacy one sends `Error{VERSION}` or just
/// hangs up. Returns `false` when the peer does not speak `version`. A peer
/// that stays silent is not a legacy peer (it would have closed), so a
/// timeout is not a rejection.
async fn read_hello_ack<S: AsyncRead + Unpin>(stream: &mut S, version: u16) -> bool {
    match tokio::time::timeout(AUTH_TIMEOUT, read_auth_media_frame(stream)).await {
        Ok(Ok(MediaMessage::Hello { version: v, .. })) => v == version,
        Ok(_) => false,
        Err(_) => true,
    }
}

/// `Error.code` an acceptor sends when it does not speak the announced
/// protocol version.
pub const VERSION_ERROR_CODE: &str = "VERSION";

async fn client_media_auth<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    secret: &str,
    local_id: NodeId,
) -> Result<(), std::io::Error> {
    let challenge = read_auth_media_frame(stream).await?;
    let MediaMessage::AuthChallenge { nonce } = challenge else {
        return Err(std::io::Error::other("expected AuthChallenge"));
    };
    let response = auth_response(secret, local_id, &nonce);
    write_media_frame(
        stream,
        &MediaMessage::Auth {
            node_id: local_id,
            response,
        },
    )
    .await?;
    match read_auth_media_frame(stream).await? {
        MediaMessage::AuthOk => Ok(()),
        _ => Err(std::io::Error::other("media auth failed")),
    }
}

/// Authenticate an inbound media connection (server: challenge → auth → hello).
pub async fn accept_auth<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    peer: IpAddr,
    secret: &str,
    local_id: NodeId,
    tls_required: bool,
    cert_node_id: Option<u64>,
) -> Result<NodeId, std::io::Error> {
    accept_auth_negotiated(stream, peer, secret, local_id, tls_required, cert_node_id)
        .await
        .map(|(node_id, _)| node_id)
}

/// [`accept_auth`], also returning the protocol version the dialer announced
/// in `Hello` (supported by this build) for the rest of the connection.
pub async fn accept_auth_negotiated<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    peer: IpAddr,
    secret: &str,
    local_id: NodeId,
    tls_required: bool,
    cert_node_id: Option<u64>,
) -> Result<(NodeId, u16), std::io::Error> {
    if cluster_auth_rate_limited(peer) {
        write_media_frame(stream, &MediaMessage::AuthFail).await?;
        return Err(std::io::Error::other("auth rate limited"));
    }
    let nonce = auth_nonce();
    write_media_frame(
        stream,
        &MediaMessage::AuthChallenge {
            nonce: nonce.clone(),
        },
    )
    .await?;
    let auth = read_auth_media_frame(stream).await?;
    let MediaMessage::Auth { node_id, response } = auth else {
        record_cluster_auth_failure(peer);
        write_media_frame(stream, &MediaMessage::AuthFail).await?;
        return Err(std::io::Error::other("expected AUTH"));
    };
    let expected = auth_response(secret, node_id, &nonce);
    if !secrets_equal(&expected, &response) {
        record_cluster_auth_failure(peer);
        write_media_frame(stream, &MediaMessage::AuthFail).await?;
        return Err(std::io::Error::other("auth fail"));
    }
    verify_tls_node_identity(tls_required, cert_node_id, node_id)?;
    clear_cluster_auth_failures(peer);
    write_media_frame(stream, &MediaMessage::AuthOk).await?;

    let hello = read_media_frame(stream).await?;
    let MediaMessage::Hello {
        version,
        node_id: hello_id,
    } = hello
    else {
        return Err(std::io::Error::other("expected HELLO"));
    };
    if !wire::is_supported_version(version) {
        write_media_frame(
            stream,
            &MediaMessage::Error {
                code: VERSION_ERROR_CODE.into(),
                message: "unsupported media protocol".into(),
                generation: 0,
            },
        )
        .await?;
        return Err(std::io::Error::other("bad version"));
    }
    if hello_id != node_id {
        return Err(std::io::Error::other("hello node_id mismatch"));
    }
    if version > MEDIA_PROTOCOL_MIN_VERSION {
        // Confirm the version (v1-framed like `Hello`), so a dialer can tell
        // an acceptor that speaks it from a legacy one that hangs up.
        write_media_frame(
            stream,
            &MediaMessage::Hello {
                version,
                node_id: local_id,
            },
        )
        .await?;
    }
    Ok((node_id, version))
}

/// Wrap an accepted TCP stream with optional mTLS, then run `accept_auth`.
pub(crate) async fn accept_tls_then_auth(
    stream: TcpStream,
    peer: IpAddr,
    secret: &str,
    local_id: NodeId,
    tls_server: Option<Arc<ServerConfig>>,
    is_peer_allowed: MediaMembershipFn,
) -> Result<(NodeId, u16, Box<dyn MediaIo>), std::io::Error> {
    let tls_required = tls_server.is_some();
    let mut io: Box<dyn MediaIo> = if let Some(cfg) = tls_server {
        let acceptor = TlsAcceptor::from(cfg);
        let tls = tokio::time::timeout(AUTH_TIMEOUT, acceptor.accept(stream))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "tls accept timeout")
            })??;
        let cert_node_id = tls
            .get_ref()
            .1
            .peer_certificates()
            .and_then(node_id_from_peer_certs);
        let mut boxed: Box<dyn MediaIo> = Box::new(tls);
        let (peer_id, version) = accept_auth_negotiated(
            &mut boxed,
            peer,
            secret,
            local_id,
            tls_required,
            cert_node_id,
        )
        .await?;
        if !(is_peer_allowed)(peer_id) {
            return Err(std::io::Error::other(
                "media peer not in cluster membership",
            ));
        }
        return Ok((peer_id, version, boxed));
    } else {
        Box::new(stream)
    };
    let (peer_id, version) =
        accept_auth_negotiated(&mut io, peer, secret, local_id, tls_required, None).await?;
    if !(is_peer_allowed)(peer_id) {
        return Err(std::io::Error::other(
            "media peer not in cluster membership",
        ));
    }
    Ok((peer_id, version, io))
}

/// Write pump for an accepted inbound media session.
///
/// Owners receive `Subscribe` on this socket; live `MediaFrame`s must go back
/// on the same connection when the owner has no outbound `MediaPeer` (default
/// plaintext clustering never dials from heartbeats).
pub struct InboundMediaSink {
    pub peer_id: NodeId,
    queue: Arc<LiveMediaQueue>,
    counters: Arc<PeerCounters>,
    closed: Arc<AtomicBool>,
}

impl InboundMediaSink {
    pub fn spawn<W>(
        peer_id: NodeId,
        queue_cfg: impl Into<LiveQueueConfig>,
        version: u16,
        mut wh: W,
    ) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let queue = Arc::new(LiveMediaQueue::new(queue_cfg.into()));
        let counters = Arc::new(PeerCounters::default());
        counters.connects.store(1, Ordering::Relaxed);
        counters.protocol_version.store(version, Ordering::Relaxed);
        let closed = Arc::new(AtomicBool::new(false));
        let q = Arc::clone(&queue);
        let ctr = Arc::clone(&counters);
        let cl = Arc::clone(&closed);
        tokio::spawn(async move {
            loop {
                if cl.load(Ordering::Relaxed) {
                    break;
                }
                let Some(msg) = q.pop().await else {
                    break;
                };
                if cl.load(Ordering::Relaxed) {
                    break;
                }
                match tokio::time::timeout(WRITE_TIMEOUT, wire::write_frame(&mut wh, &msg, version))
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        ctr.write_errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_) => {
                        ctr.write_timeouts.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
            cl.store(true, Ordering::Relaxed);
            q.close();
        });
        Self {
            peer_id,
            queue,
            counters,
            closed,
        }
    }

    pub fn try_send(&self, msg: MediaMessage) -> Result<(), ()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(());
        }
        if self.queue.push(msg).is_queued() {
            Ok(())
        } else {
            Err(())
        }
    }

    pub async fn send(&self, msg: MediaMessage) -> Result<(), ()> {
        self.try_send(msg)
    }

    /// See [`LiveMediaQueue::take_reinit`].
    pub fn take_reinit(&self, app: &str, stream: &str) -> bool {
        self.queue.take_reinit(app, stream)
    }

    pub fn mark_reinit(&self, app: &str, stream: &str) {
        self.queue.mark_reinit(app, stream);
    }

    pub fn stats(&self) -> PeerMediaStats {
        peer_stats(self.peer_id, "inbound", &self.queue, &self.counters)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.queue.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::media::live_queue::approx_size;
    use librtmp2::DeliveryHint;
    use std::net::Ipv4Addr;
    use std::path::PathBuf;
    use tokio::net::TcpListener;

    const SECRET: &str = "test-cluster-secret-32-chars-min--";
    const WAIT: Duration = Duration::from_secs(5);

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/cluster-tls")
            .join(name)
    }

    fn tls_for(node: u64) -> (Arc<ServerConfig>, Arc<ClientConfig>) {
        // Dev-dependencies pull in a second rustls backend, so the process
        // default must be pinned explicitly in tests (production only links ring).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = fixture(&format!("node{node}.pem"));
        let key = fixture(&format!("node{node}.key"));
        let ca = fixture("ca.pem");
        (
            crate::cluster::security::build_server_tls(&cert, &key, &ca).unwrap(),
            crate::cluster::security::build_client_tls(&cert, &key, &ca).unwrap(),
        )
    }

    fn test_ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, last))
    }

    fn frame(payload_len: usize) -> MediaMessage {
        MediaMessage::MediaFrame {
            app: "live".into(),
            stream: "s".into(),
            epoch: 1,
            frame_type: 1,
            timestamp: 0,
            timeline_ts: 0,
            hint: DeliveryHint::Droppable,
            payload: vec![0u8; payload_len],
        }
    }

    fn allow_all() -> MediaMembershipFn {
        Arc::new(|_| true)
    }

    async fn recv_timeout<T>(rx: &mut mpsc::Receiver<T>) -> T {
        tokio::time::timeout(WAIT, rx.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed")
    }

    /// Client side of the auth handshake with an explicit claimed node id and
    /// optional forged response, returning the server's verdict frame.
    async fn client_auth_raw<S: AsyncRead + AsyncWrite + Unpin>(
        s: &mut S,
        node_id: NodeId,
        response: Option<String>,
    ) -> MediaMessage {
        let MediaMessage::AuthChallenge { nonce } = read_media_frame(s).await.unwrap() else {
            panic!("expected challenge");
        };
        let response = response.unwrap_or_else(|| auth_response(SECRET, node_id, &nonce));
        write_media_frame(s, &MediaMessage::Auth { node_id, response })
            .await
            .unwrap();
        read_media_frame(s).await.unwrap()
    }

    #[tokio::test]
    async fn media_frame_roundtrip_and_malformed_input() {
        let (mut a, mut b) = tokio::io::duplex(1 << 16);
        write_media_frame(&mut a, &frame(10)).await.unwrap();
        match read_media_frame(&mut b).await.unwrap() {
            MediaMessage::MediaFrame { payload, .. } => assert_eq!(payload.len(), 10),
            other => panic!("unexpected {other:?}"),
        }

        // Declared length above the cap is rejected before allocation.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(MAX_FRAME + 1).to_be_bytes());
        let mut r: &[u8] = &bytes;
        let err = read_media_frame(&mut r).await.unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");

        // Garbage JSON surfaces as an error instead of a panic.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&4u32.to_be_bytes());
        bytes.extend_from_slice(b"nope");
        let mut r: &[u8] = &bytes;
        assert!(read_media_frame(&mut r).await.is_err());

        // Truncated body is an I/O error.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u32.to_be_bytes());
        bytes.extend_from_slice(b"{}");
        let mut r: &[u8] = &bytes;
        assert!(read_media_frame(&mut r).await.is_err());

        // Auth frames have a much smaller cap.
        let (mut a, mut b) = tokio::io::duplex(1 << 16);
        write_media_frame(&mut a, &frame(MAX_AUTH_FRAME as usize))
            .await
            .unwrap();
        let err = read_auth_media_frame(&mut b).await.unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
    }

    #[tokio::test]
    async fn oversized_outbound_frame_is_refused() {
        // Each zero byte serializes as "0," so this exceeds MAX_FRAME as JSON.
        let msg = frame(MAX_FRAME as usize / 2 + 1024);
        let mut sink = tokio::io::sink();
        let err = write_media_frame(&mut sink, &msg).await.unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
    }

    #[test]
    fn approx_size_accounts_payload_bytes() {
        assert_eq!(approx_size(&frame(100)), 164);
        let init = MediaMessage::InitCache {
            app: "a".into(),
            stream: "s".into(),
            epoch: 1,
            metadata: Some(vec![0; 1]),
            avc_header: Some(vec![0; 2]),
            aac_header: Some(vec![0; 3]),
            keyframe: Some((0, vec![0; 4])),
        };
        assert_eq!(approx_size(&init), 74);
        let empty_init = MediaMessage::InitCache {
            app: "a".into(),
            stream: "s".into(),
            epoch: 1,
            metadata: None,
            avc_header: None,
            aac_header: None,
            keyframe: None,
        };
        assert_eq!(approx_size(&empty_init), 64);
        assert_eq!(approx_size(&MediaMessage::AuthOk), 128);
    }

    #[tokio::test]
    async fn client_media_auth_success_and_failures() {
        // Success.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move {
            let r = accept_auth(&mut s, test_ip(1), SECRET, 9, false, None).await;
            (r, s)
        });
        client_media_auth(&mut c, SECRET, 3).await.unwrap();
        write_media_frame(
            &mut c,
            &MediaMessage::Hello {
                version: crate::cluster::media::MEDIA_PROTOCOL_VERSION,
                node_id: 3,
            },
        )
        .await
        .unwrap();
        let (r, _s) = server.await.unwrap();
        assert_eq!(r.unwrap(), 3);

        // Server opens with something other than a challenge.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        write_media_frame(&mut s, &MediaMessage::AuthOk)
            .await
            .unwrap();
        let err = client_media_auth(&mut c, SECRET, 3).await.unwrap_err();
        assert!(err.to_string().contains("AuthChallenge"), "{err}");

        // Server rejects the response.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        write_media_frame(
            &mut s,
            &MediaMessage::AuthChallenge {
                nonce: auth_nonce(),
            },
        )
        .await
        .unwrap();
        write_media_frame(&mut s, &MediaMessage::AuthFail)
            .await
            .unwrap();
        let err = client_media_auth(&mut c, SECRET, 3).await.unwrap_err();
        assert!(err.to_string().contains("media auth failed"), "{err}");
    }

    #[tokio::test]
    async fn accept_auth_rejects_bad_handshakes() {
        // Wrong message instead of Auth.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server =
            tokio::spawn(
                async move { accept_auth(&mut s, test_ip(2), SECRET, 9, false, None).await },
            );
        let _ = read_media_frame(&mut c).await.unwrap();
        write_media_frame(&mut c, &MediaMessage::AuthOk)
            .await
            .unwrap();
        assert!(matches!(
            read_media_frame(&mut c).await.unwrap(),
            MediaMessage::AuthFail
        ));
        assert!(server.await.unwrap().is_err());

        // Wrong response (different secret).
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server =
            tokio::spawn(
                async move { accept_auth(&mut s, test_ip(2), SECRET, 9, false, None).await },
            );
        let verdict = client_auth_raw(&mut c, 4, Some("00".repeat(32))).await;
        assert!(matches!(verdict, MediaMessage::AuthFail));
        let err = server.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("auth fail"), "{err}");

        // mTLS active: a valid secret response whose claimed id differs from
        // the certificate identity is rejected.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server =
            tokio::spawn(
                async move { accept_auth(&mut s, test_ip(3), SECRET, 9, true, Some(8)).await },
            );
        let MediaMessage::AuthChallenge { nonce } = read_media_frame(&mut c).await.unwrap() else {
            panic!("expected challenge")
        };
        write_media_frame(
            &mut c,
            &MediaMessage::Auth {
                node_id: 4,
                response: auth_response(SECRET, 4, &nonce),
            },
        )
        .await
        .unwrap();
        let err = server.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");

        // Hello missing after AuthOk.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server =
            tokio::spawn(
                async move { accept_auth(&mut s, test_ip(4), SECRET, 9, false, None).await },
            );
        assert!(matches!(
            client_auth_raw(&mut c, 4, None).await,
            MediaMessage::AuthOk
        ));
        write_media_frame(&mut c, &MediaMessage::AuthOk)
            .await
            .unwrap();
        let err = server.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("expected HELLO"), "{err}");

        // Unsupported protocol version is answered with an Error frame.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server =
            tokio::spawn(
                async move { accept_auth(&mut s, test_ip(4), SECRET, 9, false, None).await },
            );
        client_auth_raw(&mut c, 4, None).await;
        write_media_frame(
            &mut c,
            &MediaMessage::Hello {
                version: 999,
                node_id: 4,
            },
        )
        .await
        .unwrap();
        match read_media_frame(&mut c).await.unwrap() {
            MediaMessage::Error { code, .. } => assert_eq!(code, "VERSION"),
            other => panic!("unexpected {other:?}"),
        }
        let err = server.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("bad version"), "{err}");

        // Hello node id must match the authenticated id.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server =
            tokio::spawn(
                async move { accept_auth(&mut s, test_ip(4), SECRET, 9, false, None).await },
            );
        client_auth_raw(&mut c, 4, None).await;
        write_media_frame(
            &mut c,
            &MediaMessage::Hello {
                version: crate::cluster::media::MEDIA_PROTOCOL_VERSION,
                node_id: 5,
            },
        )
        .await
        .unwrap();
        let err = server.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("mismatch"), "{err}");
    }

    #[tokio::test]
    async fn accept_auth_rate_limits_repeat_offenders() {
        let ip = test_ip(20);
        for _ in 0..10 {
            record_cluster_auth_failure(ip);
        }
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let err = accept_auth(&mut s, ip, SECRET, 9, false, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("rate limited"), "{err}");
        assert!(matches!(
            read_media_frame(&mut c).await.unwrap(),
            MediaMessage::AuthFail
        ));
        clear_cluster_auth_failures(ip);
    }

    async fn dial_and_hello(
        addr: std::net::SocketAddr,
        tls: Option<Arc<ClientConfig>>,
        node_id: NodeId,
    ) -> Box<dyn MediaIo> {
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut io: Box<dyn MediaIo> = match tls {
            Some(cfg) => {
                let host =
                    crate::cluster::network::tls_server_name_from_addr(&addr.to_string()).unwrap();
                Box::new(TlsConnector::from(cfg).connect(host, tcp).await.unwrap())
            }
            None => Box::new(tcp),
        };
        client_media_auth(&mut io, SECRET, node_id).await.unwrap();
        write_media_frame(
            &mut io,
            &MediaMessage::Hello {
                version: crate::cluster::media::MEDIA_PROTOCOL_VERSION,
                node_id,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            read_media_frame(&mut io).await.unwrap(),
            MediaMessage::Hello { .. }
        ));
        io
    }

    #[tokio::test]
    async fn accept_tls_then_auth_plaintext_membership_gate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(dial_and_hello(addr, None, 7));
        let (s, peer) = listener.accept().await.unwrap();
        let (id, _version, _io) = accept_tls_then_auth(s, peer.ip(), SECRET, 1, None, allow_all())
            .await
            .unwrap();
        assert_eq!(id, 7);
        let _ = client.await.unwrap();

        let client = tokio::spawn(dial_and_hello(addr, None, 7));
        let (s, peer) = listener.accept().await.unwrap();
        let deny: MediaMembershipFn = Arc::new(|_| false);
        let err = match accept_tls_then_auth(s, peer.ip(), SECRET, 1, None, deny).await {
            Ok(_) => panic!("non-member must be rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("membership"), "{err}");
        let _ = client.await.unwrap();
    }

    #[tokio::test]
    async fn accept_tls_then_auth_with_mutual_tls() {
        let (server_cfg, _) = tls_for(2);
        let (_, client_cfg) = tls_for(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(dial_and_hello(addr, Some(Arc::clone(&client_cfg)), 1));
        let (s, peer) = listener.accept().await.unwrap();
        let (id, _version, mut io) = accept_tls_then_auth(
            s,
            peer.ip(),
            SECRET,
            2,
            Some(Arc::clone(&server_cfg)),
            allow_all(),
        )
        .await
        .unwrap();
        assert_eq!(id, 1, "cert identity lrtmp2-node-1 matches the claimed id");
        let mut client_io = client.await.unwrap();
        write_media_frame(&mut io, &MediaMessage::AuthOk)
            .await
            .unwrap();
        assert!(matches!(
            read_media_frame(&mut client_io).await.unwrap(),
            MediaMessage::AuthOk
        ));

        // Authenticated over TLS but not a cluster member.
        let client = tokio::spawn(dial_and_hello(addr, Some(client_cfg), 1));
        let (s, peer) = listener.accept().await.unwrap();
        let deny: MediaMembershipFn = Arc::new(|_| false);
        assert!(
            accept_tls_then_auth(s, peer.ip(), SECRET, 2, Some(server_cfg), deny)
                .await
                .is_err()
        );
        let _ = client.await.unwrap();
    }

    #[tokio::test]
    async fn media_peer_dials_relays_reconnects_and_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Capacity 1 so a burst exercises the inbound-full drop path.
        let (in_tx, mut in_rx) = mpsc::channel(1);
        let (rc_tx, mut rc_rx) = mpsc::unbounded_channel();
        let peer = MediaPeer::spawn(2, addr.to_string(), SECRET.into(), 1, 1, in_tx, None, rc_tx);
        assert!(!peer.is_closed());

        // Queued before the connection exists; delivered once it does.
        peer.try_send(MediaMessage::Subscribe {
            app: "live".into(),
            stream: "s".into(),
            epoch: 1,
            generation: 0,
        })
        .unwrap();

        let (mut s, _) = tokio::time::timeout(WAIT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let id = accept_auth(&mut s, addr.ip(), SECRET, 2, false, None)
            .await
            .unwrap();
        assert_eq!(id, 1);
        assert!(matches!(
            read_media_frame_v(&mut s, MEDIA_PROTOCOL_VERSION)
                .await
                .unwrap(),
            MediaMessage::Subscribe { .. }
        ));

        // Two frames back-to-back: the first is stamped with the peer id, the
        // second overflows the capacity-1 inbound queue and is dropped.
        wire::write_frame(&mut s, &frame(3), MEDIA_PROTOCOL_VERSION)
            .await
            .unwrap();
        wire::write_frame(&mut s, &frame(4), MEDIA_PROTOCOL_VERSION)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (from, msg) = recv_timeout(&mut in_rx).await;
        assert_eq!(from, 2);
        assert!(matches!(msg, MediaMessage::MediaFrame { ref payload, .. } if payload.len() == 3));
        assert!(in_rx.try_recv().is_err(), "overflow frame must be dropped");

        // Remote hangs up: the peer reports a reconnect and dials again.
        drop(s);
        let reconnected = tokio::time::timeout(WAIT, rc_rx.recv()).await.unwrap();
        assert_eq!(reconnected, Some(2));
        let (mut s, _) = tokio::time::timeout(WAIT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        accept_auth(&mut s, addr.ip(), SECRET, 2, false, None)
            .await
            .unwrap();
        peer.try_send(MediaMessage::InitCache {
            app: "live".into(),
            stream: "s".into(),
            epoch: 1,
            metadata: None,
            avc_header: Some(vec![1, 2]),
            aac_header: None,
            keyframe: None,
        })
        .unwrap();
        assert!(matches!(
            read_media_frame_v(&mut s, MEDIA_PROTOCOL_VERSION)
                .await
                .unwrap(),
            MediaMessage::InitCache { .. }
        ));

        // Explicit close tears the session down and rejects further sends.
        peer.close();
        assert!(peer.is_closed());
        assert!(peer.try_send(frame(1)).is_err());
        let eof = tokio::time::timeout(WAIT, read_media_frame(&mut s))
            .await
            .unwrap();
        assert!(eof.is_err(), "closed peer must drop its socket");
    }

    #[tokio::test]
    async fn media_peer_shuts_down_when_handle_is_dropped() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (in_tx, _in_rx) = mpsc::channel(4);
        let (rc_tx, _rc_rx) = mpsc::unbounded_channel();
        let peer = MediaPeer::spawn(2, addr.to_string(), SECRET.into(), 1, 1, in_tx, None, rc_tx);
        let (mut s, _) = tokio::time::timeout(WAIT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        accept_auth(&mut s, addr.ip(), SECRET, 2, false, None)
            .await
            .unwrap();
        drop(peer);
        let eof = tokio::time::timeout(WAIT, read_media_frame(&mut s))
            .await
            .unwrap();
        assert!(eof.is_err(), "dropping the handle must close the socket");
    }

    #[tokio::test]
    async fn media_peer_over_mutual_tls() {
        let (server_cfg, _) = tls_for(2);
        let (_, client_cfg) = tls_for(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (in_tx, mut in_rx) = mpsc::channel(4);
        let (rc_tx, _rc_rx) = mpsc::unbounded_channel();
        let peer = MediaPeer::spawn(
            2,
            addr.to_string(),
            SECRET.into(),
            1,
            1,
            in_tx,
            Some(client_cfg),
            rc_tx,
        );
        let (s, ip) = tokio::time::timeout(WAIT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (id, _version, mut io) =
            accept_tls_then_auth(s, ip.ip(), SECRET, 2, Some(server_cfg), allow_all())
                .await
                .unwrap();
        assert_eq!(id, 1);
        wire::write_frame(&mut io, &frame(5), MEDIA_PROTOCOL_VERSION)
            .await
            .unwrap();
        let (from, _) = recv_timeout(&mut in_rx).await;
        assert_eq!(from, 2);
        peer.close();
    }

    #[tokio::test]
    async fn media_peer_bounds_its_outbound_queue() {
        // Nothing listens here: dialing fails and the queue is never drained.
        let addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        let (in_tx, _in_rx) = mpsc::channel(4);
        let (rc_tx, _rc_rx) = mpsc::unbounded_channel();
        let peer = MediaPeer::spawn(2, addr.to_string(), SECRET.into(), 1, 0, in_tx, None, rc_tx);

        // A frame above half the (1 MiB minimum) byte bound is refused.
        assert!(peer.try_send(frame(2 * 1024 * 1024)).is_err());
        assert_eq!(peer.stats().queue.oversized_frames_dropped, 1);
        // Control messages are never evicted, so the message bound rejects
        // the one past it.
        let sub = || MediaMessage::Unsubscribe {
            app: "live".into(),
            stream: "s".into(),
        };
        for _ in 0..crate::cluster::media::live_queue::DEFAULT_MAX_MESSAGES {
            peer.try_send(sub()).unwrap();
        }
        assert!(peer.try_send(sub()).is_err());
        assert_eq!(
            peer.stats().queue.queue_messages,
            crate::cluster::media::live_queue::DEFAULT_MAX_MESSAGES
        );

        // Let at least one failed dial + backoff happen, then close.
        tokio::time::sleep(Duration::from_millis(50)).await;
        peer.close();
        assert!(peer.is_closed());
    }

    /// Test H: a peer that stops reading cannot wedge the writer. The write
    /// times out, the peer reconnects, the media queued for the dead
    /// connection is discarded and live delivery resumes at the next
    /// resync point.
    #[tokio::test]
    async fn stalled_peer_times_out_reconnects_and_drops_stale_media() {
        use crate::cluster::media::live_queue::LiveQueueConfig;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (in_tx, _in_rx) = mpsc::channel(4);
        let (rc_tx, mut rc_rx) = mpsc::unbounded_channel();
        let cfg = LiveQueueConfig::new(64, 0);
        let peer = MediaPeer::spawn(
            2,
            addr.to_string(),
            SECRET.into(),
            1,
            cfg,
            in_tx,
            None,
            rc_tx,
        );

        // First connection: authenticate, then never read again.
        let (mut stalled, _) = tokio::time::timeout(WAIT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        accept_auth_negotiated(&mut stalled, addr.ip(), SECRET, 2, false, None)
            .await
            .unwrap();
        let media = |hint, tag: u8, len| MediaMessage::MediaFrame {
            app: "live".into(),
            stream: "s".into(),
            epoch: 1,
            frame_type: 1,
            timestamp: u32::from(tag),
            timeline_ts: u32::from(tag),
            hint,
            payload: vec![tag; len],
        };
        // Far more than the socket buffers hold: the write blocks.
        peer.try_send(media(DeliveryHint::ResyncPoint, 1, 256 * 1024))
            .unwrap();
        for i in 2..80u8 {
            let _ = peer.try_send(media(DeliveryHint::Droppable, i, 256 * 1024));
        }
        let timed_out = tokio::time::timeout(WAIT, async {
            while peer.stats().write_timeouts == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(timed_out.is_ok(), "stalled write must hit WRITE_TIMEOUT");
        assert_eq!(rc_rx.recv().await, Some(2), "peer must report a reconnect");

        // The peer redials; whatever was queued for the old connection is
        // stale and must not be replayed.
        let (mut fresh, _) = tokio::time::timeout(WAIT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        accept_auth_negotiated(&mut fresh, addr.ip(), SECRET, 2, false, None)
            .await
            .unwrap();
        drop(stalled);
        let keyframe = media(DeliveryHint::ResyncPoint, 200, 1000);
        // Pushed once the reset has run (the redial implies it).
        assert!(peer.try_send(keyframe).is_ok());
        let first =
            tokio::time::timeout(WAIT, read_media_frame_v(&mut fresh, MEDIA_PROTOCOL_VERSION))
                .await
                .unwrap()
                .unwrap();
        match first {
            MediaMessage::MediaFrame { payload, hint, .. } => {
                assert_eq!(payload[0], 200, "backlog must not be replayed");
                assert_eq!(hint, DeliveryHint::ResyncPoint);
            }
            other => panic!("unexpected {other:?}"),
        }
        let stats = peer.stats();
        assert!(stats.reconnects >= 1);
        assert!(stats.queue.dropped_stale_frames > 0, "{stats:?}");
        assert!(
            stats.queue.queue_bytes < 1024 * 1024,
            "queue state cleaned: {stats:?}"
        );
        peer.close();
    }

    #[tokio::test]
    async fn inbound_sink_writes_in_order_and_enforces_limits() {
        let (w, mut r) = tokio::io::duplex(1 << 20);
        let sink = InboundMediaSink::spawn(5, 1, MEDIA_PROTOCOL_VERSION, w);
        assert_eq!(sink.peer_id, 5);
        sink.try_send(MediaMessage::Unsubscribe {
            app: "live".into(),
            stream: "s".into(),
        })
        .unwrap();
        sink.send(frame(8)).await.unwrap();
        assert!(matches!(
            read_media_frame_v(&mut r, MEDIA_PROTOCOL_VERSION)
                .await
                .unwrap(),
            MediaMessage::Unsubscribe { .. }
        ));
        assert!(matches!(
            read_media_frame_v(&mut r, MEDIA_PROTOCOL_VERSION)
                .await
                .unwrap(),
            MediaMessage::MediaFrame { .. }
        ));

        // Larger than the 1 MiB queue budget: refused by both entry points.
        assert!(sink.try_send(frame(2 * 1024 * 1024)).is_err());
        assert!(sink.send(frame(2 * 1024 * 1024)).await.is_err());

        sink.close();
        assert!(sink.is_closed());
        assert!(sink.try_send(frame(1)).is_err());
        assert!(sink.send(frame(1)).await.is_err());
        // The write pump exits and drops the writer, so the reader sees EOF.
        let eof = tokio::time::timeout(WAIT, read_media_frame_v(&mut r, MEDIA_PROTOCOL_VERSION))
            .await
            .unwrap();
        assert!(eof.is_err());
    }

    #[tokio::test]
    async fn inbound_sink_closes_on_write_error_and_bounds_channel() {
        // A tiny, never-read pipe blocks the pump on its first write so the
        // 256-slot channel fills up.
        let (w, r) = tokio::io::duplex(16);
        let sink = InboundMediaSink::spawn(6, 64, MEDIA_PROTOCOL_VERSION, w);
        let mut accepted = 0;
        for _ in 0..2000 {
            let unsub = MediaMessage::Unsubscribe {
                app: "live".into(),
                stream: "s".into(),
            };
            if sink.try_send(unsub).is_ok() {
                accepted += 1;
            }
        }
        assert!(accepted < 2000, "message bound must bound the queue");

        // Dropping the reader turns the blocked write into an error; the pump
        // then marks the sink closed.
        drop(r);
        let closed = tokio::time::timeout(WAIT, async {
            while !sink.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(closed.is_ok(), "write error must close the sink");
    }

    #[tokio::test]
    async fn inbound_sink_idle_tick_observes_close() {
        let (w, _r) = tokio::io::duplex(1024);
        let sink = InboundMediaSink::spawn(7, 1, MEDIA_PROTOCOL_VERSION, w);
        // Let the pump sit through at least one idle tick first.
        tokio::time::sleep(Duration::from_millis(150)).await;
        sink.closed.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(sink.is_closed());
    }
}
