//! Outbound/inbound media peer connection.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::cluster::security::try_reserve_inflight_bytes;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use rustls::{ClientConfig, ServerConfig};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::cluster::NodeId;
use crate::cluster::media::MediaMembershipFn;
use crate::cluster::media::protocol::MediaMessage;
use crate::cluster::security::{
    auth_nonce, auth_response, clear_cluster_auth_failures, cluster_auth_rate_limited,
    node_id_from_peer_certs, record_cluster_auth_failure, secrets_equal, verify_tls_node_identity,
};

const MAX_FRAME: u32 = 32 * 1024 * 1024;
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
const WRITE_TIMEOUT: Duration = Duration::from_secs(8);

pub(crate) trait MediaIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> MediaIo for T {}

pub async fn write_media_frame<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    msg: &MediaMessage,
) -> Result<(), std::io::Error> {
    let bytes = serde_json::to_vec(msg).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_FRAME as usize {
        return Err(std::io::Error::other("media frame too large"));
    }
    w.write_u32(bytes.len() as u32).await?;
    w.write_all(&bytes).await?;
    Ok(())
}

pub async fn read_media_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> Result<MediaMessage, std::io::Error> {
    tokio::time::timeout(MEDIA_READ_TIMEOUT, read_media_frame_max(r, MAX_FRAME))
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "media frame read timeout")
        })?
}

async fn read_media_frame_max<R: AsyncReadExt + Unpin>(
    r: &mut R,
    max: u32,
) -> Result<MediaMessage, std::io::Error> {
    let len = r.read_u32().await?;
    if len > max {
        return Err(std::io::Error::other("media frame too large"));
    }
    let _read_budget = try_reserve_inflight_bytes(
        &MEDIA_READ_BYTES_INFLIGHT,
        MAX_MEDIA_READ_BYTES_INFLIGHT,
        len as usize,
    )
    .map_err(|_| std::io::Error::other("media read memory budget exceeded"))?;
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).map_err(std::io::Error::other)
}

async fn read_auth_media_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> Result<MediaMessage, std::io::Error> {
    tokio::time::timeout(AUTH_TIMEOUT, read_media_frame_max(r, MAX_AUTH_FRAME))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "media auth timeout"))?
}

/// Multiplexed long-lived peer with bounded outbound queue.
pub struct MediaPeer {
    pub peer_id: NodeId,
    pub addr: String,
    tx: mpsc::Sender<MediaMessage>,
    queue_bytes: Arc<AtomicUsize>,
    max_queue_bytes: usize,
    closed: Arc<AtomicBool>,
}

impl MediaPeer {
    pub fn spawn(
        peer_id: NodeId,
        addr: String,
        secret: String,
        local_id: NodeId,
        max_queue_mb: u32,
        inbound: mpsc::Sender<(NodeId, MediaMessage)>,
        tls_client: Option<Arc<ClientConfig>>,
        on_reconnected: mpsc::UnboundedSender<NodeId>,
    ) -> Self {
        let max_queue_bytes = (max_queue_mb as usize)
            .saturating_mul(1024 * 1024)
            .max(1024 * 1024);
        let (tx, mut rx) = mpsc::channel::<MediaMessage>(256);
        let queue_bytes = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let qb = Arc::clone(&queue_bytes);
        let cl = Arc::clone(&closed);
        let addr_c = addr.clone();
        let on_reconnected = on_reconnected.clone();

        tokio::spawn(async move {
            let mut backoff_ms = 500u64;
            loop {
                if cl.load(Ordering::Relaxed) {
                    break;
                }
                let attempt_started = Instant::now();
                match connect_and_run(
                    &addr_c,
                    &secret,
                    local_id,
                    peer_id,
                    &mut rx,
                    &inbound,
                    &qb,
                    max_queue_bytes,
                    &cl,
                    tls_client.clone(),
                )
                .await
                {
                    Ok(ConnectEnd::Shutdown) => break,
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
        });

        Self {
            peer_id,
            addr,
            tx,
            queue_bytes,
            max_queue_bytes,
            closed,
        }
    }

    pub fn try_send(&self, msg: MediaMessage) -> Result<(), ()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(());
        }
        let approx = approx_size(&msg);
        if try_reserve_queue_bytes(&self.queue_bytes, self.max_queue_bytes, approx).is_err() {
            tracing::warn!(peer = self.peer_id, "media queue full — dropping frame");
            return Err(());
        }
        self.tx.try_send(msg).map_err(|_| {
            self.queue_bytes.fetch_sub(approx, Ordering::Relaxed);
        })
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }
}

fn try_reserve_queue_bytes(
    queue_bytes: &AtomicUsize,
    max_queue_bytes: usize,
    approx: usize,
) -> Result<(), ()> {
    loop {
        let cur = queue_bytes.load(Ordering::Acquire);
        let Some(new) = cur.checked_add(approx) else {
            return Err(());
        };
        if new > max_queue_bytes {
            return Err(());
        }
        if queue_bytes
            .compare_exchange(cur, new, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Ok(());
        }
    }
}

fn approx_size(msg: &MediaMessage) -> usize {
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

enum ConnectEnd {
    /// Intentional shutdown (`closed` or channel dropped).
    Shutdown,
    /// Peer disconnected; outer loop should reconnect.
    Transient,
}

async fn connect_and_run(
    addr: &str,
    secret: &str,
    local_id: NodeId,
    peer_id: NodeId,
    outbound: &mut mpsc::Receiver<MediaMessage>,
    inbound: &mpsc::Sender<(NodeId, MediaMessage)>,
    queue_bytes: &AtomicUsize,
    _max_queue: usize,
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
    write_media_frame(
        &mut stream,
        &MediaMessage::Hello {
            version: crate::cluster::media::MEDIA_PROTOCOL_VERSION,
            node_id: local_id,
        },
    )
    .await?;

    let (mut rh, mut wh) = tokio::io::split(stream);
    let read_closed = Arc::new(AtomicBool::new(false));
    let rc = Arc::clone(&read_closed);
    let inbound_c = inbound.clone();
    let reader = tokio::spawn(async move {
        while !rc.load(Ordering::Relaxed) {
            match read_media_frame(&mut rh).await {
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
            msg = outbound.recv() => {
                let Some(msg) = msg else {
                    end = ConnectEnd::Shutdown;
                    break;
                };
                let size = approx_size(&msg);
                // Account for this message being off the queue whether the
                // write succeeds or the socket fails — a failed write still
                // exits this loop and reconnects, and it must not leave
                // queue_bytes permanently inflated by every message dropped
                // this way (which would eventually make try_send() report
                // the queue full even though the real channel is empty).
                queue_bytes.fetch_sub(size.min(queue_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
                match tokio::time::timeout(WRITE_TIMEOUT, write_media_frame(&mut wh, &msg)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) => break,
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
    let _ = local_id;
    write_media_frame(stream, &MediaMessage::AuthOk).await?;

    let hello = read_media_frame(stream).await?;
    let MediaMessage::Hello {
        version,
        node_id: hello_id,
    } = hello
    else {
        return Err(std::io::Error::other("expected HELLO"));
    };
    if version != crate::cluster::media::MEDIA_PROTOCOL_VERSION {
        write_media_frame(
            stream,
            &MediaMessage::Error {
                code: "VERSION".into(),
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
    Ok(node_id)
}

/// Wrap an accepted TCP stream with optional mTLS, then run `accept_auth`.
pub(crate) async fn accept_tls_then_auth(
    stream: TcpStream,
    peer: IpAddr,
    secret: &str,
    local_id: NodeId,
    tls_server: Option<Arc<ServerConfig>>,
    is_peer_allowed: MediaMembershipFn,
) -> Result<(NodeId, Box<dyn MediaIo>), std::io::Error> {
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
        let peer_id = accept_auth(
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
        return Ok((peer_id, boxed));
    } else {
        Box::new(stream)
    };
    let peer_id = accept_auth(&mut io, peer, secret, local_id, tls_required, None).await?;
    if !(is_peer_allowed)(peer_id) {
        return Err(std::io::Error::other(
            "media peer not in cluster membership",
        ));
    }
    Ok((peer_id, io))
}

/// Write pump for an accepted inbound media session.
///
/// Owners receive `Subscribe` on this socket; live `MediaFrame`s must go back
/// on the same connection when the owner has no outbound `MediaPeer` (default
/// plaintext clustering never dials from heartbeats).
pub struct InboundMediaSink {
    pub peer_id: NodeId,
    tx: mpsc::Sender<MediaMessage>,
    queue_bytes: Arc<AtomicUsize>,
    max_queue_bytes: usize,
    closed: Arc<AtomicBool>,
    close_notify: Arc<Notify>,
}

impl InboundMediaSink {
    pub fn spawn<W>(peer_id: NodeId, max_queue_mb: u32, mut wh: W) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let max_queue_bytes = (max_queue_mb as usize)
            .saturating_mul(1024 * 1024)
            .max(1024 * 1024);
        let (tx, mut rx) = mpsc::channel::<MediaMessage>(256);
        let queue_bytes = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let close_notify = Arc::new(Notify::new());
        let qb = Arc::clone(&queue_bytes);
        let cl = Arc::clone(&closed);
        let notify = Arc::clone(&close_notify);
        tokio::spawn(async move {
            loop {
                if cl.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    biased;
                    _ = notify.notified() => {
                        if cl.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        if cl.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    msg = rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        if cl.load(Ordering::Relaxed) {
                            break;
                        }
                        let size = approx_size(&msg);
                        qb.fetch_sub(size.min(qb.load(Ordering::Relaxed)), Ordering::Relaxed);
                        match tokio::time::timeout(WRITE_TIMEOUT, write_media_frame(&mut wh, &msg)).await {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) | Err(_) => break,
                        }
                    }
                }
            }
            cl.store(true, Ordering::Relaxed);
        });
        Self {
            peer_id,
            tx,
            queue_bytes,
            max_queue_bytes,
            closed,
            close_notify,
        }
    }

    pub fn try_send(&self, msg: MediaMessage) -> Result<(), ()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(());
        }
        let approx = approx_size(&msg);
        if try_reserve_queue_bytes(&self.queue_bytes, self.max_queue_bytes, approx).is_err() {
            tracing::warn!(
                peer = self.peer_id,
                "inbound media queue full — dropping frame"
            );
            return Err(());
        }
        self.tx.try_send(msg).map_err(|_| {
            self.queue_bytes.fetch_sub(approx, Ordering::Relaxed);
        })
    }

    pub async fn send(&self, msg: MediaMessage) -> Result<(), ()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(());
        }
        let approx = approx_size(&msg);
        if try_reserve_queue_bytes(&self.queue_bytes, self.max_queue_bytes, approx).is_err() {
            tracing::warn!(
                peer = self.peer_id,
                "inbound media queue full — dropping frame"
            );
            return Err(());
        }
        self.tx.send(msg).await.map_err(|_| {
            self.queue_bytes.fetch_sub(
                approx.min(self.queue_bytes.load(Ordering::Relaxed)),
                Ordering::Relaxed,
            );
        })
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.close_notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn queue_byte_reservation_is_bounded() {
        let q = AtomicUsize::new(0);
        assert!(try_reserve_queue_bytes(&q, 100, 60).is_ok());
        assert!(try_reserve_queue_bytes(&q, 100, 60).is_err());
        assert!(try_reserve_queue_bytes(&q, 100, 40).is_ok());
        assert_eq!(q.load(Ordering::Relaxed), 100);
        let q = AtomicUsize::new(usize::MAX);
        assert!(try_reserve_queue_bytes(&q, usize::MAX, 1).is_err());
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
        write_media_frame(&mut s, &MediaMessage::AuthChallenge { nonce: vec![1; 16] })
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
        io
    }

    #[tokio::test]
    async fn accept_tls_then_auth_plaintext_membership_gate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(dial_and_hello(addr, None, 7));
        let (s, peer) = listener.accept().await.unwrap();
        let (id, _io) = accept_tls_then_auth(s, peer.ip(), SECRET, 1, None, allow_all())
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
        let (id, mut io) = accept_tls_then_auth(
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
            read_media_frame(&mut s).await.unwrap(),
            MediaMessage::Subscribe { .. }
        ));

        // Two frames back-to-back: the first is stamped with the peer id, the
        // second overflows the capacity-1 inbound queue and is dropped.
        write_media_frame(&mut s, &frame(3)).await.unwrap();
        write_media_frame(&mut s, &frame(4)).await.unwrap();
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
            read_media_frame(&mut s).await.unwrap(),
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
        let (id, mut io) =
            accept_tls_then_auth(s, ip.ip(), SECRET, 2, Some(server_cfg), allow_all())
                .await
                .unwrap();
        assert_eq!(id, 1);
        write_media_frame(&mut io, &frame(5)).await.unwrap();
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

        // Byte budget (1 MiB minimum) rejects a frame larger than the queue.
        assert!(peer.try_send(frame(2 * 1024 * 1024)).is_err());
        // Message-count bound (256) rejects the 257th small message and
        // returns its reserved bytes.
        for _ in 0..256 {
            peer.try_send(frame(1)).unwrap();
        }
        assert!(peer.try_send(frame(1)).is_err());
        assert_eq!(peer.queue_bytes.load(Ordering::Relaxed), 256 * 65);

        // Let at least one failed dial + backoff happen, then close.
        tokio::time::sleep(Duration::from_millis(50)).await;
        peer.close();
        assert!(peer.is_closed());
    }

    #[tokio::test]
    async fn inbound_sink_writes_in_order_and_enforces_limits() {
        let (w, mut r) = tokio::io::duplex(1 << 20);
        let sink = InboundMediaSink::spawn(5, 1, w);
        assert_eq!(sink.peer_id, 5);
        sink.try_send(MediaMessage::Unsubscribe {
            app: "live".into(),
            stream: "s".into(),
        })
        .unwrap();
        sink.send(frame(8)).await.unwrap();
        assert!(matches!(
            read_media_frame(&mut r).await.unwrap(),
            MediaMessage::Unsubscribe { .. }
        ));
        assert!(matches!(
            read_media_frame(&mut r).await.unwrap(),
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
        let eof = tokio::time::timeout(WAIT, read_media_frame(&mut r))
            .await
            .unwrap();
        assert!(eof.is_err());
    }

    #[tokio::test]
    async fn inbound_sink_closes_on_write_error_and_bounds_channel() {
        // A tiny, never-read pipe blocks the pump on its first write so the
        // 256-slot channel fills up.
        let (w, r) = tokio::io::duplex(16);
        let sink = InboundMediaSink::spawn(6, 64, w);
        let mut accepted = 0;
        for _ in 0..300 {
            if sink.try_send(frame(1)).is_ok() {
                accepted += 1;
            }
        }
        assert!(accepted < 300, "channel capacity must bound the queue");

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
        let sink = InboundMediaSink::spawn(7, 1, w);
        // Let the pump sit through at least one idle tick first.
        tokio::time::sleep(Duration::from_millis(150)).await;
        sink.closed.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(sink.is_closed());
    }
}
