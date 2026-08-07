//! Outbound/inbound media peer connection.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use rustls::{ClientConfig, ServerConfig};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::cluster::NodeId;
use crate::cluster::media::protocol::MediaMessage;
use crate::cluster::security::{auth_response, secrets_equal};

const MAX_FRAME: u32 = 32 * 1024 * 1024;
const MAX_AUTH_FRAME: u32 = 8 * 1024;
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

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
    read_media_frame_max(r, MAX_FRAME).await
}

async fn read_media_frame_max<R: AsyncReadExt + Unpin>(
    r: &mut R,
    max: u32,
) -> Result<MediaMessage, std::io::Error> {
    let len = r.read_u32().await?;
    if len > max {
        return Err(std::io::Error::other("media frame too large"));
    }
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
        inbound: mpsc::UnboundedSender<MediaMessage>,
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
        let cur = self.queue_bytes.load(Ordering::Relaxed);
        if cur.saturating_add(approx) > self.max_queue_bytes {
            tracing::warn!(peer = self.peer_id, "media queue full — resetting peer");
            self.closed.store(true, Ordering::Relaxed);
            return Err(());
        }
        self.queue_bytes.fetch_add(approx, Ordering::Relaxed);
        self.tx.try_send(msg).map_err(|_| {
            self.closed.store(true, Ordering::Relaxed);
        })
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
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
    _peer_id: NodeId,
    outbound: &mut mpsc::Receiver<MediaMessage>,
    inbound: &mpsc::UnboundedSender<MediaMessage>,
    queue_bytes: &AtomicUsize,
    _max_queue: usize,
    closed: &AtomicBool,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<ConnectEnd, std::io::Error> {
    let tcp = TcpStream::connect(addr).await?;
    let mut stream: Box<dyn MediaIo> = if let Some(cfg) = tls_client {
            let connector = TlsConnector::from(cfg);
            let host = addr.split(':').next().unwrap_or("localhost").to_string();
            let server_name = rustls::pki_types::ServerName::try_from(host)
                .map_err(|e| std::io::Error::other(e))?;
            let tls = connector.connect(server_name, tcp).await?;
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
                    let _ = inbound_c.send(msg);
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
                if write_media_frame(&mut wh, &msg).await.is_err() {
                    break;
                }
                queue_bytes.fetch_sub(size.min(queue_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
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
    read_closed.store(true, Ordering::Relaxed);
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
    let response = auth_response(secret, &nonce);
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
    secret: &str,
    local_id: NodeId,
) -> Result<NodeId, std::io::Error> {
    let nonce: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
    write_media_frame(stream, &MediaMessage::AuthChallenge { nonce: nonce.clone() }).await?;
    let auth = read_auth_media_frame(stream).await?;
    let MediaMessage::Auth { node_id, response } = auth else {
        write_media_frame(stream, &MediaMessage::AuthFail).await?;
        return Err(std::io::Error::other("expected AUTH"));
    };
    let expected = auth_response(secret, &nonce);
    if !secrets_equal(&expected, &response) {
        write_media_frame(stream, &MediaMessage::AuthFail).await?;
        return Err(std::io::Error::other("auth fail"));
    }
    let _ = local_id;
    write_media_frame(stream, &MediaMessage::AuthOk).await?;

    let hello = read_media_frame(stream).await?;
    let MediaMessage::Hello { version, node_id: hello_id } = hello else {
        return Err(std::io::Error::other("expected HELLO"));
    };
    if version != crate::cluster::media::MEDIA_PROTOCOL_VERSION {
        write_media_frame(
            stream,
            &MediaMessage::Error {
                code: "VERSION".into(),
                message: "unsupported media protocol".into(),
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
    secret: &str,
    local_id: NodeId,
    tls_server: Option<Arc<ServerConfig>>,
) -> Result<(NodeId, Box<dyn MediaIo>), std::io::Error> {
    let mut io: Box<dyn MediaIo> = if let Some(cfg) = tls_server {
        let acceptor = TlsAcceptor::from(cfg);
        Box::new(acceptor.accept(stream).await?)
    } else {
        Box::new(stream)
    };
    let peer_id = accept_auth(&mut io, secret, local_id).await?;
    Ok((peer_id, io))
}
