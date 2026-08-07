//! Outbound/inbound media peer connection.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::cluster::media::protocol::MediaMessage;
use crate::cluster::security::{auth_response, secrets_equal};
use crate::cluster::NodeId;

const MAX_FRAME: u32 = 32 * 1024 * 1024;

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
    let len = r.read_u32().await?;
    if len > MAX_FRAME {
        return Err(std::io::Error::other("media frame too large"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).map_err(std::io::Error::other)
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
    ) -> Self {
        let max_queue_bytes = (max_queue_mb as usize).saturating_mul(1024 * 1024).max(1024 * 1024);
        let (tx, mut rx) = mpsc::channel::<MediaMessage>(256);
        let queue_bytes = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let qb = Arc::clone(&queue_bytes);
        let cl = Arc::clone(&closed);
        let addr_c = addr.clone();

        tokio::spawn(async move {
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
                )
                .await
                {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::debug!(peer = peer_id, error = %e, "media peer reconnect");
                        tokio::time::sleep(Duration::from_millis(500)).await;
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
            // Backpressure: reset slow peer
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
) -> Result<(), std::io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    write_media_frame(
        &mut stream,
        &MediaMessage::Hello {
            version: crate::cluster::media::MEDIA_PROTOCOL_VERSION,
            node_id: local_id,
        },
    )
    .await?;
    let nonce: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
    let response = auth_response(secret, &nonce);
    write_media_frame(
        &mut stream,
        &MediaMessage::Auth { nonce, response },
    )
    .await?;
    match read_media_frame(&mut stream).await? {
        MediaMessage::AuthOk => {}
        _ => return Err(std::io::Error::other("media auth failed")),
    }

    let (mut rh, mut wh) = stream.into_split();
    let closed_r = closed.load(Ordering::Relaxed);
    let _ = closed_r;

    let inbound_c = inbound.clone();
    let read_closed = Arc::new(AtomicBool::new(false));
    let rc = Arc::clone(&read_closed);
    let reader = tokio::spawn(async move {
        while !rc.load(Ordering::Relaxed) {
            match read_media_frame(&mut rh).await {
                Ok(msg) => {
                    let _ = inbound_c.send(msg);
                }
                Err(_) => break,
            }
        }
    });

    while !closed.load(Ordering::Relaxed) {
        tokio::select! {
            msg = outbound.recv() => {
                let Some(msg) = msg else { break; };
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
    read_closed.store(true, Ordering::Relaxed);
    let _ = reader.await;
    let _ = BytesMut::new(); // keep bytes dep used
    Ok(())
}

/// Authenticate an inbound media connection (server side of HELLO/AUTH).
pub async fn accept_auth(
    stream: &mut TcpStream,
    secret: &str,
    local_id: NodeId,
) -> Result<NodeId, std::io::Error> {
    let hello = read_media_frame(stream).await?;
    let MediaMessage::Hello { version, node_id } = hello else {
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
    let auth = read_media_frame(stream).await?;
    let MediaMessage::Auth { nonce, response } = auth else {
        return Err(std::io::Error::other("expected AUTH"));
    };
    let expected = auth_response(secret, &nonce);
    if !secrets_equal(&expected, &response) {
        write_media_frame(stream, &MediaMessage::AuthFail).await?;
        return Err(std::io::Error::other("auth fail"));
    }
    let _ = local_id;
    write_media_frame(stream, &MediaMessage::AuthOk).await?;
    Ok(node_id)
}
