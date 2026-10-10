//! Length-prefixed JSON framing over TCP (optional mTLS) for Raft RPC + admin/join.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use openraft::BasicNode;
use openraft::error::{NetworkError, RPCError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;
use rustls::{ClientConfig, ServerConfig};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::cluster::raft::{NodeId, Raft, TypeConfig, typ};
use crate::cluster::security::{
    auth_nonce, auth_response, node_id_from_peer_certs, secrets_equal, verify_tls_node_identity,
};
use crate::cluster::state::ClusterMeta;

const MAX_FRAME: u32 = 64 * 1024 * 1024;
/// Largest accepted JSON-encoded snapshot frame. Snapshot chunks are sent in
/// `snapshot_max_chunk_size` (1 MiB) pieces, which inflate to a few MiB as a
/// JSON array of byte values. Capping the declared size keeps an
/// authenticated peer that advertises a snapshot from reserving a large slice
/// of the snapshot read budget for the whole snapshot timeout.
const MAX_SNAPSHOT_FRAME: u32 = 8 * 1024 * 1024;
/// Post-auth control-plane frames (Raft RPC, admin) — smaller than snapshot path.
const MAX_CONTROL_FRAME: u32 = 8 * 1024 * 1024;
/// Small control frames are bounded by the authenticated connection cap, so
/// keep them outside the shared byte budgets to preserve capacity for
/// heartbeats and other liveness traffic even while large reads are stalled.
const MAX_UNBUDGETED_CONTROL_FRAME: u32 = 256 * 1024;
/// Bound unauthenticated frames (challenge/auth) to limit DoS before AuthOk.
const MAX_AUTH_FRAME: u32 = 8 * 1024;
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(30);
const ACCEPT_ERROR_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Logs a failed control-plane `accept` and backs off before the listener
/// retries; a transient accept error must not stop Raft RPC handling.
async fn control_accept_backoff(e: std::io::Error) {
    tracing::warn!(error = %e, "cluster control accept failed; retrying");
    tokio::time::sleep(ACCEPT_ERROR_RETRY_DELAY).await;
}
/// Cap concurrent authenticated control-plane requests.
const MAX_CONTROL_CONN_INFLIGHT: usize = 512;
/// Cap aggregate resident memory for non-snapshot control reads above the
/// small-frame allowance.
const MAX_CONTROL_READ_BYTES_INFLIGHT: usize = 64 * 1024 * 1024;
static CONTROL_READ_BYTES_INFLIGHT: AtomicUsize = AtomicUsize::new(0);
/// Keep large snapshot reads on a separate budget so stalled snapshots cannot
/// consume the capacity required by ordinary Raft/control traffic.
const MAX_SNAPSHOT_READ_BYTES_INFLIGHT: usize = 192 * 1024 * 1024;
static SNAPSHOT_READ_BYTES_INFLIGHT: AtomicUsize = AtomicUsize::new(0);
/// Preserve a global cap on half-open TLS/auth handshakes before spawning work.
const MAX_PREAUTH_CONN_INFLIGHT: usize = 512;
/// Cap half-open auth handshakes per source IP so one source cannot consume
/// the entire global pre-authentication budget.
const MAX_PREAUTH_CONN_PER_IP: usize = 16;
static CONTROL_CONN_INFLIGHT: AtomicUsize = AtomicUsize::new(0);
static PREAUTH_CONN_INFLIGHT: AtomicUsize = AtomicUsize::new(0);
static PREAUTH_CONN_PER_IP: Mutex<BTreeMap<IpAddr, usize>> = Mutex::new(BTreeMap::new());

fn try_acquire_global_preauth_slot() -> bool {
    if PREAUTH_CONN_INFLIGHT.fetch_add(1, Ordering::AcqRel) >= MAX_PREAUTH_CONN_INFLIGHT {
        PREAUTH_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
        return false;
    }
    true
}

fn release_global_preauth_slot() {
    PREAUTH_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
}

fn try_acquire_preauth_slot(peer: IpAddr) -> bool {
    let mut guard = PREAUTH_CONN_PER_IP.lock();
    let count = guard.entry(peer).or_insert(0);
    if *count >= MAX_PREAUTH_CONN_PER_IP {
        return false;
    }
    *count += 1;
    true
}

fn release_preauth_slot(peer: IpAddr) {
    let mut guard = PREAUTH_CONN_PER_IP.lock();
    if let Some(count) = guard.get_mut(&peer) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            guard.remove(&peer);
        }
    }
}
/// Bounds an entire authenticated client round trip (connect + TLS + auth +
/// write + response read). Callers like `ClusterManager::block_on_write`
/// invoke this synchronously from the RTMP poll thread when forwarding a
/// write to the leader, so an unbounded call here can stall every
/// connection's poll for as long as the OS TCP timeout.
const ROUNDTRIP_TIMEOUT: Duration = Duration::from_secs(8);
/// Snapshots may be tens of MiB; the synchronous control-write bound is too
/// tight for serialize + transfer + install on a slow link.
pub(crate) const SNAPSHOT_ROUNDTRIP_TIMEOUT: Duration = Duration::from_secs(120);

/// Combined IO trait so we can box plain TCP or rustls streams.
trait ClusterIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ClusterIo for T {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinPeerInfo {
    pub node_id: NodeId,
    pub control_addr: String,
    pub media_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    /// Server-issued anti-replay nonce; first frame on a new connection.
    AuthChallenge {
        nonce: Vec<u8>,
    },
    Auth {
        node_id: NodeId,
        response: String,
    },
    AuthOk {
        node_id: NodeId,
    },
    AuthFail,
    /// JSON-encoded `AppendEntriesRequest<TypeConfig>`.
    RaftAppend(serde_json::Value),
    RaftAppendResp(serde_json::Value),
    /// JSON-encoded `VoteRequest`.
    RaftVote(serde_json::Value),
    RaftVoteResp(serde_json::Value),
    /// JSON-encoded `InstallSnapshotRequest<TypeConfig>`.
    RaftSnapshot(serde_json::Value),
    RaftSnapshotResp(serde_json::Value),
    JoinRequest {
        node_id: NodeId,
        control_addr: String,
        media_addr: String,
        /// HTTP-API `admin_proof` over [`join_admin_proof_payload`].
        #[serde(default)]
        proof: String,
    },
    JoinResponse {
        ok: bool,
        message: String,
        #[serde(default)]
        cluster_id: String,
        #[serde(default)]
        peers: Vec<JoinPeerInfo>,
    },
    AdminDrain {
        node_id: NodeId,
        /// HTTP-API `admin_proof` over `AdminDrain:{node_id}`.
        #[serde(default)]
        proof: String,
    },
    AdminResume {
        node_id: NodeId,
        /// HTTP-API `admin_proof` over `AdminResume:{node_id}`.
        #[serde(default)]
        proof: String,
    },
    AdminRemove {
        node_id: NodeId,
    },
    AdminOk,
    AdminErr {
        message: String,
    },
    /// Kick live RTMP sessions for a stream being deleted (every node).
    DrainStream {
        stream_id: String,
    },
    /// Revoke a viewer play key on every RTMP bridge.
    RevokeViewer {
        viewer_id: String,
    },
    /// Query how many live RTMP sessions reference `stream_id` on this node.
    SessionCountReq {
        stream_id: String,
    },
    SessionCountResp {
        count: u64,
    },
    /// Idempotent topology refresh (no membership change).
    TopologyReq,
    TopologyResp {
        ok: bool,
        message: String,
        #[serde(default)]
        cluster_id: String,
        #[serde(default)]
        peers: Vec<JoinPeerInfo>,
    },
    StatsProxyReq {
        stream_id: String,
    },
    StatsProxyResp {
        body: serde_json::Value,
    },
    Heartbeat {
        node_id: NodeId,
        health: String,
        load: f64,
        #[serde(default)]
        control_addr: String,
        #[serde(default)]
        media_addr: String,
        #[serde(default)]
        publishers: u64,
        #[serde(default)]
        players: u64,
        /// Per-stream active player counts `(stream_id, count)`.
        #[serde(default)]
        stream_players: Vec<(String, u64)>,
        /// Per-viewer (play-key) active player counts `(viewer_id, count)`.
        #[serde(default)]
        viewer_players: Vec<(String, u64)>,
    },
    /// Follower-forwarded durable mutation (`ClusterCommand` as JSON).
    ClientWrite {
        req: serde_json::Value,
        #[serde(default)]
        proof: String,
    },
    /// `{"ok":true,"data":...}` or `{"ok":false,"error":"..."}`.
    ClientWriteResp(serde_json::Value),
    /// Follower-forwarded OpenRaft membership change (JSON `ChangeMembers`).
    ChangeMembership {
        req: serde_json::Value,
        proof: String,
    },
    ChangeMembershipResp {
        ok: bool,
        message: String,
    },
}

pub async fn write_frame<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    msg: &ControlMessage,
) -> Result<(), std::io::Error> {
    let bytes = serde_json::to_vec(msg).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_FRAME as usize {
        return Err(std::io::Error::other("frame too large"));
    }
    w.write_u32(bytes.len() as u32).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> Result<
    (
        ControlMessage,
        Option<crate::cluster::security::InflightByteBudgetGuard>,
    ),
    std::io::Error,
> {
    // Client-side response reader: a peer may legally write a response up to
    // MAX_FRAME (e.g. a large StatsProxyResp), so allow large non-snapshot
    // frames here. Budgeting is still applied inside the shared reader, and the
    // guard is returned so the caller keeps it alive until the decoded payload
    // has been consumed instead of releasing the budget at decode time.
    read_budgeted_frame(r, true).await
}

async fn read_control_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> Result<
    (
        ControlMessage,
        Option<crate::cluster::security::InflightByteBudgetGuard>,
    ),
    std::io::Error,
> {
    read_budgeted_frame(r, false).await
}

/// Classify a peeked frame prefix as a Raft snapshot.
///
/// Returns `Some(true)` only when the `{"RaftSnapshot` tag is positively
/// identified, `Some(false)` when the peek positively identifies a different
/// variant, and `None` when the peek is inconclusive — the prefix is all
/// whitespace, or the tag could straddle the peek window. An inconclusive peek
/// must never be treated as proof of a snapshot: `read_frame` legitimately
/// accepts large non-snapshot responses (`StatsProxyResp`) that only
/// `allow_large_non_snapshot` admits, so a guess would reject valid traffic.
fn classify_snapshot_prefix(prefix: &[u8]) -> Option<bool> {
    const SNAPSHOT_TAG: &[u8] = b"{\"RaftSnapshot";
    match prefix.iter().position(|b| !b.is_ascii_whitespace()) {
        Some(i) if i + SNAPSHOT_TAG.len() <= prefix.len() => {
            Some(prefix[i..].starts_with(SNAPSHOT_TAG))
        }
        _ => None,
    }
}

/// Read one length-prefixed JSON control frame and reserve it against the
/// matching in-flight byte budget.
///
/// The budget class is chosen by peeking the JSON variant prefix, not the
/// attacker-declared length: serde externally-tagged enums serialize as
/// `{"Variant":...}`, so a non-snapshot frame cannot charge the shared
/// snapshot budget merely by advertising a snapshot-sized length.
/// `allow_large_non_snapshot` permits response types (e.g. `StatsProxyResp`)
/// that may legally exceed `MAX_CONTROL_FRAME` up to `MAX_FRAME`.
async fn read_budgeted_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
    allow_large_non_snapshot: bool,
) -> Result<
    (
        ControlMessage,
        Option<crate::cluster::security::InflightByteBudgetGuard>,
    ),
    std::io::Error,
> {
    // Authenticated traffic may carry RaftSnapshot payloads up to MAX_FRAME;
    // non-snapshot control messages stay capped at MAX_CONTROL_FRAME after
    // decode so a peer cannot inflate ordinary RPCs to snapshot size.
    //
    // Header and variant prefix are bounded by the standard control timeout;
    // the (potentially multi-megabyte) snapshot body instead gets the
    // snapshot round-trip timeout, matching what OpenRaft and the sending
    // transport already allow for a chunk transfer. A fixed 30 s body
    // timeout would abort chunks the sender is still allowed to deliver.
    const PEEK: usize = 32;
    let (len, prefix, prefix_len) = tokio::time::timeout(CONTROL_READ_TIMEOUT, async {
        let len = r.read_u32().await?;
        if len > MAX_FRAME {
            return Err(std::io::Error::other("frame too large"));
        }
        // Peek the variant tag before allocating/reserving so the budget
        // class matches the actual message type. serde externally-tagged
        // enums serialize as `{"Variant":...}`, so a non-snapshot frame
        // cannot charge the shared snapshot budget merely by advertising a
        // snapshot-sized length. serde_json skips leading JSON whitespace
        // before dispatching on the variant tag, so trim it first or a
        // pretty-printed frame is misclassified as a control.
        let mut prefix = [0u8; PEEK];
        let prefix_len = (len as usize).min(PEEK);
        r.read_exact(&mut prefix[..prefix_len]).await?;
        // Early reject only on a positively identified snapshot tag: an
        // inconclusive peek proves nothing, and `read_frame` accepts large
        // non-snapshot responses that `allow_large_non_snapshot` permits.
        // The authoritative snapshot cap is re-checked after decode below,
        // where the variant is known for certain.
        if classify_snapshot_prefix(&prefix[..prefix_len]) == Some(true) && len > MAX_SNAPSHOT_FRAME
        {
            return Err(std::io::Error::other("snapshot frame too large"));
        }
        Ok::<_, std::io::Error>((len, prefix, prefix_len))
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "control read timeout"))??;

    // `write_frame` emits compact JSON with no leading whitespace, so every
    // frame this codebase produces classifies conclusively here. An
    // inconclusive peek therefore means a hand-crafted padded frame, and the
    // safe direction is the *stricter* control budget: a padded snapshot must
    // not be able to reach the larger snapshot allowance. The snapshot cap
    // itself is enforced authoritatively after decode, below.
    let snapshot_prefix = classify_snapshot_prefix(&prefix[..prefix_len]) == Some(true);

    let read_budget = if len <= MAX_UNBUDGETED_CONTROL_FRAME {
        None
    } else {
        let (counter, max, error) = if snapshot_prefix {
            (
                &SNAPSHOT_READ_BYTES_INFLIGHT,
                MAX_SNAPSHOT_READ_BYTES_INFLIGHT,
                "snapshot read memory budget exceeded",
            )
        } else {
            (
                &CONTROL_READ_BYTES_INFLIGHT,
                MAX_CONTROL_READ_BYTES_INFLIGHT,
                "control read memory budget exceeded",
            )
        };
        Some(
            crate::cluster::security::try_reserve_inflight_bytes(counter, max, len as usize)
                .map_err(|_| std::io::Error::other(error))?,
        )
    };

    let body_timeout = if snapshot_prefix {
        SNAPSHOT_ROUNDTRIP_TIMEOUT
    } else {
        CONTROL_READ_TIMEOUT
    };
    tokio::time::timeout(body_timeout, async {
        let mut buf = Vec::with_capacity(len as usize);
        buf.extend_from_slice(&prefix[..prefix_len]);
        if (len as usize) > prefix_len {
            buf.resize(len as usize, 0);
            r.read_exact(&mut buf[prefix_len..]).await?;
        }
        let msg: ControlMessage = serde_json::from_slice(&buf).map_err(std::io::Error::other)?;
        let is_snapshot = matches!(
            msg,
            ControlMessage::RaftSnapshot(_) | ControlMessage::RaftSnapshotResp(_)
        );
        // Authoritative snapshot cap, applied now that the variant is known.
        // The pre-decode peek can be inconclusive (a padded tag that straddles
        // the window), so this is what actually bounds a snapshot frame at
        // MAX_SNAPSHOT_FRAME regardless of how the variant was spelled.
        if is_snapshot && len > MAX_SNAPSHOT_FRAME {
            return Err(std::io::Error::other("snapshot frame too large"));
        }
        if !is_snapshot && !allow_large_non_snapshot && len > MAX_CONTROL_FRAME {
            return Err(std::io::Error::other("frame too large"));
        }
        Ok((msg, read_budget))
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "control read timeout"))?
}

async fn read_frame_max<R: AsyncReadExt + Unpin>(
    r: &mut R,
    max: u32,
) -> Result<ControlMessage, std::io::Error> {
    let len = r.read_u32().await?;
    if len > max {
        return Err(std::io::Error::other("frame too large"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).map_err(std::io::Error::other)
}

async fn read_auth_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> Result<ControlMessage, std::io::Error> {
    tokio::time::timeout(AUTH_TIMEOUT, read_frame_max(r, MAX_AUTH_FRAME))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "auth timeout"))?
}

pub struct NetworkFactory {
    pub nodes: Arc<RwLock<BTreeMap<NodeId, BasicNode>>>,
    pub secret: String,
    pub local_id: NodeId,
    pub tls_client: Option<Arc<ClientConfig>>,
}

impl NetworkFactory {
    pub fn new(local_id: NodeId, secret: String, tls_client: Option<Arc<ClientConfig>>) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            secret,
            local_id,
            tls_client,
        }
    }

    pub fn upsert_node(&self, id: NodeId, addr: String) {
        self.nodes.write().insert(id, BasicNode { addr });
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            target,
            target_node: node.clone(),
            secret: self.secret.clone(),
            local_id: self.local_id,
            tls_client: self.tls_client.clone(),
        }
    }
}

pub struct NetworkConnection {
    target: NodeId,
    target_node: BasicNode,
    secret: String,
    local_id: NodeId,
    tls_client: Option<Arc<ClientConfig>>,
}

impl NetworkConnection {
    /// One authenticated control round trip. `hard_deadline` is the caller's
    /// own bound on top of the transport's message-type default (e.g. the
    /// openraft `RPCOption::hard_ttl()` for snapshot chunks), so an RPC never
    /// outlives the deadline openraft will cancel it at.
    async fn roundtrip(
        &mut self,
        req: ControlMessage,
        hard_deadline: Option<Duration>,
    ) -> Result<ControlMessage, RPCError<NodeId, BasicNode, typ::RaftError>> {
        let addr = &self.target_node.addr;
        let call = authed_roundtrip_inner(
            addr,
            &self.secret,
            self.local_id,
            self.tls_client.clone(),
            req,
        );
        let result = match hard_deadline {
            Some(t) => tokio::time::timeout(t, call)
                .await
                .unwrap_or_else(|_| Err(format!("control round trip to {addr} timed out"))),
            None => call.await,
        };
        result.map(|(msg, _read_budget)| msg).map_err(|e| {
            if e.contains("connect") || e.contains("tcp") {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::other(e)))
            } else {
                RPCError::Network(NetworkError::new(&std::io::Error::other(e)))
            }
        })
    }
}

/// Remap transport-level `RPCError` to a different Raft error payload type.
fn map_rpc_transport_err<E: std::error::Error>(
    e: RPCError<NodeId, BasicNode, typ::RaftError>,
) -> RPCError<NodeId, BasicNode, typ::RaftError<E>> {
    match e {
        RPCError::Timeout(x) => RPCError::Timeout(x),
        RPCError::Unreachable(x) => RPCError::Unreachable(x),
        RPCError::Network(x) => RPCError::Network(x),
        RPCError::PayloadTooLarge(x) => RPCError::PayloadTooLarge(x),
        RPCError::RemoteError(r) => {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!("{r}"))))
        }
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, typ::RPCError> {
        let req =
            serde_json::to_value(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        match self
            .roundtrip(ControlMessage::RaftAppend(req), None)
            .await?
        {
            ControlMessage::RaftAppendResp(v) => {
                let parsed: Result<AppendEntriesResponse<NodeId>, typ::RaftError> =
                    serde_json::from_value(v)
                        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
                match parsed {
                    Ok(r) => Ok(r),
                    Err(e) => Err(RPCError::RemoteError(RemoteError::new(self.target, e))),
                }
            }
            _ => Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other("unexpected response"),
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, typ::RPCError<openraft::error::InstallSnapshotError>>
    {
        let req =
            serde_json::to_value(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let msg = self
            .roundtrip(ControlMessage::RaftSnapshot(req), Some(option.hard_ttl()))
            .await
            .map_err(map_rpc_transport_err)?;
        match msg {
            ControlMessage::RaftSnapshotResp(v) => {
                let parsed: Result<
                    InstallSnapshotResponse<NodeId>,
                    typ::RaftError<openraft::error::InstallSnapshotError>,
                > = serde_json::from_value(v)
                    .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
                match parsed {
                    Ok(r) => Ok(r),
                    Err(e) => Err(RPCError::RemoteError(RemoteError::new(self.target, e))),
                }
            }
            _ => Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other("unexpected response"),
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, typ::RPCError> {
        let req =
            serde_json::to_value(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        match self.roundtrip(ControlMessage::RaftVote(req), None).await? {
            ControlMessage::RaftVoteResp(v) => {
                let parsed: Result<VoteResponse<NodeId>, typ::RaftError> =
                    serde_json::from_value(v)
                        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
                match parsed {
                    Ok(r) => Ok(r),
                    Err(e) => Err(RPCError::RemoteError(RemoteError::new(self.target, e))),
                }
            }
            _ => Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other("unexpected response"),
            ))),
        }
    }
}

/// Result of accepting a join request (topology for the joiner).
pub type JoinAcceptFn = Arc<
    dyn Fn(NodeId, String, String, String) -> Result<(String, Vec<JoinPeerInfo>), String>
        + Send
        + Sync,
>;

/// Returns true when `node_id` is in the current Raft membership.
pub type MembershipFn = Arc<dyn Fn(NodeId) -> bool + Send + Sync>;

/// Verifies an HTTP-API-signed membership change (`admin_proof`).
pub type AdminProofFn = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// Leader-forward context for inter-node `ClientWrite` on followers.
#[derive(Clone)]
pub struct ClientWriteForwardCtx {
    pub secret: String,
    pub local_id: NodeId,
    pub meta: Arc<ClusterMeta>,
    pub tls_client: Option<Arc<ClientConfig>>,
}

/// Peer heartbeat payload (addrs + session counts for aggregation).
#[derive(Debug, Clone)]
pub struct HeartbeatInfo {
    pub node_id: NodeId,
    pub health: String,
    pub load: f64,
    pub control_addr: String,
    pub media_addr: String,
    pub publishers: u64,
    pub players: u64,
    pub stream_players: Vec<(String, u64)>,
    pub viewer_players: Vec<(String, u64)>,
}

/// Accept control-plane connections and dispatch Raft / admin messages.
pub async fn serve_control_plane(
    bind: SocketAddr,
    secret: String,
    local_id: NodeId,
    raft: Raft,
    on_join: JoinAcceptFn,
    on_heartbeat: Arc<dyn Fn(HeartbeatInfo) + Send + Sync>,
    on_admin: Arc<dyn Fn(ControlMessage) -> ControlMessage + Send + Sync>,
    on_stats: Arc<dyn Fn(String) -> serde_json::Value + Send + Sync>,
    is_member: MembershipFn,
    verify_admin_proof: AdminProofFn,
    write_forward: Option<ClientWriteForwardCtx>,
    tls_server: Option<Arc<ServerConfig>>,
) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(bind).await?;
    serve_control_plane_listener(
        listener,
        secret,
        local_id,
        raft,
        on_join,
        on_heartbeat,
        on_admin,
        on_stats,
        is_member,
        verify_admin_proof,
        write_forward,
        tls_server,
    )
    .await
}

/// Serve on an already-bound listener (bind-before-spawn startup).
pub async fn serve_control_plane_listener(
    listener: TcpListener,
    secret: String,
    local_id: NodeId,
    raft: Raft,
    on_join: JoinAcceptFn,
    on_heartbeat: Arc<dyn Fn(HeartbeatInfo) + Send + Sync>,
    on_admin: Arc<dyn Fn(ControlMessage) -> ControlMessage + Send + Sync>,
    on_stats: Arc<dyn Fn(String) -> serde_json::Value + Send + Sync>,
    is_member: MembershipFn,
    verify_admin_proof: AdminProofFn,
    write_forward: Option<ClientWriteForwardCtx>,
    tls_server: Option<Arc<ServerConfig>>,
) -> Result<(), std::io::Error> {
    let acceptor = tls_server.map(TlsAcceptor::from);
    let tls_required = acceptor.is_some();
    let bind = listener
        .local_addr()
        .unwrap_or_else(|_| "0.0.0.0:0".parse().expect("static bind parse"));
    tracing::info!(%bind, tls = tls_required, "cluster control plane listening");
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                control_accept_backoff(e).await;
                continue;
            }
        };
        if !try_acquire_global_preauth_slot() {
            tracing::debug!(%peer, "control connection rejected: global preauth limit");
            continue;
        }
        if !try_acquire_preauth_slot(peer.ip()) {
            release_global_preauth_slot();
            tracing::debug!(%peer, "control connection rejected: preauth per-ip limit");
            continue;
        }
        let secret = secret.clone();
        let raft = raft.clone();
        let on_join = Arc::clone(&on_join);
        let on_heartbeat = Arc::clone(&on_heartbeat);
        let on_admin = Arc::clone(&on_admin);
        let on_stats = Arc::clone(&on_stats);
        let is_member = Arc::clone(&is_member);
        let verify_admin_proof = Arc::clone(&verify_admin_proof);
        let write_forward = write_forward.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            struct PreauthGuard(IpAddr);
            impl Drop for PreauthGuard {
                fn drop(&mut self) {
                    release_preauth_slot(self.0);
                    release_global_preauth_slot();
                }
            }
            let preauth = PreauthGuard(peer.ip());
            struct InflightGuard;
            impl Drop for InflightGuard {
                fn drop(&mut self) {
                    CONTROL_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
                }
            }
            let result = async {
                if let Some(ref acc) = acceptor {
                    let tls = tokio::time::timeout(AUTH_TIMEOUT, acc.accept(stream))
                        .await
                        .map_err(|_| {
                            std::io::Error::new(std::io::ErrorKind::TimedOut, "tls accept timeout")
                        })??;
                    let cert_node_id = tls
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(node_id_from_peer_certs);
                    let mut tls_stream = tls;
                    let peer_id = server_auth_handshake(
                        &mut tls_stream,
                        peer.ip(),
                        &secret,
                        local_id,
                        tls_required,
                        cert_node_id,
                    )
                    .await?;
                    drop(preauth);
                    if CONTROL_CONN_INFLIGHT.fetch_add(1, Ordering::AcqRel)
                        >= MAX_CONTROL_CONN_INFLIGHT
                    {
                        CONTROL_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
                        return Err(std::io::Error::other(
                            "control connection rejected: inflight limit",
                        ));
                    }
                    let _inflight = InflightGuard;
                    handle_authenticated_control_conn(
                        &mut tls_stream,
                        peer_id,
                        local_id,
                        raft,
                        on_join,
                        on_heartbeat,
                        on_admin,
                        on_stats,
                        is_member,
                        verify_admin_proof,
                        write_forward,
                    )
                    .await
                } else {
                    let mut tcp = stream;
                    let peer_id = server_auth_handshake(
                        &mut tcp,
                        peer.ip(),
                        &secret,
                        local_id,
                        tls_required,
                        None,
                    )
                    .await?;
                    drop(preauth);
                    if CONTROL_CONN_INFLIGHT.fetch_add(1, Ordering::AcqRel)
                        >= MAX_CONTROL_CONN_INFLIGHT
                    {
                        CONTROL_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
                        return Err(std::io::Error::other(
                            "control connection rejected: inflight limit",
                        ));
                    }
                    let _inflight = InflightGuard;
                    handle_authenticated_control_conn(
                        &mut tcp,
                        peer_id,
                        local_id,
                        raft,
                        on_join,
                        on_heartbeat,
                        on_admin,
                        on_stats,
                        is_member,
                        verify_admin_proof,
                        write_forward,
                    )
                    .await
                }
            }
            .await;
            if let Err(e) = result {
                tracing::debug!(%peer, error=%e, "control connection closed");
            }
        });
    }
}

async fn server_auth_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    peer: IpAddr,
    secret: &str,
    local_id: NodeId,
    tls_required: bool,
    cert_node_id: Option<u64>,
) -> Result<NodeId, std::io::Error> {
    if crate::cluster::security::cluster_auth_rate_limited(peer) {
        write_frame(stream, &ControlMessage::AuthFail).await?;
        return Err(std::io::Error::other("auth rate limited"));
    }
    let nonce = auth_nonce();
    write_frame(
        stream,
        &ControlMessage::AuthChallenge {
            nonce: nonce.clone(),
        },
    )
    .await?;
    let auth = read_auth_frame(stream).await?;
    let ControlMessage::Auth { node_id, response } = auth else {
        crate::cluster::security::record_cluster_auth_failure(peer);
        write_frame(stream, &ControlMessage::AuthFail).await?;
        return Err(std::io::Error::other("expected Auth"));
    };
    let expected = auth_response(secret, node_id, &nonce);
    if !secrets_equal(&expected, &response) {
        crate::cluster::security::record_cluster_auth_failure(peer);
        write_frame(stream, &ControlMessage::AuthFail).await?;
        return Err(std::io::Error::other("auth fail"));
    }
    verify_tls_node_identity(tls_required, cert_node_id, node_id)?;
    crate::cluster::security::clear_cluster_auth_failures(peer);
    write_frame(stream, &ControlMessage::AuthOk { node_id: local_id }).await?;
    Ok(node_id)
}

async fn client_auth_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    secret: &str,
    local_id: NodeId,
) -> Result<(), std::io::Error> {
    let challenge = read_auth_frame(stream).await?;
    let ControlMessage::AuthChallenge { nonce } = challenge else {
        return Err(std::io::Error::other("expected AuthChallenge"));
    };
    let response = auth_response(secret, local_id, &nonce);
    write_frame(
        stream,
        &ControlMessage::Auth {
            node_id: local_id,
            response,
        },
    )
    .await?;
    match read_auth_frame(stream).await? {
        ControlMessage::AuthOk { .. } => Ok(()),
        _ => Err(std::io::Error::other("auth failed")),
    }
}

async fn handle_authenticated_control_conn<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    peer_id: NodeId,
    local_id: NodeId,
    raft: Raft,
    on_join: JoinAcceptFn,
    on_heartbeat: Arc<dyn Fn(HeartbeatInfo) + Send + Sync>,
    on_admin: Arc<dyn Fn(ControlMessage) -> ControlMessage + Send + Sync>,
    on_stats: Arc<dyn Fn(String) -> serde_json::Value + Send + Sync>,
    is_member: MembershipFn,
    verify_admin_proof: AdminProofFn,
    write_forward: Option<ClientWriteForwardCtx>,
) -> Result<(), std::io::Error> {
    let _ = local_id;
    // Keep the byte-budget guard alive for the entire request handling path,
    // including Raft snapshot installation, so decoded payloads cannot escape
    // aggregate accounting merely because the socket read has completed.
    let (msg, _read_budget) = read_control_frame(stream).await?;
    let resp = match msg {
        ControlMessage::RaftAppend(req) => {
            // No `is_member` gate here: the auth handshake above (shared
            // secret, plus per-node mTLS identity when enabled) is already
            // the trust boundary for raft transport RPCs. A learner freshly
            // added via add_learner cannot be in its own local membership
            // view yet — that view is only populated by processing the log
            // entries carried in exactly these RPCs — so gating on
            // `is_member` here would deadlock every new node's bootstrap.
            let parsed: AppendEntriesRequest<TypeConfig> =
                serde_json::from_value(req).map_err(std::io::Error::other)?;
            let r = raft.append_entries(parsed).await;
            let v =
                serde_json::to_value(&r).unwrap_or_else(|_| serde_json::json!({"error":"encode"}));
            ControlMessage::RaftAppendResp(v)
        }
        ControlMessage::RaftVote(req) => {
            // See RaftAppend above: no is_member gate, same bootstrap reason.
            let parsed: VoteRequest<NodeId> =
                serde_json::from_value(req).map_err(std::io::Error::other)?;
            let r = raft.vote(parsed).await;
            let v =
                serde_json::to_value(&r).unwrap_or_else(|_| serde_json::json!({"error":"encode"}));
            ControlMessage::RaftVoteResp(v)
        }
        ControlMessage::RaftSnapshot(req) => {
            // See RaftAppend above: no is_member gate, same bootstrap reason.
            let parsed: InstallSnapshotRequest<TypeConfig> =
                serde_json::from_value(req).map_err(std::io::Error::other)?;
            let r = raft.install_snapshot(parsed).await;
            let v =
                serde_json::to_value(&r).unwrap_or_else(|_| serde_json::json!({"error":"encode"}));
            ControlMessage::RaftSnapshotResp(v)
        }
        ControlMessage::JoinRequest {
            node_id,
            control_addr,
            media_addr,
            proof,
        } => {
            // Direct joins must authenticate as the joining node. Existing
            // members may proxy a JoinRequest for a fresh learner (follower
            // forwarding to the leader after ForwardToLeader).
            if node_id != peer_id && !is_member(peer_id) {
                return Err(std::io::Error::other(
                    "join node_id must match authenticated peer",
                ));
            }
            let payload = crate::cluster::security::join_admin_proof_payload(
                node_id,
                &control_addr,
                &media_addr,
            );
            if !verify_admin_proof(&proof, &payload) {
                return Err(std::io::Error::other("invalid join admin proof"));
            }
            match on_join(node_id, control_addr, media_addr, proof) {
                Ok((cluster_id, peers)) => ControlMessage::JoinResponse {
                    ok: true,
                    message: "joined as learner".into(),
                    cluster_id,
                    peers,
                },
                Err(message) => ControlMessage::JoinResponse {
                    ok: false,
                    message,
                    cluster_id: String::new(),
                    peers: Vec::new(),
                },
            }
        }
        ControlMessage::TopologyReq => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            on_admin(ControlMessage::TopologyReq)
        }
        ControlMessage::Heartbeat {
            node_id,
            health,
            load,
            control_addr,
            media_addr,
            publishers,
            players,
            stream_players,
            viewer_players,
        } => {
            // Bind heartbeats to the authenticated peer — a secret-holder must
            // not rewrite another member's addrs by spoofing node_id.
            if node_id != peer_id || !is_member(peer_id) {
                return Err(std::io::Error::other("heartbeat identity rejected"));
            }
            on_heartbeat(HeartbeatInfo {
                node_id: peer_id,
                health,
                load,
                control_addr,
                media_addr,
                publishers,
                players,
                stream_players,
                viewer_players,
            });
            ControlMessage::AdminOk
        }
        ControlMessage::StatsProxyReq { stream_id } => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            ControlMessage::StatsProxyResp {
                body: on_stats(stream_id),
            }
        }
        ControlMessage::ClientWrite { req, proof } => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            use crate::cluster::command::ClusterCommand;
            let cmd: ClusterCommand =
                serde_json::from_value(req.clone()).map_err(std::io::Error::other)?;
            // Every ClientWrite must carry an HTTP-API admin_proof (see
            // ClusterCommand::requires_admin_proof).
            if cmd.requires_admin_proof() {
                let req_str = serde_json::to_string(&req)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                if !verify_admin_proof(&proof, &req_str) {
                    return Err(std::io::Error::other("invalid client write admin proof"));
                }
            }
            use openraft::error::{ClientWriteError, RaftError};
            let body = match raft.client_write(cmd.clone()).await {
                Ok(resp) => serde_json::json!({
                    "ok": true,
                    "data": resp.data,
                }),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ftl))) => {
                    let Some(ctx) = write_forward.as_ref() else {
                        return Err(std::io::Error::other(
                            "client write forward unavailable on this node",
                        ));
                    };
                    let leader_addr =
                        ftl.leader_node
                            .as_ref()
                            .map(|n| n.addr.clone())
                            .or_else(|| {
                                ftl.leader_id
                                    .and_then(|id| ctx.meta.get(id).map(|(ctrl, _)| ctrl))
                            });
                    match leader_addr {
                        Some(addr) => match send_client_write(
                            &addr,
                            &ctx.secret,
                            ctx.local_id,
                            cmd,
                            proof,
                            ctx.tls_client.clone(),
                        )
                        .await
                        {
                            Ok(data) => serde_json::json!({
                                "ok": true,
                                "data": data,
                            }),
                            Err(e) => serde_json::json!({
                                "ok": false,
                                "error": e,
                            }),
                        },
                        None => serde_json::json!({
                            "ok": false,
                            "error": "no leader available to forward write",
                        }),
                    }
                }
                Err(e) => serde_json::json!({
                    "ok": false,
                    "error": e.to_string(),
                }),
            };
            ControlMessage::ClientWriteResp(body)
        }
        ControlMessage::ChangeMembership { req, proof } => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            let req_str =
                serde_json::to_string(&req).map_err(|e| std::io::Error::other(e.to_string()))?;
            if !verify_admin_proof(&proof, &req_str) {
                return Err(std::io::Error::other("invalid membership admin proof"));
            }
            use openraft::ChangeMembers;
            let change: ChangeMembers<NodeId, BasicNode> =
                serde_json::from_value(req).map_err(std::io::Error::other)?;
            match raft.change_membership(change, false).await {
                Ok(_) => ControlMessage::ChangeMembershipResp {
                    ok: true,
                    message: "ok".into(),
                },
                Err(e) => ControlMessage::ChangeMembershipResp {
                    ok: false,
                    message: e.to_string(),
                },
            }
        }
        admin @ (ControlMessage::AdminDrain { .. }
        | ControlMessage::AdminResume { .. }
        | ControlMessage::AdminRemove { .. }
        | ControlMessage::DrainStream { .. }
        | ControlMessage::RevokeViewer { .. }
        | ControlMessage::SessionCountReq { .. }) => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            if let ControlMessage::AdminDrain { node_id, proof } = &admin {
                let payload = format!("AdminDrain:{node_id}");
                if !verify_admin_proof(proof, &payload) {
                    return Err(std::io::Error::other("invalid admin drain proof"));
                }
            }
            if let ControlMessage::AdminResume { node_id, proof } = &admin {
                let payload = format!("AdminResume:{node_id}");
                if !verify_admin_proof(proof, &payload) {
                    return Err(std::io::Error::other("invalid admin resume proof"));
                }
            }
            on_admin(admin)
        }
        other => {
            let _ = other;
            ControlMessage::AdminErr {
                message: "unsupported".into(),
            }
        }
    };
    write_frame(stream, &resp).await?;
    Ok(())
}

/// Extract TLS server name (host/IP without brackets or port) from an authority.
pub fn tls_server_name_from_addr(
    addr: &str,
) -> Result<rustls::pki_types::ServerName<'static>, String> {
    let host = if let Ok(sock) = addr.parse::<SocketAddr>() {
        match sock.ip() {
            std::net::IpAddr::V4(v4) => v4.to_string(),
            std::net::IpAddr::V6(v6) => v6.to_string(),
        }
    } else if let Some(rest) = addr.strip_prefix('[') {
        // [ipv6]:port
        rest.split(']').next().unwrap_or("localhost").to_string()
    } else {
        addr.rsplit_once(':')
            .map(|(h, _)| h.to_string())
            .unwrap_or_else(|| addr.to_string())
    };
    rustls::pki_types::ServerName::try_from(host).map_err(|e| format!("tls server name: {e}"))
}

async fn connect_raw(
    addr: &str,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<Box<dyn ClusterIo>, String> {
    let tcp = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect {addr}: {e}"))?;
    if let Some(cfg) = tls_client {
        let connector = TlsConnector::from(cfg);
        let server_name = tls_server_name_from_addr(addr)?;
        let tls = tokio::time::timeout(AUTH_TIMEOUT, connector.connect(server_name, tcp))
            .await
            .map_err(|_| format!("tls connect to {addr} timed out"))?
            .map_err(|e| format!("tls connect: {e}"))?;
        Ok(Box::new(tls))
    } else {
        Ok(Box::new(tcp))
    }
}

async fn authed_roundtrip_inner(
    addr: &str,
    secret: &str,
    local_id: NodeId,
    tls_client: Option<Arc<ClientConfig>>,
    msg: ControlMessage,
) -> Result<
    (
        ControlMessage,
        Option<crate::cluster::security::InflightByteBudgetGuard>,
    ),
    String,
> {
    let timeout = match &msg {
        ControlMessage::RaftSnapshot(_) | ControlMessage::RaftSnapshotResp(_) => {
            SNAPSHOT_ROUNDTRIP_TIMEOUT
        }
        _ => ROUNDTRIP_TIMEOUT,
    };
    tokio::time::timeout(
        timeout,
        authed_roundtrip_unbounded(addr, secret, local_id, tls_client, msg),
    )
    .await
    .unwrap_or_else(|_| Err(format!("control round trip to {addr} timed out")))
}

async fn authed_roundtrip_unbounded(
    addr: &str,
    secret: &str,
    local_id: NodeId,
    tls_client: Option<Arc<ClientConfig>>,
    msg: ControlMessage,
) -> Result<
    (
        ControlMessage,
        Option<crate::cluster::security::InflightByteBudgetGuard>,
    ),
    String,
> {
    let mut stream = connect_raw(addr, tls_client).await?;
    client_auth_handshake(&mut stream, secret, local_id)
        .await
        .map_err(|e| e.to_string())?;
    write_frame(&mut stream, &msg)
        .await
        .map_err(|e| e.to_string())?;
    read_frame(&mut stream).await.map_err(|e| e.to_string())
}

/// Client helper: authenticated join against a bootstrap/leader node.
/// Returns `(cluster_id, peers)` on success.
pub async fn send_join(
    leader_addr: &str,
    secret: &str,
    local_id: NodeId,
    control_addr: String,
    media_addr: String,
    proof: String,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<(String, Vec<JoinPeerInfo>), String> {
    if proof.is_empty() {
        return Err(
            "CLUSTER_JOIN_PROOF is required for a fresh join; mint one via POST /api/v1/cluster/join-proof on an existing cluster member"
                .into(),
        );
    }
    send_join_with_hops(
        leader_addr,
        secret,
        local_id,
        control_addr,
        media_addr,
        proof,
        tls_client,
        0,
    )
    .await
}

const MAX_JOIN_FORWARD_HOPS: u8 = 3;

async fn send_join_with_hops(
    leader_addr: &str,
    secret: &str,
    local_id: NodeId,
    control_addr: String,
    media_addr: String,
    proof: String,
    tls_client: Option<Arc<ClientConfig>>,
    hops: u8,
) -> Result<(String, Vec<JoinPeerInfo>), String> {
    let (msg, _read_budget) = authed_roundtrip_inner(
        leader_addr,
        secret,
        local_id,
        tls_client.clone(),
        ControlMessage::JoinRequest {
            node_id: local_id,
            control_addr: control_addr.clone(),
            media_addr: media_addr.clone(),
            proof: proof.clone(),
        },
    )
    .await?;
    match msg {
        ControlMessage::JoinResponse {
            ok: true,
            cluster_id,
            peers,
            ..
        } => Ok((cluster_id, peers)),
        ControlMessage::JoinResponse {
            ok: false,
            message,
            peers,
            ..
        } if message.contains("forward_to_leader") => {
            if hops >= MAX_JOIN_FORWARD_HOPS {
                return Err(format!(
                    "join forward hop limit ({MAX_JOIN_FORWARD_HOPS}) exceeded: {message}"
                ));
            }
            let Some(leader) = peers.first() else {
                return Err(message);
            };
            if leader.control_addr == leader_addr {
                return Err(format!("join forward cycle at {leader_addr}"));
            }
            Box::pin(send_join_with_hops(
                &leader.control_addr,
                secret,
                local_id,
                control_addr,
                media_addr,
                proof,
                tls_client,
                hops + 1,
            ))
            .await
        }
        ControlMessage::JoinResponse {
            ok: false, message, ..
        } => Err(message),
        _ => Err("unexpected join response".into()),
    }
}

/// Forward a join for `joiner_id` while authenticating as `local_id` (follower proxy).
pub async fn forward_join(
    leader_addr: &str,
    secret: &str,
    local_id: NodeId,
    joiner_id: NodeId,
    control_addr: String,
    media_addr: String,
    proof: String,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<(String, Vec<JoinPeerInfo>), String> {
    let (msg, _read_budget) = authed_roundtrip_inner(
        leader_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::JoinRequest {
            node_id: joiner_id,
            control_addr,
            media_addr,
            proof,
        },
    )
    .await?;
    match msg {
        ControlMessage::JoinResponse {
            ok: true,
            cluster_id,
            peers,
            ..
        } => Ok((cluster_id, peers)),
        ControlMessage::JoinResponse {
            ok: false, message, ..
        } => Err(message),
        _ => Err("unexpected join response".into()),
    }
}

pub async fn send_topology(
    peer_addr: &str,
    secret: &str,
    local_id: NodeId,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<(String, Vec<JoinPeerInfo>), String> {
    let (msg, _read_budget) = authed_roundtrip_inner(
        peer_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::TopologyReq,
    )
    .await?;
    match msg {
        ControlMessage::TopologyResp {
            ok: true,
            cluster_id,
            peers,
            ..
        } => Ok((cluster_id, peers)),
        ControlMessage::TopologyResp {
            ok: false, message, ..
        } => Err(message),
        _ => Err("unexpected topology response".into()),
    }
}

pub async fn send_heartbeat(
    peer_addr: &str,
    secret: &str,
    local_id: NodeId,
    health: &str,
    load: f64,
    control_addr: String,
    media_addr: String,
    publishers: u64,
    players: u64,
    stream_players: Vec<(String, u64)>,
    viewer_players: Vec<(String, u64)>,
    tls_client: Option<Arc<ClientConfig>>,
) {
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        let _ = authed_roundtrip_inner(
            peer_addr,
            secret,
            local_id,
            tls_client,
            ControlMessage::Heartbeat {
                node_id: local_id,
                health: health.to_string(),
                load,
                control_addr,
                media_addr,
                publishers,
                players,
                stream_players,
                viewer_players,
            },
        )
        .await;
    })
    .await;
}

pub async fn send_session_count(
    peer_addr: &str,
    secret: &str,
    local_id: NodeId,
    stream_id: String,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<u64, String> {
    let (msg, _read_budget) = authed_roundtrip_inner(
        peer_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::SessionCountReq { stream_id },
    )
    .await?;
    match msg {
        ControlMessage::SessionCountResp { count } => Ok(count),
        ControlMessage::AdminErr { message } => Err(message),
        _ => Err("unexpected session count response".into()),
    }
}

pub async fn send_admin(
    peer_addr: &str,
    secret: &str,
    local_id: NodeId,
    msg: ControlMessage,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<(), String> {
    let (msg, _read_budget) =
        authed_roundtrip_inner(peer_addr, secret, local_id, tls_client, msg).await?;
    match msg {
        ControlMessage::AdminOk => Ok(()),
        ControlMessage::AdminErr { message } => Err(message),
        _ => Err("unexpected admin response".into()),
    }
}

pub async fn send_stats_proxy(
    peer_addr: &str,
    secret: &str,
    local_id: NodeId,
    stream_id: String,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<
    (
        serde_json::Value,
        Option<crate::cluster::security::InflightByteBudgetGuard>,
    ),
    String,
> {
    let (msg, read_budget) = authed_roundtrip_inner(
        peer_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::StatsProxyReq { stream_id },
    )
    .await?;
    match msg {
        // Keep the read-budget guard coupled to the returned body so a large
        // StatsProxyResp stays accounted for until the caller has consumed it.
        ControlMessage::StatsProxyResp { body } => Ok((body, read_budget)),
        ControlMessage::AdminErr { message } => Err(message),
        _ => Err("unexpected stats proxy response".into()),
    }
}

/// Forward a durable `ClusterCommand` to the current Raft leader over the control plane.
pub async fn send_client_write(
    leader_addr: &str,
    secret: &str,
    local_id: NodeId,
    cmd: crate::cluster::command::ClusterCommand,
    proof: String,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<crate::cluster::command::ClusterResponse, String> {
    let req = serde_json::to_value(&cmd).map_err(|e| e.to_string())?;
    let (msg, _read_budget) = authed_roundtrip_inner(
        leader_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::ClientWrite { req, proof },
    )
    .await?;
    match msg {
        ControlMessage::ClientWriteResp(body) => {
            let ok = body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
            if ok {
                let data = body
                    .get("data")
                    .cloned()
                    .ok_or_else(|| "client write response missing data".to_string())?;
                serde_json::from_value(data).map_err(|e| e.to_string())
            } else {
                Err(body
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("client write failed")
                    .to_string())
            }
        }
        ControlMessage::AdminErr { message } => Err(message),
        _ => Err("unexpected client write response".into()),
    }
}

/// Forward an OpenRaft membership change to the current leader.
pub async fn send_change_membership(
    leader_addr: &str,
    secret: &str,
    local_id: NodeId,
    change: openraft::ChangeMembers<NodeId, BasicNode>,
    proof: String,
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<(), String> {
    let req = serde_json::to_value(&change).map_err(|e| e.to_string())?;
    let (msg, _read_budget) = authed_roundtrip_inner(
        leader_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::ChangeMembership { req, proof },
    )
    .await?;
    match msg {
        ControlMessage::ChangeMembershipResp { ok: true, .. } => Ok(()),
        ControlMessage::ChangeMembershipResp { ok: false, message } => Err(message),
        ControlMessage::AdminErr { message } => Err(message),
        _ => Err("unexpected change membership response".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::command::{ClusterCommand, ClusterResponse};
    use crate::cluster::raft::{LogStore, StateMachineStore};
    use crate::db::Db;
    use openraft::error::{Fatal, PayloadTooLarge, RaftError, Timeout};
    use openraft::network::RPCTypes;
    use openraft::{LogId, SnapshotMeta, Vote};
    use std::collections::BTreeSet;
    use std::net::Ipv4Addr;

    #[tokio::test]
    async fn control_accept_backoff_survives_transient_errors() {
        control_accept_backoff(std::io::Error::from(std::io::ErrorKind::ConnectionAborted)).await;
        control_accept_backoff(std::io::Error::from(std::io::ErrorKind::Interrupted)).await;
    }

    #[tokio::test]
    async fn declared_length_above_max_frame_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(super::MAX_FRAME + 1).to_be_bytes());
        let mut reader: &[u8] = &bytes;

        let err = match read_budgeted_frame(&mut reader, false).await {
            Ok(_) => panic!("declared length above MAX_FRAME must be rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("frame too large"), "{err}");
    }

    #[tokio::test]
    async fn oversized_snapshot_frame_is_rejected_before_budget_reservation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&super::MAX_FRAME.to_be_bytes());
        let mut prefix = [b' '; 32];
        prefix[..15].copy_from_slice(b"{\"RaftSnapshot\"");
        bytes.extend_from_slice(&prefix);
        let mut reader: &[u8] = &bytes;

        let err = match read_budgeted_frame(&mut reader, false).await {
            Ok(_) => panic!("oversized snapshot frames must be capped"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("snapshot frame too large"),
            "{err}"
        );
    }

    /// Build a length-prefixed frame whose body is `json` preceded by enough
    /// JSON whitespace that the variant tag lands past the 32-byte peek, so the
    /// pre-decode classification is inconclusive.
    fn padded_frame_beyond_peek(json: &str, filler: usize) -> Vec<u8> {
        const LEAD: usize = 40;
        const {
            assert!(
                LEAD + 14 > 32,
                "the peek must be inconclusive for this helper to be meaningful"
            )
        };
        let body = format!("{}{json}{}", " ".repeat(LEAD), " ".repeat(filler));
        let payload = body.as_bytes();
        assert!(payload.len() as u32 > super::MAX_SNAPSHOT_FRAME);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    #[tokio::test]
    async fn oversized_snapshot_frame_past_the_peek_window_is_capped_after_decode() {
        // A padded tag escapes the pre-decode peek, so the snapshot cap must
        // still be applied once the frame is decoded and the variant is known
        // for certain, otherwise the tag dodges MAX_SNAPSHOT_FRAME entirely.
        let json = "{\"RaftSnapshot\":{\"meta\":{\"last_log_id\":{\"term\":1,\"index\":0},\
                    \"last_applied\":null,\"last_applied_log_id\":null,\"snapshot_meta\":null,\
                    \"pending_request\":null,\"membership\":null}}}";
        let mut reader: &[u8] = &padded_frame_beyond_peek(json, super::MAX_SNAPSHOT_FRAME as usize);

        let err = match read_budgeted_frame(&mut reader, false).await {
            Ok(_) => panic!("an oversized snapshot must be capped past the peek window"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("snapshot frame too large"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn large_non_snapshot_response_past_the_peek_window_is_still_accepted() {
        // read_frame passes allow_large_non_snapshot = true so a peer may send a
        // StatsProxyResp up to MAX_FRAME. An inconclusive peek must not be
        // treated as proof of a snapshot, or such a response is rejected before
        // allow_large_non_snapshot is ever consulted.
        let json = format!(
            "{{\"StatsProxyResp\":{{\"body\":\"{}\"}}}}",
            "x".repeat(super::MAX_SNAPSHOT_FRAME as usize)
        );
        let mut reader: &[u8] = &padded_frame_beyond_peek(&json, 0);

        let (decoded, _budget) = read_budgeted_frame(&mut reader, true)
            .await
            .expect("a whitespace-padded large non-snapshot response must be accepted");
        assert!(matches!(decoded, ControlMessage::StatsProxyResp { .. }));
    }

    #[test]
    fn tls_server_name_from_bracketed_ipv6_authority() {
        let name = tls_server_name_from_addr("[2001:db8::1]:1940").expect("parse");
        assert_eq!(name.to_str(), "2001:db8::1");
    }

    #[test]
    fn tls_server_name_from_ipv4_socket_addr() {
        let name = tls_server_name_from_addr("203.0.113.5:1940").expect("parse");
        assert_eq!(name.to_str(), "203.0.113.5");
    }

    #[test]
    fn tls_server_name_from_hostnames_and_invalid_input() {
        assert_eq!(
            tls_server_name_from_addr("node-a.cluster:1940")
                .unwrap()
                .to_str(),
            "node-a.cluster"
        );
        assert_eq!(
            tls_server_name_from_addr("node-a.cluster")
                .unwrap()
                .to_str(),
            "node-a.cluster"
        );
        assert!(tls_server_name_from_addr("bad host!:1").is_err());
    }

    // ---- helpers -------------------------------------------------------

    const SECRET: &str = "test-cluster-secret-32-chars-min--";
    const GOOD_PROOF: &str = "good-proof";
    const WAIT: Duration = Duration::from_secs(10);

    fn test_ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, last))
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

    struct RaftNode {
        raft: Raft,
        _dir: tempfile::TempDir,
    }

    async fn raft_node(id: NodeId) -> RaftNode {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Db::open(dir.path().join("raft.db").to_str().unwrap()).unwrap());
        let cfg = openraft::Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap();
        let raft = openraft::Raft::new(
            id,
            Arc::new(cfg),
            NetworkFactory::new(id, SECRET.into(), None),
            LogStore::new(Arc::clone(&db)),
            StateMachineStore::new(db).unwrap(),
        )
        .await
        .unwrap();
        RaftNode { raft, _dir: dir }
    }

    #[derive(Default)]
    struct Recorded {
        joins: Mutex<Vec<(NodeId, String, String)>>,
        heartbeats: Mutex<Vec<HeartbeatInfo>>,
        admin: Mutex<Vec<String>>,
    }

    struct Ctl {
        addr: String,
        rec: Arc<Recorded>,
    }

    /// Serve the real control plane on an ephemeral port. Members are 1–4;
    /// `GOOD_PROOF` is the only valid admin proof; joiner 66 is refused.
    async fn serve_ctl(
        raft: Raft,
        write_forward: Option<ClientWriteForwardCtx>,
        tls_server: Option<Arc<ServerConfig>>,
    ) -> Ctl {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let rec = Arc::new(Recorded::default());
        let r = Arc::clone(&rec);
        let on_join: JoinAcceptFn = Arc::new(move |id, ctrl, media, _proof| {
            r.joins.lock().push((id, ctrl.clone(), media));
            if id == 66 {
                return Err("join refused".into());
            }
            Ok((
                "cid".into(),
                vec![JoinPeerInfo {
                    node_id: 1,
                    control_addr: ctrl,
                    media_addr: String::new(),
                }],
            ))
        });
        let r = Arc::clone(&rec);
        let on_heartbeat = Arc::new(move |hb: HeartbeatInfo| r.heartbeats.lock().push(hb));
        let r = Arc::clone(&rec);
        let on_admin = Arc::new(move |msg: ControlMessage| {
            r.admin.lock().push(format!("{msg:?}"));
            match msg {
                ControlMessage::TopologyReq => ControlMessage::TopologyResp {
                    ok: true,
                    message: String::new(),
                    cluster_id: "cid".into(),
                    peers: Vec::new(),
                },
                ControlMessage::SessionCountReq { .. } => {
                    ControlMessage::SessionCountResp { count: 3 }
                }
                ControlMessage::AdminRemove { node_id: 99 } => ControlMessage::AdminErr {
                    message: "cannot remove".into(),
                },
                _ => ControlMessage::AdminOk,
            }
        });
        let on_stats = Arc::new(|sid: String| serde_json::json!({ "stream": sid }));
        let is_member: MembershipFn = Arc::new(|id| (1..=4).contains(&id));
        let verify: AdminProofFn = Arc::new(|proof, _payload| proof == GOOD_PROOF);
        tokio::spawn(serve_control_plane_listener(
            listener,
            SECRET.into(),
            100,
            raft,
            on_join,
            on_heartbeat,
            on_admin,
            on_stats,
            is_member,
            verify,
            write_forward,
            tls_server,
        ));
        Ctl { addr, rec }
    }

    /// Scripted control server: authenticates each connection and answers
    /// the single request with the next canned response.
    async fn fake_ctl(responses: Vec<ControlMessage>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            for resp in responses {
                let Ok((mut s, peer)) = listener.accept().await else {
                    return;
                };
                if server_auth_handshake(&mut s, peer.ip(), SECRET, 100, false, None)
                    .await
                    .is_err()
                {
                    continue;
                }
                let _ = read_control_frame(&mut s).await;
                let _ = write_frame(&mut s, &resp).await;
            }
        });
        addr
    }

    fn dead_addr() -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    }

    fn set_token(token: &str) -> ClusterCommand {
        ClusterCommand::SetApiToken {
            token: token.into(),
        }
    }

    async fn wait_for_leader(raft: &Raft, leader: NodeId) {
        let mut m = raft.metrics();
        tokio::time::timeout(WAIT, async {
            loop {
                if m.borrow().current_leader == Some(leader) {
                    return;
                }
                m.changed().await.unwrap();
            }
        })
        .await
        .expect("leader was never observed");
    }

    fn frame_bytes(msg: &ControlMessage) -> Vec<u8> {
        let body = serde_json::to_vec(msg).unwrap();
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    // ---- framing ----------------------------------------------------------

    #[tokio::test]
    async fn frames_roundtrip_small_and_budgeted_sizes() {
        let (mut a, mut b) = tokio::io::duplex(1 << 16);
        write_frame(&mut a, &ControlMessage::TopologyReq)
            .await
            .unwrap();
        let (msg, guard) = read_frame(&mut b).await.unwrap();
        assert!(matches!(msg, ControlMessage::TopologyReq));
        assert!(guard.is_none(), "short frames are not budgeted");

        // > 256 KiB non-snapshot frames reserve the control budget.
        let big = ControlMessage::StatsProxyResp {
            body: serde_json::Value::String("x".repeat(300 * 1024)),
        };
        let bytes = frame_bytes(&big);
        let mut r: &[u8] = &bytes;
        let (_, guard) = read_control_frame(&mut r).await.unwrap();
        assert!(guard.is_some());

        // Snapshot-tagged frames use the snapshot budget.
        let snap = ControlMessage::RaftSnapshot(serde_json::Value::String("y".repeat(300 * 1024)));
        let bytes = frame_bytes(&snap);
        let mut r: &[u8] = &bytes;
        let (msg, guard) = read_control_frame(&mut r).await.unwrap();
        assert!(matches!(msg, ControlMessage::RaftSnapshot(_)));
        assert!(guard.is_some());
    }

    #[tokio::test]
    async fn oversized_non_snapshot_control_frame_rejected_unless_allowed() {
        let big = ControlMessage::StatsProxyResp {
            body: serde_json::Value::String("z".repeat(MAX_CONTROL_FRAME as usize + 16)),
        };
        let bytes = frame_bytes(&big);
        let mut r: &[u8] = &bytes;
        let err = match read_control_frame(&mut r).await {
            Ok(_) => panic!("server-side reads must cap non-snapshot frames"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("frame too large"), "{err}");

        // Client-side response reads allow large responses.
        let mut r: &[u8] = &bytes;
        assert!(read_frame(&mut r).await.is_ok());
    }

    #[tokio::test]
    async fn malformed_truncated_and_over_budget_frames_error() {
        let mut bytes = 8u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(b"notjson!");
        let mut r: &[u8] = &bytes;
        assert!(read_control_frame(&mut r).await.is_err());

        let mut bytes = 100u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[b'{'; 40]);
        let mut r: &[u8] = &bytes;
        assert!(read_control_frame(&mut r).await.is_err());

        // Exhaust the control budget, then a budgeted read must fail fast.
        let hold = crate::cluster::security::try_reserve_inflight_bytes(
            &CONTROL_READ_BYTES_INFLIGHT,
            MAX_CONTROL_READ_BYTES_INFLIGHT,
            MAX_CONTROL_READ_BYTES_INFLIGHT - CONTROL_READ_BYTES_INFLIGHT.load(Ordering::Acquire),
        )
        .unwrap();
        let big = ControlMessage::StatsProxyResp {
            body: serde_json::Value::String("x".repeat(300 * 1024)),
        };
        let bytes = frame_bytes(&big);
        let mut r: &[u8] = &bytes;
        let err = match read_control_frame(&mut r).await {
            Ok(_) => panic!("budget must be enforced"),
            Err(e) => e,
        };
        drop(hold);
        assert!(err.to_string().contains("budget exceeded"), "{err}");

        // Auth frames are capped at MAX_AUTH_FRAME.
        let mut bytes = (MAX_AUTH_FRAME + 1).to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0; 16]);
        let mut r: &[u8] = &bytes;
        let err = read_auth_frame(&mut r).await.unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
    }

    // ---- handshakes ---------------------------------------------------------

    #[tokio::test]
    async fn server_auth_handshake_rejects_bad_clients() {
        // Rate-limited source.
        let ip = test_ip(60);
        for _ in 0..10 {
            crate::cluster::security::record_cluster_auth_failure(ip);
        }
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        assert!(
            server_auth_handshake(&mut s, ip, SECRET, 1, false, None)
                .await
                .is_err()
        );
        assert!(matches!(
            read_auth_frame(&mut c).await.unwrap(),
            ControlMessage::AuthFail
        ));
        crate::cluster::security::clear_cluster_auth_failures(ip);

        // Wrong first message.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move {
            server_auth_handshake(&mut s, test_ip(61), SECRET, 1, false, None).await
        });
        let _ = read_auth_frame(&mut c).await.unwrap();
        write_frame(&mut c, &ControlMessage::TopologyReq)
            .await
            .unwrap();
        assert!(matches!(
            read_auth_frame(&mut c).await.unwrap(),
            ControlMessage::AuthFail
        ));
        assert!(server.await.unwrap().is_err());

        // Wrong secret (client helper sees AuthFail).
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move {
            server_auth_handshake(&mut s, test_ip(61), SECRET, 1, false, None).await
        });
        let err = client_auth_handshake(&mut c, "wrong-secret", 2)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("auth failed"), "{err}");
        assert!(server.await.unwrap().is_err());

        // mTLS identity mismatch.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move {
            server_auth_handshake(&mut s, test_ip(62), SECRET, 1, true, Some(3)).await
        });
        let _ = client_auth_handshake(&mut c, SECRET, 2).await;
        let err = server.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");

        // Success.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move {
            server_auth_handshake(&mut s, test_ip(63), SECRET, 1, true, Some(2)).await
        });
        client_auth_handshake(&mut c, SECRET, 2).await.unwrap();
        assert_eq!(server.await.unwrap().unwrap(), 2);

        // Client: server does not start with a challenge.
        let (mut c, mut s) = tokio::io::duplex(1 << 16);
        write_frame(&mut s, &ControlMessage::AdminOk).await.unwrap();
        let err = client_auth_handshake(&mut c, SECRET, 2).await.unwrap_err();
        assert!(err.to_string().contains("AuthChallenge"), "{err}");
    }

    // ---- served control plane: admin / topology / heartbeat ----------------

    #[tokio::test]
    async fn control_plane_dispatches_member_requests() {
        let node = raft_node(100).await;
        let ctl = serve_ctl(node.raft.clone(), None, None).await;

        let (cid, peers) = send_topology(&ctl.addr, SECRET, 1, None).await.unwrap();
        assert_eq!(cid, "cid");
        assert!(peers.is_empty());
        // Non-members are refused (the server closes without a response).
        assert!(send_topology(&ctl.addr, SECRET, 9, None).await.is_err());

        assert_eq!(
            send_session_count(&ctl.addr, SECRET, 1, "s".into(), None)
                .await
                .unwrap(),
            3
        );

        let (body, _guard) = send_stats_proxy(&ctl.addr, SECRET, 1, "s1".into(), None)
            .await
            .unwrap();
        assert_eq!(body["stream"], "s1");
        assert!(
            send_stats_proxy(&ctl.addr, SECRET, 9, "s1".into(), None)
                .await
                .is_err()
        );

        for msg in [
            ControlMessage::AdminDrain {
                node_id: 2,
                proof: GOOD_PROOF.into(),
            },
            ControlMessage::AdminResume {
                node_id: 2,
                proof: GOOD_PROOF.into(),
            },
            ControlMessage::AdminRemove { node_id: 3 },
            ControlMessage::DrainStream {
                stream_id: "s".into(),
            },
            ControlMessage::RevokeViewer {
                viewer_id: "v".into(),
            },
        ] {
            send_admin(&ctl.addr, SECRET, 1, msg, None).await.unwrap();
        }
        assert_eq!(ctl.rec.admin.lock().len(), 7);

        let err = send_admin(
            &ctl.addr,
            SECRET,
            1,
            ControlMessage::AdminRemove { node_id: 99 },
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err, "cannot remove");
        for bad in [
            ControlMessage::AdminDrain {
                node_id: 2,
                proof: "bad".into(),
            },
            ControlMessage::AdminResume {
                node_id: 2,
                proof: "bad".into(),
            },
        ] {
            assert!(send_admin(&ctl.addr, SECRET, 1, bad, None).await.is_err());
        }
        assert!(
            send_admin(
                &ctl.addr,
                SECRET,
                9,
                ControlMessage::DrainStream {
                    stream_id: "s".into()
                },
                None
            )
            .await
            .is_err()
        );
        // Messages that are not requests get an explicit "unsupported".
        let err = send_admin(&ctl.addr, SECRET, 1, ControlMessage::AdminOk, None)
            .await
            .unwrap_err();
        assert_eq!(err, "unsupported");

        // Heartbeats are bound to the authenticated member identity.
        send_heartbeat(
            &ctl.addr,
            SECRET,
            2,
            "healthy",
            0.5,
            "c".into(),
            "m".into(),
            1,
            2,
            vec![("s".into(), 2)],
            vec![("v".into(), 1)],
            None,
        )
        .await;
        send_heartbeat(
            &ctl.addr,
            SECRET,
            9,
            "healthy",
            0.5,
            String::new(),
            String::new(),
            0,
            0,
            Vec::new(),
            Vec::new(),
            None,
        )
        .await;
        {
            let hbs = ctl.rec.heartbeats.lock();
            assert_eq!(hbs.len(), 1);
            assert_eq!(hbs[0].node_id, 2);
            assert_eq!(hbs[0].players, 2);
            assert_eq!(hbs[0].stream_players, vec![("s".to_string(), 2)]);
        }

        // Wrong secret: the client reports an auth failure.
        let err = send_topology(&ctl.addr, "wrong-secret", 1, None)
            .await
            .unwrap_err();
        assert!(err.contains("auth failed"), "{err}");
        crate::cluster::security::clear_cluster_auth_failures(IpAddr::V4(Ipv4Addr::LOCALHOST));

        // Nothing listening: reported as a connect error.
        let err = send_topology(&dead_addr(), SECRET, 1, None)
            .await
            .unwrap_err();
        assert!(err.contains("connect"), "{err}");
        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn control_plane_join_flow() {
        let node = raft_node(100).await;
        let ctl = serve_ctl(node.raft.clone(), None, None).await;

        let err = send_join(
            &ctl.addr,
            SECRET,
            5,
            "c".into(),
            "m".into(),
            String::new(),
            None,
        )
        .await
        .unwrap_err();
        assert!(err.contains("CLUSTER_JOIN_PROOF"), "{err}");

        let (cid, peers) = send_join(
            &ctl.addr,
            SECRET,
            5,
            "c5".into(),
            "m5".into(),
            GOOD_PROOF.into(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(cid, "cid");
        assert_eq!(peers[0].control_addr, "c5");
        assert_eq!(
            ctl.rec.joins.lock()[0],
            (5, "c5".to_string(), "m5".to_string())
        );

        // Invalid proof: server drops the connection.
        assert!(
            send_join(
                &ctl.addr,
                SECRET,
                5,
                "c".into(),
                "m".into(),
                "bad".into(),
                None
            )
            .await
            .is_err()
        );
        // Join callback refusal is surfaced verbatim.
        let err = send_join(
            &ctl.addr,
            SECRET,
            66,
            "c".into(),
            "m".into(),
            GOOD_PROOF.into(),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err, "join refused");

        // A member may proxy a join for another node; a non-member may not.
        let (cid, _) = forward_join(
            &ctl.addr,
            SECRET,
            2,
            7,
            "c7".into(),
            "m7".into(),
            GOOD_PROOF.into(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(cid, "cid");
        assert!(
            forward_join(
                &ctl.addr,
                SECRET,
                8,
                7,
                "c7".into(),
                "m7".into(),
                GOOD_PROOF.into(),
                None
            )
            .await
            .is_err()
        );
        let err = forward_join(
            &ctl.addr,
            SECRET,
            2,
            66,
            "c".into(),
            "m".into(),
            GOOD_PROOF.into(),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err, "join refused");
        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn control_plane_over_mutual_tls() {
        let node = raft_node(100).await;
        let (server_cfg, _) = tls_for(2);
        let (_, client_cfg) = tls_for(1);
        let ctl = serve_ctl(node.raft.clone(), None, Some(server_cfg)).await;
        let addr = ctl.addr.clone();

        // Cert identity lrtmp2-node-1 matches the claimed node id.
        let (cid, _) = send_topology(&addr, SECRET, 1, Some(Arc::clone(&client_cfg)))
            .await
            .unwrap();
        assert_eq!(cid, "cid");
        // Claiming another node id with node 1's certificate is rejected.
        assert!(
            send_topology(&addr, SECRET, 3, Some(Arc::clone(&client_cfg)))
                .await
                .is_err()
        );
        // Plaintext against a TLS listener fails the handshake.
        assert!(send_topology(&addr, SECRET, 1, None).await.is_err());
        node.raft.shutdown().await.unwrap();
    }

    // ---- client helpers against scripted responses --------------------------

    #[tokio::test]
    async fn join_follows_forward_to_leader_redirects() {
        let leader = fake_ctl(vec![ControlMessage::JoinResponse {
            ok: true,
            message: String::new(),
            cluster_id: "cid".into(),
            peers: Vec::new(),
        }])
        .await;
        let follower = fake_ctl(vec![ControlMessage::JoinResponse {
            ok: false,
            message: "forward_to_leader".into(),
            cluster_id: String::new(),
            peers: vec![JoinPeerInfo {
                node_id: 1,
                control_addr: leader.clone(),
                media_addr: String::new(),
            }],
        }])
        .await;
        let (cid, _) = send_join(
            &follower,
            SECRET,
            5,
            "c".into(),
            "m".into(),
            "p".into(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(cid, "cid");

        // Redirect without a leader hint.
        let no_hint = fake_ctl(vec![ControlMessage::JoinResponse {
            ok: false,
            message: "forward_to_leader: unknown".into(),
            cluster_id: String::new(),
            peers: Vec::new(),
        }])
        .await;
        let err = send_join(
            &no_hint,
            SECRET,
            5,
            "c".into(),
            "m".into(),
            "p".into(),
            None,
        )
        .await
        .unwrap_err();
        assert!(err.contains("forward_to_leader"), "{err}");

        // Redirect back to itself.
        let cyc = fake_ctl_redirect_to_self().await;
        let err = send_join(&cyc, SECRET, 5, "c".into(), "m".into(), "p".into(), None)
            .await
            .unwrap_err();
        assert!(err.contains("cycle"), "{err}");

        // Endless redirects between two nodes hit the hop limit.
        let (a, b) = fake_ctl_ping_pong().await;
        assert_ne!(a, b);
        let err = send_join(&a, SECRET, 5, "c".into(), "m".into(), "p".into(), None)
            .await
            .unwrap_err();
        assert!(err.contains("hop limit"), "{err}");

        // Unexpected response type.
        let odd = fake_ctl(vec![ControlMessage::AdminOk, ControlMessage::AdminOk]).await;
        let err = send_join(&odd, SECRET, 5, "c".into(), "m".into(), "p".into(), None)
            .await
            .unwrap_err();
        assert_eq!(err, "unexpected join response");
        let err = forward_join(&odd, SECRET, 2, 5, "c".into(), "m".into(), "p".into(), None)
            .await
            .unwrap_err();
        assert_eq!(err, "unexpected join response");
    }

    /// A fake node whose join answer redirects to its own address.
    async fn fake_ctl_redirect_to_self() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let resp = ControlMessage::JoinResponse {
            ok: false,
            message: "forward_to_leader".into(),
            cluster_id: String::new(),
            peers: vec![JoinPeerInfo {
                node_id: 1,
                control_addr: addr.clone(),
                media_addr: String::new(),
            }],
        };
        serve_fake(listener, vec![resp]);
        addr
    }

    /// Two fake nodes that redirect joins to each other forever.
    async fn fake_ctl_ping_pong() -> (String, String) {
        let la = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let lb = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = la.local_addr().unwrap().to_string();
        let b = lb.local_addr().unwrap().to_string();
        let redirect = |to: &str| ControlMessage::JoinResponse {
            ok: false,
            message: "forward_to_leader".into(),
            cluster_id: String::new(),
            peers: vec![JoinPeerInfo {
                node_id: 1,
                control_addr: to.to_string(),
                media_addr: String::new(),
            }],
        };
        serve_fake(la, vec![redirect(&b), redirect(&b), redirect(&b)]);
        serve_fake(lb, vec![redirect(&a), redirect(&a), redirect(&a)]);
        (a, b)
    }

    fn serve_fake(listener: TcpListener, responses: Vec<ControlMessage>) {
        tokio::spawn(async move {
            for resp in responses {
                let Ok((mut s, peer)) = listener.accept().await else {
                    return;
                };
                if server_auth_handshake(&mut s, peer.ip(), SECRET, 100, false, None)
                    .await
                    .is_ok()
                {
                    let _ = read_control_frame(&mut s).await;
                    let _ = write_frame(&mut s, &resp).await;
                }
            }
        });
    }

    #[tokio::test]
    async fn client_helpers_map_error_and_unexpected_responses() {
        let admin_err = || ControlMessage::AdminErr {
            message: "nope".into(),
        };
        let addr = fake_ctl(vec![
            ControlMessage::TopologyResp {
                ok: false,
                message: "not ready".into(),
                cluster_id: String::new(),
                peers: Vec::new(),
            },
            ControlMessage::AdminOk,
            admin_err(),
            ControlMessage::AdminOk,
            ControlMessage::SessionCountReq {
                stream_id: String::new(),
            },
            admin_err(),
            ControlMessage::AdminOk,
            admin_err(),
            ControlMessage::AdminOk,
            ControlMessage::ClientWriteResp(serde_json::json!({ "ok": true })),
            ControlMessage::ClientWriteResp(serde_json::json!({ "ok": false, "error": "e1" })),
            ControlMessage::ClientWriteResp(serde_json::json!({ "ok": false })),
            ControlMessage::ClientWriteResp(serde_json::json!({ "ok": true, "data": 5 })),
            ControlMessage::ChangeMembershipResp {
                ok: false,
                message: "cm".into(),
            },
            admin_err(),
            ControlMessage::AdminOk,
        ])
        .await;

        assert_eq!(
            send_topology(&addr, SECRET, 1, None).await.unwrap_err(),
            "not ready"
        );
        assert_eq!(
            send_topology(&addr, SECRET, 1, None).await.unwrap_err(),
            "unexpected topology response"
        );
        assert_eq!(
            send_session_count(&addr, SECRET, 1, "s".into(), None)
                .await
                .unwrap_err(),
            "nope"
        );
        assert_eq!(
            send_session_count(&addr, SECRET, 1, "s".into(), None)
                .await
                .unwrap_err(),
            "unexpected session count response"
        );
        assert_eq!(
            send_admin(&addr, SECRET, 1, ControlMessage::TopologyReq, None)
                .await
                .unwrap_err(),
            "unexpected admin response"
        );
        assert_eq!(
            send_stats_proxy(&addr, SECRET, 1, "s".into(), None)
                .await
                .err()
                .unwrap(),
            "nope"
        );
        assert_eq!(
            send_stats_proxy(&addr, SECRET, 1, "s".into(), None)
                .await
                .err()
                .unwrap(),
            "unexpected stats proxy response"
        );
        let cw = || send_client_write(&addr, SECRET, 1, set_token("t"), "p".into(), None);
        assert_eq!(cw().await.unwrap_err(), "nope");
        assert_eq!(cw().await.unwrap_err(), "unexpected client write response");
        assert_eq!(
            cw().await.unwrap_err(),
            "client write response missing data"
        );
        assert_eq!(cw().await.unwrap_err(), "e1");
        assert_eq!(cw().await.unwrap_err(), "client write failed");
        assert!(cw().await.is_err(), "undecodable data must be an error");
        let cm = || {
            send_change_membership(
                &addr,
                SECRET,
                1,
                openraft::ChangeMembers::AddVoterIds(BTreeSet::from([2])),
                "p".into(),
                None,
            )
        };
        assert_eq!(cm().await.unwrap_err(), "cm");
        assert_eq!(cm().await.unwrap_err(), "nope");
        assert_eq!(
            cm().await.unwrap_err(),
            "unexpected change membership response"
        );
    }

    // ---- raft RPC transport -------------------------------------------------

    fn vote_req() -> VoteRequest<NodeId> {
        VoteRequest::new(Vote::new(1, 7), None)
    }

    fn append_req() -> AppendEntriesRequest<TypeConfig> {
        AppendEntriesRequest {
            vote: Vote::new_committed(1, 7),
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        }
    }

    fn snapshot_req() -> InstallSnapshotRequest<TypeConfig> {
        InstallSnapshotRequest {
            vote: Vote::new_committed(1, 7),
            meta: SnapshotMeta {
                last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(1, 7), 0)),
                last_membership: Default::default(),
                snapshot_id: "snap-1".into(),
            },
            offset: 0,
            data: Vec::new(),
            done: false,
        }
    }

    async fn client_for(addr: &str) -> NetworkConnection {
        let mut factory = NetworkFactory::new(7, SECRET.into(), None);
        factory.upsert_node(1, addr.to_string());
        assert_eq!(factory.nodes.read().get(&1).unwrap().addr, addr);
        factory
            .new_client(
                1,
                &BasicNode {
                    addr: addr.to_string(),
                },
            )
            .await
    }

    #[tokio::test]
    async fn raft_rpcs_roundtrip_through_control_plane() {
        let node = raft_node(100).await;
        let ctl = serve_ctl(node.raft.clone(), None, None).await;
        let mut conn = client_for(&ctl.addr).await;
        let opt = RPCOption::new(Duration::from_secs(5));

        let v = conn.vote(vote_req(), opt.clone()).await.unwrap();
        assert!(v.vote_granted, "an idle node grants a higher-term vote");
        conn.append_entries(append_req(), opt.clone())
            .await
            .unwrap();
        let snap = conn.install_snapshot(snapshot_req(), opt.clone()).await;
        assert!(snap.is_ok(), "{snap:?}");

        // A dead target is reported as unreachable for every RPC kind.
        let mut dead = client_for(&dead_addr()).await;
        assert!(matches!(
            dead.vote(vote_req(), opt.clone()).await,
            Err(RPCError::Unreachable(_))
        ));
        assert!(matches!(
            dead.append_entries(append_req(), opt.clone()).await,
            Err(RPCError::Unreachable(_))
        ));
        assert!(matches!(
            dead.install_snapshot(snapshot_req(), opt).await,
            Err(RPCError::Unreachable(_))
        ));
        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_rpcs_map_remote_errors_and_bad_payloads() {
        let fatal = || RaftError::<NodeId>::Fatal(Fatal::Stopped);
        let remote_append =
            serde_json::to_value(Err::<AppendEntriesResponse<NodeId>, _>(fatal())).unwrap();
        let remote_vote = serde_json::to_value(Err::<VoteResponse<NodeId>, _>(fatal())).unwrap();
        let remote_snap = serde_json::to_value(Err::<
            InstallSnapshotResponse<NodeId>,
            RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >(RaftError::Fatal(Fatal::Stopped)))
        .unwrap();
        let junk = serde_json::json!({"junk": true});
        let addr = fake_ctl(vec![
            ControlMessage::RaftAppendResp(remote_append),
            ControlMessage::RaftAppendResp(junk.clone()),
            ControlMessage::AdminOk,
            ControlMessage::RaftVoteResp(remote_vote),
            ControlMessage::RaftVoteResp(junk.clone()),
            ControlMessage::AdminOk,
            ControlMessage::RaftSnapshotResp(remote_snap),
            ControlMessage::RaftSnapshotResp(junk),
            ControlMessage::AdminOk,
        ])
        .await;
        let mut conn = client_for(&addr).await;
        let opt = RPCOption::new(Duration::from_secs(5));

        assert!(matches!(
            conn.append_entries(append_req(), opt.clone()).await,
            Err(RPCError::RemoteError(_))
        ));
        assert!(matches!(
            conn.append_entries(append_req(), opt.clone()).await,
            Err(RPCError::Network(_))
        ));
        assert!(matches!(
            conn.append_entries(append_req(), opt.clone()).await,
            Err(RPCError::Network(_))
        ));
        assert!(matches!(
            conn.vote(vote_req(), opt.clone()).await,
            Err(RPCError::RemoteError(_))
        ));
        assert!(matches!(
            conn.vote(vote_req(), opt.clone()).await,
            Err(RPCError::Network(_))
        ));
        assert!(matches!(
            conn.vote(vote_req(), opt.clone()).await,
            Err(RPCError::Network(_))
        ));
        assert!(matches!(
            conn.install_snapshot(snapshot_req(), opt.clone()).await,
            Err(RPCError::RemoteError(_))
        ));
        assert!(matches!(
            conn.install_snapshot(snapshot_req(), opt.clone()).await,
            Err(RPCError::Network(_))
        ));
        assert!(matches!(
            conn.install_snapshot(snapshot_req(), opt).await,
            Err(RPCError::Network(_))
        ));
    }

    #[tokio::test]
    async fn roundtrip_hard_deadline_and_transport_error_mapping() {
        // A listener that accepts but never speaks: the hard deadline fires
        // and is reported as a network error (not unreachable).
        let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = silent.local_addr().unwrap().to_string();
        let mut conn = client_for(&addr).await;
        let err = conn
            .roundtrip(
                ControlMessage::TopologyReq,
                Some(Duration::from_millis(100)),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RPCError::Network(_)), "{err:?}");
        drop(silent);

        let timeout: RPCError<NodeId, BasicNode, typ::RaftError> = RPCError::Timeout(Timeout {
            action: RPCTypes::Vote,
            id: 1,
            target: 2,
            timeout: Duration::from_millis(1),
        });
        assert!(matches!(
            map_rpc_transport_err::<openraft::error::InstallSnapshotError>(timeout),
            RPCError::Timeout(_)
        ));
        let too_large: RPCError<NodeId, BasicNode, typ::RaftError> =
            RPCError::PayloadTooLarge(PayloadTooLarge::new_entries_hint(1));
        assert!(matches!(
            map_rpc_transport_err::<openraft::error::InstallSnapshotError>(too_large),
            RPCError::PayloadTooLarge(_)
        ));
        let remote: RPCError<NodeId, BasicNode, typ::RaftError> =
            RPCError::RemoteError(RemoteError::new(2, RaftError::Fatal(Fatal::Stopped)));
        assert!(matches!(
            map_rpc_transport_err::<openraft::error::InstallSnapshotError>(remote),
            RPCError::Network(_)
        ));
        let net: RPCError<NodeId, BasicNode, typ::RaftError> =
            RPCError::Network(NetworkError::new(&std::io::Error::other("x")));
        assert!(matches!(
            map_rpc_transport_err::<openraft::error::InstallSnapshotError>(net),
            RPCError::Network(_)
        ));
    }

    // ---- client writes and membership changes -------------------------------

    #[tokio::test]
    async fn client_write_and_membership_on_uninitialized_node() {
        let node = raft_node(100).await;
        // No forward context: a follower without a leader drops the request.
        let ctl = serve_ctl(node.raft.clone(), None, None).await;
        assert!(
            send_client_write(
                &ctl.addr,
                SECRET,
                1,
                set_token("t"),
                GOOD_PROOF.into(),
                None
            )
            .await
            .is_err()
        );
        // Bad proof / non-member are refused before touching Raft.
        assert!(
            send_client_write(&ctl.addr, SECRET, 1, set_token("t"), "bad".into(), None)
                .await
                .is_err()
        );
        assert!(
            send_client_write(
                &ctl.addr,
                SECRET,
                9,
                set_token("t"),
                GOOD_PROOF.into(),
                None
            )
            .await
            .is_err()
        );
        // Undecodable command body.
        let err = authed_roundtrip_inner(
            &ctl.addr,
            SECRET,
            1,
            None,
            ControlMessage::ClientWrite {
                req: serde_json::json!({"NotACommand": 1}),
                proof: GOOD_PROOF.into(),
            },
        )
        .await;
        assert!(err.is_err());

        // With a forward context but no known leader the error is reported.
        let fwd = ClientWriteForwardCtx {
            secret: SECRET.into(),
            local_id: 100,
            meta: Arc::new(ClusterMeta::new()),
            tls_client: None,
        };
        let ctl2 = serve_ctl(node.raft.clone(), Some(fwd), None).await;
        let err = send_client_write(
            &ctl2.addr,
            SECRET,
            1,
            set_token("t"),
            GOOD_PROOF.into(),
            None,
        )
        .await
        .unwrap_err();
        assert!(err.contains("no leader"), "{err}");

        // Membership change on an uninitialized node fails remotely.
        let change = || openraft::ChangeMembers::AddVoterIds(BTreeSet::from([100]));
        assert!(
            send_change_membership(&ctl.addr, SECRET, 1, change(), "bad".into(), None)
                .await
                .is_err()
        );
        assert!(
            send_change_membership(&ctl.addr, SECRET, 9, change(), GOOD_PROOF.into(), None)
                .await
                .is_err()
        );
        let err = send_change_membership(&ctl.addr, SECRET, 1, change(), GOOD_PROOF.into(), None)
            .await
            .unwrap_err();
        assert!(!err.is_empty());
        // Proof valid but body is not a ChangeMembers value.
        let err = authed_roundtrip_inner(
            &ctl.addr,
            SECRET,
            1,
            None,
            ControlMessage::ChangeMembership {
                req: serde_json::json!({"bogus": 1}),
                proof: GOOD_PROOF.into(),
            },
        )
        .await;
        assert!(err.is_err());
        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn follower_forwards_client_write_to_leader() {
        // Leader (id 1) and a learner (id 2), each serving the control plane.
        let leader = raft_node(1).await;
        let leader_ctl = serve_ctl(leader.raft.clone(), None, None).await;
        let follower = raft_node(2).await;
        let fwd = ClientWriteForwardCtx {
            secret: SECRET.into(),
            local_id: 2,
            meta: Arc::new(ClusterMeta::new()),
            tls_client: None,
        };
        let follower_ctl = serve_ctl(follower.raft.clone(), Some(fwd), None).await;

        leader
            .raft
            .initialize(BTreeMap::from([(
                1,
                BasicNode {
                    addr: leader_ctl.addr.clone(),
                },
            )]))
            .await
            .unwrap();
        wait_for_leader(&leader.raft, 1).await;

        // A write sent straight to the leader commits.
        let resp = send_client_write(
            &leader_ctl.addr,
            SECRET,
            3,
            set_token("t1"),
            GOOD_PROOF.into(),
            None,
        )
        .await
        .unwrap();
        assert!(matches!(resp, ClusterResponse::Ok), "{resp:?}");

        // Replicate to the learner over the control plane (append_entries).
        leader
            .raft
            .add_learner(
                2,
                BasicNode {
                    addr: follower_ctl.addr.clone(),
                },
                true,
            )
            .await
            .unwrap();
        wait_for_leader(&follower.raft, 1).await;

        // The learner forwards to the leader it learned from replication.
        let resp = send_client_write(
            &follower_ctl.addr,
            SECRET,
            3,
            set_token("t2"),
            GOOD_PROOF.into(),
            None,
        )
        .await
        .unwrap();
        assert!(matches!(resp, ClusterResponse::Ok), "{resp:?}");

        // Membership change through the control plane on the leader.
        send_change_membership(
            &leader_ctl.addr,
            SECRET,
            3,
            openraft::ChangeMembers::AddVoterIds(BTreeSet::from([2])),
            GOOD_PROOF.into(),
            None,
        )
        .await
        .unwrap();

        leader.raft.shutdown().await.unwrap();
        follower.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn serve_control_plane_binds_and_reports_conflicts() {
        let node = raft_node(100).await;
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let noop_join: JoinAcceptFn = Arc::new(|_, _, _, _| Err("x".into()));
        let res = serve_control_plane(
            taken.local_addr().unwrap(),
            SECRET.into(),
            100,
            node.raft.clone(),
            noop_join,
            Arc::new(|_| {}),
            Arc::new(|_| ControlMessage::AdminOk),
            Arc::new(|_| serde_json::Value::Null),
            Arc::new(|_| true),
            Arc::new(|_, _| true),
            None,
            None,
        )
        .await;
        assert!(res.is_err(), "bind conflict must be reported");

        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let bind: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        tokio::spawn(serve_control_plane(
            bind,
            SECRET.into(),
            100,
            node.raft.clone(),
            Arc::new(|_, _, _, _| Err("x".into())),
            Arc::new(|_| {}),
            Arc::new(|_| ControlMessage::AdminOk),
            Arc::new(|_| serde_json::Value::Null),
            Arc::new(|_| true),
            Arc::new(|_, _| true),
            None,
            None,
        ));
        let addr = bind.to_string();
        let ok = tokio::time::timeout(WAIT, async {
            loop {
                if send_admin(
                    &addr,
                    SECRET,
                    1,
                    ControlMessage::RevokeViewer {
                        viewer_id: "v".into(),
                    },
                    None,
                )
                .await
                .is_ok()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(ok.is_ok());
        node.raft.shutdown().await.unwrap();
    }

    #[test]
    fn control_preauth_slots_are_bounded_per_ip() {
        let ip = test_ip(70);
        for _ in 0..MAX_PREAUTH_CONN_PER_IP {
            assert!(try_acquire_preauth_slot(ip));
        }
        assert!(!try_acquire_preauth_slot(ip));
        for _ in 0..MAX_PREAUTH_CONN_PER_IP {
            release_preauth_slot(ip);
        }
        assert!(!PREAUTH_CONN_PER_IP.lock().contains_key(&ip));
        release_preauth_slot(ip);
        // Shared with concurrently running tests, so only check it is usable.
        assert!(try_acquire_global_preauth_slot());
        release_global_preauth_slot();
    }
}
