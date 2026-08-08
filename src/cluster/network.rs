//! Length-prefixed JSON framing over TCP (optional mTLS) for Raft RPC + admin/join.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
/// Post-auth control-plane frames (Raft RPC, admin) — smaller than snapshot path.
const MAX_CONTROL_FRAME: u32 = 8 * 1024 * 1024;
/// Bound unauthenticated frames (challenge/auth) to limit DoS before AuthOk.
const MAX_AUTH_FRAME: u32 = 8 * 1024;
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Cap concurrent control-plane handshakes (pre-auth + one request).
const MAX_CONTROL_CONN_INFLIGHT: usize = 512;
static CONTROL_CONN_INFLIGHT: AtomicUsize = AtomicUsize::new(0);
/// Bounds an entire authenticated client round trip (connect + TLS + auth +
/// write + response read). Callers like `ClusterManager::block_on_write`
/// invoke this synchronously from the RTMP poll thread when forwarding a
/// write to the leader, so an unbounded call here can stall every
/// connection's poll for as long as the OS TCP timeout.
const ROUNDTRIP_TIMEOUT: Duration = Duration::from_secs(8);

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
    },
    AdminResume {
        node_id: NodeId,
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
    },
    /// Follower-forwarded durable mutation (`ClusterCommand` as JSON).
    ClientWrite(serde_json::Value),
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
    let bytes = serde_json::to_vec(msg).map_err(|e| std::io::Error::other(e))?;
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
) -> Result<ControlMessage, std::io::Error> {
    read_frame_max(r, MAX_CONTROL_FRAME).await
}

async fn read_control_frame<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> Result<ControlMessage, std::io::Error> {
    tokio::time::timeout(CONTROL_READ_TIMEOUT, read_frame_max(r, MAX_CONTROL_FRAME))
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
    serde_json::from_slice(&buf).map_err(|e| std::io::Error::other(e))
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
    async fn roundtrip(
        &mut self,
        req: ControlMessage,
    ) -> Result<ControlMessage, RPCError<NodeId, BasicNode, typ::RaftError>> {
        let addr = &self.target_node.addr;
        authed_roundtrip_inner(
            addr,
            &self.secret,
            self.local_id,
            self.tls_client.clone(),
            req,
        )
        .await
        .map_err(|e| {
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
        match self.roundtrip(ControlMessage::RaftAppend(req)).await? {
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
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, typ::RPCError<openraft::error::InstallSnapshotError>>
    {
        let req =
            serde_json::to_value(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let msg = self
            .roundtrip(ControlMessage::RaftSnapshot(req))
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
        match self.roundtrip(ControlMessage::RaftVote(req)).await? {
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
    dyn Fn(NodeId, String, String) -> Result<(String, Vec<JoinPeerInfo>), String> + Send + Sync,
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
        let (stream, peer) = listener.accept().await?;
        if CONTROL_CONN_INFLIGHT.fetch_add(1, Ordering::AcqRel) >= MAX_CONTROL_CONN_INFLIGHT {
            CONTROL_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
            tracing::debug!(%peer, "control connection rejected: inflight limit");
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
            struct InflightGuard;
            impl Drop for InflightGuard {
                fn drop(&mut self) {
                    CONTROL_CONN_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
                }
            }
            let _inflight = InflightGuard;
            let result = async {
                if let Some(ref acc) = acceptor {
                    let mut tls = tokio::time::timeout(AUTH_TIMEOUT, acc.accept(stream))
                        .await
                        .map_err(|_| {
                            std::io::Error::new(std::io::ErrorKind::TimedOut, "tls accept timeout")
                        })??;
                    let cert_node_id = tls
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(node_id_from_peer_certs);
                    handle_control_conn(
                        &mut tls,
                        &secret,
                        local_id,
                        raft,
                        on_join,
                        on_heartbeat,
                        on_admin,
                        on_stats,
                        is_member,
                        verify_admin_proof,
                        write_forward,
                        tls_required,
                        cert_node_id,
                    )
                    .await
                } else {
                    let mut tcp = stream;
                    handle_control_conn(
                        &mut tcp,
                        &secret,
                        local_id,
                        raft,
                        on_join,
                        on_heartbeat,
                        on_admin,
                        on_stats,
                        is_member,
                        verify_admin_proof,
                        write_forward,
                        tls_required,
                        None,
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
    secret: &str,
    local_id: NodeId,
    tls_required: bool,
    cert_node_id: Option<u64>,
) -> Result<NodeId, std::io::Error> {
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
        write_frame(stream, &ControlMessage::AuthFail).await?;
        return Err(std::io::Error::other("expected Auth"));
    };
    let expected = auth_response(secret, node_id, &nonce);
    if !secrets_equal(&expected, &response) {
        write_frame(stream, &ControlMessage::AuthFail).await?;
        return Err(std::io::Error::other("auth fail"));
    }
    verify_tls_node_identity(tls_required, cert_node_id, node_id)?;
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

async fn handle_control_conn<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    secret: &str,
    local_id: NodeId,
    raft: Raft,
    on_join: JoinAcceptFn,
    on_heartbeat: Arc<dyn Fn(HeartbeatInfo) + Send + Sync>,
    on_admin: Arc<dyn Fn(ControlMessage) -> ControlMessage + Send + Sync>,
    on_stats: Arc<dyn Fn(String) -> serde_json::Value + Send + Sync>,
    is_member: MembershipFn,
    verify_admin_proof: AdminProofFn,
    write_forward: Option<ClientWriteForwardCtx>,
    tls_required: bool,
    cert_node_id: Option<u64>,
) -> Result<(), std::io::Error> {
    let peer_id =
        server_auth_handshake(stream, secret, local_id, tls_required, cert_node_id).await?;

    // One-shot request/response for this connection (matches client roundtrip).
    let msg = read_control_frame(stream).await?;
    let resp = match msg {
        ControlMessage::RaftAppend(req) => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            let parsed: AppendEntriesRequest<TypeConfig> =
                serde_json::from_value(req).map_err(|e| std::io::Error::other(e))?;
            let r = raft.append_entries(parsed).await;
            let v =
                serde_json::to_value(&r).unwrap_or_else(|_| serde_json::json!({"error":"encode"}));
            ControlMessage::RaftAppendResp(v)
        }
        ControlMessage::RaftVote(req) => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            let parsed: VoteRequest<NodeId> =
                serde_json::from_value(req).map_err(|e| std::io::Error::other(e))?;
            let r = raft.vote(parsed).await;
            let v =
                serde_json::to_value(&r).unwrap_or_else(|_| serde_json::json!({"error":"encode"}));
            ControlMessage::RaftVoteResp(v)
        }
        ControlMessage::RaftSnapshot(req) => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            let parsed: InstallSnapshotRequest<TypeConfig> =
                serde_json::from_value(req).map_err(|e| std::io::Error::other(e))?;
            let r = raft.install_snapshot(parsed).await;
            let v =
                serde_json::to_value(&r).unwrap_or_else(|_| serde_json::json!({"error":"encode"}));
            ControlMessage::RaftSnapshotResp(v)
        }
        ControlMessage::JoinRequest {
            node_id,
            control_addr,
            media_addr,
        } => {
            if node_id != peer_id {
                return Err(std::io::Error::other(
                    "join node_id must match authenticated peer",
                ));
            }
            match on_join(node_id, control_addr, media_addr) {
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
        ControlMessage::ClientWrite(req) => {
            if !is_member(peer_id) {
                return Err(std::io::Error::other("peer not in membership"));
            }
            use crate::cluster::command::ClusterCommand;
            let cmd: ClusterCommand =
                serde_json::from_value(req).map_err(|e| std::io::Error::other(e))?;
            if !cmd.allowed_on_control_plane() {
                return Err(std::io::Error::other(
                    "cluster command not allowed on control plane",
                ));
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
                    let leader_addr = ftl
                        .leader_node
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
                serde_json::from_value(req).map_err(|e| std::io::Error::other(e))?;
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
) -> Result<ControlMessage, String> {
    tokio::time::timeout(
        ROUNDTRIP_TIMEOUT,
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
) -> Result<ControlMessage, String> {
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
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<(String, Vec<JoinPeerInfo>), String> {
    send_join_with_hops(
        leader_addr,
        secret,
        local_id,
        control_addr,
        media_addr,
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
    tls_client: Option<Arc<ClientConfig>>,
    hops: u8,
) -> Result<(String, Vec<JoinPeerInfo>), String> {
    match authed_roundtrip_inner(
        leader_addr,
        secret,
        local_id,
        tls_client.clone(),
        ControlMessage::JoinRequest {
            node_id: local_id,
            control_addr: control_addr.clone(),
            media_addr: media_addr.clone(),
        },
    )
    .await?
    {
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
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<(String, Vec<JoinPeerInfo>), String> {
    match authed_roundtrip_inner(
        leader_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::JoinRequest {
            node_id: joiner_id,
            control_addr,
            media_addr,
        },
    )
    .await?
    {
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
    match authed_roundtrip_inner(
        peer_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::TopologyReq,
    )
    .await?
    {
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
    match authed_roundtrip_inner(
        peer_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::SessionCountReq { stream_id },
    )
    .await?
    {
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
    match authed_roundtrip_inner(peer_addr, secret, local_id, tls_client, msg).await? {
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
) -> Result<serde_json::Value, String> {
    match authed_roundtrip_inner(
        peer_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::StatsProxyReq { stream_id },
    )
    .await?
    {
        ControlMessage::StatsProxyResp { body } => Ok(body),
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
    tls_client: Option<Arc<ClientConfig>>,
) -> Result<crate::cluster::command::ClusterResponse, String> {
    let req = serde_json::to_value(&cmd).map_err(|e| e.to_string())?;
    match authed_roundtrip_inner(
        leader_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::ClientWrite(req),
    )
    .await?
    {
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
    match authed_roundtrip_inner(
        leader_addr,
        secret,
        local_id,
        tls_client,
        ControlMessage::ChangeMembership { req, proof },
    )
    .await?
    {
        ControlMessage::ChangeMembershipResp { ok: true, .. } => Ok(()),
        ControlMessage::ChangeMembershipResp { ok: false, message } => Err(message),
        ControlMessage::AdminErr { message } => Err(message),
        _ => Err("unexpected change membership response".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::tls_server_name_from_addr;

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
}
