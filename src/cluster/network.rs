//! Length-prefixed JSON framing over TCP for Raft RPC + admin/join.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::error::{NetworkError, RPCError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::cluster::raft::{NodeId, Raft, TypeConfig, typ};
use crate::cluster::security::{auth_response, secrets_equal};

const MAX_FRAME: u32 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    Auth {
        node_id: NodeId,
        nonce: Vec<u8>,
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
    },
    /// Follower-forwarded durable mutation (`ClusterCommand` as JSON).
    ClientWrite(serde_json::Value),
    /// `{"ok":true,"data":...}` or `{"ok":false,"error":"..."}`.
    ClientWriteResp(serde_json::Value),
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
    let len = r.read_u32().await?;
    if len > MAX_FRAME {
        return Err(std::io::Error::other("frame too large"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).map_err(|e| std::io::Error::other(e))
}

pub struct NetworkFactory {
    pub nodes: Arc<RwLock<BTreeMap<NodeId, BasicNode>>>,
    pub secret: String,
    pub local_id: NodeId,
}

impl NetworkFactory {
    pub fn new(local_id: NodeId, secret: String) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            secret,
            local_id,
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
        }
    }
}

pub struct NetworkConnection {
    target: NodeId,
    target_node: BasicNode,
    secret: String,
    local_id: NodeId,
}

impl NetworkConnection {
    async fn connect_authed(
        &self,
    ) -> Result<TcpStream, RPCError<NodeId, BasicNode, typ::RaftError>> {
        let addr = &self.target_node.addr;
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let nonce: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
        let response = auth_response(&self.secret, &nonce);
        write_frame(
            &mut stream,
            &ControlMessage::Auth {
                node_id: self.local_id,
                nonce,
                response,
            },
        )
        .await
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        match read_frame(&mut stream)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
        {
            ControlMessage::AuthOk { .. } => Ok(stream),
            _ => Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other("auth failed"),
            ))),
        }
    }

    async fn roundtrip(
        &mut self,
        req: ControlMessage,
    ) -> Result<ControlMessage, RPCError<NodeId, BasicNode, typ::RaftError>> {
        let mut stream = self.connect_authed().await?;
        write_frame(&mut stream, &req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        read_frame(&mut stream)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
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

/// Accept control-plane connections and dispatch Raft / admin messages.
pub async fn serve_control_plane(
    bind: SocketAddr,
    secret: String,
    local_id: NodeId,
    raft: Raft,
    on_join: Arc<dyn Fn(NodeId, String, String) -> Result<(), String> + Send + Sync>,
    on_heartbeat: Arc<dyn Fn(NodeId, String, f64) + Send + Sync>,
    on_admin: Arc<dyn Fn(ControlMessage) -> ControlMessage + Send + Sync>,
    on_stats: Arc<dyn Fn(String) -> serde_json::Value + Send + Sync>,
) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(bind).await?;
    tracing::info!(%bind, "cluster control plane listening");
    loop {
        let (mut stream, peer) = listener.accept().await?;
        let secret = secret.clone();
        let raft = raft.clone();
        let on_join = Arc::clone(&on_join);
        let on_heartbeat = Arc::clone(&on_heartbeat);
        let on_admin = Arc::clone(&on_admin);
        let on_stats = Arc::clone(&on_stats);
        tokio::spawn(async move {
            if let Err(e) = handle_control_conn(
                &mut stream,
                &secret,
                local_id,
                raft,
                on_join,
                on_heartbeat,
                on_admin,
                on_stats,
            )
            .await
            {
                tracing::debug!(%peer, error=%e, "control connection closed");
            }
        });
    }
}

async fn handle_control_conn(
    stream: &mut TcpStream,
    secret: &str,
    local_id: NodeId,
    raft: Raft,
    on_join: Arc<dyn Fn(NodeId, String, String) -> Result<(), String> + Send + Sync>,
    on_heartbeat: Arc<dyn Fn(NodeId, String, f64) + Send + Sync>,
    on_admin: Arc<dyn Fn(ControlMessage) -> ControlMessage + Send + Sync>,
    on_stats: Arc<dyn Fn(String) -> serde_json::Value + Send + Sync>,
) -> Result<(), std::io::Error> {
    // First frame must be Auth.
    let first = read_frame(stream).await?;
    let ControlMessage::Auth {
        node_id: _peer_id,
        nonce,
        response,
    } = first
    else {
        write_frame(stream, &ControlMessage::AuthFail).await?;
        return Ok(());
    };
    let expected = auth_response(secret, &nonce);
    if !secrets_equal(&expected, &response) {
        write_frame(stream, &ControlMessage::AuthFail).await?;
        return Ok(());
    }
    write_frame(stream, &ControlMessage::AuthOk { node_id: local_id }).await?;

    // One-shot request/response for this connection (matches client roundtrip).
    let msg = read_frame(stream).await?;
    let resp = match msg {
        ControlMessage::RaftAppend(req) => {
            let parsed: AppendEntriesRequest<TypeConfig> =
                serde_json::from_value(req).map_err(|e| std::io::Error::other(e))?;
            let r = raft.append_entries(parsed).await;
            let v =
                serde_json::to_value(&r).unwrap_or_else(|_| serde_json::json!({"error":"encode"}));
            ControlMessage::RaftAppendResp(v)
        }
        ControlMessage::RaftVote(req) => {
            let parsed: VoteRequest<NodeId> =
                serde_json::from_value(req).map_err(|e| std::io::Error::other(e))?;
            let r = raft.vote(parsed).await;
            let v =
                serde_json::to_value(&r).unwrap_or_else(|_| serde_json::json!({"error":"encode"}));
            ControlMessage::RaftVoteResp(v)
        }
        ControlMessage::RaftSnapshot(req) => {
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
        } => match on_join(node_id, control_addr, media_addr) {
            Ok(()) => ControlMessage::JoinResponse {
                ok: true,
                message: "joined as learner".into(),
            },
            Err(message) => ControlMessage::JoinResponse { ok: false, message },
        },
        ControlMessage::Heartbeat {
            node_id,
            health,
            load,
        } => {
            on_heartbeat(node_id, health, load);
            ControlMessage::AdminOk
        }
        ControlMessage::StatsProxyReq { stream_id } => ControlMessage::StatsProxyResp {
            body: on_stats(stream_id),
        },
        ControlMessage::ClientWrite(req) => {
            use crate::cluster::command::ClusterCommand;
            let cmd: ClusterCommand =
                serde_json::from_value(req).map_err(|e| std::io::Error::other(e))?;
            let body = match raft.client_write(cmd).await {
                Ok(resp) => serde_json::json!({
                    "ok": true,
                    "data": resp.data,
                }),
                Err(e) => serde_json::json!({
                    "ok": false,
                    "error": e.to_string(),
                }),
            };
            ControlMessage::ClientWriteResp(body)
        }
        admin @ (ControlMessage::AdminDrain { .. }
        | ControlMessage::AdminResume { .. }
        | ControlMessage::AdminRemove { .. }) => on_admin(admin),
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

/// Client helper: authenticated join against a bootstrap/leader node.
pub async fn send_join(
    leader_addr: &str,
    secret: &str,
    local_id: NodeId,
    control_addr: String,
    media_addr: String,
) -> Result<(), String> {
    let mut stream = TcpStream::connect(leader_addr)
        .await
        .map_err(|e| format!("connect join target: {e}"))?;
    let nonce: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
    let response = auth_response(secret, &nonce);
    write_frame(
        &mut stream,
        &ControlMessage::Auth {
            node_id: local_id,
            nonce,
            response,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    match read_frame(&mut stream).await.map_err(|e| e.to_string())? {
        ControlMessage::AuthOk { .. } => {}
        _ => return Err("join auth failed".into()),
    }
    write_frame(
        &mut stream,
        &ControlMessage::JoinRequest {
            node_id: local_id,
            control_addr,
            media_addr,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    match read_frame(&mut stream).await.map_err(|e| e.to_string())? {
        ControlMessage::JoinResponse { ok: true, .. } => Ok(()),
        ControlMessage::JoinResponse { ok: false, message } => Err(message),
        _ => Err("unexpected join response".into()),
    }
}

pub async fn send_heartbeat(
    peer_addr: &str,
    secret: &str,
    local_id: NodeId,
    health: &str,
    load: f64,
) {
    let Ok(mut stream) = TcpStream::connect(peer_addr).await else {
        return;
    };
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        let nonce: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
        let response = auth_response(secret, &nonce);
        write_frame(
            &mut stream,
            &ControlMessage::Auth {
                node_id: local_id,
                nonce,
                response,
            },
        )
        .await
        .ok()?;
        read_frame(&mut stream).await.ok()?;
        write_frame(
            &mut stream,
            &ControlMessage::Heartbeat {
                node_id: local_id,
                health: health.to_string(),
                load,
            },
        )
        .await
        .ok()?;
        let _ = read_frame(&mut stream).await;
        Some(())
    })
    .await;
}

async fn authed_roundtrip(
    addr: &str,
    secret: &str,
    local_id: NodeId,
    msg: ControlMessage,
) -> Result<ControlMessage, String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let nonce: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
    let response = auth_response(secret, &nonce);
    write_frame(
        &mut stream,
        &ControlMessage::Auth {
            node_id: local_id,
            nonce,
            response,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    match read_frame(&mut stream).await.map_err(|e| e.to_string())? {
        ControlMessage::AuthOk { .. } => {}
        _ => return Err("auth failed".into()),
    }
    write_frame(&mut stream, &msg)
        .await
        .map_err(|e| e.to_string())?;
    read_frame(&mut stream).await.map_err(|e| e.to_string())
}

pub async fn send_admin(
    peer_addr: &str,
    secret: &str,
    local_id: NodeId,
    msg: ControlMessage,
) -> Result<(), String> {
    match authed_roundtrip(peer_addr, secret, local_id, msg).await? {
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
) -> Result<serde_json::Value, String> {
    match authed_roundtrip(
        peer_addr,
        secret,
        local_id,
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
) -> Result<crate::cluster::command::ClusterResponse, String> {
    let req = serde_json::to_value(&cmd).map_err(|e| e.to_string())?;
    match authed_roundtrip(
        leader_addr,
        secret,
        local_id,
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
