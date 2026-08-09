//! Cluster metrics snapshot for authenticated health / admin APIs.

use serde::Serialize;

use crate::cluster::NodeId;

#[derive(Debug, Clone, Serialize)]
pub struct ClusterMetrics {
    pub node_id: NodeId,
    pub health: String,
    pub load: f64,
    pub is_leader: bool,
    pub leader_id: Option<NodeId>,
    pub current_term: u64,
    pub last_applied: Option<u64>,
    pub voter_count: usize,
    pub learner_count: usize,
    pub peer_count: usize,
    pub media_peers: usize,
    pub owned_streams: usize,
}

/// Node row matching panel `cluster_nodes` expectations.
#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub name: String,
    pub role: String,
    pub voter: bool,
    pub state: String,
    pub healthy: bool,
    pub rx_mbps: f64,
    pub tx_mbps: f64,
    pub capacity_mbps: f64,
    pub publishers: usize,
    pub players: usize,
    pub last_heartbeat: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_addr: Option<String>,
}

/// Stream ownership row matching panel `cluster_streams` expectations.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterStreamInfo {
    pub stream_id: String,
    pub owner_node_id: Option<NodeId>,
    pub epoch: Option<u64>,
    pub subscribed_nodes: Vec<NodeId>,
    pub standby_nodes: Vec<NodeId>,
    pub cluster_players: usize,
}
