//! Node health states and heartbeat tracking (ephemeral — not Raft).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Serialize;

use crate::cluster::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeHealthState {
    Ready,
    Draining,
    Down,
    Isolated,
    Joining,
    Learner,
    Leaving,
}

impl NodeHealthState {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeHealthState::Ready => "READY",
            NodeHealthState::Draining => "DRAINING",
            NodeHealthState::Down => "DOWN",
            NodeHealthState::Isolated => "ISOLATED",
            NodeHealthState::Joining => "JOINING",
            NodeHealthState::Learner => "LEARNER",
            NodeHealthState::Leaving => "LEAVING",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "READY" => Some(NodeHealthState::Ready),
            "DRAINING" => Some(NodeHealthState::Draining),
            "DOWN" => Some(NodeHealthState::Down),
            "ISOLATED" => Some(NodeHealthState::Isolated),
            "JOINING" => Some(NodeHealthState::Joining),
            "LEARNER" => Some(NodeHealthState::Learner),
            "LEAVING" => Some(NodeHealthState::Leaving),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerHealth {
    pub state: NodeHealthState,
    pub load: f64,
    pub last_seen: Instant,
    pub control_addr: String,
    pub media_addr: String,
}

pub struct HealthTracker {
    local: Mutex<NodeHealthState>,
    peers: Mutex<HashMap<NodeId, PeerHealth>>,
    miss_limit: Duration,
}

impl HealthTracker {
    pub fn new(heartbeat: Duration) -> Arc<Self> {
        Arc::new(Self {
            local: Mutex::new(NodeHealthState::Joining),
            peers: Mutex::new(HashMap::new()),
            // Quorum-aware: require several missed heartbeats before DOWN.
            miss_limit: heartbeat.saturating_mul(5).max(Duration::from_secs(2)),
        })
    }

    pub fn set_local(&self, state: NodeHealthState) {
        *self.local.lock() = state;
    }

    pub fn local(&self) -> NodeHealthState {
        *self.local.lock()
    }

    pub fn note_peer(
        &self,
        id: NodeId,
        state: NodeHealthState,
        load: f64,
        control_addr: Option<String>,
        media_addr: Option<String>,
    ) {
        let mut peers = self.peers.lock();
        let entry = peers.entry(id).or_insert(PeerHealth {
            state,
            load,
            last_seen: Instant::now(),
            control_addr: String::new(),
            media_addr: String::new(),
        });
        entry.state = state;
        entry.load = load;
        entry.last_seen = Instant::now();
        if let Some(a) = control_addr {
            entry.control_addr = a;
        }
        if let Some(a) = media_addr {
            entry.media_addr = a;
        }
    }

    /// Mark peers that have missed heartbeats as DOWN. Returns newly-down node ids
    /// (callers may propose `ReleaseOwnersForNode` after quorum confirms).
    pub fn sweep_stale(&self) -> Vec<NodeId> {
        let now = Instant::now();
        let mut down = Vec::new();
        let mut peers = self.peers.lock();
        for (id, peer) in peers.iter_mut() {
            if peer.state == NodeHealthState::Down {
                continue;
            }
            if now.duration_since(peer.last_seen) > self.miss_limit {
                peer.state = NodeHealthState::Down;
                down.push(*id);
            }
        }
        down
    }

    pub fn peers_snapshot(&self) -> Vec<(NodeId, PeerHealth)> {
        self.peers
            .lock()
            .iter()
            .map(|(id, p)| (*id, p.clone()))
            .collect()
    }

    pub fn alive_voters(&self, voter_ids: &[NodeId], local_id: NodeId) -> usize {
        let peers = self.peers.lock();
        let mut n = 0;
        for id in voter_ids {
            if *id == local_id {
                if matches!(
                    *self.local.lock(),
                    NodeHealthState::Ready | NodeHealthState::Draining | NodeHealthState::Learner
                ) {
                    n += 1;
                }
                continue;
            }
            if let Some(p) = peers.get(id)
                && p.state != NodeHealthState::Down
                && p.state != NodeHealthState::Isolated
            {
                n += 1;
            }
        }
        n
    }
}
