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
    /// When each peer was first marked DOWN (for delayed ownership release).
    down_since: Mutex<HashMap<NodeId, Instant>>,
    /// Peers whose ownership was already released after the DOWN grace window.
    ownership_released: Mutex<std::collections::HashSet<NodeId>>,
}

impl HealthTracker {
    pub fn new(heartbeat: Duration) -> Arc<Self> {
        Arc::new(Self {
            local: Mutex::new(NodeHealthState::Joining),
            peers: Mutex::new(HashMap::new()),
            // Quorum-aware: require several missed heartbeats before DOWN.
            miss_limit: heartbeat.saturating_mul(5).max(Duration::from_secs(2)),
            down_since: Mutex::new(HashMap::new()),
            ownership_released: Mutex::new(std::collections::HashSet::new()),
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
        if state != NodeHealthState::Down {
            self.down_since.lock().remove(&id);
            self.ownership_released.lock().remove(&id);
        }
        if let Some(a) = control_addr {
            entry.control_addr = a;
        }
        if let Some(a) = media_addr {
            entry.media_addr = a;
        }
    }

    pub fn remove(&self, id: NodeId) {
        self.peers.lock().remove(&id);
        self.down_since.lock().remove(&id);
        self.ownership_released.lock().remove(&id);
    }

    /// Mark peers that have missed heartbeats as DOWN. Returns newly-down node ids.
    pub fn sweep_stale(&self) -> Vec<NodeId> {
        let now = Instant::now();
        let mut down = Vec::new();
        let mut peers = self.peers.lock();
        let mut down_since = self.down_since.lock();
        for (id, peer) in peers.iter_mut() {
            if peer.state == NodeHealthState::Down {
                continue;
            }
            if now.duration_since(peer.last_seen) > self.miss_limit {
                peer.state = NodeHealthState::Down;
                down_since.entry(*id).or_insert(now);
                down.push(*id);
            }
        }
        down
    }

    /// Peers that have been DOWN for at least `grace` and not yet had ownership released.
    pub fn peers_ready_for_ownership_release(&self, grace: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        let down_since = self.down_since.lock();
        let released = self.ownership_released.lock();
        down_since
            .iter()
            .filter(|(id, since)| !released.contains(*id) && now.duration_since(**since) >= grace)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn mark_ownership_released(&self, id: NodeId) {
        self.ownership_released.lock().insert(id);
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
                // The local voter is always "alive" for this purpose — it's
                // the process doing the counting. In particular it must
                // still count while ISOLATED, or a node can never observe
                // its own recovery: two isolated voters in a three-voter
                // cluster would each undercount themselves and neither could
                // recognize the majority the two of them already form.
                if !matches!(*self.local.lock(), NodeHealthState::Down) {
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
