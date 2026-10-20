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

    /// Health to record for a peer from its heartbeat advertisement.
    ///
    /// `DOWN` and `ISOLATED` are deliberately *not* taken from the wire:
    /// liveness is derived locally from heartbeat timeliness (`sweep_stale`)
    /// and quorum state from this node's own voter view. Accepting a peer's
    /// claim would let a heartbeat with `health=DOWN` arm `down_since` (and
    /// thus the leader's ownership release) or drop a live voter out of
    /// `alive_voters`, forging quorum loss — on a plaintext cluster any holder
    /// of `CLUSTER_SECRET` can send such a heartbeat for a member id. Benign
    /// admission states (`READY`, `DRAINING`, `JOINING`, `LEARNER`, `LEAVING`)
    /// are still kept as hints.
    pub fn from_heartbeat(advertised: &str) -> Self {
        match Self::parse(advertised) {
            Some(NodeHealthState::Down | NodeHealthState::Isolated) | None => {
                NodeHealthState::Ready
            }
            Some(other) => other,
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
        if state == NodeHealthState::Down {
            // A locally derived DOWN (sweep_stale, admin fencing) arms the
            // delayed ownership release here too: `sweep_stale` skips peers
            // that are already Down, so this is the only place `down_since`
            // can be set for them -- and `peers_ready_for_ownership_release`
            // reads nothing else. Heartbeat advertisements are sanitized by
            // [`NodeHealthState::from_heartbeat`] before reaching this method,
            // so a peer cannot arm its own release.
            self.down_since.lock().entry(id).or_insert(Instant::now());
        } else {
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

    /// Health last recorded for `id`, if this node has heard from it.
    pub fn peer_state(&self, id: NodeId) -> Option<NodeHealthState> {
        self.peers.lock().get(&id).map(|p| p.state)
    }

    /// Whether heartbeat-derived data for `id` is too old to trust.
    ///
    /// A peer that is DOWN (or that has missed the liveness window) is no
    /// longer refreshing its cached session counts: its heartbeats have
    /// stopped and nothing else clears the cache, so callers that wait for
    /// those counts to reach zero would wait forever. Callers must skip both
    /// the RPC and the cached fallback for such a peer.
    pub fn peer_report_stale(&self, id: NodeId) -> bool {
        self.peers.lock().get(&id).is_some_and(|p| {
            p.state == NodeHealthState::Down || p.last_seen.elapsed() > self.miss_limit
        })
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
            // Reachable peers still advertising ISOLATED must count too —
            // otherwise every node in a restored majority filters every
            // other recovering voter out and never transitions back to READY.
            if let Some(p) = peers.get(id)
                && p.state != NodeHealthState::Down
            {
                n += 1;
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forged_down_or_isolated_heartbeat_cannot_arm_ownership_release() {
        let tracker = HealthTracker::new(Duration::from_millis(200));

        // A heartbeat advertising DOWN must be recorded as a benign state:
        // liveness is derived locally from `last_seen`.
        tracker.note_peer(2, NodeHealthState::from_heartbeat("DOWN"), 0.0, None, None);
        assert_eq!(tracker.peer_state(2), Some(NodeHealthState::Ready));
        assert!(
            tracker
                .peers_ready_for_ownership_release(Duration::ZERO)
                .is_empty(),
            "a forged DOWN must not queue an ownership release"
        );
        assert_eq!(tracker.alive_voters(&[1, 2], 1), 2);

        // ISOLATED is likewise this node's own verdict, not the peer's.
        tracker.note_peer(
            3,
            NodeHealthState::from_heartbeat("ISOLATED"),
            0.0,
            None,
            None,
        );
        assert_eq!(tracker.peer_state(3), Some(NodeHealthState::Ready));
        assert_eq!(tracker.alive_voters(&[1, 2, 3], 1), 3);

        // Benign admission states are still kept as hints, and a genuinely
        // locally derived DOWN still arms the delayed release.
        for state in ["READY", "DRAINING", "JOINING", "LEARNER", "LEAVING"] {
            assert_eq!(
                NodeHealthState::from_heartbeat(state),
                NodeHealthState::parse(state).unwrap()
            );
        }
        assert_eq!(
            NodeHealthState::from_heartbeat("not-a-state"),
            NodeHealthState::Ready
        );
        tracker.note_peer(4, NodeHealthState::Down, 0.0, None, None);
        assert_eq!(
            tracker.peers_ready_for_ownership_release(Duration::ZERO),
            vec![4]
        );
    }

    #[test]
    fn down_or_aged_peer_reports_stale_counts() {
        let tracker = HealthTracker::new(Duration::from_millis(1));
        assert!(!tracker.peer_report_stale(9), "unknown peers are dialed");
        tracker.note_peer(2, NodeHealthState::Ready, 0.0, None, None);
        assert!(!tracker.peer_report_stale(2));
        tracker.note_peer(2, NodeHealthState::Down, 0.0, None, None);
        assert!(tracker.peer_report_stale(2));
        // A fresh heartbeat from a recovered peer makes its counts usable again.
        tracker.note_peer(2, NodeHealthState::Ready, 0.0, None, None);
        assert!(!tracker.peer_report_stale(2));

        // `last_seen` older than the miss limit is stale even before
        // `sweep_stale` flips the state to DOWN. The tracker floors the miss
        // limit at 2s, so wait a little past it.
        let tracker = HealthTracker::new(Duration::from_millis(1));
        tracker.note_peer(3, NodeHealthState::Ready, 0.0, None, None);
        std::thread::sleep(Duration::from_millis(2100));
        assert!(tracker.peer_report_stale(3));
    }
}
