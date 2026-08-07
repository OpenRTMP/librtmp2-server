//! Local ownership epoch tracking for media fencing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::db::StreamOwner;

#[derive(Default)]
pub struct OwnershipTracker {
    /// stream_id -> (node_id, epoch)
    owners: Mutex<HashMap<String, (u64, u64)>>,
    /// Monotonic epoch source for this node.
    next_epoch: AtomicU64,
}

impl OwnershipTracker {
    pub fn new() -> Self {
        Self {
            owners: Mutex::new(HashMap::new()),
            next_epoch: AtomicU64::new(1),
        }
    }

    pub fn next_epoch(&self) -> u64 {
        self.next_epoch.fetch_add(1, Ordering::Relaxed)
    }

    pub fn set(&self, stream_id: &str, node_id: u64, epoch: u64) {
        self.owners
            .lock()
            .insert(stream_id.to_string(), (node_id, epoch));
        if epoch >= self.next_epoch.load(Ordering::Relaxed) {
            self.next_epoch
                .store(epoch.saturating_add(1), Ordering::Relaxed);
        }
    }

    pub fn clear(&self, stream_id: &str) {
        self.owners.lock().remove(stream_id);
    }

    pub fn clear_for_node(&self, node_id: u64) {
        self.owners.lock().retain(|_, (nid, _)| *nid != node_id);
    }

    /// Replace tracker contents from durable `stream_owners` rows (followers / restart).
    pub fn hydrate_from(&self, owners: &[StreamOwner]) {
        let mut g = self.owners.lock();
        g.clear();
        let mut max_epoch = 0u64;
        for o in owners {
            g.insert(o.stream_id.clone(), (o.owner_node_id, o.epoch));
            max_epoch = max_epoch.max(o.epoch);
        }
        drop(g);
        let cur = self.next_epoch.load(Ordering::Relaxed);
        if max_epoch >= cur {
            self.next_epoch
                .store(max_epoch.saturating_add(1), Ordering::Relaxed);
        }
    }

    /// Sync from DB list; returns (stream_id, previous (node, epoch)) for every
    /// entry whose owner changed, so the caller can unsubscribe from the
    /// actual previous owner — this map is already replaced with the new
    /// owners by the time this returns, so a lookup afterward would only
    /// ever see the new owner.
    /// Advances the local epoch counter past the highest observed epoch so a
    /// follower never reallocates a stale fencing token after release.
    pub fn sync_from(&self, owners: &[StreamOwner]) -> Vec<(String, Option<(u64, u64)>)> {
        let mut changed = Vec::new();
        let mut next = HashMap::with_capacity(owners.len());
        let mut max_epoch = 0u64;
        for o in owners {
            next.insert(o.stream_id.clone(), (o.owner_node_id, o.epoch));
            max_epoch = max_epoch.max(o.epoch);
        }
        let mut g = self.owners.lock();
        for (sid, new) in &next {
            match g.get(sid) {
                Some(old) if old == new => {}
                old => changed.push((sid.clone(), old.copied())),
            }
        }
        for sid in g.keys() {
            if !next.contains_key(sid) {
                changed.push((sid.clone(), g.get(sid).copied()));
            }
        }
        *g = next;
        drop(g);
        let cur = self.next_epoch.load(Ordering::Relaxed);
        if max_epoch >= cur {
            self.next_epoch
                .store(max_epoch.saturating_add(1), Ordering::Relaxed);
        }
        changed
    }

    /// Ensure local counter is past `epoch` (e.g. after Raft-assigned log index).
    pub fn advance_past(&self, epoch: u64) {
        let cur = self.next_epoch.load(Ordering::Relaxed);
        if epoch >= cur {
            self.next_epoch
                .store(epoch.saturating_add(1), Ordering::Relaxed);
        }
    }

    pub fn get(&self, stream_id: &str) -> Option<(u64, u64)> {
        self.owners.lock().get(stream_id).copied()
    }

    pub fn epoch_of(&self, app: &str, stream: &str) -> Option<u64> {
        // Ownership is keyed by stream_id; app is unused but kept for call-site clarity.
        let _ = app;
        self.get(stream).map(|(_, e)| e)
    }

    /// Accept a media frame only when epoch matches a known owner epoch.
    /// Unknown ownership is denied so followers fence until Raft/SQLite catches up.
    pub fn accepts_epoch(&self, stream_id: &str, epoch: u64) -> bool {
        match self.owners.lock().get(stream_id) {
            Some((_, e)) => *e == epoch,
            None => false,
        }
    }
}
