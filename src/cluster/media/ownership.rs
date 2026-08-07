//! Local ownership epoch tracking for media fencing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

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
    }

    pub fn clear(&self, stream_id: &str) {
        self.owners.lock().remove(stream_id);
    }

    pub fn get(&self, stream_id: &str) -> Option<(u64, u64)> {
        self.owners.lock().get(stream_id).copied()
    }

    pub fn epoch_of(&self, app: &str, stream: &str) -> Option<u64> {
        // Ownership is keyed by stream_id; app is unused but kept for call-site clarity.
        let _ = app;
        self.get(stream).map(|(_, e)| e)
    }

    /// Accept a media frame only when epoch matches current owner epoch.
    pub fn accepts_epoch(&self, stream_id: &str, epoch: u64) -> bool {
        match self.owners.lock().get(stream_id) {
            Some((_, e)) => *e == epoch,
            None => true, // no owner recorded yet — allow until acquire lands
        }
    }
}
