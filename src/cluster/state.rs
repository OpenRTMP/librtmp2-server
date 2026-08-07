//! Ephemeral cluster meta mirrored locally (not durable via Raft).

use parking_lot::Mutex;
use std::collections::HashMap;

use crate::cluster::NodeId;

#[derive(Default)]
pub struct ClusterMeta {
    /// node_id -> (control_addr, media_addr)
    pub addrs: Mutex<HashMap<NodeId, (String, String)>>,
}

impl ClusterMeta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_addrs(&self, id: NodeId, control: String, media: String) {
        self.addrs.lock().insert(id, (control, media));
    }

    pub fn get(&self, id: NodeId) -> Option<(String, String)> {
        self.addrs.lock().get(&id).cloned()
    }

    pub fn all(&self) -> Vec<(NodeId, String, String)> {
        self.addrs
            .lock()
            .iter()
            .map(|(id, (c, m))| (*id, c.clone(), m.clone()))
            .collect()
    }
}
