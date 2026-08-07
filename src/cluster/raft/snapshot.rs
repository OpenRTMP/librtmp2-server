//! Durable app-state snapshot payload (streams/viewers/settings/owners).

use serde::{Deserialize, Serialize};

use crate::db::{Stream, StreamOwner, StreamViewer};

/// Snapshot of replicated durable state — never includes publishers/players/stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub streams: Vec<Stream>,
    pub viewers: Vec<StreamViewer>,
    pub owners: Vec<StreamOwner>,
    pub api_token: Option<String>,
    #[serde(default)]
    pub cluster_id: Option<String>,
    pub last_applied_index: Option<u64>,
}
