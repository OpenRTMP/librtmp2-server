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
    /// Stream ids with `pending_delete=1` at snapshot time. `Stream` itself
    /// doesn't carry this flag (it's queried live), so without it a snapshot
    /// taken after `BeginDeleteStream` — followed by log compaction past
    /// that entry — would restore the stream with `pending_delete` reset to
    /// 0, silently abandoning the in-flight deletion forever.
    #[serde(default)]
    pub pending_delete_stream_ids: Vec<String>,
}
