//! Raft application commands and responses.

use serde::{Deserialize, Serialize};

use crate::db::{Stream, StreamViewer};

/// Durable mutations replicated through OpenRaft.
///
/// All IDs, keys, and timestamps must be generated **before** `client_write`
/// so every node applies identical values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ClusterCommand {
    /// Create stream + default viewer with IDs generated **before** propose
    /// so every replica applies identical rows (no per-node keygen on apply).
    CreateStream {
        stream: Stream,
        default_viewer: StreamViewer,
    },
    BeginDeleteStream {
        id: String,
    },
    FinalizeDeleteStream {
        id: String,
    },
    SetStreamEnabled {
        id: String,
        enabled: bool,
    },
    CreateViewer {
        viewer: StreamViewer,
    },
    DeleteViewer {
        stream_id: String,
        viewer_id: String,
    },
    SetApiToken {
        token: String,
    },
    AcquireStreamOwner {
        stream_id: String,
        node_id: u64,
        epoch: u64,
        acquired_at: i64,
    },
    ReleaseStreamOwner {
        stream_id: String,
        epoch: u64,
    },
    ReleaseOwnersForNode {
        node_id: u64,
    },
    /// Seed bootstrap node from an existing standalone DB snapshot.
    SeedFromStandalone {
        streams: Vec<Stream>,
        viewers: Vec<StreamViewer>,
        api_token: Option<String>,
    },
    /// Replicated cluster identity (UUID); written on bootstrap, restored via snapshot.
    SetClusterId {
        id: String,
    },
}

/// Response returned by the state machine after applying a command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterResponse {
    Ok,
    Duplicate,
    NotFound,
    Conflict,
    /// Successful ownership acquire returns the epoch that was stored.
    OwnerEpoch(u64),
    Error(String),
}

impl ClusterCommand {
    /// Whether a `ClientWrite` over the control plane must carry an HTTP-API
    /// `admin_proof`. All durable mutations require proof so a member that
    /// only knows `CLUSTER_SECRET` cannot mutate replicated state (including
    /// stream ownership). Local leaders still apply via `raft.client_write`
    /// after their own HTTP/session path; followers mint proof from the
    /// session-hook API token when forwarding.
    pub fn requires_admin_proof(&self) -> bool {
        let _ = self;
        true
    }
}

impl ClusterResponse {
    pub fn into_coord_result(self) -> Result<(), crate::state::CoordError> {
        match self {
            ClusterResponse::Ok | ClusterResponse::OwnerEpoch(_) => Ok(()),
            ClusterResponse::Duplicate => Err(crate::state::CoordError::Duplicate),
            ClusterResponse::NotFound => Err(crate::state::CoordError::NotFound),
            ClusterResponse::Conflict => Err(crate::state::CoordError::Conflict),
            ClusterResponse::Error(msg) => Err(crate::state::CoordError::Cluster(msg)),
        }
    }

    pub fn into_owner_epoch(self) -> Result<u64, crate::state::CoordError> {
        match self {
            ClusterResponse::OwnerEpoch(e) => Ok(e),
            ClusterResponse::Ok => Err(crate::state::CoordError::Cluster(
                "missing owner epoch".into(),
            )),
            other => other.into_coord_result().map(|()| 0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClusterCommand;

    #[test]
    fn all_client_writes_require_admin_proof() {
        assert!(
            ClusterCommand::AcquireStreamOwner {
                stream_id: "s".into(),
                node_id: 1,
                epoch: 1,
                acquired_at: 0,
            }
            .requires_admin_proof()
        );
        assert!(
            ClusterCommand::ReleaseStreamOwner {
                stream_id: "s".into(),
                epoch: 1,
            }
            .requires_admin_proof()
        );
        assert!(ClusterCommand::ReleaseOwnersForNode { node_id: 1 }.requires_admin_proof());
        assert!(ClusterCommand::SetApiToken { token: "t".into() }.requires_admin_proof());
        assert!(
            ClusterCommand::CreateStream {
                stream: crate::db::Stream {
                    id: "s".into(),
                    name: "n".into(),
                    app: "live".into(),
                    publish_key: "p".into(),
                    play_key: "k".into(),
                    stats_key: "st".into(),
                    enabled: true,
                    created_at: 0,
                },
                default_viewer: crate::db::StreamViewer {
                    id: "v".into(),
                    stream_id: "s".into(),
                    name: "default".into(),
                    play_key: "k".into(),
                    enabled: true,
                    created_at: 0,
                },
            }
            .requires_admin_proof()
        );
    }
}
