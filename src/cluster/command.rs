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
