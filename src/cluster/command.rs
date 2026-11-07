//! Raft application commands and responses.

use serde::{Deserialize, Serialize};

use crate::db::{Stream, StreamViewer};

/// Default epoch fence for [`ClusterCommand::ReleaseOwnersForNode`] commands
/// written before the field existed: unbounded, i.e. the old behaviour.
fn unbounded_owners_epoch() -> u64 {
    u64::MAX
}

/// Durable mutations replicated through OpenRaft.
///
/// All IDs, keys, and timestamps must be generated **before** `client_write`
/// so every node applies identical values.
///
/// Version note: entries are JSON. Fields added later (`expected_created_at`,
/// `max_epoch`) are `#[serde(default)]`, so an older entry still applies on a
/// newer node. An *older* node applying a *newer* entry ignores those fields
/// and falls back to the pre-fence behaviour, so all voters must run the same
/// build before those fences are relied on (see `docs/clustering.md`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ClusterCommand {
    /// Create stream + default viewer with IDs generated **before** propose
    /// so every replica applies identical rows (no per-node keygen on apply).
    CreateStream {
        stream: Stream,
        default_viewer: StreamViewer,
        /// Per-row delete identity, generated before propose so every replica
        /// stores the same value. Empty for entries written before the field
        /// existed; the apply path derives a deterministic value in that case.
        #[serde(default)]
        delete_token: String,
    },
    BeginDeleteStream {
        id: String,
        /// Row identity captured when the delete was accepted. When present,
        /// the row is only marked pending while `created_at` still matches, so
        /// a retried begin-delete can never disable a replacement that reused
        /// the same stream id. Commands written before this field existed
        /// carry `None` (no identity check).
        #[serde(default)]
        expected_created_at: Option<i64>,
        /// Per-row random identity captured when the delete was accepted.
        /// Preferred over `created_at` (one-second resolution) so a
        /// same-second replacement cannot be disabled by a retried begin.
        /// Commands written before this field existed carry `None`.
        #[serde(default)]
        expected_delete_token: Option<String>,
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
        /// Release only rows with `epoch <= max_epoch`. A row the same node
        /// acquired after this fence (a rejoin) has a higher Raft epoch and
        /// survives. Commands written before the fence existed default to
        /// unbounded, preserving their original behaviour.
        #[serde(default = "unbounded_owners_epoch")]
        max_epoch: u64,
    },
    /// Seed bootstrap node from an existing standalone DB snapshot.
    SeedFromStandalone {
        streams: Vec<Stream>,
        viewers: Vec<StreamViewer>,
        /// Per-row delete identities captured from the standalone DB. Without
        /// these the state-machine apply generates a fresh random token on
        /// every replica, so a fenced delete conflicts on all non-bootstrap
        /// nodes. Empty for entries written before the field existed.
        #[serde(default)]
        delete_tokens: Vec<(String, String)>,
        api_token: Option<String>,
        /// Streams the standalone DB had left mid-delete (`pending_delete=1`);
        /// the apply path re-marks them so `FinalizeDeleteStream` can complete
        /// instead of leaving a disabled ghost stream.
        pending_delete_stream_ids: Vec<String>,
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
    /// Successful node-wide ownership release returns the log index the
    /// release was applied at. Callers use it as an upper bound for a repair
    /// pass: every ownership row acquired before this entry has a lower epoch.
    Released(u64),
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
            ClusterResponse::Ok | ClusterResponse::OwnerEpoch(_) | ClusterResponse::Released(_) => {
                Ok(())
            }
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
        assert!(
            ClusterCommand::ReleaseOwnersForNode {
                node_id: 1,
                max_epoch: u64::MAX
            }
            .requires_admin_proof()
        );
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
                delete_token: "tok".into(),
            }
            .requires_admin_proof()
        );
    }
}
