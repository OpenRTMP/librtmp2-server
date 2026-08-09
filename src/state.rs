//! Durable-state coordinator: standalone SQLite vs Raft-backed cluster.

use std::sync::Arc;

use crate::db::{Db, Stream, StreamAddError, StreamViewer};

#[cfg(feature = "cluster")]
use crate::cluster::ClusterManager;

/// Result of a durable mutation routed through [`StateCoordinator`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordError {
    Duplicate,
    NotFound,
    Conflict,
    Db,
    #[cfg(feature = "cluster")]
    Cluster(String),
}

impl std::fmt::Display for CoordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordError::Duplicate => write!(f, "duplicate"),
            CoordError::NotFound => write!(f, "not found"),
            CoordError::Conflict => write!(f, "conflict"),
            CoordError::Db => write!(f, "db error"),
            #[cfg(feature = "cluster")]
            CoordError::Cluster(msg) => write!(f, "cluster error: {msg}"),
        }
    }
}

impl std::error::Error for CoordError {}

impl From<StreamAddError> for CoordError {
    fn from(value: StreamAddError) -> Self {
        match value {
            StreamAddError::Duplicate => CoordError::Duplicate,
            StreamAddError::Db => CoordError::Db,
        }
    }
}

/// Routes durable stream/viewer/token/ownership mutations.
///
/// Reads always go through the local [`Db`]. When clustering is disabled
/// (or the `cluster` feature is not compiled), this is a thin wrapper around
/// direct DB calls so standalone behavior stays unchanged.
pub enum StateCoordinator {
    Standalone(Arc<Db>),
    #[cfg(feature = "cluster")]
    Cluster(Arc<ClusterManager>),
}

impl StateCoordinator {
    pub fn standalone(db: Arc<Db>) -> Self {
        StateCoordinator::Standalone(db)
    }

    #[cfg(feature = "cluster")]
    pub fn cluster(manager: Arc<ClusterManager>) -> Self {
        StateCoordinator::Cluster(manager)
    }

    /// Local DB handle for reads (and for bridge session rows).
    pub fn db(&self) -> &Arc<Db> {
        match self {
            StateCoordinator::Standalone(db) => db,
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.db(),
        }
    }

    /// Returns the default viewer created alongside the stream, so callers can
    /// build a response without a local read that may lag behind the write on
    /// a cluster follower.
    pub fn create_stream(&self, stream: &Stream) -> Result<StreamViewer, CoordError> {
        match self {
            StateCoordinator::Standalone(db) => db.stream_add(stream).map_err(Into::into),
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.create_stream(stream),
        }
    }

    pub fn begin_delete_stream(&self, id: &str) -> Result<(), CoordError> {
        match self {
            StateCoordinator::Standalone(db) => match db.stream_disable(id) {
                Some(true) => Ok(()),
                Some(false) => Err(CoordError::NotFound),
                None => Err(CoordError::Db),
            },
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.begin_delete_stream(id),
        }
    }

    pub fn finalize_delete_stream(&self, id: &str) -> Result<(), CoordError> {
        match self {
            StateCoordinator::Standalone(db) => match db.stream_delete(id) {
                Some(true) => Ok(()),
                Some(false) => Err(CoordError::NotFound),
                None => Err(CoordError::Db),
            },
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.finalize_delete_stream(id),
        }
    }

    pub fn set_stream_enabled(&self, id: &str, enabled: bool) -> Result<(), CoordError> {
        match self {
            StateCoordinator::Standalone(db) => {
                if db.stream_set_enabled(id, enabled) {
                    Ok(())
                } else {
                    match db.stream_get(id) {
                        crate::db::DbLookup::Missing => Err(CoordError::NotFound),
                        _ => Err(CoordError::Db),
                    }
                }
            }
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.set_stream_enabled(id, enabled),
        }
    }

    pub fn create_viewer(&self, viewer: &StreamViewer) -> Result<(), CoordError> {
        match self {
            StateCoordinator::Standalone(db) => match db.viewer_add(viewer) {
                Ok(()) => Ok(()),
                Err(crate::db::ViewerAddError::Duplicate) => Err(CoordError::Duplicate),
                Err(crate::db::ViewerAddError::Db) => Err(CoordError::Db),
            },
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.create_viewer(viewer),
        }
    }

    pub fn delete_viewer(&self, stream_id: &str, viewer_id: &str) -> Result<(), CoordError> {
        match self {
            StateCoordinator::Standalone(db) => match db.viewer_delete(stream_id, viewer_id) {
                Some(true) => Ok(()),
                Some(false) => Err(CoordError::NotFound),
                None => Err(CoordError::Db),
            },
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.delete_viewer(stream_id, viewer_id),
        }
    }

    pub fn set_api_token(&self, token: &str) -> Result<(), CoordError> {
        match self {
            StateCoordinator::Standalone(db) => match db.token_set(token) {
                Ok(_) => Ok(()),
                Err(_) => Err(CoordError::Db),
            },
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.set_api_token(token),
        }
    }

    pub fn acquire_stream_owner(
        &self,
        stream_id: &str,
        node_id: u64,
        epoch: u64,
        acquired_at: i64,
    ) -> Result<u64, CoordError> {
        match self {
            // Standalone has no multi-node fencing; accept immediately.
            StateCoordinator::Standalone(_) => {
                let _ = (stream_id, node_id, acquired_at);
                Ok(epoch)
            }
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => {
                mgr.acquire_stream_owner(stream_id, node_id, epoch, acquired_at)
            }
        }
    }

    pub fn release_stream_owner(&self, stream_id: &str, epoch: u64) -> Result<(), CoordError> {
        match self {
            StateCoordinator::Standalone(_) => {
                let _ = (stream_id, epoch);
                Ok(())
            }
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.release_stream_owner(stream_id, epoch),
        }
    }

    pub fn release_owners_for_node(&self, node_id: u64) -> Result<(), CoordError> {
        match self {
            StateCoordinator::Standalone(_) => {
                let _ = node_id;
                Ok(())
            }
            #[cfg(feature = "cluster")]
            StateCoordinator::Cluster(mgr) => mgr.release_owners_for_node(node_id),
        }
    }

    #[cfg(feature = "cluster")]
    pub fn cluster_manager(&self) -> Option<&Arc<ClusterManager>> {
        match self {
            StateCoordinator::Cluster(mgr) => Some(mgr),
            StateCoordinator::Standalone(_) => None,
        }
    }
}
