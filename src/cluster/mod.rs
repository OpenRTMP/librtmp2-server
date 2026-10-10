//! Optional HA clustering (OpenRaft control plane + media mesh).
//!
//! Gated by Cargo feature `cluster`. Runtime still defaults to
//! `CLUSTER_ENABLED=false` (standalone).

// The Raft storage traits fix `StorageError` as the error type, and the media
// and control-plane task spawners take their shared handles explicitly.
#![allow(
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::result_unit_err
)]

pub mod admission;
pub mod command;
pub mod config;
pub mod health;
pub mod manager;
pub mod media;
pub mod membership;
pub mod metrics;
pub mod network;
pub mod raft;
pub mod security;
pub mod state;

pub use command::{ClusterCommand, ClusterResponse};
pub use config::ClusterConfig;
pub use health::NodeHealthState;
pub use manager::{ClusterManager, SessionHooks};
pub use raft::TypeConfig;

pub type NodeId = u64;

// Re-export media frame types for server poll loop.
pub use media::hub::{ExportedFrame, InjectedFrame};
