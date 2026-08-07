//! Optional HA clustering (OpenRaft control plane + media mesh).
//!
//! Gated by Cargo feature `cluster`. Runtime still defaults to
//! `CLUSTER_ENABLED=false` (standalone).

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
pub use manager::ClusterManager;
pub use raft::TypeConfig;

pub type NodeId = u64;

// Re-export media frame types for server poll loop.
pub use media::hub::{ExportedFrame, InjectedFrame};
