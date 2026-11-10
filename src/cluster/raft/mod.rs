//! OpenRaft type config and SQLite-backed storage.

pub mod log_store;
pub mod snapshot;
pub mod state_machine;

// `declare_raft_types!` expands to code that references `Cursor` in this scope.
#[allow(unused_imports)]
use std::io::Cursor;

use crate::cluster::command::{ClusterCommand, ClusterResponse};

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ClusterCommand,
        R = ClusterResponse,
);

pub type NodeId = u64;
pub type Raft = openraft::Raft<TypeConfig, StateMachineStore>;
pub type LogStore = log_store::SqliteLogStore;
pub type StateMachineStore = state_machine::SqliteStateMachine;

pub mod typ {
    use super::TypeConfig;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TypeConfig, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<TypeConfig, RaftError<E>>;
    pub type ClientWriteError = openraft::error::ClientWriteError<TypeConfig>;
    pub type InitializeError = openraft::error::InitializeError<TypeConfig>;
    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}
