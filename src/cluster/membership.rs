//! Membership helpers: bootstrap, add learner, promote, remove.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::{BasicNode, ChangeMembers};

use crate::cluster::raft::{NodeId, Raft};
use crate::db::Db;

/// Refuse join when local DB has unrelated raft or standalone conflict.
pub fn check_join_reseed(db: &Db, joining: bool) -> Result<(), String> {
    if !joining {
        return Ok(());
    }
    if db.raft_has_state() {
        return Err(
            "local database already has raft_* state from another cluster; \
             reseed with an empty DB (delete server.db*) before joining"
                .into(),
        );
    }
    if db.has_populated_app_state() {
        return Err(
            "local database already has streams; joining as learner would conflict. \
             Use an empty DB or bootstrap this node instead (see docs/clustering.md#reseed)"
                .into(),
        );
    }
    Ok(())
}

pub async fn bootstrap_single_node(
    raft: &Raft,
    node_id: NodeId,
    addr: String,
) -> Result<(), String> {
    let mut nodes = BTreeMap::new();
    nodes.insert(node_id, BasicNode { addr });
    raft.initialize(nodes)
        .await
        .map_err(|e| format!("raft initialize: {e}"))?;
    Ok(())
}

pub async fn add_learner(raft: &Raft, node_id: NodeId, addr: String) -> Result<(), String> {
    let node = BasicNode { addr };
    raft.add_learner(node_id, node, true)
        .await
        .map_err(|e| format!("add_learner: {e}"))?;
    Ok(())
}

pub async fn promote_learners(
    raft: &Raft,
    learners: impl IntoIterator<Item = NodeId>,
) -> Result<(), String> {
    let members: std::collections::BTreeSet<_> = learners.into_iter().collect();
    if members.is_empty() {
        return Ok(());
    }
    raft.change_membership(ChangeMembers::AddVoterIds(members), false)
        .await
        .map_err(|e| format!("promote: {e}"))?;
    Ok(())
}

pub async fn remove_node(raft: &Raft, node_id: NodeId) -> Result<(), String> {
    let mut set = std::collections::BTreeSet::new();
    set.insert(node_id);
    raft.change_membership(ChangeMembers::RemoveVoters(set), false)
        .await
        .map_err(|e| format!("remove: {e}"))?;
    Ok(())
}

/// Seed replicated state from existing standalone rows (bootstrap only).
pub async fn seed_from_local_db(raft: &Raft, db: &Arc<Db>) -> Result<(), String> {
    let streams = db.stream_list();
    let viewers = db.viewer_list_all();
    let api_token = db.token_get().map_err(|e| e)?;
    if streams.is_empty() && viewers.is_empty() && api_token.is_none() {
        return Ok(());
    }
    use crate::cluster::command::ClusterCommand;
    raft.client_write(ClusterCommand::SeedFromStandalone {
        streams,
        viewers,
        api_token,
    })
    .await
    .map_err(|e| format!("seed write: {e}"))?;
    Ok(())
}

pub type MembershipError = crate::cluster::raft::typ::ClientWriteError;
