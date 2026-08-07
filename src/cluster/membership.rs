//! Membership helpers: bootstrap, add learner, promote, remove.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::{BasicNode, ChangeMembers};

use crate::cluster::raft::{NodeId, Raft};
use crate::db::Db;

/// How a joining node should proceed given local DB state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinReseedAction {
    /// Empty DB — perform join handshake.
    FreshJoin,
    /// Local raft state already belongs to this deployment; skip join, resume.
    /// Caller must still verify stored `cluster_id` against the target peer.
    ResumeExisting,
}

/// Refuse join when local DB has unrelated raft or standalone conflict.
///
/// Allow restart of an existing member that still has `CLUSTER_JOIN` set when
/// local raft state already exists (same cluster resume). Only refuse an
/// unrelated populated DB / wrong-cluster leftover without raft membership.
///
/// When returning [`JoinReseedAction::ResumeExisting`], the caller must compare
/// the local `cluster_id` to the join target (e.g. via topology) before
/// applying peer metadata — see [`verify_cluster_identity`].
pub fn check_join_reseed(db: &Db, joining: bool) -> Result<JoinReseedAction, String> {
    if !joining {
        return Ok(JoinReseedAction::FreshJoin);
    }
    if db.raft_has_state() {
        // Existing member restart with CLUSTER_JOIN still configured.
        // Identity vs the join target is verified later (needs network).
        return Ok(JoinReseedAction::ResumeExisting);
    }
    if db.has_populated_app_state() {
        return Err(
            "local database already has streams without raft_* state; joining as learner would conflict. \
             Use an empty DB or bootstrap this node instead (see docs/clustering.md#reseed)"
                .into(),
        );
    }
    if db.setting_get("cluster_id").is_some() {
        return Err(
            "local database has a cluster_id but no raft state; refuse join of unrelated leftover. \
             Reseed with an empty DB (see docs/clustering.md#reseed)"
                .into(),
        );
    }
    Ok(JoinReseedAction::FreshJoin)
}

/// Fail when a persisted node would resume against a different cluster identity.
pub fn verify_cluster_identity(local: Option<&str>, remote: &str) -> Result<(), String> {
    match local {
        Some(local) if !remote.is_empty() && local != remote => Err(format!(
            "local cluster_id '{local}' does not match target cluster_id '{remote}'; \
             refuse to mix deployments. Reseed with an empty DB \
             (see docs/clustering.md#reseed)"
        )),
        _ => Ok(()),
    }
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
