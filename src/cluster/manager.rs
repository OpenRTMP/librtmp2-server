//! ClusterManager: Raft lifecycle, durable mutations, media hub.

use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config as RaftConfig;
use openraft::RaftMetrics;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::cluster::NodeId;
use crate::cluster::admission::{
    AdmissionController, IngressEligibility, measure_interface_utilization,
};
use crate::cluster::command::{ClusterCommand, ClusterResponse};
use crate::cluster::config::ClusterConfig;
use crate::cluster::health::{HealthTracker, NodeHealthState};
use crate::cluster::media::hub::{ExportedFrame, InjectedFrame, MediaHub};
use crate::cluster::media::ownership::OwnershipTracker;
use crate::cluster::membership::{
    add_learner, bootstrap_single_node, check_join_reseed, promote_learners, remove_node,
    seed_from_local_db,
};
use crate::cluster::metrics::{ClusterMetrics, ClusterStreamInfo, NodeInfo};
use crate::cluster::network::{self, NetworkFactory};
use crate::cluster::raft::Raft;
use crate::cluster::raft::log_store::SqliteLogStore;
use crate::cluster::raft::state_machine::SqliteStateMachine;
use crate::cluster::state::ClusterMeta;
use crate::db::{Db, Stream, StreamViewer};
use crate::state::CoordError;

const CLUSTER_ID_SETTING: &str = "cluster_id";

pub struct ClusterManager {
    pub config: ClusterConfig,
    db: Arc<Db>,
    raft: Raft,
    network: Arc<NetworkFactory>,
    health: Arc<HealthTracker>,
    admission: Arc<AdmissionController>,
    media: Arc<MediaHub>,
    meta: Arc<ClusterMeta>,
    ownership: Arc<OwnershipTracker>,
    inject_rx: Mutex<mpsc::UnboundedReceiver<InjectedFrame>>,
    rt_handle: tokio::runtime::Handle,
    last_metrics: Mutex<Option<RaftMetrics<NodeId, BasicNode>>>,
}

impl ClusterManager {
    pub async fn start(
        config: ClusterConfig,
        db: Arc<Db>,
        rt_handle: tokio::runtime::Handle,
    ) -> Result<Arc<Self>, String> {
        if !config.enabled {
            return Err("ClusterManager::start called with CLUSTER_ENABLED=false".into());
        }
        config.validate()?;

        let joining = config.join.is_some();
        check_join_reseed(&db, joining)?;

        let raft_cfg = RaftConfig {
            heartbeat_interval: config.heartbeat.as_millis() as u64,
            election_timeout_min: (config.heartbeat.as_millis() as u64)
                .saturating_mul(3)
                .max(1000),
            election_timeout_max: (config.heartbeat.as_millis() as u64)
                .saturating_mul(6)
                .max(2000),
            ..Default::default()
        };
        let raft_cfg = Arc::new(
            raft_cfg
                .validate()
                .map_err(|e| format!("raft config: {e}"))?,
        );

        let log_store = SqliteLogStore::new(Arc::clone(&db));
        let state_machine = SqliteStateMachine::new(Arc::clone(&db));
        let network = Arc::new(NetworkFactory::new(config.node_id, config.secret.clone()));
        let mut net_factory = NetworkFactory::new(config.node_id, config.secret.clone());
        net_factory.nodes = Arc::clone(&network.nodes);

        let raft = openraft::Raft::new(
            config.node_id,
            raft_cfg,
            net_factory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| format!("raft new: {e}"))?;

        let health = HealthTracker::new(config.heartbeat);
        let admission = AdmissionController::new(config.clone(), Arc::clone(&health));
        let ownership = Arc::new(OwnershipTracker::new());
        let (inject_tx, inject_rx) = mpsc::unbounded_channel();
        let media = MediaHub::new(
            config.node_id,
            config.secret.clone(),
            config.media_queue_mb,
            config.media_replicas,
            Arc::clone(&ownership),
            inject_tx,
        );
        let meta = Arc::new(ClusterMeta::new());

        let advertise = config.advertise_control();
        let media_advertise = config.advertise_media();
        meta.set_addrs(config.node_id, advertise.clone(), media_advertise.clone());
        network.upsert_node(config.node_id, advertise.clone());

        let mgr = Arc::new(Self {
            config: config.clone(),
            db: Arc::clone(&db),
            raft: raft.clone(),
            network: Arc::clone(&network),
            health: Arc::clone(&health),
            admission: Arc::clone(&admission),
            media: Arc::clone(&media),
            meta: Arc::clone(&meta),
            ownership,
            inject_rx: Mutex::new(inject_rx),
            rt_handle,
            last_metrics: Mutex::new(None),
        });

        // Control + media listeners
        {
            let mgr_join = Arc::clone(&mgr);
            let on_join: Arc<dyn Fn(NodeId, String, String) -> Result<(), String> + Send + Sync> =
                Arc::new(move |nid, ctrl, media_addr| {
                    let mgr = Arc::clone(&mgr_join);
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current()
                            .block_on(async move { mgr.accept_join(nid, ctrl, media_addr).await })
                    })
                });
            let health_hb = Arc::clone(&mgr.health);
            let on_heartbeat: Arc<dyn Fn(NodeId, String, f64) + Send + Sync> =
                Arc::new(move |nid, health_s, load| {
                    let state = NodeHealthState::parse(&health_s).unwrap_or(NodeHealthState::Ready);
                    health_hb.note_peer(nid, state, load, None, None);
                });
            let mgr_admin = Arc::clone(&mgr);
            let on_admin: Arc<
                dyn Fn(network::ControlMessage) -> network::ControlMessage + Send + Sync,
            > = Arc::new(move |msg| mgr_admin.handle_admin_control(msg));
            let mgr_stats = Arc::clone(&mgr);
            let on_stats: Arc<dyn Fn(String) -> serde_json::Value + Send + Sync> =
                Arc::new(move |stream_id| mgr_stats.local_stream_stats_json(&stream_id));

            let bind = config
                .control_socket_addr()
                .map_err(|e| format!("CLUSTER_BIND: {e}"))?;
            let raft_c = mgr.raft.clone();
            let secret = config.secret.clone();
            let local_id = config.node_id;
            tokio::spawn(async move {
                if let Err(e) = network::serve_control_plane(
                    bind,
                    secret,
                    local_id,
                    raft_c,
                    on_join,
                    on_heartbeat,
                    on_admin,
                    on_stats,
                )
                .await
                {
                    crate::log_error!("Cluster control plane stopped: {e}");
                }
            });
        }

        let media_bind = config
            .media_socket_addr()
            .map_err(|e| format!("CLUSTER_MEDIA_BIND: {e}"))?;
        media.start(media_bind).await?;

        if config.bootstrap {
            bootstrap_single_node(&raft, config.node_id, advertise.clone()).await?;
            tokio::time::sleep(Duration::from_millis(300)).await;
            if db.setting_get(CLUSTER_ID_SETTING).is_none() {
                let cid = uuid::Uuid::new_v4().to_string();
                let _ = db.setting_set(CLUSTER_ID_SETTING, &cid);
            }
            seed_from_local_db(&raft, &db).await?;
            health.set_local(NodeHealthState::Ready);
            crate::log_info!(
                "Cluster: initialized new cluster (node_id={})",
                config.node_id
            );
        } else if let Some(ref join_addr) = config.join {
            health.set_local(NodeHealthState::Joining);
            network::send_join(
                join_addr,
                &config.secret,
                config.node_id,
                advertise.clone(),
                media_advertise.clone(),
            )
            .await?;
            health.set_local(NodeHealthState::Learner);
            crate::log_info!(
                "Cluster: node {} joined as learner via {join_addr}",
                config.node_id
            );
        }

        let bg = Arc::clone(&mgr);
        tokio::spawn(async move { bg.metrics_loop().await });
        let bg = Arc::clone(&mgr);
        tokio::spawn(async move { bg.health_loop().await });

        Ok(mgr)
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    pub fn media(&self) -> &Arc<MediaHub> {
        &self.media
    }

    pub fn ownership(&self) -> &Arc<OwnershipTracker> {
        &self.ownership
    }

    pub fn health(&self) -> &Arc<HealthTracker> {
        &self.health
    }

    pub fn admission(&self) -> &Arc<AdmissionController> {
        &self.admission
    }

    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    pub fn meta(&self) -> &Arc<ClusterMeta> {
        &self.meta
    }

    pub fn enqueue_export(&self, frame: ExportedFrame) {
        let media = Arc::clone(&self.media);
        self.rt_handle.spawn(async move {
            media.fanout_local_frame(frame).await;
        });
    }

    pub fn drain_injects(&self) -> Vec<InjectedFrame> {
        let mut out = Vec::new();
        let mut rx = self.inject_rx.lock();
        while let Ok(frame) = rx.try_recv() {
            out.push(frame);
        }
        out
    }

    pub fn create_stream(&self, stream: &Stream) -> Result<(), CoordError> {
        let viewer_id = crate::keygen::keygen_stream_key(crate::keygen::PREFIX_VIEWER_ID)
            .map_err(|e| CoordError::Cluster(format!("viewer id: {e}")))?;
        let default_viewer = StreamViewer {
            id: viewer_id,
            stream_id: stream.id.clone(),
            name: "Player 1".to_string(),
            play_key: stream.play_key.clone(),
            enabled: true,
            created_at: stream.created_at,
        };
        self.block_on_write(ClusterCommand::CreateStream {
            stream: stream.clone(),
            default_viewer,
        })?
        .into_coord_result()
    }

    pub fn begin_delete_stream(&self, id: &str) -> Result<(), CoordError> {
        self.block_on_write(ClusterCommand::BeginDeleteStream { id: id.to_string() })?
            .into_coord_result()
    }

    pub fn finalize_delete_stream(&self, id: &str) -> Result<(), CoordError> {
        self.block_on_write(ClusterCommand::FinalizeDeleteStream { id: id.to_string() })?
            .into_coord_result()
    }

    pub fn set_stream_enabled(&self, id: &str, enabled: bool) -> Result<(), CoordError> {
        self.block_on_write(ClusterCommand::SetStreamEnabled {
            id: id.to_string(),
            enabled,
        })?
        .into_coord_result()
    }

    pub fn create_viewer(&self, viewer: &StreamViewer) -> Result<(), CoordError> {
        self.block_on_write(ClusterCommand::CreateViewer {
            viewer: viewer.clone(),
        })?
        .into_coord_result()
    }

    pub fn delete_viewer(&self, stream_id: &str, viewer_id: &str) -> Result<(), CoordError> {
        self.block_on_write(ClusterCommand::DeleteViewer {
            stream_id: stream_id.to_string(),
            viewer_id: viewer_id.to_string(),
        })?
        .into_coord_result()
    }

    pub fn set_api_token(&self, token: &str) -> Result<(), CoordError> {
        self.block_on_write(ClusterCommand::SetApiToken {
            token: token.to_string(),
        })?
        .into_coord_result()
    }

    pub fn acquire_stream_owner(
        &self,
        stream_id: &str,
        node_id: u64,
        epoch: u64,
        acquired_at: i64,
    ) -> Result<u64, CoordError> {
        if matches!(
            self.health.local(),
            NodeHealthState::Isolated | NodeHealthState::Down | NodeHealthState::Leaving
        ) {
            return Err(CoordError::Cluster(
                "node cannot acquire ownership without quorum".into(),
            ));
        }
        if !self.admission.should_accept_new_publish() {
            return Err(CoordError::Cluster(
                "node is draining; refusing new publish ownership".into(),
            ));
        }
        let resp = self.block_on_write(ClusterCommand::AcquireStreamOwner {
            stream_id: stream_id.to_string(),
            node_id,
            epoch,
            acquired_at,
        })?;
        let epoch = resp.into_owner_epoch()?;
        self.ownership.set(stream_id, node_id, epoch);
        crate::log_info!("Cluster: stream {stream_id} acquired by node {node_id} epoch={epoch}");
        Ok(epoch)
    }

    pub fn release_stream_owner(&self, stream_id: &str, epoch: u64) -> Result<(), CoordError> {
        self.block_on_write(ClusterCommand::ReleaseStreamOwner {
            stream_id: stream_id.to_string(),
            epoch,
        })?
        .into_coord_result()?;
        self.ownership.clear(stream_id);
        Ok(())
    }

    pub fn release_owners_for_node(&self, node_id: u64) -> Result<(), CoordError> {
        self.block_on_write(ClusterCommand::ReleaseOwnersForNode { node_id })?
            .into_coord_result()
    }

    fn block_on_write(&self, cmd: ClusterCommand) -> Result<ClusterResponse, CoordError> {
        let raft = self.raft.clone();
        let secret = self.config.secret.clone();
        let local_id = self.config.node_id;
        let meta = Arc::clone(&self.meta);
        let fut = async move {
            use openraft::error::{ClientWriteError, RaftError};

            match raft.client_write(cmd.clone()).await {
                Ok(resp) => Ok(resp.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ftl))) => {
                    let addr = ftl
                        .leader_node
                        .map(|n| n.addr)
                        .or_else(|| {
                            ftl.leader_id
                                .and_then(|id| meta.get(id).map(|(ctrl, _)| ctrl))
                        })
                        .ok_or_else(|| {
                            CoordError::Cluster("no leader available to forward write".into())
                        })?;
                    network::send_client_write(&addr, &secret, local_id, cmd)
                        .await
                        .map_err(CoordError::Cluster)
                }
                Err(e) => Err(CoordError::Cluster(format!("raft write: {e}"))),
            }
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.rt_handle.block_on(fut))
        } else {
            self.rt_handle.block_on(fut)
        }
    }

    pub async fn drain_node(&self, node_id: NodeId) -> Result<(), String> {
        if node_id == self.config.node_id {
            self.admission.force_drain();
            return Ok(());
        }
        self.forward_admin(node_id, network::ControlMessage::AdminDrain { node_id })
            .await
    }

    pub async fn resume_node(&self, node_id: NodeId) -> Result<(), String> {
        if node_id == self.config.node_id {
            self.admission.force_resume();
            return Ok(());
        }
        self.forward_admin(node_id, network::ControlMessage::AdminResume { node_id })
            .await
    }

    pub async fn remove_peer(&self, node_id: NodeId) -> Result<(), String> {
        remove_node(&self.raft, node_id)
            .await
            .map_err(|e| format!("remove node: {e}"))?;
        crate::log_info!("Cluster: removed node {node_id}");
        Ok(())
    }

    pub async fn promote_learner(&self, node_id: NodeId) -> Result<(), String> {
        promote_learners(&self.raft, [node_id])
            .await
            .map_err(|e| format!("promote: {e}"))?;
        crate::log_info!("Cluster: node {node_id} promoted to voter");
        Ok(())
    }

    async fn forward_admin(
        &self,
        node_id: NodeId,
        msg: network::ControlMessage,
    ) -> Result<(), String> {
        let addr = self
            .meta
            .get(node_id)
            .map(|(c, _)| c)
            .or_else(|| {
                self.network
                    .nodes
                    .read()
                    .get(&node_id)
                    .map(|n| n.addr.clone())
            })
            .ok_or_else(|| format!("unknown node {node_id}"))?;
        network::send_admin(&addr, &self.config.secret, self.config.node_id, msg).await
    }

    pub async fn accept_join(
        &self,
        node_id: NodeId,
        control_addr: String,
        media_addr: String,
    ) -> Result<(), String> {
        self.network.upsert_node(node_id, control_addr.clone());
        self.meta
            .set_addrs(node_id, control_addr.clone(), media_addr.clone());
        add_learner(&self.raft, node_id, control_addr.clone())
            .await
            .map_err(|e| format!("add_learner: {e}"))?;
        let _ = self.media.connect_peer(node_id, &media_addr).await;
        self.health.note_peer(
            node_id,
            NodeHealthState::Learner,
            0.0,
            Some(control_addr),
            Some(media_addr),
        );
        crate::log_info!("Cluster: node {node_id} joined as learner");
        Ok(())
    }

    fn handle_admin_control(&self, msg: network::ControlMessage) -> network::ControlMessage {
        match msg {
            network::ControlMessage::AdminDrain { node_id } if node_id == self.config.node_id => {
                self.admission.force_drain();
                network::ControlMessage::AdminOk
            }
            network::ControlMessage::AdminResume { node_id } if node_id == self.config.node_id => {
                self.admission.force_resume();
                network::ControlMessage::AdminOk
            }
            network::ControlMessage::AdminRemove { node_id } => {
                // Removals must go through Raft on the leader; acknowledge only.
                let _ = node_id;
                network::ControlMessage::AdminErr {
                    message: "use DELETE /api/v1/cluster/nodes/{id} on a voter".into(),
                }
            }
            _ => network::ControlMessage::AdminErr {
                message: "unsupported admin".into(),
            },
        }
    }

    pub fn local_stream_stats_json(&self, stream_id: &str) -> serde_json::Value {
        let pubs = self.db.publisher_list(Some(stream_id));
        let players = self.db.player_list(Some(stream_id));
        serde_json::json!({
            "stream_id": stream_id,
            "owner_node_id": self.config.node_id,
            "publishers": pubs.len(),
            "players": players.len(),
            "publisher_rows": pubs,
            "player_rows": players,
        })
    }

    /// Proxy stats from the owning node when this node is not the owner.
    pub async fn proxy_stream_stats(&self, stream_id: &str) -> Option<serde_json::Value> {
        let owner = self.db.stream_owner_get(stream_id)?;
        if owner.owner_node_id == self.config.node_id {
            return Some(self.local_stream_stats_json(stream_id));
        }
        let addr = self.meta.get(owner.owner_node_id).map(|(c, _)| c)?;
        network::send_stats_proxy(
            &addr,
            &self.config.secret,
            self.config.node_id,
            stream_id.to_string(),
        )
        .await
        .ok()
    }

    /// Notify media hub that a local player needs frames from a remote owner.
    pub fn notify_play_subscription(&self, app: &str, stream_id: &str) {
        let Some(owner) = self.db.stream_owner_get(stream_id) else {
            return;
        };
        if owner.owner_node_id == self.config.node_id {
            return;
        }
        let Some((_, media_addr)) = self.meta.get(owner.owner_node_id) else {
            return;
        };
        let media = Arc::clone(&self.media);
        let app = app.to_string();
        let stream = stream_id.to_string();
        let peer = owner.owner_node_id;
        let epoch = owner.epoch;
        self.rt_handle.spawn(async move {
            media
                .subscribe_remote(&media_addr, peer, &app, &stream, epoch)
                .await;
        });
    }

    async fn metrics_loop(self: Arc<Self>) {
        let mut metrics = self.raft.metrics();
        loop {
            let m = metrics.borrow().clone();
            let prev_leader = self
                .last_metrics
                .lock()
                .as_ref()
                .and_then(|x| x.current_leader);
            if prev_leader != m.current_leader {
                crate::log_info!(
                    "Cluster: leader changed {:?} -> {:?}",
                    prev_leader,
                    m.current_leader
                );
            }
            *self.last_metrics.lock() = Some(m);
            if metrics.changed().await.is_err() {
                break;
            }
        }
    }

    async fn health_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(self.config.heartbeat);
        loop {
            ticker.tick().await;
            if !self.config.bandwidth_interface.is_empty() {
                match measure_interface_utilization(
                    &self.config.bandwidth_interface,
                    self.config.bandwidth_max_mbps,
                ) {
                    Ok(load) => self.admission.update_load(load),
                    Err(e) => {
                        crate::log_warn!("Cluster bandwidth: {e}");
                    }
                }
            }
            let down = self.health.sweep_stale();
            let metrics = self.last_metrics.lock().clone();
            let is_leader =
                metrics.as_ref().and_then(|m| m.current_leader) == Some(self.config.node_id);
            if is_leader {
                for id in down {
                    crate::log_info!("Cluster: node {id} DOWN — releasing owners");
                    let _ = self.release_owners_for_node(id);
                }
            }
            // Emit heartbeats to known peers
            for (id, ctrl, _) in self.meta.all() {
                if id == self.config.node_id {
                    continue;
                }
                network::send_heartbeat(
                    &ctrl,
                    &self.config.secret,
                    self.config.node_id,
                    self.health.local().as_str(),
                    self.admission.current_load(),
                )
                .await;
            }
            if let Some(m) = metrics {
                let voters: Vec<NodeId> = m.membership_config.membership().voter_ids().collect();
                let alive = self.health.alive_voters(&voters, self.config.node_id);
                let need = voters.len() / 2 + 1;
                if !voters.is_empty() && alive < need {
                    if self.health.local() != NodeHealthState::Isolated {
                        crate::log_warn!("Cluster: quorum lost");
                        self.health.set_local(NodeHealthState::Isolated);
                    }
                } else if self.health.local() == NodeHealthState::Isolated {
                    crate::log_info!("Cluster: quorum restored");
                    self.health.set_local(NodeHealthState::Ready);
                }
            }
        }
    }

    pub async fn metrics(&self) -> ClusterMetrics {
        self.snapshot_metrics()
    }

    pub fn snapshot_metrics(&self) -> ClusterMetrics {
        let m = self.last_metrics.lock().clone();
        let (is_leader, leader_id, term, last_applied, voter_count, learner_count) = match m {
            Some(ref m) => {
                let voters = m.membership_config.membership().voter_ids().count();
                let learners = m.membership_config.membership().learner_ids().count();
                (
                    m.current_leader == Some(self.config.node_id),
                    m.current_leader,
                    m.current_term,
                    m.last_applied.map(|l| l.index),
                    voters,
                    learners,
                )
            }
            None => (false, None, 0, None, 0, 0),
        };
        ClusterMetrics {
            node_id: self.config.node_id,
            health: self.health.local().as_str().to_string(),
            load: self.admission.current_load(),
            is_leader,
            leader_id,
            current_term: term,
            last_applied,
            voter_count,
            learner_count,
            peer_count: self.health.peers_snapshot().len(),
            media_peers: self.media.peer_count(),
            owned_streams: self
                .db
                .stream_owner_list()
                .iter()
                .filter(|o| o.owner_node_id == self.config.node_id)
                .count(),
        }
    }

    pub fn cluster_id(&self) -> String {
        self.db
            .setting_get(CLUSTER_ID_SETTING)
            .unwrap_or_else(|| format!("node-{}", self.config.node_id))
    }

    pub fn cluster_status_json(&self) -> serde_json::Value {
        let m = self.snapshot_metrics();
        let peers = self.health.peers_snapshot();
        let healthy = peers
            .iter()
            .filter(|(_, p)| p.state != NodeHealthState::Down)
            .count()
            + if m.health != "DOWN" { 1 } else { 0 };
        let unavailable = peers
            .iter()
            .filter(|(_, p)| p.state == NodeHealthState::Down)
            .count();
        let pubs = self
            .db
            .publisher_list_all()
            .iter()
            .filter(|p| p.active)
            .count();
        let players = self
            .db
            .player_list_all()
            .iter()
            .filter(|p| p.active)
            .count();
        let load = self.admission.current_load();
        let cap = if self.config.bandwidth_max_mbps > 0.0 {
            self.config.bandwidth_max_mbps
        } else {
            0.0
        };
        let approx_mbps = load * cap;
        serde_json::json!({
            "enabled": true,
            "cluster_id": self.cluster_id(),
            "node_id": m.node_id,
            "node_name": format!("node-{}", m.node_id),
            "role": if m.is_leader { "leader" } else if self.health.local() == NodeHealthState::Learner {
                "learner"
            } else {
                "follower"
            },
            "leader_id": m.leader_id,
            "term": m.current_term,
            "quorum": self.health.local() != NodeHealthState::Isolated,
            "state": m.health.to_lowercase(),
            "voter_count": m.voter_count,
            "learner_count": m.learner_count,
            "healthy_nodes": healthy,
            "unavailable_nodes": unavailable,
            "total_publishers": pubs,
            "total_players": players,
            "total_rx_mbps": approx_mbps,
            "total_tx_mbps": approx_mbps,
            "load": {
                "rx_mbps": approx_mbps,
                "tx_mbps": approx_mbps,
                "capacity_mbps": cap,
                "admission": if self.admission.should_accept_new_publish() { "ready" } else { "draining" }
            },
            "owned_streams": m.owned_streams,
        })
    }

    pub fn health_cluster_block(&self) -> serde_json::Value {
        let status = self.cluster_status_json();
        serde_json::json!({
            "enabled": true,
            "cluster_id": status.get("cluster_id"),
            "node_id": status.get("node_id"),
            "node_name": status.get("node_name"),
            "role": status.get("role"),
            "leader_id": status.get("leader_id"),
            "term": status.get("term"),
            "quorum": status.get("quorum"),
            "state": status.get("state"),
            "load": status.get("load"),
        })
    }

    pub fn nodes_info(&self) -> Vec<NodeInfo> {
        let m = self.snapshot_metrics();
        let cap = self.config.bandwidth_max_mbps;
        let load = m.load;
        let approx = load * cap;
        let voter_ids: std::collections::HashSet<NodeId> = self
            .last_metrics
            .lock()
            .as_ref()
            .map(|rm| rm.membership_config.membership().voter_ids().collect())
            .unwrap_or_default();

        let local_state = self.health.local();
        let mut out = vec![NodeInfo {
            id: self.config.node_id,
            name: format!("node-{}", self.config.node_id),
            role: if m.is_leader {
                "leader".into()
            } else if local_state == NodeHealthState::Learner {
                "learner".into()
            } else {
                "follower".into()
            },
            voter: voter_ids.contains(&self.config.node_id) || m.is_leader || m.voter_count > 0,
            state: local_state.as_str().to_lowercase(),
            healthy: !matches!(
                local_state,
                NodeHealthState::Down | NodeHealthState::Isolated
            ),
            rx_mbps: approx,
            tx_mbps: approx,
            capacity_mbps: cap,
            publishers: self
                .db
                .publisher_list_all()
                .iter()
                .filter(|p| p.active)
                .count(),
            players: self
                .db
                .player_list_all()
                .iter()
                .filter(|p| p.active)
                .count(),
            last_heartbeat: "local".into(),
            control_addr: self.meta.get(self.config.node_id).map(|(c, _)| c),
            media_addr: self.meta.get(self.config.node_id).map(|(_, med)| med),
        }];
        // Fix local voter flag from membership when available
        if let Some(rm) = self.last_metrics.lock().as_ref() {
            let voters: std::collections::HashSet<_> =
                rm.membership_config.membership().voter_ids().collect();
            out[0].voter = voters.contains(&self.config.node_id);
        }

        for (id, peer) in self.health.peers_snapshot() {
            let age = peer.last_seen.elapsed().as_secs();
            let is_voter = voter_ids.contains(&id);
            out.push(NodeInfo {
                id,
                name: format!("node-{id}"),
                role: if Some(id) == m.leader_id {
                    "leader".into()
                } else if peer.state == NodeHealthState::Learner || !is_voter {
                    "learner".into()
                } else {
                    "follower".into()
                },
                voter: is_voter,
                state: peer.state.as_str().to_lowercase(),
                healthy: !matches!(
                    peer.state,
                    NodeHealthState::Down | NodeHealthState::Isolated
                ),
                rx_mbps: peer.load * cap,
                tx_mbps: peer.load * cap,
                capacity_mbps: cap,
                publishers: 0,
                players: 0,
                last_heartbeat: format!("{age}s ago"),
                control_addr: if peer.control_addr.is_empty() {
                    None
                } else {
                    Some(peer.control_addr)
                },
                media_addr: if peer.media_addr.is_empty() {
                    None
                } else {
                    Some(peer.media_addr)
                },
            });
        }
        out
    }

    pub fn streams_info(&self) -> Vec<ClusterStreamInfo> {
        self.db
            .stream_list()
            .into_iter()
            .map(|s| {
                let owner = self.db.stream_owner_get(&s.id);
                let subscribed = self.media.subscribed_nodes_for(&s.app, &s.id);
                let local_players = self
                    .db
                    .player_list(Some(&s.id))
                    .iter()
                    .filter(|p| p.active)
                    .count();
                // Standby = configured replica slots we are pushing to beyond subscribers.
                let standby: Vec<NodeId> = if self.config.media_replicas > 0 {
                    self.meta
                        .all()
                        .into_iter()
                        .map(|(id, _, _)| id)
                        .filter(|id| {
                            *id != self.config.node_id
                                && !subscribed.contains(id)
                                && owner
                                    .as_ref()
                                    .map(|o| o.owner_node_id != *id)
                                    .unwrap_or(true)
                        })
                        .take(self.config.media_replicas as usize)
                        .collect()
                } else {
                    Vec::new()
                };
                ClusterStreamInfo {
                    stream_id: s.id,
                    owner_node_id: owner.as_ref().map(|o| o.owner_node_id),
                    epoch: owner.as_ref().map(|o| o.epoch),
                    subscribed_nodes: subscribed,
                    standby_nodes: standby,
                    cluster_players: local_players,
                }
            })
            .collect()
    }

    pub fn aggregate_stats_json(&self) -> serde_json::Value {
        let status = self.cluster_status_json();
        serde_json::json!({
            "cluster": {
                "enabled": true,
                "cluster_id": status.get("cluster_id"),
                "total_publishers": status.get("total_publishers"),
                "total_players": status.get("total_players"),
                "total_rx_mbps": status.get("total_rx_mbps"),
                "total_tx_mbps": status.get("total_tx_mbps"),
                "healthy_nodes": status.get("healthy_nodes"),
                "voter_count": status.get("voter_count"),
            }
        })
    }

    pub async fn shutdown(&self) {
        self.health.set_local(NodeHealthState::Leaving);
        self.admission.force_drain();
        self.media.shutdown();
        let _ = self.raft.shutdown().await;
    }

    /// Sync wrapper for graceful shutdown from non-async contexts.
    pub fn shutdown_blocking(&self) {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| {
                self.rt_handle.block_on(self.shutdown());
            });
        } else {
            self.rt_handle.block_on(self.shutdown());
        }
    }
}

#[allow(dead_code)]
fn _cluster_manager_markers() {}
