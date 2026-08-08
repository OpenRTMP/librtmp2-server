//! ClusterManager: Raft lifecycle, durable mutations, media hub.

use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::Config as RaftConfig;
use openraft::RaftMetrics;
use parking_lot::Mutex;
use tokio::net::TcpListener;

use crate::cluster::NodeId;
use crate::cluster::admission::{
    AdmissionController, IngressEligibility, measure_interface_utilization,
};
use crate::cluster::command::{ClusterCommand, ClusterResponse};
use crate::cluster::config::ClusterConfig;
use crate::cluster::health::{HealthTracker, NodeHealthState};
use crate::cluster::media::hub::{
    ExportQueue, ExportedFrame, InjectQueue, InjectedFrame, MediaHub,
};
use crate::cluster::media::ownership::OwnershipTracker;
use crate::cluster::membership::{
    JoinReseedAction, bootstrap_single_node, check_join_reseed, seed_from_local_db,
    verify_cluster_identity,
};
use crate::cluster::metrics::{ClusterMetrics, ClusterStreamInfo, NodeInfo};
use crate::cluster::network::{self, JoinPeerInfo, NetworkFactory};
use crate::cluster::raft::Raft;
use crate::cluster::raft::log_store::SqliteLogStore;
use crate::cluster::raft::state_machine::SqliteStateMachine;
use crate::cluster::security::{build_client_tls, build_server_tls};
use crate::cluster::state::ClusterMeta;
use crate::db::{Db, Stream, StreamViewer};
use crate::state::CoordError;

use rustls::ClientConfig;

const CLUSTER_ID_SETTING: &str = "cluster_id";
/// Written only after a bootstrap node's cluster-ID init and standalone-data
/// seed both complete. Lets a restart tell a genuinely-already-seeded node
/// apart from one that crashed between `raft.initialize()` (which already
/// left durable Raft state, so `raft_has_state()` alone can't tell) and the
/// seed actually landing.
const BOOTSTRAP_SEEDED_SETTING: &str = "bootstrap_seeded";
const RAFT_WRITE_TIMEOUT_MSG: &str = "raft write timed out";

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
    inject: Arc<InjectQueue>,
    /// Ordered export path — one worker preserves RTMP frame order.
    /// Byte-bounded (drop-oldest) so librtmp2 drain cannot grow unbounded.
    export_q: Arc<ExportQueue>,
    rt_handle: tokio::runtime::Handle,
    last_metrics: Mutex<Option<RaftMetrics<NodeId, BasicNode>>>,
    tls_client: Option<Arc<ClientConfig>>,
    /// Optional hooks into HTTP/RTMP markers + live API bearer.
    session_hooks: Mutex<Option<SessionHooks>>,
    /// Per-peer publisher/player counts from heartbeats.
    peer_session_counts: Mutex<std::collections::HashMap<NodeId, (u64, u64)>>,
    /// Per-peer per-stream player counts from heartbeats.
    peer_stream_players:
        Mutex<std::collections::HashMap<NodeId, std::collections::HashMap<String, u64>>>,
    /// Raft state-machine side-effect channel (drain/revoke/token).
    effects_rx:
        Mutex<Option<std::sync::mpsc::Receiver<crate::cluster::raft::state_machine::StateEffect>>>,
    /// Streams whose `ClearDrainStream` effect arrived while this node still
    /// had live local RTMP sessions on them (e.g. mid catch-up, having
    /// applied `DrainStream` and `ClearDrainStream` back to back before the
    /// RTMP poll loop reacted to the marker). Retried until local sessions
    /// actually drain — see `retry_pending_drain_clears`.
    pending_drain_clears: Mutex<std::collections::HashSet<String>>,
    /// (stream_id, node_id, acquired_at) for `AcquireStreamOwner` writes whose
    /// client-side 2s deadline elapsed before Raft answered. Dropping that
    /// future does not cancel an in-flight write, so it can still commit
    /// afterward; reconciled on the next health tick — see
    /// `reconcile_ambiguous_acquires`.
    pending_ambiguous_acquires: Mutex<Vec<(String, u64, i64)>>,
    /// (app, stream_id) -> owner node this node currently holds a
    /// standby-replica media subscription against. Reconciled every health
    /// tick against the deterministic replica placement so CLUSTER_MEDIA_REPLICAS
    /// actually causes standby nodes to receive continuous media, not just be
    /// reported as replicas with no data behind them.
    standby_subs: Mutex<std::collections::HashMap<(String, String), NodeId>>,
    state_machine: SqliteStateMachine,
}

/// Shared with AppState so Raft applies can mark deleted streams / revoked viewers
/// and refresh the in-memory bearer token on every node.
#[derive(Clone)]
pub struct SessionHooks {
    pub deleted_streams: Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
    pub revoked_viewers: Arc<parking_lot::Mutex<std::collections::HashSet<String>>>,
    pub api_token: Arc<parking_lot::RwLock<String>>,
    /// Force local RTMP publishers off a stream (stale epoch after partition).
    pub force_unpublish_stream: Arc<dyn Fn(&str) + Send + Sync>,
    /// Optional local live-session counter for drain wait aggregation.
    pub local_stream_sessions: Arc<dyn Fn(&str) -> u64 + Send + Sync>,
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
        let join_action = check_join_reseed(&db, joining)?;

        let (tls_server, tls_client) = if config.tls_enabled {
            let server = build_server_tls(
                &config.tls_cert_file,
                &config.tls_key_file,
                &config.tls_ca_file,
            )?;
            let client = build_client_tls(
                &config.tls_cert_file,
                &config.tls_key_file,
                &config.tls_ca_file,
            )?;
            (Some(server), Some(client))
        } else {
            (None, None)
        };

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
        let state_machine = SqliteStateMachine::new(Arc::clone(&db))
            .map_err(|e| format!("state machine init: {e}"))?;
        let sm_handle = state_machine.clone();
        let (effects_tx, effects_rx) = std::sync::mpsc::channel();
        sm_handle.set_effects_tx(effects_tx);
        let network = Arc::new(NetworkFactory::new(
            config.node_id,
            config.secret.clone(),
            tls_client.clone(),
        ));
        let mut net_factory =
            NetworkFactory::new(config.node_id, config.secret.clone(), tls_client.clone());
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
        ownership.hydrate_from(&db.stream_owner_list());
        let inject = InjectQueue::new(config.media_queue_mb);
        let meta = Arc::new(ClusterMeta::new());
        let sm_for_media = sm_handle.clone();
        let local_node = config.node_id;
        let meta_for_media = Arc::clone(&meta);
        let is_peer_allowed: crate::cluster::media::MediaMembershipFn = Arc::new(move |id| {
            if id == local_node {
                return true;
            }
            // Control-plane join records addresses before Raft membership is visible.
            if meta_for_media.get(id).is_some() {
                return true;
            }
            sm_for_media
                .last_membership()
                .membership()
                .nodes()
                .any(|(nid, _)| *nid == id)
        });
        let media = MediaHub::new(
            config.node_id,
            config.secret.clone(),
            config.media_queue_mb,
            config.media_replicas,
            Arc::clone(&ownership),
            Arc::clone(&inject),
            tls_server.clone(),
            tls_client.clone(),
            is_peer_allowed,
        );
        // Byte-bounded ordered export (drop-oldest on overload) — see ExportQueue.
        let export_q = ExportQueue::new(config.media_queue_mb);
        {
            let media_export = Arc::clone(&media);
            let export_q = Arc::clone(&export_q);
            tokio::spawn(async move {
                loop {
                    let frames = export_q.wait_and_drain().await;
                    for frame in frames {
                        media_export.fanout_local_frame(frame).await;
                    }
                }
            });
        }
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
            inject,
            export_q,
            rt_handle,
            last_metrics: Mutex::new(None),
            tls_client: tls_client.clone(),
            session_hooks: Mutex::new(None),
            peer_session_counts: Mutex::new(std::collections::HashMap::new()),
            peer_stream_players: Mutex::new(std::collections::HashMap::new()),
            effects_rx: Mutex::new(Some(effects_rx)),
            pending_drain_clears: Mutex::new(std::collections::HashSet::new()),
            pending_ambiguous_acquires: Mutex::new(Vec::new()),
            standby_subs: Mutex::new(std::collections::HashMap::new()),
            state_machine: sm_handle.clone(),
        });

        // Control + media listeners
        {
            let mgr_join = Arc::clone(&mgr);
            let on_join: network::JoinAcceptFn = Arc::new(move |nid, ctrl, media_addr| {
                let mgr = Arc::clone(&mgr_join);
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(async move { mgr.accept_join(nid, ctrl, media_addr).await })
                })
            });
            let health_hb = Arc::clone(&mgr.health);
            let meta_hb = Arc::clone(&mgr.meta);
            let media_hb = Arc::clone(&mgr.media);
            let network_hb = Arc::clone(&mgr.network);
            let counts_hb = Arc::clone(&mgr);
            let on_heartbeat: Arc<dyn Fn(network::HeartbeatInfo) + Send + Sync> =
                Arc::new(move |info: network::HeartbeatInfo| {
                    if !counts_hb.peer_in_membership(info.node_id) {
                        return;
                    }
                    let state =
                        NodeHealthState::parse(&info.health).unwrap_or(NodeHealthState::Ready);
                    let ctrl = if info.control_addr.is_empty() {
                        None
                    } else {
                        Some(info.control_addr.clone())
                    };
                    let media_a = if info.media_addr.is_empty() {
                        None
                    } else {
                        Some(info.media_addr.clone())
                    };
                    health_hb.note_peer(
                        info.node_id,
                        state,
                        info.load,
                        ctrl.clone(),
                        media_a.clone(),
                    );
                    if let (Some(c), Some(m)) = (ctrl, media_a) {
                        meta_hb.set_addrs(info.node_id, c.clone(), m.clone());
                        network_hb.upsert_node(info.node_id, c);
                        let hub = Arc::clone(&media_hb);
                        let mid = info.node_id;
                        tokio::spawn(async move {
                            let _ = hub.connect_peer(mid, &m).await;
                        });
                    }
                    counts_hb
                        .peer_session_counts
                        .lock()
                        .insert(info.node_id, (info.publishers, info.players));
                    counts_hb
                        .peer_stream_players
                        .lock()
                        .insert(info.node_id, info.stream_players.into_iter().collect());
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
            // Bind before spawn so startup fails hard on port conflict (same as media).
            let listener = TcpListener::bind(bind)
                .await
                .map_err(|e| format!("CLUSTER_BIND {bind}: {e}"))?;
            let raft_c = mgr.raft.clone();
            let secret = config.secret.clone();
            let local_id = config.node_id;
            let tls_srv = tls_server.clone();
            let mgr_member = Arc::clone(&mgr);
            let is_member: network::MembershipFn =
                Arc::new(move |id| mgr_member.peer_in_membership(id));
            let mgr_proof = Arc::clone(&mgr);
            let verify_admin_proof: network::AdminProofFn =
                Arc::new(move |proof, payload| mgr_proof.verify_admin_proof(proof, payload));
            let write_forward = network::ClientWriteForwardCtx {
                secret: config.secret.clone(),
                local_id: config.node_id,
                meta: Arc::clone(&mgr.meta),
                tls_client: tls_client.clone(),
            };
            tokio::spawn(async move {
                if let Err(e) = network::serve_control_plane_listener(
                    listener,
                    secret,
                    local_id,
                    raft_c,
                    on_join,
                    on_heartbeat,
                    on_admin,
                    on_stats,
                    is_member,
                    verify_admin_proof,
                    Some(write_forward),
                    tls_srv,
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

        // Restore control/media peers from persisted Raft membership when resuming.
        mgr.restore_topology_from_membership(&sm_handle);

        if config.bootstrap {
            if db.raft_has_state() {
                // Existing voter restart — do not re-initialize OpenRaft storage.
                if let Some(ref join_addr) = config.join {
                    mgr.refresh_topology_from_any(join_addr).await?;
                }
                if db.setting_get(BOOTSTRAP_SEEDED_SETTING).is_none() {
                    // raft_has_state() only proves raft.initialize() ran; a
                    // crash between that and the seed landing would otherwise
                    // permanently strand this node's pre-existing standalone
                    // streams/credentials outside the cluster. Finish the
                    // interrupted bootstrap — both steps are idempotent.
                    crate::log_warn!(
                        "Cluster: bootstrap node {} has Raft state but never finished seeding — completing it now",
                        config.node_id
                    );
                    if db.setting_get(CLUSTER_ID_SETTING).is_none() {
                        let cid = uuid::Uuid::new_v4().to_string();
                        raft.client_write(ClusterCommand::SetClusterId { id: cid })
                            .await
                            .map_err(|e| format!("set cluster_id: {e}"))?;
                    }
                    seed_from_local_db(&raft, &db).await?;
                    let _ = db.setting_set(BOOTSTRAP_SEEDED_SETTING, "1");
                }
                health.set_local(NodeHealthState::Ready);
                crate::log_info!(
                    "Cluster: resumed bootstrap node {} with existing Raft state",
                    config.node_id
                );
            } else {
                bootstrap_single_node(&raft, config.node_id, advertise.clone()).await?;
                tokio::time::sleep(Duration::from_millis(300)).await;
                if db.setting_get(CLUSTER_ID_SETTING).is_none() {
                    let cid = uuid::Uuid::new_v4().to_string();
                    raft.client_write(ClusterCommand::SetClusterId { id: cid })
                        .await
                        .map_err(|e| format!("set cluster_id: {e}"))?;
                }
                seed_from_local_db(&raft, &db).await?;
                let _ = db.setting_set(BOOTSTRAP_SEEDED_SETTING, "1");
                health.set_local(NodeHealthState::Ready);
                crate::log_info!(
                    "Cluster: initialized new cluster (node_id={})",
                    config.node_id
                );
            }
        } else if let Some(ref join_addr) = config.join {
            match join_action {
                JoinReseedAction::ResumeExisting => {
                    mgr.refresh_topology_from_any(join_addr).await?;
                    health.set_local(NodeHealthState::Ready);
                    crate::log_info!(
                        "Cluster: resuming existing member node {} (CLUSTER_JOIN set but local raft state present)",
                        config.node_id
                    );
                }
                JoinReseedAction::FreshJoin => {
                    health.set_local(NodeHealthState::Joining);
                    let (cluster_id, peers) = network::send_join(
                        join_addr,
                        &config.secret,
                        config.node_id,
                        advertise.clone(),
                        media_advertise.clone(),
                        tls_client.clone(),
                    )
                    .await?;
                    let local_id = db.setting_get(CLUSTER_ID_SETTING);
                    verify_cluster_identity(local_id.as_deref(), &cluster_id)?;
                    if !cluster_id.is_empty() {
                        let _ = db.setting_set(CLUSTER_ID_SETTING, &cluster_id);
                    }
                    for p in peers {
                        if p.node_id == config.node_id {
                            continue;
                        }
                        mgr.network.upsert_node(p.node_id, p.control_addr.clone());
                        mgr.meta
                            .set_addrs(p.node_id, p.control_addr.clone(), p.media_addr.clone());
                        let _ = mgr.media.connect_peer(p.node_id, &p.media_addr).await;
                        mgr.health.note_peer(
                            p.node_id,
                            NodeHealthState::Ready,
                            0.0,
                            Some(p.control_addr),
                            Some(p.media_addr),
                        );
                    }
                    health.set_local(NodeHealthState::Learner);
                    crate::log_info!(
                        "Cluster: node {} joined as learner via {join_addr}",
                        config.node_id
                    );
                }
            }
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

    pub fn should_accept_new_play(&self) -> bool {
        self.admission.should_accept_new_play()
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
        // Single ordered worker — do not spawn per-frame tasks (reorder risk).
        self.export_q.push(frame);
    }

    pub fn drain_injects(&self) -> Vec<InjectedFrame> {
        self.inject.drain()
    }

    /// Returns the default viewer applied by the state machine. Since its ID
    /// and fields are generated here (before propose) rather than read back
    /// from the local DB, the caller gets an authoritative value even when
    /// this node is a follower whose local apply may still be lagging behind
    /// the leader's commit.
    pub fn create_stream(&self, stream: &Stream) -> Result<StreamViewer, CoordError> {
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
            default_viewer: default_viewer.clone(),
        })?
        .into_coord_result()?;
        Ok(default_viewer)
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
        let resp = match self.block_on_write(ClusterCommand::AcquireStreamOwner {
            stream_id: stream_id.to_string(),
            node_id,
            epoch,
            acquired_at,
        }) {
            Ok(resp) => resp,
            Err(CoordError::Cluster(msg)) if msg == RAFT_WRITE_TIMEOUT_MSG => {
                // The caller gives up on this acquire, but the write may
                // still commit later; check on the next health tick and
                // release it if it does, rather than leaving a ghost owner
                // no local publisher is actually using.
                self.pending_ambiguous_acquires.lock().push((
                    stream_id.to_string(),
                    node_id,
                    acquired_at,
                ));
                return Err(CoordError::Cluster(msg));
            }
            Err(e) => return Err(e),
        };
        let epoch = resp.into_owner_epoch()?;
        self.ownership.set(stream_id, node_id, epoch);
        self.ownership.advance_past(epoch);
        self.refresh_subscriptions_for(&[stream_id.to_string()]);
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
        self.refresh_subscriptions_for(&[stream_id.to_string()]);
        Ok(())
    }

    pub fn release_owners_for_node(&self, node_id: u64) -> Result<(), CoordError> {
        let affected: Vec<String> = self
            .db
            .stream_owner_list()
            .into_iter()
            .filter(|o| o.owner_node_id == node_id)
            .map(|o| o.stream_id)
            .collect();
        self.block_on_write(ClusterCommand::ReleaseOwnersForNode { node_id })?
            .into_coord_result()?;
        self.ownership.clear_for_node(node_id);
        self.ownership.hydrate_from(&self.db.stream_owner_list());
        self.refresh_subscriptions_for(&affected);
        Ok(())
    }

    fn block_on_write(&self, cmd: ClusterCommand) -> Result<ClusterResponse, CoordError> {
        let raft = self.raft.clone();
        let secret = self.config.secret.clone();
        let local_id = self.config.node_id;
        let meta = Arc::clone(&self.meta);
        let tls_client = self.tls_client.clone();
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
                    network::send_client_write(&addr, &secret, local_id, cmd, tls_client)
                        .await
                        .map_err(CoordError::Cluster)
                }
                Err(e) => Err(CoordError::Cluster(format!("raft write: {e}"))),
            }
        };
        const RAFT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
        let timed = async move {
            tokio::time::timeout(RAFT_WRITE_TIMEOUT, fut)
                .await
                .map_err(|_| CoordError::Cluster(RAFT_WRITE_TIMEOUT_MSG.into()))?
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.rt_handle.block_on(timed))
        } else {
            self.rt_handle.block_on(timed)
        }
    }

    fn peer_in_membership(&self, node_id: NodeId) -> bool {
        if let Some(rm) = self.last_metrics.lock().as_ref() {
            return rm
                .membership_config
                .membership()
                .nodes()
                .any(|(id, _)| *id == node_id);
        }
        self.state_machine
            .last_membership()
            .membership()
            .nodes()
            .any(|(id, _)| *id == node_id)
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
        // Commit the membership removal first. Releasing ownership before
        // this could commit lets other nodes reclaim (and start publishing)
        // streams while a failed membership change left `node_id` still a
        // full cluster member — possibly still publishing them locally under
        // its now-stale epoch.
        self.change_membership_forwarded(ChangeMembers::RemoveVoters(
            std::collections::BTreeSet::from([node_id]),
        ))
        .await
        .map_err(|e| format!("remove node: {e}"))?;
        if let Err(e) = self.release_owners_for_node(node_id) {
            crate::log_warn!(
                "Cluster: node {node_id} removed from membership but releasing its stream \
                 ownership failed ({e:?}); affected streams stay assigned to the removed node \
                 until the next health sweep or a manual retry"
            );
        }
        self.media.disconnect_peer(node_id);
        self.meta.remove(node_id);
        self.network.nodes.write().remove(&node_id);
        crate::log_info!("Cluster: removed node {node_id}");
        Ok(())
    }

    pub async fn promote_learner(&self, node_id: NodeId) -> Result<(), String> {
        self.change_membership_forwarded(ChangeMembers::AddVoterIds(
            std::collections::BTreeSet::from([node_id]),
        ))
        .await
        .map_err(|e| format!("promote: {e}"))?;
        crate::log_info!("Cluster: node {node_id} promoted to voter");
        Ok(())
    }

    /// Run `change_membership` locally or forward to the current leader.
    async fn change_membership_forwarded(
        &self,
        change: ChangeMembers<NodeId, BasicNode>,
    ) -> Result<(), String> {
        use openraft::error::{ClientWriteError, RaftError};

        let proof = self.membership_admin_proof(&change)?;

        match self.raft.change_membership(change.clone(), false).await {
            Ok(_) => Ok(()),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ftl))) => {
                let leader_addr = ftl
                    .leader_node
                    .map(|n| n.addr)
                    .or_else(|| {
                        ftl.leader_id
                            .and_then(|id| self.meta.get(id).map(|(ctrl, _)| ctrl))
                    })
                    .ok_or_else(|| "forward_to_leader: no leader address available".to_string())?;
                network::send_change_membership(
                    &leader_addr,
                    &self.config.secret,
                    self.config.node_id,
                    change,
                    proof,
                    self.tls_client.clone(),
                )
                .await
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn membership_admin_proof(
        &self,
        change: &ChangeMembers<NodeId, BasicNode>,
    ) -> Result<String, String> {
        let req = serde_json::to_string(change).map_err(|e| e.to_string())?;
        let token = self
            .session_hooks
            .lock()
            .as_ref()
            .map(|h| h.api_token.read().clone())
            .filter(|t| !t.is_empty())
            .ok_or_else(|| "API token unavailable for membership change".to_string())?;
        Ok(crate::cluster::security::admin_proof(&token, &req))
    }

    fn verify_admin_proof(&self, proof: &str, payload: &str) -> bool {
        let Some(hooks) = self.session_hooks.lock().as_ref() else {
            return false;
        };
        let token = hooks.api_token.read();
        if token.is_empty() {
            return false;
        }
        crate::cluster::security::secrets_equal(
            &crate::cluster::security::admin_proof(&token, payload),
            proof,
        )
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
        network::send_admin(
            &addr,
            &self.config.secret,
            self.config.node_id,
            msg,
            self.tls_client.clone(),
        )
        .await
    }

    pub async fn accept_join(
        &self,
        node_id: NodeId,
        control_addr: String,
        media_addr: String,
    ) -> Result<(String, Vec<JoinPeerInfo>), String> {
        self.network.upsert_node(node_id, control_addr.clone());
        self.meta
            .set_addrs(node_id, control_addr.clone(), media_addr.clone());

        use openraft::error::{ClientWriteError, RaftError};
        match self
            .raft
            .add_learner(
                node_id,
                BasicNode {
                    addr: control_addr.clone(),
                },
                true,
            )
            .await
        {
            Ok(_) => {}
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ftl))) => {
                let leader_addr = ftl
                    .leader_node
                    .map(|n| n.addr)
                    .or_else(|| {
                        ftl.leader_id
                            .and_then(|id| self.meta.get(id).map(|(ctrl, _)| ctrl))
                    })
                    .ok_or_else(|| "forward_to_leader: no leader address available".to_string())?;
                return network::forward_join(
                    &leader_addr,
                    &self.config.secret,
                    self.config.node_id,
                    node_id,
                    control_addr,
                    media_addr,
                    self.tls_client.clone(),
                )
                .await;
            }
            Err(e) => {
                let msg = e.to_string();
                if !(msg.contains("already") || msg.contains("Exists") || msg.contains("exist")) {
                    return Err(format!("add_learner: {e}"));
                }
                // A reseeded/replaced node can rejoin under its previous node
                // ID with a fresh, empty database — often at a new address.
                // Treating "already exists" as a no-op success would leave
                // Raft membership (and thus replication) still targeting the
                // old address, so this blank learner would never catch up.
                // SetNodes replaces the registered address for an existing
                // member, which lets OpenRaft's normal log/snapshot
                // catch-up target the right place.
                crate::log_warn!(
                    "Cluster: node {node_id} rejoined with an existing ID at {control_addr}; \
                     updating registered address for catch-up (likely a reseeded replacement)"
                );
                self.raft
                    .change_membership(
                        ChangeMembers::SetNodes(std::collections::BTreeMap::from([(
                            node_id,
                            BasicNode {
                                addr: control_addr.clone(),
                            },
                        )])),
                        false,
                    )
                    .await
                    .map_err(|e| format!("SetNodes for rejoined node {node_id}: {e}"))?;
            }
        }

        let _ = self.media.connect_peer(node_id, &media_addr).await;
        self.health.note_peer(
            node_id,
            NodeHealthState::Learner,
            0.0,
            Some(control_addr.clone()),
            Some(media_addr.clone()),
        );
        let peers: Vec<JoinPeerInfo> = self
            .meta
            .all()
            .into_iter()
            .map(|(id, ctrl, media)| JoinPeerInfo {
                node_id: id,
                control_addr: ctrl,
                media_addr: media,
            })
            .collect();
        crate::log_info!("Cluster: node {node_id} joined as learner");
        Ok((self.cluster_id(), peers))
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
                let _ = node_id;
                network::ControlMessage::AdminErr {
                    message: "use DELETE /api/v1/cluster/nodes/{id} on a voter".into(),
                }
            }
            network::ControlMessage::DrainStream { stream_id } => {
                self.mark_stream_draining(&stream_id);
                network::ControlMessage::AdminOk
            }
            network::ControlMessage::RevokeViewer { viewer_id } => {
                self.mark_viewer_revoked(&viewer_id);
                network::ControlMessage::AdminOk
            }
            network::ControlMessage::SessionCountReq { stream_id } => {
                let count = self.local_live_sessions(&stream_id);
                network::ControlMessage::SessionCountResp { count }
            }
            network::ControlMessage::TopologyReq => {
                let peers: Vec<JoinPeerInfo> = self
                    .meta
                    .all()
                    .into_iter()
                    .map(|(id, ctrl, media)| JoinPeerInfo {
                        node_id: id,
                        control_addr: ctrl,
                        media_addr: media,
                    })
                    .collect();
                network::ControlMessage::TopologyResp {
                    ok: true,
                    message: "ok".into(),
                    cluster_id: self.cluster_id(),
                    peers,
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

    pub fn register_session_hooks(&self, hooks: SessionHooks) {
        *self.session_hooks.lock() = Some(hooks);
    }

    fn mark_stream_draining(&self, stream_id: &str) {
        if let Some(hooks) = self.session_hooks.lock().as_ref() {
            hooks.deleted_streams.lock().insert(stream_id.to_string());
        }
    }

    fn mark_stream_drain_clear(&self, stream_id: &str) {
        if self.local_live_sessions(stream_id) > 0 {
            // This node can apply DrainStream and ClearDrainStream back to
            // back while catching up (e.g. it was unreachable when the
            // leader counted remote sessions as zero and finalized the
            // delete), before the RTMP poll loop has had a chance to react
            // to the marker. Keep it set — and thus keep kicking local
            // sessions — until they actually drain.
            self.pending_drain_clears
                .lock()
                .insert(stream_id.to_string());
            return;
        }
        if let Some(hooks) = self.session_hooks.lock().as_ref() {
            hooks.deleted_streams.lock().remove(stream_id);
        }
    }

    /// Retry streams whose drain marker was kept because this node still had
    /// live local sessions when `ClearDrainStream` was applied. Called on
    /// every health-loop tick so the marker still gets cleared promptly once
    /// those sessions actually disconnect.
    fn retry_pending_drain_clears(&self) {
        let candidates: Vec<String> = self.pending_drain_clears.lock().iter().cloned().collect();
        for stream_id in candidates {
            if self.local_live_sessions(&stream_id) == 0 {
                self.pending_drain_clears.lock().remove(&stream_id);
                if let Some(hooks) = self.session_hooks.lock().as_ref() {
                    hooks.deleted_streams.lock().remove(&stream_id);
                }
            }
        }
    }

    /// Check each acquire whose client-side deadline elapsed for whether the
    /// write actually landed after all; if it did, release it — the caller
    /// already gave up and no local publisher is using that reservation.
    fn reconcile_ambiguous_acquires(&self) {
        let pending: Vec<(String, u64, i64)> =
            self.pending_ambiguous_acquires.lock().drain(..).collect();
        for (stream_id, node_id, acquired_at) in pending {
            let Some(owner) = self.db.stream_owner_get(&stream_id) else {
                continue;
            };
            if owner.owner_node_id != node_id || owner.acquired_at != acquired_at {
                // Either it never committed, or a different (later, genuine)
                // acquire has since taken the route — leave it alone.
                continue;
            }
            crate::log_warn!(
                "Cluster: acquire for stream {stream_id} timed out client-side but later \
                 committed at epoch={}; releasing the unclaimed reservation",
                owner.epoch
            );
            if let Err(e) = self.release_stream_owner(&stream_id, owner.epoch) {
                crate::log_warn!(
                    "Cluster: failed to release ambiguously-committed acquire for {stream_id}: {e:?}"
                );
                // Still ambiguous — retry on the next tick.
                self.pending_ambiguous_acquires
                    .lock()
                    .push((stream_id, node_id, acquired_at));
            }
        }
    }

    /// After returning to READY from an isolation/partition, this node's
    /// local active publishers may still hold `stream_owners` rows and keep
    /// exporting under an epoch a majority already released (and possibly
    /// reassigned) while this node was cut off. Reacquire ownership for each
    /// such stream so exports resume under a fresh, correctly fenced epoch.
    /// A conflict (a replacement owner already claimed it) forces the local
    /// publisher off via `session_hooks.force_unpublish_stream` when hooks are
    /// registered (normal RTMP runtime); otherwise the conflict is only logged.
    fn reconcile_publishers_after_isolation(&self) {
        let node_id = self.config.node_id;
        for p in self.db.publisher_list_all() {
            let owner = self.db.stream_owner_get(&p.stream_id);
            if owner.as_ref().map(|o| o.owner_node_id) == Some(node_id) {
                continue;
            }
            match self.acquire_stream_owner(&p.stream_id, node_id, 0, crate::db::now_ts()) {
                Ok(epoch) => {
                    crate::log_info!(
                        "Cluster: reacquired ownership of stream {} at epoch {epoch} after isolation cleared",
                        p.stream_id
                    );
                }
                Err(e) => {
                    crate::log_warn!(
                        "Cluster: could not reacquire ownership of stream {} after isolation cleared ({e:?}); \
                         forcing local publisher off",
                        p.stream_id
                    );
                    if let Some(hooks) = self.session_hooks.lock().as_ref() {
                        (hooks.force_unpublish_stream)(&p.stream_id);
                        self.mark_stream_draining(&p.stream_id);
                    }
                }
            }
        }
    }

    fn mark_viewer_revoked(&self, viewer_id: &str) {
        if let Some(hooks) = self.session_hooks.lock().as_ref() {
            hooks.revoked_viewers.lock().insert(viewer_id.to_string());
        }
    }

    fn apply_api_token(&self, token: &str) {
        if let Some(hooks) = self.session_hooks.lock().as_ref() {
            *hooks.api_token.write() = token.to_string();
        }
    }

    fn local_live_sessions(&self, stream_id: &str) -> u64 {
        if let Some(hooks) = self.session_hooks.lock().as_ref() {
            return (hooks.local_stream_sessions)(stream_id);
        }
        0
    }

    pub fn poll_side_effects(&self) {
        self.process_state_effects();
    }

    fn process_state_effects(&self) {
        let guard = self.effects_rx.lock();
        let Some(rx) = guard.as_ref() else {
            return;
        };
        use crate::cluster::raft::state_machine::StateEffect;
        while let Ok(effect) = rx.try_recv() {
            match effect {
                StateEffect::DrainStream(id) => self.mark_stream_draining(&id),
                StateEffect::ClearDrainStream(id) => self.mark_stream_drain_clear(&id),
                StateEffect::RevokeViewer(id) => self.mark_viewer_revoked(&id),
                StateEffect::ApiToken(token) => self.apply_api_token(&token),
                StateEffect::OwnershipChanged => self.sync_ownership_from_db(),
            }
        }
    }

    fn restore_topology_from_membership(&self, sm: &SqliteStateMachine) {
        let membership = sm.last_membership();
        for (id, node) in membership.membership().nodes() {
            if *id == self.config.node_id {
                continue;
            }
            let ctrl = node.addr.clone();
            self.network.upsert_node(*id, ctrl.clone());
            // Media addr unknown until topology/heartbeat; seed control only.
            let media = self.meta.get(*id).map(|(_, m)| m).unwrap_or_default();
            self.meta.set_addrs(*id, ctrl.clone(), media);
            self.health
                .note_peer(*id, NodeHealthState::Ready, 0.0, Some(ctrl), None);
        }
    }

    /// Drop meta/health/media/network cache entries for nodes no longer in
    /// Raft membership. Runs on every node so removals are not initiator-only.
    fn prune_topology_to_membership(&self) {
        let Some(rm) = self.last_metrics.lock().clone() else {
            return;
        };
        // Promoting a learner to voter changes Raft membership but the
        // node's own local health stays `Learner` (set once at join) unless
        // reconciled here; otherwise it keeps advertising/reporting itself
        // as a learner indefinitely after a successful promotion.
        if self.health.local() == NodeHealthState::Learner
            && rm
                .membership_config
                .membership()
                .voter_ids()
                .any(|id| id == self.config.node_id)
        {
            self.health.set_local(NodeHealthState::Ready);
            crate::log_info!(
                "Cluster: node {} promoted to voter, health Learner -> Ready",
                self.config.node_id
            );
        }
        let members: std::collections::HashSet<NodeId> = rm
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .collect();
        if members.is_empty() {
            return;
        }
        // If Raft membership no longer includes this node (another member
        // removed us), stop admitting and tear down local publishers — do not
        // keep serving after ownership can be reassigned elsewhere.
        if !members.contains(&self.config.node_id)
            && self.health.local() != NodeHealthState::Leaving
        {
            crate::log_warn!(
                "Cluster: this node {} is no longer in Raft membership; entering Leaving",
                self.config.node_id
            );
            self.health.set_local(NodeHealthState::Leaving);
            self.admission.force_drain();
            if let Some(hooks) = self.session_hooks.lock().as_ref() {
                for p in self.db.publisher_list_all() {
                    (hooks.force_unpublish_stream)(&p.stream_id);
                    self.mark_stream_draining(&p.stream_id);
                }
            }
        }
        for (id, _, _) in self.meta.all() {
            if id == self.config.node_id || members.contains(&id) {
                continue;
            }
            self.meta.remove(id);
            self.health.remove(id);
            self.media.disconnect_peer(id);
            self.network.nodes.write().remove(&id);
            self.peer_session_counts.lock().remove(&id);
            self.peer_stream_players.lock().remove(&id);
            crate::log_info!("Cluster: pruned removed node {id} from local topology caches");
        }
    }

    async fn refresh_topology_from(&self, peer_addr: &str) -> Result<(), String> {
        let (cluster_id, peers) = network::send_topology(
            peer_addr,
            &self.config.secret,
            self.config.node_id,
            self.tls_client.clone(),
        )
        .await?;
        let local = self.db.setting_get(CLUSTER_ID_SETTING);
        verify_cluster_identity(local.as_deref(), &cluster_id)?;
        if !cluster_id.is_empty() && local.is_none() {
            let _ = self.db.setting_set(CLUSTER_ID_SETTING, &cluster_id);
        }
        for p in peers {
            if p.node_id == self.config.node_id {
                continue;
            }
            self.network.upsert_node(p.node_id, p.control_addr.clone());
            self.meta
                .set_addrs(p.node_id, p.control_addr.clone(), p.media_addr.clone());
            let _ = self.media.connect_peer(p.node_id, &p.media_addr).await;
            self.health.note_peer(
                p.node_id,
                NodeHealthState::Ready,
                0.0,
                Some(p.control_addr),
                Some(p.media_addr),
            );
        }
        Ok(())
    }

    /// Like `refresh_topology_from`, but if the given address doesn't answer,
    /// fall back to any peer control address already restored from persisted
    /// Raft membership (`restore_topology_from_membership` runs before this
    /// is called). A surviving majority of an existing cluster must be able
    /// to restart even if the original bootstrap/join address is
    /// permanently gone, as long as some other persisted member responds.
    async fn refresh_topology_from_any(&self, primary_addr: &str) -> Result<(), String> {
        let primary_err = match self.refresh_topology_from(primary_addr).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        let fallbacks: Vec<String> = self
            .meta
            .all()
            .into_iter()
            .map(|(_, ctrl, _)| ctrl)
            .filter(|ctrl| ctrl != primary_addr)
            .collect();
        for ctrl in fallbacks {
            if self.refresh_topology_from(&ctrl).await.is_ok() {
                return Ok(());
            }
        }
        Err(primary_err)
    }

    /// Sum live RTMP sessions for `stream_id` across peers (excludes local).
    /// Uses heartbeat-cached per-stream player counts to avoid control-plane RPC storms.
    pub fn remote_stream_session_count_cached(&self, stream_id: &str) -> u64 {
        let counts = self.peer_stream_players.lock();
        counts
            .values()
            .map(|m| m.get(stream_id).copied().unwrap_or(0))
            .sum()
    }

    /// Sum live RTMP sessions for `stream_id` across peers (excludes local).
    pub async fn remote_stream_session_count(&self, stream_id: &str) -> u64 {
        let cached = self.remote_stream_session_count_cached(stream_id);
        let mut rpc_total = 0u64;
        let mut rpc_peers = 0u64;
        let mut rpc_ok = 0u64;
        for (id, ctrl, _) in self.meta.all() {
            if id == self.config.node_id {
                continue;
            }
            rpc_peers += 1;
            if let Ok(n) = network::send_session_count(
                &ctrl,
                &self.config.secret,
                self.config.node_id,
                stream_id.to_string(),
                self.tls_client.clone(),
            )
            .await
            {
                rpc_ok += 1;
                rpc_total = rpc_total.saturating_add(n);
            }
        }
        if rpc_peers == 0 {
            return 0;
        }
        if rpc_ok > 0 {
            rpc_total
        } else {
            cached
        }
    }

    /// Best-effort broadcast of drain/revoke markers to peers (Raft apply also marks locally).
    pub async fn broadcast_drain_stream(&self, stream_id: &str) {
        self.mark_stream_draining(stream_id);
        for (id, ctrl, _) in self.meta.all() {
            if id == self.config.node_id {
                continue;
            }
            let _ = network::send_admin(
                &ctrl,
                &self.config.secret,
                self.config.node_id,
                network::ControlMessage::DrainStream {
                    stream_id: stream_id.to_string(),
                },
                self.tls_client.clone(),
            )
            .await;
        }
    }

    pub async fn broadcast_revoke_viewer(&self, viewer_id: &str) {
        self.mark_viewer_revoked(viewer_id);
        for (id, ctrl, _) in self.meta.all() {
            if id == self.config.node_id {
                continue;
            }
            let _ = network::send_admin(
                &ctrl,
                &self.config.secret,
                self.config.node_id,
                network::ControlMessage::RevokeViewer {
                    viewer_id: viewer_id.to_string(),
                },
                self.tls_client.clone(),
            )
            .await;
        }
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
            self.tls_client.clone(),
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

    /// Drop remote media subscription when the last local player for a stream leaves.
    pub fn notify_play_unsubscribe(&self, app: &str, stream_id: &str) {
        let Some(owner) = self.ownership.get(stream_id).or_else(|| {
            self.db
                .stream_owner_get(stream_id)
                .map(|o| (o.owner_node_id, o.epoch))
        }) else {
            return;
        };
        let (owner_node, _) = owner;
        if owner_node == self.config.node_id {
            return;
        }
        let media = Arc::clone(&self.media);
        let app = app.to_string();
        let stream = stream_id.to_string();
        self.rt_handle.spawn(async move {
            media.unsubscribe_remote(owner_node, &app, &stream).await;
        });
    }

    /// Re-subscribe local players after ownership epoch/node changes.
    fn refresh_subscriptions_for(&self, stream_ids: &[String]) {
        // Keep receive-side timeline remappers across owner changes so players
        // see a monotonic handoff (new owner often restarts timestamps at 0).
        for sid in stream_ids {
            let app = match self.db.stream_get(sid) {
                crate::db::DbLookup::Ok(s) => s.app,
                _ => "live".to_string(),
            };
            self.notify_play_unsubscribe(&app, sid);
            if self.db.player_list(Some(sid)).iter().any(|p| p.active) {
                self.notify_play_subscription(&app, sid);
            }
        }
    }

    /// Sync in-memory ownership from SQLite (followers apply Raft → DB).
    fn sync_ownership_from_db(&self) {
        let owners = self.db.stream_owner_list();
        let changed = self.ownership.sync_from(&owners);
        if changed.is_empty() {
            return;
        }
        // sync_from() already replaced the tracker with the new owners, so
        // looking them up now (as refresh_subscriptions_for's
        // notify_play_unsubscribe does) would only ever find the new owner,
        // never unsubscribing from the actual previous one. Unsubscribe
        // directly from the (node, epoch) sync_from captured before the
        // swap.
        for (stream_id, old_owner) in &changed {
            let Some((old_node, _)) = old_owner else {
                continue;
            };
            if *old_node == self.config.node_id {
                continue;
            }
            let app = match self.db.stream_get(stream_id) {
                crate::db::DbLookup::Ok(s) => s.app,
                _ => "live".to_string(),
            };
            let media = Arc::clone(&self.media);
            let owner_node = *old_node;
            let stream = stream_id.clone();
            self.rt_handle.spawn(async move {
                media.unsubscribe_remote(owner_node, &app, &stream).await;
            });
        }
        let ids: Vec<String> = changed.into_iter().map(|(id, _)| id).collect();
        for sid in &ids {
            let app = match self.db.stream_get(sid) {
                crate::db::DbLookup::Ok(s) => s.app,
                _ => "live".to_string(),
            };
            self.media.evict_init_cache(&app, sid);
        }
        self.resubscribe_active_players(&ids);
    }

    /// Re-subscribe local players to their (already-updated) current owner.
    /// Does not unsubscribe — callers that know the previous owner should
    /// unsubscribe from it explicitly first (see `sync_ownership_from_db`).
    fn resubscribe_active_players(&self, stream_ids: &[String]) {
        for sid in stream_ids {
            if self.db.player_list(Some(sid)).iter().any(|p| p.active) {
                let app = match self.db.stream_get(sid) {
                    crate::db::DbLookup::Ok(s) => s.app,
                    _ => "live".to_string(),
                };
                self.notify_play_subscription(&app, sid);
            }
        }
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
            self.prune_topology_to_membership();
            if metrics.changed().await.is_err() {
                break;
            }
        }
    }

    async fn health_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(self.config.heartbeat);
        loop {
            ticker.tick().await;
            self.process_state_effects();
            self.retry_pending_drain_clears();
            self.reconcile_ambiguous_acquires();
            if !self.config.bandwidth_interface.is_empty() {
                let iface = self.config.bandwidth_interface.clone();
                let max = self.config.bandwidth_max_mbps;
                let mode = self.config.bandwidth_mode;
                // Probe sleeps ~200ms — keep it off the Tokio worker.
                match tokio::task::spawn_blocking(move || {
                    measure_interface_utilization(&iface, max, mode)
                })
                .await
                {
                    Ok(Ok(load)) => self.admission.update_load(load),
                    Ok(Err(e)) => crate::log_warn!("Cluster bandwidth: {e}"),
                    Err(e) => crate::log_warn!("Cluster bandwidth probe join: {e}"),
                }
            }
            let down = self.health.sweep_stale();
            let metrics = self.last_metrics.lock().clone();
            let is_leader =
                metrics.as_ref().and_then(|m| m.current_leader) == Some(self.config.node_id);
            if is_leader {
                let grace = self.config.heartbeat.saturating_mul(5);
                for id in self.health.peers_ready_for_ownership_release(grace) {
                    crate::log_info!("Cluster: node {id} DOWN for {:?} — releasing owners", grace);
                    match self.release_owners_for_node(id) {
                        Ok(()) => self.health.mark_ownership_released(id),
                        Err(e) => crate::log_warn!("Cluster: release owners for {id}: {e:?}"),
                    }
                }
            }
            for id in &down {
                self.peer_session_counts.lock().remove(id);
                self.peer_stream_players.lock().remove(id);
            }
            // Followers: keep OwnershipTracker in sync with Raft-applied SQLite.
            self.sync_ownership_from_db();
            self.reconcile_media_replicas();
            let local_pubs = self
                .db
                .publisher_list_all()
                .iter()
                .filter(|p| p.active)
                .count() as u64;
            let local_players = self
                .db
                .player_list_all()
                .iter()
                .filter(|p| p.active)
                .count() as u64;
            let mut stream_players: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            for p in self.db.player_list_all().iter().filter(|p| p.active) {
                *stream_players.entry(p.stream_id.clone()).or_insert(0) += 1;
            }
            let stream_players: Vec<(String, u64)> = stream_players.into_iter().collect();
            let advertise = self.config.advertise_control();
            let media_advertise = self.config.advertise_media();
            self.prune_topology_to_membership();
            // Emit heartbeats concurrently so a few dead peers cannot exceed
            // the stale threshold waiting on sequential 500ms timeouts.
            let peers: Vec<_> = self
                .meta
                .all()
                .into_iter()
                .filter(|(id, _, _)| *id != self.config.node_id)
                .collect();
            let secret = self.config.secret.clone();
            let node_id = self.config.node_id;
            let health = self.health.local().as_str().to_string();
            let load = self.admission.current_load();
            let tls = self.tls_client.clone();
            let futs = peers.into_iter().map(|(id, ctrl, _)| {
                let secret = secret.clone();
                let advertise = advertise.clone();
                let media_advertise = media_advertise.clone();
                let health = health.clone();
                let stream_players = stream_players.clone();
                let tls = tls.clone();
                async move {
                    let _ = id;
                    network::send_heartbeat(
                        &ctrl,
                        &secret,
                        node_id,
                        &health,
                        load,
                        advertise,
                        media_advertise,
                        local_pubs,
                        local_players,
                        stream_players,
                        tls,
                    )
                    .await;
                }
            });
            futures::future::join_all(futs).await;
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
                    self.reconcile_publishers_after_isolation();
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
            .count() as u64;
        let players = self
            .db
            .player_list_all()
            .iter()
            .filter(|p| p.active)
            .count() as u64;
        let peer_counts = self.peer_session_counts.lock();
        let peer_pubs: u64 = peer_counts.values().map(|(p, _)| *p).sum();
        let peer_players: u64 = peer_counts.values().map(|(_, pl)| *pl).sum();
        drop(peer_counts);
        let total_publishers = pubs.saturating_add(peer_pubs);
        let total_players = players.saturating_add(peer_players);
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
            "total_publishers": total_publishers,
            "total_players": total_players,
            "local_publishers": pubs,
            "local_players": players,
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
            let (peer_pubs, peer_players) = self
                .peer_session_counts
                .lock()
                .get(&id)
                .copied()
                .unwrap_or((0, 0));
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
                publishers: peer_pubs as usize,
                players: peer_players as usize,
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

    /// Deterministic replica placement: the first `media_replicas` other
    /// member node IDs (excluding `owner_node` and anything in `exclude`),
    /// sorted by ID so every node computes the same set independently —
    /// `meta.all()`'s HashMap iteration order is not consistent across
    /// processes and cannot be used directly for a cluster-wide agreement.
    fn standby_candidates(&self, owner_node: Option<NodeId>, exclude: &[NodeId]) -> Vec<NodeId> {
        if self.config.media_replicas == 0 {
            return Vec::new();
        }
        let mut candidates: Vec<NodeId> = self
            .meta
            .all()
            .into_iter()
            .map(|(id, _, _)| id)
            .filter(|id| !exclude.contains(id) && owner_node.map(|o| o != *id).unwrap_or(true))
            .collect();
        candidates.sort_unstable();
        candidates.truncate(self.config.media_replicas as usize);
        candidates
    }

    /// Subscribe to (or drop) each stream's owner as a standby replica when
    /// this node enters or leaves the deterministic replica placement,
    /// reusing the normal Subscribe/InitCache/MediaFrame path so standbys
    /// actually receive continuous media instead of just being reported.
    fn reconcile_media_replicas(&self) {
        if self.config.media_replicas == 0 {
            return;
        }
        let mut desired: std::collections::HashMap<(String, String), NodeId> =
            std::collections::HashMap::new();
        for s in self.db.stream_list() {
            let Some(owner) = self.db.stream_owner_get(&s.id) else {
                continue;
            };
            if owner.owner_node_id == self.config.node_id {
                continue;
            }
            let candidates = self.standby_candidates(Some(owner.owner_node_id), &[]);
            if candidates.contains(&self.config.node_id) {
                desired.insert((s.app, s.id), owner.owner_node_id);
            }
        }

        // (app, stream, old_owner_to_drop, new_owner_to_subscribe)
        let mut ops: Vec<(String, String, Option<NodeId>, Option<NodeId>)> = Vec::new();
        {
            let mut current = self.standby_subs.lock();
            for (key, &new_owner) in &desired {
                match current.get(key) {
                    Some(&old_owner) if old_owner == new_owner => {}
                    Some(&old_owner) => ops.push((
                        key.0.clone(),
                        key.1.clone(),
                        Some(old_owner),
                        Some(new_owner),
                    )),
                    None => ops.push((key.0.clone(), key.1.clone(), None, Some(new_owner))),
                }
            }
            for (key, &old_owner) in current.iter() {
                if !desired.contains_key(key) {
                    ops.push((key.0.clone(), key.1.clone(), Some(old_owner), None));
                }
            }
            *current = desired;
        }

        for (app, stream_id, old_owner, new_owner) in ops {
            if let Some(old) = old_owner {
                let media = Arc::clone(&self.media);
                let app_u = app.clone();
                let stream_u = stream_id.clone();
                self.rt_handle.spawn(async move {
                    media.unsubscribe_remote(old, &app_u, &stream_u).await;
                });
            }
            if let Some(new_owner) = new_owner {
                let Some((_, media_addr)) = self.meta.get(new_owner) else {
                    continue;
                };
                let epoch = self
                    .db
                    .stream_owner_get(&stream_id)
                    .map(|o| o.epoch)
                    .unwrap_or(0);
                let media = Arc::clone(&self.media);
                self.rt_handle.spawn(async move {
                    media
                        .subscribe_remote(&media_addr, new_owner, &app, &stream_id, epoch)
                        .await;
                });
            }
        }
    }

    pub fn streams_info(&self) -> Vec<ClusterStreamInfo> {
        let peer_stream = self.peer_stream_players.lock().clone();
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
                let remote_players: u64 = peer_stream
                    .values()
                    .map(|m| m.get(&s.id).copied().unwrap_or(0))
                    .sum();
                // Standby placement must match reconcile_media_replicas()'s own
                // computation exactly (no extra exclusions) — a standby node
                // subscribes through the same SubscriptionTable as an ordinary
                // viewer, so excluding `subscribed` here would silently report
                // a different node than the one actually replicating.
                let standby = self.standby_candidates(owner.as_ref().map(|o| o.owner_node_id), &[]);
                ClusterStreamInfo {
                    stream_id: s.id,
                    owner_node_id: owner.as_ref().map(|o| o.owner_node_id),
                    epoch: owner.as_ref().map(|o| o.epoch),
                    subscribed_nodes: subscribed,
                    standby_nodes: standby,
                    cluster_players: local_players.saturating_add(remote_players as usize),
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
