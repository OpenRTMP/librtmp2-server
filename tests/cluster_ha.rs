//! Multi-node HA cluster integration tests (feature = cluster + test-support).

#![cfg(all(feature = "cluster", feature = "test-support"))]

use std::sync::Arc;
use std::time::Duration;

use librtmp2_server::cluster::command::ClusterCommand;
use librtmp2_server::cluster::config::ClusterConfig;
use librtmp2_server::cluster::media::subscription::SubscriptionTable;
use librtmp2_server::cluster::media::timeline::TimelineRemapper;
use librtmp2_server::cluster::membership::check_join_reseed;
use librtmp2_server::cluster::ClusterManager;
use librtmp2_server::db::{Db, Stream, StreamViewer};
use librtmp2_server::state::StateCoordinator;
use tempfile::TempDir;

fn secret() -> String {
    "test-cluster-secret!!".to_string()
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn cluster_cfg(node_id: u64, bootstrap: bool, join: Option<String>) -> (ClusterConfig, u16, u16) {
    let control = free_port();
    let media = free_port();
    let mut cfg = ClusterConfig::default();
    cfg.enabled = true;
    cfg.node_id = node_id;
    cfg.bind = format!("127.0.0.1:{control}");
    cfg.media_bind = format!("127.0.0.1:{media}");
    cfg.bootstrap = bootstrap;
    cfg.join = join;
    cfg.secret = secret();
    cfg.heartbeat = Duration::from_millis(100);
    cfg.advertise_addr = Some(cfg.bind.clone());
    cfg.media_advertise_addr = Some(cfg.media_bind.clone());
    (cfg, control, media)
}

async fn start_node(node_id: u64, bootstrap: bool, join: Option<String>) -> (Arc<ClusterManager>, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join(format!("n{node_id}.db"));
    let db = Arc::new(Db::open(db_path.to_str().unwrap()).unwrap());
    let (cfg, _, _) = cluster_cfg(node_id, bootstrap, join);
    let mgr = ClusterManager::start(cfg, db, tokio::runtime::Handle::current())
        .await
        .expect("start cluster node");
    (mgr, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_bootstrap_and_leader() {
    let (n1, _d1) = start_node(1, true, None).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let join = Some(n1.config.advertise_control());
    let (n2, _d2) = start_node(2, false, join.clone()).await;
    let (n3, _d3) = start_node(3, false, join).await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let m1 = n1.metrics().await;
    assert!(m1.is_leader || m1.leader_id.is_some());
    let _ = (n2, n3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_stream_via_follower_forwards_to_leader() {
    let (n1, _d1) = start_node(1, true, None).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let join = Some(n1.config.advertise_control());
    let (n2, _d2) = start_node(2, false, join).await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Promote learner so it can participate; writes still forward via OpenRaft.
    let _ = n1.promote_learner(2).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let stream = Stream {
        id: "sf".into(),
        name: "SF".into(),
        app: "live".into(),
        publish_key: "pub_wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww".into(),
        play_key: "play_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".into(),
        stats_key: "stat_yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy".into(),
        enabled: true,
        created_at: 1,
    };
    n2.create_stream(&stream).expect("create via follower");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(matches!(
        n1.db().stream_get("sf"),
        librtmp2_server::db::DbLookup::Ok(_)
    ));
    // Default viewer must exist with identical play_key on both nodes.
    let v1 = n1.db().viewer_list("sf");
    let v2 = n2.db().viewer_list("sf");
    assert_eq!(v1.len(), 1);
    assert_eq!(v1[0].id, v2[0].id);
    assert_eq!(v1[0].play_key, stream.play_key);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_stream_via_leader_replicates() {
    let (n1, _d1) = start_node(1, true, None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let join = Some(n1.config.advertise_control());
    let (n2, _d2) = start_node(2, false, join).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let stream = Stream {
        id: "s1".into(),
        name: "S1".into(),
        app: "live".into(),
        publish_key: "pub_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        play_key: "play_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
        stats_key: "stat_cccccccccccccccccccccccccccccccc".into(),
        enabled: true,
        created_at: 1,
    };
    n1.create_stream(&stream).expect("create");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(matches!(n2.db().stream_get("s1"), librtmp2_server::db::DbLookup::Ok(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viewer_and_delete_replicate() {
    let (n1, _d1) = start_node(1, true, None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let join = Some(n1.config.advertise_control());
    let (n2, _d2) = start_node(2, false, join).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let stream = Stream {
        id: "sv".into(),
        name: "SV".into(),
        app: "live".into(),
        publish_key: "pub_dddddddddddddddddddddddddddddddd".into(),
        play_key: "play_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".into(),
        stats_key: "stat_ffffffffffffffffffffffffffffffff".into(),
        enabled: true,
        created_at: 1,
    };
    n1.create_stream(&stream).unwrap();
    let viewer = StreamViewer {
        id: "v1".into(),
        stream_id: "sv".into(),
        name: "viewer".into(),
        play_key: "play_gggggggggggggggggggggggggggggggg".into(),
        enabled: true,
        created_at: 1,
    };
    n1.create_viewer(&viewer).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        n2.db().viewer_get("sv", "v1"),
        librtmp2_server::db::DbLookup::Ok(_)
    ));

    n1.begin_delete_stream("sv").unwrap();
    n1.finalize_delete_stream("sv").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        n2.db().stream_get("sv"),
        librtmp2_server::db::DbLookup::Missing
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_conflict_and_release() {
    let (n1, _d1) = start_node(1, true, None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stream = Stream {
        id: "own".into(),
        name: "Own".into(),
        app: "live".into(),
        publish_key: "pub_hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh".into(),
        play_key: "play_iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii".into(),
        stats_key: "stat_jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj".into(),
        enabled: true,
        created_at: 1,
    };
    n1.create_stream(&stream).unwrap();
    let ep = n1
        .acquire_stream_owner("own", 1, 10, 1)
        .expect("acquire");
    assert_eq!(ep, 10);
    let err = n1.acquire_stream_owner("own", 2, 11, 1);
    assert!(err.is_err());
    n1.release_stream_owner("own", 10).unwrap();
    assert!(n1.acquire_stream_owner("own", 2, 12, 1).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_failover_via_release_owners_for_node() {
    let (n1, _d1) = start_node(1, true, None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stream = Stream {
        id: "fo".into(),
        name: "FO".into(),
        app: "live".into(),
        publish_key: "pub_kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk".into(),
        play_key: "play_llllllllllllllllllllllllllllllll".into(),
        stats_key: "stat_mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm".into(),
        enabled: true,
        created_at: 1,
    };
    n1.create_stream(&stream).unwrap();
    n1.acquire_stream_owner("fo", 99, 1, 1).unwrap();
    n1.release_owners_for_node(99).unwrap();
    assert!(n1.acquire_stream_owner("fo", 1, 2, 1).is_ok());
}

#[test]
fn subscription_dedup() {
    let t = SubscriptionTable::new();
    assert!(t.add(1, "live", "s"));
    assert!(!t.add(1, "live", "s"));
    assert!(!t.remove(1, "live", "s"));
    assert!(t.remove(1, "live", "s"));
}

#[test]
fn timeline_wrap_monotonic() {
    let mut t = TimelineRemapper::new();
    let a = t.map(1, 100);
    let b = t.map(2, 0);
    assert!(b > a);
}

#[test]
fn reseed_refuses_populated_join() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path().join("x.db").to_str().unwrap()).unwrap();
    let s = Stream {
        id: "x".into(),
        name: "x".into(),
        app: "live".into(),
        publish_key: "pub_nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn".into(),
        play_key: "play_oooooooooooooooooooooooooooooooo".into(),
        stats_key: "stat_pppppppppppppppppppppppppppppppp".into(),
        enabled: true,
        created_at: 1,
    };
    db.stream_add(&s).unwrap();
    let err = check_join_reseed(&db, true).unwrap_err();
    assert!(err.contains("reseed") || err.contains("empty"));
}

#[tokio::test]
async fn standalone_coordinator_with_cluster_feature() {
    let db = Arc::new(Db::open(":memory:").unwrap());
    let coord = StateCoordinator::standalone(Arc::clone(&db));
    let s = Stream {
        id: "local".into(),
        name: "local".into(),
        app: "live".into(),
        publish_key: "pub_qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq".into(),
        play_key: "play_rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr".into(),
        stats_key: "stat_ssssssssssssssssssssssssssssssss".into(),
        enabled: true,
        created_at: 1,
    };
    coord.create_stream(&s).unwrap();
    assert!(matches!(
        db.stream_get("local"),
        librtmp2_server::db::DbLookup::Ok(_)
    ));
    assert!(coord.cluster_manager().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_write_command_roundtrip() {
    let (n1, _d1) = start_node(1, true, None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Exercise SetStreamEnabled via coordinator path after create.
    let stream = Stream {
        id: "en".into(),
        name: "en".into(),
        app: "live".into(),
        publish_key: "pub_tttttttttttttttttttttttttttttttt".into(),
        play_key: "play_uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu".into(),
        stats_key: "stat_vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv".into(),
        enabled: true,
        created_at: 1,
    };
    n1.create_stream(&stream).unwrap();
    n1.set_stream_enabled("en", false).unwrap();
    let DbLookup = n1.db().stream_get("en");
    match DbLookup {
        librtmp2_server::db::DbLookup::Ok(s) => assert!(!s.enabled),
        other => panic!("unexpected {other:?}"),
    }
    let _ = ClusterCommand::SetApiToken {
        token: "x".into(),
    };
}
