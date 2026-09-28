//! Runs the real `ServerApp` (the same entry point as the binary) in-process:
//! HTTP API, sharded RTMP poll loops, auth worker, stats flushing, media
//! outputs and shutdown.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use librtmp2::client::Client;
use librtmp2::types::{ErrorCode, Frame, FrameType, VideoCodec};
use librtmp2_server::config::ServerConfig;
use librtmp2_server::server::ServerApp;
use serial_test::serial;
use tokio::sync::oneshot;

const API_TOKEN: &str = "server-run-test-token-0123456789abcdef";
const PUB_KEY: &str = "pub_run_key_with_sufficient_length_here01";
const PLAY_KEY: &str = "play_run_key_with_sufficient_length_here1";
const STATS_KEY: &str = "st_run_key_with_sufficient_length_here001";

static PLAYER_FRAMES: AtomicUsize = AtomicUsize::new(0);

fn on_player_frame(frame: &Frame) {
    if frame.frame_type == FrameType::Video && frame.size >= 16 {
        PLAYER_FRAMES.fetch_add(1, Ordering::SeqCst);
    }
}

struct RunningServer {
    http_base: String,
    rtmp_port: u16,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<Result<(), String>>>,
}

impl RunningServer {
    /// Starts `ServerApp::run_until` on its own runtime and waits until the
    /// HTTP API answers.
    fn start(dir: &Path, http_port: u16, rtmp_port: u16, config_file: Option<&Path>) -> Self {
        // SAFETY: the tests in this file are serial and nothing else in this
        // process reads these variables concurrently.
        unsafe {
            std::env::set_var("LRTMP2_DB", dir.join("server.db"));
            std::env::set_var("LRTMP2_API_TOKEN", API_TOKEN);
        }
        let config = ServerConfig {
            http_bind: format!("127.0.0.1:{http_port}"),
            rtmp_bind: format!("127.0.0.1:{rtmp_port}"),
            config_file: config_file
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            ..Default::default()
        };
        let app = ServerApp::create(config).expect("create server app");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let thread = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(app.run_until(async {
                let _ = shutdown_rx.await;
            }))
        });

        let http_base = format!("http://127.0.0.1:{http_port}");
        let client = reqwest::blocking::Client::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let up = client
                .get(format!("{http_base}/api/v1/health"))
                .send()
                .is_ok_and(|r| r.status().is_success());
            if up {
                break;
            }
            assert!(Instant::now() < deadline, "server did not come up");
            thread::sleep(Duration::from_millis(20));
        }
        Self {
            http_base,
            rtmp_port,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        }
    }

    fn create_stream(&self, id: &str) {
        let resp = reqwest::blocking::Client::new()
            .post(format!("{}/api/v1/streams", self.http_base))
            .header("Authorization", format!("Bearer {API_TOKEN}"))
            .json(&serde_json::json!({
                "id": id,
                "name": "Run Stream",
                "app": "live",
                "publish_key": PUB_KEY,
                "play_key": PLAY_KEY,
                "stats_key": STATS_KEY,
            }))
            .send()
            .expect("create stream");
        assert_eq!(resp.status(), 201);
    }

    fn delete_stream(&self, id: &str) -> u16 {
        reqwest::blocking::Client::new()
            .delete(format!("{}/api/v1/streams/{id}", self.http_base))
            .header("Authorization", format!("Bearer {API_TOKEN}"))
            .send()
            .expect("delete stream")
            .status()
            .as_u16()
    }

    fn stats(&self, id: &str) -> serde_json::Value {
        reqwest::blocking::Client::new()
            .get(format!("{}/api/v1/streams/{id}/stats", self.http_base))
            .header("Authorization", format!("Bearer {API_TOKEN}"))
            .send()
            .expect("stats")
            .json()
            .expect("stats json")
    }

    fn stop(mut self) -> Result<(), String> {
        let _ = self.shutdown.take().unwrap().send(());
        self.thread.take().unwrap().join().expect("server thread")
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("lrtmp2-server-run-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn keyframe() -> [u8; 16] {
    [
        0x17, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00,
    ]
}

fn send_keyframe(publisher: &mut Client, timestamp: u32) -> Result<(), ErrorCode> {
    let data = keyframe();
    let frame = Frame {
        frame_type: FrameType::Video,
        timestamp,
        size: data.len() as u32,
        data: data.as_ptr(),
        video_codec: VideoCodec::H264,
        video_frame_type: 1,
        ..Default::default()
    };
    publisher.send_frame(&frame)
}

/// Publishes, plays until the player got a frame, and returns the publisher
/// so the caller decides when it disconnects.
fn publish_and_play(rtmp_port: u16) -> Result<Client, ErrorCode> {
    PLAYER_FRAMES.store(0, Ordering::SeqCst);
    let mut publisher = Client::new();
    publisher.connect(&format!("rtmp://127.0.0.1:{rtmp_port}/live/{PUB_KEY}"))?;
    publisher.publish()?;

    let mut player = Client::new();
    player.on_frame_cb = Some(on_player_frame);
    player.connect(&format!("rtmp://127.0.0.1:{rtmp_port}/live/{PLAY_KEY}"))?;
    player.play()?;

    let deadline = Instant::now() + Duration::from_secs(8);
    let mut ts = 0;
    while PLAYER_FRAMES.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        send_keyframe(&mut publisher, ts)?;
        ts += 40;
        player.poll(50)?;
    }
    Ok(publisher)
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(8);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
#[serial]
fn run_serves_http_and_relays_rtmp_until_shutdown() {
    let dir = temp_dir("relay");
    let server = RunningServer::start(&dir, 19781, 19782, None);
    server.create_stream("run-stream");

    let rtmp_port = server.rtmp_port;
    let publisher = thread::spawn(move || publish_and_play(rtmp_port))
        .join()
        .unwrap()
        .expect("publish/play through ServerApp::run");
    assert!(
        PLAYER_FRAMES.load(Ordering::SeqCst) > 0,
        "player should receive a relayed frame"
    );
    wait_for("stream to show as live", || {
        server.stats("run-stream")["summary"]["publishers"] == 1
    });

    // Deleting a live stream drains its RTMP sessions before the row goes.
    let status = server.delete_stream("run-stream");
    assert!(status == 200 || status == 202, "delete status {status}");
    drop(publisher);
    wait_for("stream to be deleted", || {
        reqwest::blocking::Client::new()
            .get(format!("{}/api/v1/streams", server.http_base))
            .header("Authorization", format!("Bearer {API_TOKEN}"))
            .send()
            .and_then(|r| r.json::<Vec<serde_json::Value>>())
            .is_ok_and(|list| !list.iter().any(|s| s["id"] == "run-stream"))
    });

    server.stop().expect("run_until returns Ok after shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[serial]
fn run_with_media_outputs_records_and_runs_hooks() {
    let dir = temp_dir("media");
    let recordings = dir.join("recordings");
    let hook_log = dir.join("hooks.log");
    let config_file = dir.join("server.env");
    std::fs::write(
        &config_file,
        format!(
            "MEDIA_RECORDING_ENABLED=true\n\
             MEDIA_RECORDING_PATH={}\n\
             MEDIA_EXEC_PUBLISH=echo \"publish $OPENRTMP_STREAM_ID\" >> {log}\n\
             MEDIA_EXEC_PUBLISH_DONE=echo \"done $OPENRTMP_STREAM_ID\" >> {log}\n",
            recordings.display(),
            log = hook_log.display(),
        ),
    )
    .unwrap();

    let server = RunningServer::start(&dir, 19783, 19784, Some(&config_file));
    server.create_stream("rec-stream");

    let rtmp_port = server.rtmp_port;
    let publisher = thread::spawn(move || publish_and_play(rtmp_port))
        .join()
        .unwrap()
        .expect("publish/play with media outputs");
    drop(publisher);

    wait_for("publish-done hook", || {
        std::fs::read_to_string(&hook_log).is_ok_and(|log| log.contains("done"))
    });
    server.stop().expect("run_until returns Ok after shutdown");

    let log = std::fs::read_to_string(&hook_log).unwrap();
    assert!(log.contains("publish"), "publish hook ran: {log:?}");

    let flv = walk_files(&recordings)
        .into_iter()
        .find(|p| p.extension().is_some_and(|e| e == "flv"))
        .expect("a recording file");
    let bytes = std::fs::read(&flv).unwrap();
    assert!(
        bytes.starts_with(b"FLV"),
        "recording starts with an FLV header"
    );
    assert!(bytes.len() > 13, "recording holds at least one tag");
    let _ = std::fs::remove_dir_all(&dir);
}

fn walk_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

#[test]
#[serial]
fn run_with_shards_relays_across_shards() {
    // SAFETY: serial test; see `RunningServer::start`.
    unsafe { std::env::set_var("LRTMP2_RTMP_SHARDS", "4") };
    let dir = temp_dir("shards");
    let server = RunningServer::start(&dir, 19785, 19786, None);
    server.create_stream("shard-stream");

    let rtmp_port = server.rtmp_port;
    let result = thread::spawn(move || -> Result<usize, ErrorCode> {
        PLAYER_FRAMES.store(0, Ordering::SeqCst);
        let mut publisher = Client::new();
        publisher.connect(&format!("rtmp://127.0.0.1:{rtmp_port}/live/{PUB_KEY}"))?;
        publisher.publish()?;

        // SO_REUSEPORT spreads connections over the shards, so with several
        // players at least one usually lands on another shard than the
        // publisher and is fed through the cross-shard relay.
        let mut players = Vec::new();
        for _ in 0..4 {
            let mut player = Client::new();
            player.on_frame_cb = Some(on_player_frame);
            player.connect(&format!("rtmp://127.0.0.1:{rtmp_port}/live/{PLAY_KEY}"))?;
            player.play()?;
            players.push(player);
        }

        let deadline = Instant::now() + Duration::from_secs(8);
        let mut ts = 0;
        while PLAYER_FRAMES.load(Ordering::SeqCst) < 8 && Instant::now() < deadline {
            send_keyframe(&mut publisher, ts)?;
            ts += 40;
            for player in &mut players {
                player.poll(10)?;
            }
        }
        Ok(PLAYER_FRAMES.load(Ordering::SeqCst))
    })
    .join()
    .unwrap();
    unsafe { std::env::remove_var("LRTMP2_RTMP_SHARDS") };

    let frames = result.expect("publish/play across shards");
    assert!(frames > 0, "players should receive relayed frames");
    server.stop().expect("run_until returns Ok after shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
