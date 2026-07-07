//! End-to-end tests: HTTP API + RTMP publish/play through librtmp2-server.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use librtmp2::client::Client;
use librtmp2::types::{Frame, FrameType, VideoCodec};
use librtmp2_server::test_support::TestServer;
use serial_test::serial;

const API_TOKEN: &str = "a-strong-random-secret-value-for-e2e-tests-only";
const RTMP_PORT: u16 = 19701;

const PUB_KEY: &str = "pub_e2e_key_with_sufficient_length_here01";
const PLAY_KEY: &str = "play_e2e_key_with_sufficient_length_here01";
const STATS_KEY: &str = "st_e2e_key_with_sufficient_length_here001";

static PLAYER_FRAMES: AtomicUsize = AtomicUsize::new(0);

fn on_player_frame(frame: &Frame) {
    if frame.frame_type == FrameType::Video && frame.size >= 16 {
        PLAYER_FRAMES.fetch_add(1, Ordering::SeqCst);
    }
}

fn create_stream_via_http(server: &TestServer, stream_id: &str) -> serde_json::Value {
    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(format!("{}/api/v1/streams", server.http_base))
        .header("Authorization", format!("Bearer {}", server.api_token))
        .json(&serde_json::json!({
            "id": stream_id,
            "name": "E2E Stream",
            "app": "live",
            "publish_key": PUB_KEY,
            "play_key": PLAY_KEY,
            "stats_key": STATS_KEY,
        }))
        .send()
        .expect("create stream request");
    assert_eq!(
        resp.status(),
        201,
        "stream create failed: {}",
        resp.status()
    );
    resp.json().expect("create stream json")
}

#[test]
#[serial]
fn health_endpoint_reports_rtmp_port() {
    let server = TestServer::start(RTMP_PORT, API_TOKEN);
    let client = reqwest::blocking::Client::new();
    let public: serde_json::Value = client
        .get(format!("{}/api/v1/health", server.http_base))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(public["status"], "ok");
    assert!(public.get("rtmp_port").is_none());

    let health: serde_json::Value = client
        .get(format!("{}/api/v1/health", server.http_base))
        .header("Authorization", format!("Bearer {API_TOKEN}"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(health["rtmp_port"], RTMP_PORT);
}

#[test]
#[serial]
fn http_create_stream_then_rtmp_publish_and_play() {
    PLAYER_FRAMES.store(0, Ordering::SeqCst);

    let server = TestServer::start(RTMP_PORT, API_TOKEN);
    create_stream_via_http(&server, "e2e-stream");

    let rtmp_port = server.rtmp_port;
    let client_result = thread::spawn(move || {
        let mut publisher = Client::new();
        publisher.connect(&format!("rtmp://127.0.0.1:{rtmp_port}/live/{PUB_KEY}"))?;
        publisher.publish()?;

        let mut player = Client::new();
        player.on_frame_cb = Some(on_player_frame);
        player.connect(&format!("rtmp://127.0.0.1:{rtmp_port}/live/{PLAY_KEY}"))?;
        player.play()?;

        thread::sleep(Duration::from_millis(150));

        let data = [
            0x17u8, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];
        let frame = Frame {
            frame_type: FrameType::Video,
            timestamp: 0,
            composition_time: 0,
            size: data.len() as u32,
            data: data.as_ptr(),
            audio_codec: Default::default(),
            audio_sample_rate: 0,
            audio_channels: 0,
            audio_bit_depth: 0,
            audio_fourcc: Default::default(),
            video_codec: VideoCodec::H264,
            video_fourcc: Default::default(),
            video_frame_type: 1,
            is_metadata: 0,
        };
        publisher.send_frame(&frame)?;

        let deadline = Instant::now() + Duration::from_secs(8);
        while PLAYER_FRAMES.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            player.poll(50)?;
        }
        Ok::<(), librtmp2::types::ErrorCode>(())
    })
    .join()
    .unwrap();

    client_result.expect("RTMP publish/play client failed");
    assert!(
        PLAYER_FRAMES.load(Ordering::SeqCst) > 0,
        "player should receive relayed video frame after HTTP-provisioned stream"
    );
}

#[test]
#[serial]
fn rtmp_publish_rejected_for_unknown_key() {
    let server = TestServer::start(RTMP_PORT + 1, API_TOKEN);
    create_stream_via_http(&server, "auth-stream");

    let rtmp_port = server.rtmp_port;
    let publish_result = thread::spawn(move || {
        let mut publisher = Client::new();
        publisher.connect(&format!(
            "rtmp://127.0.0.1:{rtmp_port}/live/unknown_key_with_sufficient_len_x"
        ))?;
        publisher.publish()
    })
    .join()
    .unwrap();

    assert!(
        publish_result.is_err(),
        "publish with unknown key should be rejected: {publish_result:?}"
    );
    assert!(
        server
            .db
            .publisher_list(Some("auth-stream"))
            .iter()
            .all(|publisher| !publisher.active),
        "unknown publish key must not activate a publisher row"
    );
}

#[test]
#[serial]
fn delete_stream_via_http_then_list_excludes_it() {
    let server = TestServer::start(RTMP_PORT + 2, API_TOKEN);
    create_stream_via_http(&server, "delete-me");

    let client = reqwest::blocking::Client::new();
    let del = client
        .delete(format!("{}/api/v1/streams/delete-me", server.http_base))
        .header("Authorization", format!("Bearer {}", server.api_token))
        .send()
        .unwrap();
    assert_eq!(del.status(), 200);

    let list: Vec<serde_json::Value> = client
        .get(format!("{}/api/v1/streams", server.http_base))
        .header("Authorization", format!("Bearer {}", server.api_token))
        .send()
        .unwrap()
        .json()
        .unwrap();

    assert!(
        !list.iter().any(|s| s["id"] == "delete-me"),
        "deleted stream should not appear in list"
    );
}

#[test]
#[serial]
fn public_stats_json_offline_before_publish() {
    let server = TestServer::start(RTMP_PORT + 3, API_TOKEN);
    create_stream_via_http(&server, "stats-stream");

    let client = reqwest::blocking::Client::new();
    let resp = client
        .get(format!("{}/stats", server.http_base))
        .query(&[("key", STATS_KEY)])
        .send()
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().unwrap();
    assert_eq!(body, "Stream offline");
}
