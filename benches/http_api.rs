use std::sync::atomic::{AtomicU64, Ordering};

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use librtmp2_server::config::ServerConfig;
use librtmp2_server::test_support::TestServer;

const BENCH_TOKEN: &str = "bench_api_token_with_sufficient_length_for_http_tests01";
const RTMP_PORT: u16 = 19801;
/// `list_streams` measures a full-table scan, so its dataset must be fixed
/// rather than whatever the preceding `create_stream` benchmark inserted.
/// It therefore gets its own server and in-memory DB, seeded with exactly
/// [`LIST_SEED_STREAMS`] rows before measurement.
const LIST_RTMP_PORT: u16 = 19802;
const LIST_SEED_STREAMS: usize = 1_000;

static STREAM_SEQ: AtomicU64 = AtomicU64::new(0);

fn bench_config() -> ServerConfig {
    // Criterion drives far more requests per minute than the production
    // per-peer limits allow (60/120 per 60s), so the bench would be answered
    // with 429 -- and `error_for_status().unwrap()` below would panic -- long
    // before it finished. Keep the limiter in the path, but size its window
    // and caps so it cannot throttle a benchmark.
    ServerConfig {
        http_rate_limit_window_secs: 1,
        http_rate_limit_api: 1_000_000,
        http_rate_limit_stats: 1_000_000,
        http_rate_limit_default: 1_000_000,
        ..Default::default()
    }
}

fn shared_server(rtmp_port: u16) -> TestServer {
    TestServer::start_with_config(rtmp_port, BENCH_TOKEN, bench_config())
}

/// Insert a fixed number of streams through the public API so the seeded rows
/// are indistinguishable from operator-created ones (including their default
/// viewer row, which `GET /api/v1/streams` reads per stream).
fn seed_streams(server: &TestServer, count: usize) {
    let client = reqwest::blocking::Client::new();
    let auth = format!("Bearer {}", server.api_token);
    for n in 0..count {
        client
            .post(format!("{}/api/v1/streams", server.http_base))
            .header("Authorization", &auth)
            .json(&serde_json::json!({
                "id": format!("seed{n}"),
                "name": "Seed",
                "app": "live",
                "publish_key": format!("pub_seed_key_with_sufficient_length_{n:05}"),
                "play_key": format!("play_seed_key_with_sufficient_length_{n:05}"),
                "stats_key": format!("st_seed_key_with_sufficient_length_{n:05}"),
            }))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();
    }
}

fn bench_http_api(c: &mut Criterion) {
    let server = shared_server(RTMP_PORT);
    let client = reqwest::blocking::Client::new();
    let base = server.http_base.clone();
    let auth = format!("Bearer {}", server.api_token);

    let list_server = shared_server(LIST_RTMP_PORT);
    seed_streams(&list_server, LIST_SEED_STREAMS);
    let list_base = list_server.http_base.clone();
    let list_auth = format!("Bearer {}", list_server.api_token);

    let mut group = c.benchmark_group("http_api");

    group.bench_function("health", |b| {
        b.iter(|| {
            client
                .get(format!("{base}/api/v1/health"))
                .send()
                .unwrap()
                .error_for_status()
                .unwrap();
        });
    });

    group.bench_function("create_stream", |b| {
        b.iter(|| {
            let n = STREAM_SEQ.fetch_add(1, Ordering::Relaxed);
            let stream_id = format!("bench{n}");
            let resp = client
                .post(format!("{base}/api/v1/streams"))
                .header("Authorization", &auth)
                .json(&serde_json::json!({
                    "id": stream_id,
                    "name": "Bench",
                    "app": "live",
                    "publish_key": format!("pub_bench_key_with_sufficient_length_{n:04}"),
                    "play_key": format!("play_bench_key_with_sufficient_length_{n:04}"),
                    "stats_key": format!("st_bench_key_with_sufficient_length_{n:04}"),
                }))
                .send()
                .unwrap()
                .error_for_status()
                .unwrap();
            black_box(resp.text().unwrap());
        });
    });

    // A separate, fixed-size dataset: the result no longer depends on how
    // many rows `create_stream` happened to insert on this host.
    group.bench_function("list_streams", |b| {
        b.iter(|| {
            let resp = client
                .get(format!("{list_base}/api/v1/streams"))
                .header("Authorization", &list_auth)
                .send()
                .unwrap()
                .error_for_status()
                .unwrap();
            black_box(resp.text().unwrap());
        });
    });

    group.finish();
}

criterion_group!(http_api, bench_http_api);
criterion_main!(http_api);
