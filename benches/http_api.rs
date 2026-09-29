use std::sync::atomic::{AtomicU64, Ordering};

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use librtmp2_server::config::ServerConfig;
use librtmp2_server::test_support::TestServer;

const BENCH_TOKEN: &str = "bench_api_token_with_sufficient_length_for_http_tests01";
const RTMP_PORT: u16 = 19801;

static STREAM_SEQ: AtomicU64 = AtomicU64::new(0);

fn shared_server() -> TestServer {
    // Criterion drives far more requests per minute than the production
    // per-peer limits allow (60/120 per 60s), so the bench would be answered
    // with 429 -- and `error_for_status().unwrap()` below would panic -- long
    // before it finished. Keep the limiter in the path, but size its window
    // and caps so it cannot throttle a benchmark.
    let config = ServerConfig {
        http_rate_limit_window_secs: 1,
        http_rate_limit_api: 1_000_000,
        http_rate_limit_stats: 1_000_000,
        http_rate_limit_default: 1_000_000,
        ..Default::default()
    };
    TestServer::start_with_config(RTMP_PORT, BENCH_TOKEN, config)
}

fn bench_http_api(c: &mut Criterion) {
    let server = shared_server();
    let client = reqwest::blocking::Client::new();
    let base = server.http_base.clone();
    let auth = format!("Bearer {}", server.api_token);

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

    group.bench_function("list_streams", |b| {
        b.iter(|| {
            let resp = client
                .get(format!("{base}/api/v1/streams"))
                .header("Authorization", &auth)
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
