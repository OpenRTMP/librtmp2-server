use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use librtmp2_server::test_support::TestServer;

const BENCH_TOKEN: &str = "bench_api_token_with_sufficient_length_for_http_tests01";
const RTMP_PORT: u16 = 19801;

static STREAM_SEQ: AtomicU64 = AtomicU64::new(0);

fn shared_server() -> TestServer {
    TestServer::start(RTMP_PORT, BENCH_TOKEN)
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
