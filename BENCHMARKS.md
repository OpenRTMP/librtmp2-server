# Benchmarks

Real, reproducible numbers for `librtmp2-server`'s RTMP ingest/relay path,
plus a same-machine comparison against nginx-rtmp and MediaMTX using the
*same* RTMP client for all three, so the comparison isn't skewed by
differences between test clients.

**Read this before quoting a number from it:** every result below is from
one run on one shared 4-vCPU VM, with all three servers
benchmarked one at a time (not simultaneously) to avoid CPU contention
between them skewing the comparison. Treat the *relative* shape of the
results — where the numbers behave the same or differently across servers —
as the useful signal, and re-run `scripts/run_rtmp_benchmarks.sh` on your
own target hardware before using any of this for capacity planning.

## What's being compared

| | librtmp2-server | nginx-rtmp | MediaMTX |
|---|---|---|---|
| Version | 0.3.0 (this repo) | nginx 1.24.0 + `libnginx-mod-rtmp` 1.2.2 (Ubuntu package) | v1.21.1 |
| Language | Rust | C | Go |
| Role | what this repo ships | most common existing RTMP relay | modern multi-protocol media server with RTMP support |

**SRS is not included.** nginx-rtmp was installed from the Ubuntu package
archive and MediaMTX was fetched via the Go module proxy and built from
source (working around its release-time-only generated asset step — see the
script); SRS ships primarily as a Docker image, which wasn't set up for this
run. `scripts/run_rtmp_benchmarks.sh` is structured so adding a fourth
`relay_sweep` block for SRS is the only change needed — contributions
welcome.

## Methodology

Every test uses `librtmp2`'s own `examples/bench_handshake.rs` and
`examples/bench_relay.rs` (see
[librtmp2's `BENCHMARKS.md`](https://github.com/OpenRTMP/librtmp2/blob/main/BENCHMARKS.md#examplesbench_handshakers-and-examplesbench_relayrs))
as the client against all three servers, and a real `ffmpeg`-encoded source
(`testsrc` 1280x720@30 + a sine tone, libx264 veryfast/zerolatency @ 2.5 Mbps
video + AAC @ 128 kbps audio, 2s GOP) as the publisher, so every server is
relaying genuine, codec-valid H.264/AAC — not synthetic garbage bytes, which
some servers (MediaMTX in particular, since it parses codec data rather than
blindly repeating bytes) would reject.

- **Handshake latency**: `bench_handshake` runs 120 connect+publish
  handshakes (up to `NetStream.Publish.Start`) at concurrency 30, from a
  cold process each time. Pure RTMP protocol, no media, so directly
  comparable across servers.
- **Concurrent-viewer relay**: for N ∈ {1, 25, 100}, start one ffmpeg
  publisher, wait 3s for it to stabilize, then start N concurrent
  `bench_relay` viewers against the live stream for 15s, discarding the
  first 3s of each viewer's own reception (`--warmup-ms 3000`) to exclude
  the initial GOP/burst. Reports join latency (connect → first frame
  received) and steady-state aggregate throughput/frame rate.

Full commands, including exact server configs, are in
[`scripts/run_rtmp_benchmarks.sh`](scripts/run_rtmp_benchmarks.sh).

### Two things this uncovered worth knowing regardless of the numbers

- **nginx-rtmp's live relay state is per worker process.** With
  `worker_processes auto` (4, matching this box's core count — the normal
  recommendation for nginx), a publisher and a viewer for the same stream
  can land on different workers via the kernel's connection-accept
  balancing, and since nginx-rtmp doesn't share stream state across worker
  processes, a viewer on the "wrong" worker gets **zero frames**, silently.
  We measured this directly: the same test that gets ~73 combined
  audio+video frames/sec/viewer at `worker_processes 1` dropped to ~9-19
  frames/sec/viewer at `worker_processes auto`, purely from viewers landing
  on workers with no publisher. The results below use `worker_processes 1`,
  which is the configuration commonly recommended for nginx-rtmp
  specifically (unlike plain HTTP nginx, where more workers is close to
  always better) — but it means nginx-rtmp on more cores needs an external
  sticky-routing layer (consistent hashing on stream name at a load
  balancer, e.g.) to actually use them for one live stream, something
  neither `librtmp2-server` nor MediaMTX need.
- **`librtmp2-server` caps concurrent connections per `play_key` at 5 by
  design** (`db::MAX_CONNECTIONS_PER_PLAY_KEY`). A 6th simultaneous viewer
  on the same key is rejected, and enough rejections from one IP trip a
  separate per-IP auth-failure rate limiter that then blocks *all* auth
  attempts (including valid ones) from that IP for 60s — a real
  brute-force protection, but one that will surprise anyone fanning out
  many viewers behind one NAT/CGNAT IP with a single shared `play_key`.
  The intended pattern is one `play_key` per viewer (`POST
  /api/v1/streams/:id/players`), which is what the 25/100-viewer results
  below actually provision (25 keys, ≤5 viewers each) — worth calling out
  explicitly in the README/docs for anyone building on this API, since it's
  not obvious from the "unique keys per stream" framing alone.

## Environment

- CPU: Intel Xeon @ 2.80GHz, 4 vCPUs (a shared VM — not bare metal)
- RAM: 15 GiB, Linux 6.18 x86_64
- rustc 1.98.1, ffmpeg 6.1.1
- Date: 2026-09-22

## Handshake latency (connect + publish, count=120, concurrency=30)

| Server | Success rate | Handshakes/s | avg | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| librtmp2-server | 100% | 211.9/s | 139.4 ms | 145.5 ms | 157.4 ms | 179.3 ms | 179.4 ms |
| nginx-rtmp | 100% | 329.2/s | 87.0 ms | 89.3 ms | 92.4 ms | 95.4 ms | 96.1 ms |
| MediaMTX | 100% | 656.7/s | 42.8 ms | 43.9 ms | 49.0 ms | 49.8 ms | 49.9 ms |

librtmp2-server's higher handshake latency here lines up with it being the
newest, least-optimized-for-this-path implementation of the three; it's a
reasonable first profiling target if connect-time latency matters for a
given deployment (e.g. very short clips, fast channel-surfing UIs).

## Concurrent-viewer relay throughput and join latency

Combined audio+video frame rate for this source is ~73 tags/sec/viewer
(30 fps video + ~43 fps audio); "steady fps/player" close to 73 means every
viewer received the full stream with no drops.

| Server | Players | Join latency avg / p95 / max | Steady throughput | Steady fps/player |
|---|---|---|---|---|
| librtmp2-server | 1 | 204.7 / 204.7 / 204.7 ms | 1.10 Mbps | 73.1 |
| librtmp2-server | 25 | 201.1 / 202.0 / 202.0 ms | 27.36 Mbps | 72.8 |
| librtmp2-server | 100 | 291.2 / 295.1 / 296.0 ms | 109.93 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 1 | 129.3 / 129.3 / 129.3 ms | 1.10 Mbps | 73.2 |
| nginx-rtmp (1 worker) | 25 | 129.5 / 134.5 / 135.1 ms | 27.38 Mbps | 73.0 |
| nginx-rtmp (1 worker) | 100 | 137.9 / 148.1 / 148.5 ms | 109.62 Mbps | 73.0 |
| MediaMTX | 1 | 46.0 / 46.0 / 46.0 ms | 1.10 Mbps | 73.1 |
| MediaMTX | 25 | 43.0 / 51.3 / 51.4 ms | 27.42 Mbps | 73.1 |
| MediaMTX | 100 | 63.0 / 81.7 / 84.9 ms | 109.65 Mbps | 73.1 |

Takeaways:

- **All three relayed every frame to every viewer with zero loss** at up to
  100 concurrent viewers of one stream on this 4-vCPU box (steady fps/player
  ≈ 73 across the board) — none of the three is anywhere near saturated at
  this concurrency on this hardware.
- **Join latency**: MediaMTX joins fastest (43-85 ms), nginx-rtmp next
  (129-148 ms), librtmp2-server slowest (201-296 ms) and the only one whose
  join latency clearly increases with viewer count. This is the most
  actionable finding here for `librtmp2-server`'s own roadmap — worth
  profiling the play/subscribe path (GOP replay, cache-lookup, or per-viewer
  setup cost) since 100 viewers joining an already-live stream is a very
  ordinary "stream just went viral" scenario.
- Aggregate throughput scales linearly with viewer count for all three, as
  expected for a simple relay (no transcoding) — 100 viewers at ~1.1 Mbps
  each is ~110 Mbps served, consistent across all three implementations.

## Component microbenchmark: `benches/http_api.rs`

This repo also ships a Criterion bench for the HTTP/REST API
(`cargo bench --bench http_api --features test-support`). As of this
writing it doesn't run to completion out of the box: `TestServer` starts
with the server's default `HTTP_RATE_LIMIT_DEFAULT` (60 requests/60s per
IP), and Criterion's own warm-up phase for the `health` benchmark alone
sends well over 60 requests in a few seconds, so the bench panics on a 429
partway through warm-up. Worth fixing by giving `TestServer` a way to raise
or disable the rate limit for benchmarking — filed here rather than fixed
in this PR since it's a pre-existing gap in the bench harness, not part of
publishing the RTMP-path numbers above.

## Reproducing this

```bash
# from a checkout of librtmp2-server, with a sibling ../librtmp2 checkout:
(cd ../librtmp2 && cargo build --release --example bench_handshake --example bench_relay)
cargo build --release
MEDIAMTX_BIN=/path/to/mediamtx scripts/run_rtmp_benchmarks.sh
```

MediaMTX isn't vendored or built by this script (no Go toolchain
dependency is added to this repo for it) — build or download it separately
and point `MEDIAMTX_BIN` at the binary; the nginx-rtmp and librtmp2-server
legs run without it.
