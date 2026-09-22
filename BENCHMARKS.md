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
- Publisher and viewer URLs always target the exact same stream name for a
  given server/N combination — for nginx-rtmp and MediaMTX in particular, a
  mismatch here silently relays nothing (nginx returns one stray control
  frame and stops; MediaMTX's `play()` fails outright), so this is worth
  double-checking in any reproduction that customizes the script.

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
| librtmp2-server | 100% | 471.4/s | 61.3 ms | 59.5 ms | 73.9 ms | 78.0 ms | 80.2 ms |
| nginx-rtmp | 100% | 325.9/s | 87.3 ms | 88.0 ms | 95.8 ms | 96.0 ms | 96.1 ms |
| MediaMTX | 100% | 622.9/s | 42.0 ms | 47.0 ms | 48.9 ms | 49.4 ms | 49.5 ms |

`librtmp2-server`'s RTMP poll loop polls every 5ms while any connection is
still negotiating (handshake/connect/createStream/publish|play), backing off
to a 50ms interval only once nothing is, and every publish/play
authorization commits its SQLite write with `synchronous=NORMAL` rather than
paying an fsync per commit — together these keep it clearly ahead of
nginx-rtmp here, and second only to MediaMTX.

## Concurrent-viewer relay throughput and join latency

Combined audio+video frame rate for this source is ~73 tags/sec/viewer
(30 fps video + ~43 fps audio); "steady fps/player" close to 73 means every
viewer received the full stream with no drops.

| Server | Players | Join latency avg / p95 / max | Steady throughput | Steady fps/player |
|---|---|---|---|---|
| librtmp2-server | 1 | 118.3 / 118.3 / 118.3 ms | 1.09 Mbps | 72.8 |
| librtmp2-server | 25 | 122.7 / 133.7 / 133.9 ms | 27.42 Mbps | 73.1 |
| librtmp2-server | 100 | 142.1 / 148.5 / 149.5 ms | 109.52 Mbps | 72.9 |
| nginx-rtmp (1 worker) | 1 | 129.4 / 129.4 / 129.4 ms | 1.10 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 25 | 130.3 / 133.1 / 133.1 ms | 27.43 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 100 | 129.5 / 133.8 / 134.8 ms | 109.72 Mbps | 73.1 |
| MediaMTX | 1 | 41.9 / 41.9 / 41.9 ms | 1.10 Mbps | 73.2 |
| MediaMTX | 25 | 44.4 / 49.2 / 49.8 ms | 27.44 Mbps | 73.2 |
| MediaMTX | 100 | 55.4 / 71.2 / 72.6 ms | 109.65 Mbps | 73.1 |

Takeaways:

- **All three relayed every frame to every viewer with zero loss** at up to
  100 concurrent viewers of one stream on this 4-vCPU box (steady fps/player
  ≈ 73 across the board) — none of the three is anywhere near saturated at
  this concurrency on this hardware.
- **Join latency at 1 and 25 viewers**: `librtmp2-server` (118-123 ms) is at
  or ahead of nginx-rtmp (129-130 ms); MediaMTX is fastest to join at this
  concurrency (42-44 ms).
- **Join latency at 100 viewers is `librtmp2-server`'s one remaining gap**:
  142 ms vs. 129.5 ms (nginx-rtmp) and 55 ms (MediaMTX), and it's the one of
  the three whose join latency increases most with viewer count (118 ms at 1
  viewer → 142 ms at 100). The poll-loop interval and the SQLite commit path
  are both already tuned for this (see the handshake section above); the
  remaining cost is per-connection work inside publish/play authorization
  itself (DB lookups, key generation, logging) running one connection after
  another on the single RTMP thread — worth profiling further if this needs
  to come down more.
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
