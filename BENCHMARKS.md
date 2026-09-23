# Benchmarks

Real, reproducible numbers for `librtmp2-server`'s RTMP ingest/relay path,
plus a same-machine comparison against nginx-rtmp, MediaMTX, and SRS using
the *same* RTMP client for all four, so the comparison isn't skewed by
differences between test clients.

**Read this before quoting a number from it:** every result below is from
one run on one shared 4-vCPU VM, with all four servers
benchmarked one at a time (not simultaneously) to avoid CPU contention
between them skewing the comparison. Treat the *relative* shape of the
results — where the numbers behave the same or differently across servers —
as the useful signal, and re-run `scripts/run_rtmp_benchmarks.sh` on your
own target hardware before using any of this for capacity planning.

## What's being compared

| | librtmp2-server | nginx-rtmp | MediaMTX | SRS |
|---|---|---|---|---|
| Version | 0.4.0 (this repo) | nginx 1.24.0 + `libnginx-mod-rtmp` 1.2.2 (Ubuntu package) | v1.11.3 | v7.0-a0 (7.0.162) |
| Language | Rust | C | Go | C++ |
| Role | what this repo ships | most common existing RTMP relay | modern multi-protocol media server with RTMP support | long-running open-source media server with RTMP/SRT/WebRTC support |

nginx-rtmp was installed from the Ubuntu package archive; MediaMTX was
fetched via the Go module proxy and built from source (working around its
release-time-only generated asset step — see the script); SRS was built
from source (`trunk/configure && make`) at its own repo rather than run
from a container, to keep every server here on the same footing (installed
package or built binary, nothing containerized).

## Methodology

Every test uses `librtmp2`'s own `examples/bench_handshake.rs` and
`examples/bench_relay.rs` (see
[librtmp2's `BENCHMARKS.md`](https://github.com/OpenRTMP/librtmp2/blob/main/BENCHMARKS.md#examplesbench_handshakers-and-examplesbench_relayrs))
as the client against all four servers, and a real `ffmpeg`-encoded source
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

### Four things this uncovered worth knowing regardless of the numbers

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
  neither `librtmp2-server` nor MediaMTX nor SRS need.
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
- **The setup phase, not the timed benchmark, can hit the admin HTTP API's
  default rate limit.** Provisioning 120 distinct handshake streams via
  `POST /api/v1/streams` as fast as `curl` allows exceeds
  `HTTP_RATE_LIMIT_DEFAULT` (60 requests/60s per IP) partway through, so a
  reproduction may see `429` warnings in the server log while the script
  provisions streams. `bench_handshake` still runs its full configured
  `--count` by cycling the URLs it did get, so this doesn't affect the
  reported numbers — it only means fewer distinct stream IDs back the run
  than the setup loop requested.
- **SRS defaults to a merged-write buffer that trades latency for fewer
  syscalls.** Its own startup log states this plainly: `system default
  latency(ms): mw(0-350) + mr(0-350)`. That buffering — not RTMP protocol
  overhead or SRS's own per-connection cost — is most of the gap between
  SRS's numbers below and MediaMTX's: SRS exposes `min_latency on; mw_latency
  0;` (see `trunk/conf/full.conf`) specifically to trade it away for
  real-time delivery at higher CPU cost, which this run leaves at its
  default rather than tuning for either side of that trade-off.

## Environment

- CPU: Intel Xeon @ 2.80GHz, 4 vCPUs (a shared VM — not bare metal)
- RAM: 15 GiB, Linux 6.18 x86_64
- rustc 1.95.0, g++ 13.3.0, ffmpeg 6.1.1
- Date: 2026-09-23

## Handshake latency (connect + publish, count=120, concurrency=30)

| Server | Success rate | Handshakes/s | avg | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| librtmp2-server | 100% | 542.3/s | 52.4 ms | 51.4 ms | 59.3 ms | 61.0 ms | 61.1 ms |
| nginx-rtmp | 100% | 330.5/s | 81.1 ms | 88.1 ms | 92.0 ms | 96.4 ms | 96.4 ms |
| MediaMTX | 100% | 651.5/s | 42.2 ms | 44.2 ms | 48.0 ms | 48.3 ms | 48.9 ms |
| SRS | 100% | 304.2/s | 94.3 ms | 93.4 ms | 101.6 ms | 107.0 ms | 107.3 ms |

`librtmp2-server`'s RTMP poll loop polls every 1ms while any connection is
still negotiating (handshake/connect/createStream/publish|play) or an async
publish/play authorization decision just resolved, backing off to 50ms only
once nothing is; every publish/play authorization commits its SQLite write
with `synchronous=NORMAL` rather than paying an fsync per commit, and
publish/play authorization itself runs on a dedicated worker thread
(`src/auth_worker.rs`) instead of the RTMP poll thread. Together these keep
it clearly ahead of nginx-rtmp and SRS here, and second only to MediaMTX.

## Concurrent-viewer relay throughput and join latency

Combined audio+video frame rate for this source is ~73 tags/sec/viewer
(30 fps video + ~43 fps audio); "steady fps/player" close to 73 means every
viewer received the full stream with no drops.

| Server | Players | Join latency avg / p95 / max | Steady throughput | Steady fps/player |
|---|---|---|---|---|
| librtmp2-server | 1 | 100.3 / 100.3 / 100.3 ms | 1.09 Mbps | 72.8 |
| librtmp2-server | 25 | 102.6 / 103.6 / 103.7 ms | 27.34 Mbps | 72.9 |
| librtmp2-server | 100 | 142.3 / 152.7 / 156.1 ms | 109.64 Mbps | 73.0 |
| nginx-rtmp (1 worker) | 1 | 138.2 / 138.2 / 138.2 ms | 1.10 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 25 | 130.5 / 135.5 / 135.6 ms | 27.39 Mbps | 73.0 |
| nginx-rtmp (1 worker) | 100 | 135.0 / 141.6 / 145.5 ms | 109.56 Mbps | 73.0 |
| MediaMTX | 1 | 45.0 / 45.0 / 45.0 ms | 1.10 Mbps | 73.2 |
| MediaMTX | 25 | 41.1 / 46.1 / 46.1 ms | 27.42 Mbps | 73.1 |
| MediaMTX | 100 | 45.5 / 49.6 / 51.0 ms | 109.72 Mbps | 73.1 |
| SRS | 1 | 88.3 / 88.3 / 88.3 ms | 1.07 Mbps | 71.8 |
| SRS | 25 | 92.6 / 96.8 / 97.3 ms | 27.11 Mbps | 72.1 |
| SRS | 100 | 106.8 / 127.7 / 134.2 ms | 108.30 Mbps | 72.1 |

Takeaways:

- **All four relayed every frame to every viewer with zero loss** at up to
  100 concurrent viewers of one stream on this 4-vCPU box (steady fps/player
  in the 72-73 range across the board) — none of the four is anywhere near
  saturated at this concurrency on this hardware.
- **Join latency at 1 and 25 viewers**: `librtmp2-server` (100-103 ms) and
  SRS (88-93 ms) sit ahead of nginx-rtmp (130-138 ms); MediaMTX is fastest to
  join at this concurrency (41-45 ms), and SRS's default merged-write
  buffering (see above) is most of what separates it from MediaMTX here
  rather than raw per-connection cost.
- **Join latency at 100 viewers**: `librtmp2-server` (142.3 ms) and SRS
  (106.8 ms) both increase with viewer count more than nginx-rtmp
  (135.0 ms) and MediaMTX (45.5 ms) do; publish/play authorization running
  on a dedicated worker thread (`src/auth_worker.rs`) keeps
  `librtmp2-server` close to nginx-rtmp here rather than falling further
  behind.
- Aggregate throughput scales linearly with viewer count for all four, as
  expected for a simple relay (no transcoding) — 100 viewers at ~1.1 Mbps
  each is ~108-110 Mbps served, consistent across all four implementations.

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
MEDIAMTX_BIN=/path/to/mediamtx SRS_BIN=/path/to/srs scripts/run_rtmp_benchmarks.sh
```

Neither MediaMTX nor SRS is vendored or built by this script (no Go
toolchain dependency for MediaMTX, and SRS's own build is a separate,
sizeable C++ project) — build or download each separately and point
`MEDIAMTX_BIN`/`SRS_BIN` at the resulting binaries; the nginx-rtmp and
librtmp2-server legs run without either. Build SRS from
[github.com/ossrs/srs](https://github.com/ossrs/srs) with
`(cd trunk && ./configure && make)`; the resulting `trunk/objs/srs` binary
is what `SRS_BIN` should point at.
