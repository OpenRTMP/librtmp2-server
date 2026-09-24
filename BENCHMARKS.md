# Benchmarks

Real, reproducible numbers for `librtmp2-server`'s RTMP ingest/relay path,
plus a same-machine comparison against nginx-rtmp, MediaMTX, SRS and LiveForge
using the *same* RTMP client for all five, so the comparison isn't skewed by
differences between test clients.

**Read this before quoting a number from it:** the full five-server sweep
below is one run on one shared 4-vCPU VM, and single runs vary by roughly
±7-10 ms at 100 viewers. The closest competitors (MediaMTX and LiveForge)
were therefore also measured over repeated, interleaved rounds against the
current state of this branch — see
[Repeated rounds](#repeated-rounds-librtmp2-server-vs-mediamtx-vs-liveforge),
which is the section to compare against. The sweep was run with all five servers
benchmarked one at a time (not simultaneously) to avoid CPU contention
between them skewing the comparison. Treat the *relative* shape of the
results — where the numbers behave the same or differently across servers —
as the useful signal, and re-run `scripts/run_rtmp_benchmarks.sh` on your
own target hardware before using any of this for capacity planning.

## What's being compared

| | librtmp2-server | nginx-rtmp | MediaMTX | SRS | LiveForge |
|---|---|---|---|---|---|
| Version | 0.5.0, built against librtmp2 0.10.0 | nginx 1.24.0 + `libnginx-mod-rtmp` 1.2.2 (Ubuntu package) | v1.11.3 | v8.0.44 (`develop`, bundled FFmpeg) | `main` @ 4e70fb3 (built with Go 1.26) |
| Language | Rust | C | Go | C++ | Go |
| Role | what this repo ships | most common existing RTMP relay | modern multi-protocol media server with RTMP support | long-running open-source media server with RTMP/SRT/WebRTC support | newer multi-protocol Go live server (RTMP/RTSP/SRT/WebRTC/HLS) |

nginx-rtmp was installed from the Ubuntu package archive; MediaMTX was
fetched via the Go module proxy and built from source (working around its
release-time-only generated asset step — see the script); SRS was built
from source (`trunk/configure --ffmpeg-fit=on --sys-ffmpeg=off --https=off
--gb28181=off && make`; SRS 8 no longer builds against the system FFmpeg
6.1 headers) at its own repo rather than run
from a container, to keep every server here on the same footing (installed
package or built binary, nothing containerized). LiveForge was built from
source (`go build ./cmd/liveforge`) and run with RTMP only: its sample
config also enables recording to disk and a dozen other listeners, all
switched off here (see the script).

## Methodology

Every test uses `librtmp2`'s own `examples/bench_handshake.rs` and
`examples/bench_relay.rs` (see
[librtmp2's `BENCHMARKS.md`](https://github.com/OpenRTMP/librtmp2/blob/main/BENCHMARKS.md#examplesbench_handshakers-and-examplesbench_relayrs))
as the client against all five servers, and a real `ffmpeg`-encoded source
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
- Date: 2026-09-25

## Handshake latency (connect + publish, count=120, concurrency=30)

Single sweep of `scripts/run_rtmp_benchmarks.sh`, `librtmp2-server` 0.5.0
on librtmp2 0.10.0 (default sharding, 4 poll threads on this box):

| Server | Success rate | Handshakes/s | avg | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| librtmp2-server | 100% | **5330.6/s** | **4.27 ms** | **3.82 ms** | **8.25 ms** | **9.05 ms** | **9.60 ms** |
| nginx-rtmp | 100% | 605.1/s | 47.68 ms | 47.95 ms | 54.45 ms | 58.37 ms | 59.13 ms |
| MediaMTX | 100% | 4320.1/s | 5.75 ms | 5.67 ms | 9.51 ms | 10.54 ms | 10.59 ms |
| SRS | 100% | 438.1/s | 64.63 ms | 65.31 ms | 77.24 ms | 77.58 ms | 77.58 ms |
| LiveForge | 100% | 4491.8/s | 5.60 ms | 5.31 ms | 10.47 ms | 11.79 ms | 11.84 ms |

`librtmp2-server` is fastest on every column, ahead of MediaMTX and
LiveForge and roughly 10x faster than nginx-rtmp and SRS, even though it is
the only server here that authenticates every publish against a database
(per-stream keys in SQLite, via the auth worker); the others were run
accepting any stream name. What gets it there: the poll loop waits on a
persistent `epoll(7)` set and is woken through an `eventfd` as soon as an
authorization completes, new connections are accepted immediately, every
socket has `TCP_NODELAY`, authorizations are group-committed off the poll
thread with `synchronous=NORMAL`, stats are batched on a separate thread,
and connections are spread over one `SO_REUSEPORT` poll shard per CPU (up
to 4). Before this release it measured 9.5 ms avg / 19.1 ms p95 here.

## Concurrent-viewer relay throughput and join latency

Combined audio+video frame rate for this source is ~73 tags/sec/viewer
(30 fps video + ~43 fps audio); "steady fps/player" close to 73 means every
viewer received the full stream with no drops. Same single sweep as above.

| Server | Players | Join latency avg / p95 / max | Steady throughput | Steady fps/player |
|---|---|---|---|---|
| librtmp2-server | 1 | **1.69** / 1.69 / 1.69 ms | 1.09 Mbps | 73.0 |
| librtmp2-server | 25 | **4.52** / **6.57** / **6.66** ms | 27.35 Mbps | 73.0 |
| librtmp2-server | 100 | 21.72 / 27.77 / 28.71 ms | 109.53 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 1 | 90.37 / 90.37 / 90.37 ms | 1.10 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 25 | 89.11 / 93.45 / 94.45 ms | 27.43 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 100 | 92.11 / 97.56 / 100.53 ms | 109.61 Mbps | 73.0 |
| MediaMTX | 1 | 3.60 / 3.60 / 3.60 ms | 1.10 Mbps | 73.3 |
| MediaMTX | 25 | 5.04 / 6.63 / 7.46 ms | 27.40 Mbps | 73.1 |
| MediaMTX | 100 | **13.53** / **20.65** / **22.11** ms | 110.01 Mbps | 73.4 |
| SRS | 1 | 46.84 / 46.84 / 46.84 ms | 1.07 Mbps | 71.8 |
| SRS | 25 | 56.35 / 66.04 / 66.13 ms | 26.83 Mbps | 71.9 |
| SRS | 100 | 92.19 / 139.94 / 159.98 ms | 107.48 Mbps | 71.9 |
| LiveForge | 1 | 4.43 / 4.43 / 4.43 ms | 1.09 Mbps | 73.0 |
| LiveForge | 25 | 8.07 / 18.64 / 19.46 ms | 27.38 Mbps | 73.1 |
| LiveForge | 100 | 19.88 / 48.01 / 61.67 ms | 109.59 Mbps | 73.1 |

Takeaways:

- **All five relayed every frame to every viewer with zero loss** at up to
  100 concurrent viewers of one stream on this 4-vCPU box (steady fps/player
  in the 72-73 range across the board).
- **Join latency at 1 and 25 viewers**: `librtmp2-server` is fastest
  (1.7 / 4.5 ms), ahead of MediaMTX (3.6 / 5.0 ms) and LiveForge
  (4.4 / 8.1 ms), and far ahead of SRS (46.8 / 56.4 ms) and nginx-rtmp
  (90.4 / 89.1 ms). SRS's default merged-write buffering (see above) is most
  of what separates it here rather than raw per-connection cost.
- **Join latency at 100 viewers**: in this single sweep MediaMTX was fastest
  (13.5 ms avg), then LiveForge (19.9 ms, but 48 ms p95) and
  `librtmp2-server` (21.7 ms, 27.8 ms p95), well ahead of nginx-rtmp and SRS
  (~92 ms). Single runs on this shared VM vary by roughly ±7-10 ms at this
  concurrency; averaged over interleaved rounds (next section)
  `librtmp2-server` comes out ahead at 11.9 ms vs MediaMTX's 14.5 ms and
  LiveForge's 24.8 ms. Before this release it measured ~38 ms here.
- Aggregate throughput scales linearly with viewer count for all five, as
  expected for a simple relay (no transcoding) — 100 viewers at ~1.1 Mbps
  each is ~108-110 Mbps served, consistent across all five implementations.

## Repeated rounds: librtmp2-server vs MediaMTX vs LiveForge

Same box, same client tools and ffmpeg source as above, current state of
this branch. Servers run one at a time, interleaved round by round
(librtmp2-server, LiveForge, MediaMTX, repeat), and averaged, which evens
out the VM's run-to-run noise that a single sweep can't:

| Metric | librtmp2-server | LiveForge | MediaMTX |
|---|---|---|---|
| connect+publish, sequential (`bench_handshake --concurrency 1`, 100 × 2 runs), avg / p50 | **0.55 / 0.52 ms** | 0.67 / 0.61 ms | 0.77 / 0.68 ms |
| single-viewer join (20 sequential `bench_relay --players 1` joins), avg / p50 | **1.19 / 1.19 ms** | 1.23 / 1.19 ms | 1.36 / 1.30 ms |
| 100-viewer join (3 rounds), avg / p50 / p95 | **11.9 / 7.7 / 23.2 ms** | 24.8 / 23.8 / 43.4 ms | 14.5 / 12.2 / 26.1 ms |
| connect+publish, 30 concurrent (3 rounds × 120), avg / p50 / p95 | **4.6 / 3.9 / 9.2 ms** | 5.2 / 4.9 / 10.6 ms | 6.6 / 6.3 / 12.6 ms |
| steady fps per viewer at 100 viewers | 73.0-73.2 | 73.1-73.2 | 73.1-73.3 |

`librtmp2-server` is the only one of the three that authenticates every
publish and play here (per-stream keys looked up and session rows written in
SQLite); the other two accept any stream name. Even so it now leads every
row: the lowest fixed per-connection cost (sequential connect+publish and
single-viewer join) and, since RTMP connections are spread over one
`SO_REUSEPORT` poll shard per CPU by default (up to 4; see CHANGELOG), also
the fastest 30-concurrent connect+publish and 100-viewer join. The
concurrent rows were re-measured after that change (3 interleaved rounds);
the sequential rows are from before it and are unaffected by it (a single
connection only ever touches one shard).

Sharding on its own, 1 vs 4 shards of the same build (4 interleaved rounds):
30 concurrent connect+publish 4.95 -> 3.63 ms avg (p95 6.8 -> 5.9 ms),
100-viewer join 16.5 -> 10.3 ms avg (p95 28.6 -> 22.5 ms), single-viewer
join unchanged (~1.5 ms), full frame rate for every viewer in both. Earlier
sharding runs showed no gain because a shard only noticed relayed frames on
its next poll tick; the receiving shard is now woken through its `eventfd`.
Set `LRTMP2_RTMP_SHARDS=1` to get the single-thread loop back.

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
MEDIAMTX_BIN=/path/to/mediamtx SRS_BIN=/path/to/srs LIVEFORGE_BIN=/path/to/liveforge \
  scripts/run_rtmp_benchmarks.sh
```

None of MediaMTX, SRS or LiveForge is vendored or built by this script (no
Go toolchain dependency for MediaMTX/LiveForge, and SRS's own build is a
separate, sizeable C++ project) — build or download each separately and
point `MEDIAMTX_BIN`/`SRS_BIN`/`LIVEFORGE_BIN` at the resulting binaries;
the nginx-rtmp and librtmp2-server legs run without them. Build LiveForge
from [github.com/im-pingo/liveforge](https://github.com/im-pingo/liveforge)
with `go build -o liveforge ./cmd/liveforge` (needs Go 1.26+). Build SRS from
[github.com/ossrs/srs](https://github.com/ossrs/srs) with
`(cd trunk && ./configure --ffmpeg-fit=on --sys-ffmpeg=off --https=off --gb28181=off && make)`; the resulting `trunk/objs/srs` binary
is what `SRS_BIN` should point at.
