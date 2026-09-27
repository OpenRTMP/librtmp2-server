# Benchmarks

Real, reproducible numbers for `librtmp2-server`'s RTMP ingest/relay path,
plus a same-machine comparison against nginx-rtmp, MediaMTX, SRS and LiveForge
using the *same* RTMP client for all five, so the comparison isn't skewed by
differences between test clients.

**Read this before quoting a number from it:** everything below comes from
one shared 4-vCPU VM, where single runs vary by roughly ±7-10 ms at 100
viewers. The five-server tables are therefore the mean of three full
sweeps, and the closest competitors (MediaMTX and LiveForge) were also
measured over repeated, interleaved rounds — see
[Repeated rounds](#repeated-rounds-librtmp2-server-vs-mediamtx-vs-liveforge).
Every sweep benchmarks the five servers one at a time (not simultaneously),
and each server is stopped before the next one starts, so no two servers
compete for CPU. Treat the *relative* shape of the
results — where the numbers behave the same or differently across servers —
as the useful signal, and re-run `scripts/run_rtmp_benchmarks.sh` on your
own target hardware before using any of this for capacity planning.

## What's being compared

| | librtmp2-server | nginx-rtmp | MediaMTX | SRS | LiveForge |
|---|---|---|---|---|---|
| Version | 0.5.0, built against librtmp2 0.10.0 | nginx 1.24.0 + `libnginx-mod-rtmp` 1.2.2 (Ubuntu package) | v1.11.3 | v8.0.48 (`v8.0-d0`, the SRS 8.0 release; bundled FFmpeg) | `main` @ 4e70fb3 (built with Go 1.26) |
| Language | Rust | C | Go | C++ | Go |
| Role | what this repo ships | most common existing RTMP relay | modern multi-protocol media server with RTMP support | long-running open-source media server with RTMP/SRT/WebRTC support | newer multi-protocol Go live server (RTMP/RTSP/SRT/WebRTC/HLS) |

nginx-rtmp was installed from the Ubuntu package archive; MediaMTX is its
official prebuilt v1.11.3 release binary; SRS was built from source at the
`v8.0-d0` release tag (`trunk/configure --ffmpeg-fit=on --sys-ffmpeg=off
--https=off --gb28181=off && make`; SRS 8 does not build against the system
FFmpeg 6.1 headers) rather than run from a container, to keep every server
here on the same footing (installed package or binary, nothing
containerized). LiveForge was built from
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

- CPU: Intel Xeon @ 2.10GHz, 4 vCPUs (a shared VM — not bare metal)
- RAM: 15 GiB, Linux 6.18 x86_64
- rustc 1.95.0, g++ 13.3.0, Go 1.26.0 (LiveForge), ffmpeg 6.1.1
- Date: 2026-09-27

## Handshake latency (connect + publish, count=120, concurrency=30)

Mean of three full sweeps of `scripts/run_rtmp_benchmarks.sh`,
`librtmp2-server` 0.5.0 on librtmp2 0.10.0 (default sharding, 4 poll
threads on this box):

| Server | Success rate | Handshakes/s | avg | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| librtmp2-server | 100% | **8630.7/s** | **2.76 ms** | **2.69 ms** | **4.37 ms** | **5.25 ms** | **5.58 ms** |
| nginx-rtmp | 100% | 658.9/s | 44.34 ms | 44.06 ms | 46.47 ms | 47.00 ms | 47.41 ms |
| MediaMTX | 100% | 5176.2/s | 6.73 ms | 3.80 ms | 17.99 ms | 18.96 ms | 20.11 ms |
| SRS 8.0 | 100% | 511.8/s | 55.21 ms | 55.59 ms | 61.61 ms | 62.71 ms | 64.32 ms |
| LiveForge | 100% | 6903.0/s | 3.35 ms | 2.98 ms | 6.93 ms | 8.22 ms | 9.89 ms |

`librtmp2-server` is fastest on every column, ahead of LiveForge and
MediaMTX and more than 10x faster than nginx-rtmp and SRS, even though it is
the only server here that authenticates every publish against a database
(per-stream keys in SQLite, via the auth worker); the others were run
accepting any stream name. MediaMTX's mean includes one slow sweep (12.9 ms
avg; 4.0 and 3.3 ms in the other two). What gets `librtmp2-server` there:
the poll loop waits on a persistent `epoll(7)` set and is woken through an
`eventfd` as soon as an authorization completes, new connections are
accepted immediately, every socket has `TCP_NODELAY`, authorizations are
group-committed off the poll thread with `synchronous=NORMAL`, stats are
batched on a separate thread, and connections are spread over one
`SO_REUSEPORT` poll shard per CPU (up to 4). SRS 8.0 lands between
nginx-rtmp and the three low-latency servers at ~55 ms.

## Concurrent-viewer relay throughput and join latency

Combined audio+video frame rate for this source is ~73 tags/sec/viewer
(30 fps video + ~43 fps audio); "steady fps/player" close to 73 means every
viewer received the full stream with no drops. Mean of the same three sweeps as above.

| Server | Players | Join latency avg / p95 / max | Steady throughput | Steady fps/player |
|---|---|---|---|---|
| librtmp2-server | 1 | **0.84** / **0.84** / **0.84** ms | 1.10 Mbps | 73.1 |
| librtmp2-server | 25 | **1.71** / **2.75** / **3.01** ms | 27.39 Mbps | 73.1 |
| librtmp2-server | 100 | 13.07 / 21.79 / 22.72 ms | 109.58 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 1 | 88.55 / 88.55 / 88.55 ms | 1.10 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 25 | 90.56 / 93.04 / 93.14 ms | 27.41 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 100 | 90.44 / 94.22 / 95.58 ms | 109.77 Mbps | 73.1 |
| MediaMTX | 1 | 2.37 / 2.37 / 2.37 ms | 1.10 Mbps | 73.1 |
| MediaMTX | 25 | 2.15 / 3.59 / 3.91 ms | 27.39 Mbps | 73.1 |
| MediaMTX | 100 | 11.08 / **18.68** / **19.42** ms | 109.62 Mbps | 73.1 |
| SRS 8.0 | 1 | 44.86 / 44.86 / 44.86 ms | 1.08 Mbps | 72.0 |
| SRS 8.0 | 25 | 48.21 / 51.30 / 51.48 ms | 26.85 Mbps | 71.9 |
| SRS 8.0 | 100 | 68.72 / 80.82 / 83.38 ms | 108.77 Mbps | 72.4 |
| LiveForge | 1 | 1.08 / 1.08 / 1.08 ms | 1.10 Mbps | 73.1 |
| LiveForge | 25 | 3.57 / 6.20 / 7.84 ms | 27.37 Mbps | 73.1 |
| LiveForge | 100 | **9.73** / 21.12 / 23.91 ms | 109.62 Mbps | 73.1 |

Takeaways:

- **All five relayed every frame to every viewer with zero loss** at up to
  100 concurrent viewers of one stream on this 4-vCPU box (steady fps/player
  in the 72-73 range across the board).
- **Join latency at 1 viewer**: `librtmp2-server` (0.8 ms), LiveForge
  (1.1 ms) and MediaMTX (2.4 ms) are all within a couple of milliseconds of
  each other, far ahead of SRS 8.0 (44.9 ms) and nginx-rtmp (88.6 ms). A
  single join is one sample per sweep, so the repeated rounds below (60
  joins per server) are the better comparison for this row.
- **Join latency at 25 viewers**: `librtmp2-server` is fastest (1.7 ms avg,
  2.8 ms p95), ahead of MediaMTX (2.2 ms) and LiveForge (3.6 ms), and far
  ahead of SRS 8.0 (48.2 ms) and nginx-rtmp (90.6 ms). SRS's default
  merged-write buffering (see above) is most of what separates it here
  rather than raw per-connection cost.
- **Join latency at 100 viewers**: LiveForge (9.7 ms avg), MediaMTX
  (11.1 ms) and `librtmp2-server` (13.1 ms) are close, with MediaMTX's the
  lowest tail (18.7 ms p95 vs 21.1 and 21.8 ms), all well ahead of SRS 8.0
  (68.7 ms) and nginx-rtmp (90.4 ms). Individual sweeps at this concurrency
  ranged from 10.1 to 17.3 ms for `librtmp2-server`, 8.7 to 13.5 ms for
  MediaMTX and 7.1 to 14.1 ms for LiveForge, wider than the gaps between
  them, so see the repeated rounds below before ranking them.
- Aggregate throughput scales linearly with viewer count for all five, as
  expected for a simple relay (no transcoding) — 100 viewers at ~1.1 Mbps
  each is ~108-110 Mbps served, consistent across all five implementations.

## Repeated rounds: librtmp2-server vs MediaMTX vs LiveForge

Same box, same client tools and ffmpeg source as above. Servers run one at
a time, interleaved round by round (librtmp2-server, LiveForge, MediaMTX,
repeat) over three rounds, and averaged, which evens out the VM's
run-to-run noise that a single sweep can't. In each round every server gets
100 sequential connect+publish handshakes, 120 at concurrency 30, 20
sequential single-viewer joins and one 100-viewer join against a publisher
that has been live for about 25 seconds:

| Metric | librtmp2-server | LiveForge | MediaMTX |
|---|---|---|---|
| connect+publish, sequential (`bench_handshake --concurrency 1`, 3 × 100), avg / p50 | 0.54 / 0.41 ms | **0.43 / 0.39 ms** | 0.48 / 0.44 ms |
| single-viewer join (3 × 20 sequential `bench_relay --players 1` joins), avg / p50 | **0.81 / 0.77 ms** | 1.10 / 1.06 ms | 1.15 / 1.13 ms |
| 100-viewer join (3 rounds), avg / p50 / p95 | **3.6 / 3.2 / 7.6 ms** | 12.4 / 11.4 / 24.7 ms | 7.2 / 7.4 / 10.8 ms |
| connect+publish, 30 concurrent (3 × 120), avg / p50 / p95 | **2.8 / 2.6 / 4.5 ms** | 3.0 / 2.8 / 5.9 ms | 3.5 / 3.3 / 6.5 ms |
| steady fps per viewer at 100 viewers | 73.0-73.1 | 73.1-73.2 | 73.0-73.2 |

`librtmp2-server` is the only one of the three that authenticates every
publish and play here (per-stream keys looked up and session rows written in
SQLite); the other two accept any stream name. It is fastest on three of
the four rows: single-viewer joins (0.8 ms against 1.1 ms), 100-viewer
joins (3.6 ms avg against 7.2 ms for MediaMTX and 12.4 ms for LiveForge)
and 30 concurrent connect+publish. Plays are answered from an in-memory
snapshot of the play keys as soon as the request arrives, and the viewer's
session row is written by the auth worker right after, so a join no
longer waits on SQLite. Sequential connect+publish is where the remaining
per-connection auth cost shows: 0.54 ms against 0.43-0.48 ms, since each
publish still waits for its session row to be committed. Every one of the
60 single-viewer joins completed in under 2 ms.

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
with `go build -o liveforge ./cmd/liveforge` (needs Go 1.26+). Build SRS 8.0 from
[github.com/ossrs/srs](https://github.com/ossrs/srs) at the `v8.0-d0` tag with
`(cd trunk && ./configure --ffmpeg-fit=on --sys-ffmpeg=off --https=off --gb28181=off && make)`; the resulting `trunk/objs/srs` binary
is what `SRS_BIN` should point at.
