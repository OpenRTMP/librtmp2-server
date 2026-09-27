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
| Version | 0.6.0, built against librtmp2 0.10.1 | nginx 1.31.6 + nginx-rtmp-module `master` @ 6c7719d | v1.21.1 | v8.0.48 (`v8.0-d0`, the SRS 8.0 release; bundled FFmpeg) | `main` @ 4e70fb3 (built with Go 1.26) |
| Language | Rust | C | Go | C++ | Go |
| Role | what this repo ships | most common existing RTMP relay | modern multi-protocol media server with RTMP support | long-running open-source media server with RTMP/SRT/WebRTC support | newer multi-protocol Go live server (RTMP/RTSP/SRT/WebRTC/HLS) |

Every server is its latest release (or latest `main`, where the project
has no newer release). nginx was built from source at the `release-1.31.6`
tag with the newest nginx-rtmp-module commit as a dynamic module
(`auto/configure --with-compat --add-dynamic-module=<nginx-rtmp-module>`);
MediaMTX is its official prebuilt v1.21.1 release binary; SRS was built
from source at the `v8.0-d0` release tag (`trunk/configure --ffmpeg-fit=on
--sys-ffmpeg=off --https=off --gb28181=off && make`; SRS 8 does not build
against the system FFmpeg 6.1 headers) rather than run from a container, to
keep every server here on the same footing (native binary, nothing
containerized). LiveForge was built from source (`go build ./cmd/liveforge`)
and run with RTMP only: its sample config also enables recording to disk
and a dozen other listeners, all switched off here (see the script).

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
- **Play handshake latency**: `bench_handshake --play` runs 120
  connect+play handshakes (up to `NetStream.Play.Start`) at concurrency 30
  against a live stream.
- **500 and 1000 viewers**: the relay test at N ∈ {500, 1000} for 20s
  (`--warmup-ms 5000`), with the server's CPU time and resident memory
  read from `/proc` while all viewers are connected.
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
- rustc 1.95.0, gcc/g++ 13.3.0, Go 1.26.0 (LiveForge), ffmpeg 6.1.1
- Date: 2026-09-27

## Handshake latency (connect + publish, count=120, concurrency=30)

Mean of three full sweeps of `scripts/run_rtmp_benchmarks.sh`,
`librtmp2-server` 0.6.0 on librtmp2 0.10.1 (default sharding, 4 poll
threads on this box):

| Server | Success rate | Handshakes/s | avg | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| librtmp2-server | 100% | **10421.6/s** | **2.15 ms** | **1.84 ms** | **4.97 ms** | **5.89 ms** | **6.79 ms** |
| nginx-rtmp | 100% | 662.0/s | 44.08 ms | 43.98 ms | 47.11 ms | 47.68 ms | 47.80 ms |
| MediaMTX | 100% | 7068.5/s | 3.60 ms | 3.20 ms | 7.79 ms | 8.95 ms | 9.36 ms |
| SRS 8.0 | 100% | 491.8/s | 57.09 ms | 57.51 ms | 64.82 ms | 67.86 ms | 68.04 ms |
| LiveForge | 100% | 6322.5/s | 3.84 ms | 3.69 ms | 7.36 ms | 8.85 ms | 9.78 ms |

`librtmp2-server` leads on throughput and every latency column (all five
servers complete every handshake), ahead of MediaMTX and
LiveForge and 20x faster than nginx-rtmp and 27x faster than SRS on average, even though it
is the only server here that authenticates every publish against a
database (per-stream keys in SQLite); the others were run accepting any
stream name. What gets it there: a publish whose key is in the in-memory
key snapshot is answered as soon as the request arrives and its session
row is written by the auth worker right after (anything the snapshot
can't answer goes through the worker, which group-commits with
`synchronous=NORMAL`), the poll loop waits on a persistent `epoll(7)` set
and is woken through an `eventfd` as soon as an authorization completes,
new connections are accepted immediately, every socket has `TCP_NODELAY`,
stats are batched on a separate thread, and connections are spread over
one `SO_REUSEPORT` poll shard per CPU (up to 4). SRS 8.0 lands between
nginx-rtmp and the three low-latency servers at ~57 ms.

## Play handshake latency (connect + play, count=120, concurrency=30)

The player-side counterpart: `bench_handshake --play` against a stream
that is already live, timing each connect up to `NetStream.Play.Start`.
Same three sweeps:

| Server | Success rate | Handshakes/s | avg | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| librtmp2-server | 100% | **10928.1/s** | **2.03 ms** | **1.82 ms** | **4.28 ms** | **5.66 ms** | **6.16 ms** |
| nginx-rtmp | 100% | 335.2/s | 88.89 ms | 89.12 ms | 91.63 ms | 92.27 ms | 92.35 ms |
| MediaMTX | 100% | 7312.5/s | 2.96 ms | 2.79 ms | 5.28 ms | 6.79 ms | 7.66 ms |
| SRS 8.0 | 100% | 557.8/s | 50.63 ms | 50.45 ms | 56.60 ms | 57.29 ms | 59.83 ms |
| LiveForge | 100% | 7324.3/s | 3.00 ms | 2.17 ms | 6.75 ms | 8.89 ms | 10.16 ms |

`librtmp2-server` is fastest here too: 2.0 ms on average against 3.0 ms
for MediaMTX and LiveForge, 51 ms for SRS 8.0 and 89 ms for nginx-rtmp,
and it answers about 10,900 play requests per second against about 7,300
for MediaMTX and LiveForge. As with publishing, it is the only server here
that checks every play against a key (one `play_key` per viewer, answered
from the in-memory key snapshot); the others accept any stream name.

## Concurrent-viewer relay throughput and join latency

Combined audio+video frame rate for this source is ~73 tags/sec/viewer
(30 fps video + ~43 fps audio); "steady fps/player" close to 73 means every
viewer received the full stream with no drops. Mean of the same three sweeps as above.

| Server | Players | Join latency avg / p95 / max | Steady throughput | Steady fps/player |
|---|---|---|---|---|
| librtmp2-server | 1 | 1.15 / 1.15 / 1.15 ms | 1.09 Mbps | 73.0 |
| librtmp2-server | 25 | **1.81** / **3.03** / **3.33** ms | 27.36 Mbps | 73.0 |
| librtmp2-server | 100 | **3.40** / **7.07** / **8.48** ms | 109.48 Mbps | 73.0 |
| nginx-rtmp (1 worker) | 1 | 86.69 / 86.69 / 86.69 ms | 1.10 Mbps | 73.2 |
| nginx-rtmp (1 worker) | 25 | 87.95 / 89.74 / 89.99 ms | 27.44 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 100 | 89.93 / 93.83 / 95.08 ms | 109.78 Mbps | 73.1 |
| MediaMTX | 1 | 1.20 / 1.20 / 1.20 ms | 1.10 Mbps | 73.0 |
| MediaMTX | 25 | 2.34 / 4.07 / 5.14 ms | 27.38 Mbps | 73.1 |
| MediaMTX | 100 | 7.25 / 12.58 / 15.95 ms | 109.60 Mbps | 73.1 |
| SRS 8.0 | 1 | 44.37 / 44.37 / 44.37 ms | 1.07 Mbps | 71.9 |
| SRS 8.0 | 25 | 50.10 / 52.49 / 52.72 ms | 26.83 Mbps | 71.8 |
| SRS 8.0 | 100 | 69.27 / 81.38 / 82.16 ms | 108.74 Mbps | 72.5 |
| LiveForge | 1 | **1.09** / **1.09** / **1.09** ms | 1.09 Mbps | 73.1 |
| LiveForge | 25 | 4.43 / 7.81 / 9.12 ms | 27.39 Mbps | 73.1 |
| LiveForge | 100 | 15.33 / 28.05 / 32.03 ms | 109.64 Mbps | 73.1 |

Takeaways:

- **All five relayed every frame to every viewer with zero loss** at up to
  100 concurrent viewers of one stream on this 4-vCPU box (steady fps/player
  in the 72-73 range across the board), and at 500 and 1000 viewers too
  (see below).
- **Join latency at 1 viewer**: LiveForge (1.09 ms), `librtmp2-server`
  (1.15 ms) and MediaMTX (1.20 ms) are within a tenth of a millisecond of
  each other, far ahead of SRS 8.0 (44.4 ms) and nginx-rtmp (86.7 ms). A single join is
  one sample per sweep, so the repeated rounds below (60 joins per server)
  are the better comparison for this row.
- **Join latency at 25 viewers**: `librtmp2-server` (1.8 ms avg, 3.0 ms
  p95) is ahead of MediaMTX (2.3 / 4.1 ms) and LiveForge (4.4 / 7.8 ms)
  and far ahead of SRS 8.0 (50.1 ms) and nginx-rtmp (88.0 ms). SRS's
  default merged-write buffering (see above) is most of what separates it
  here rather than raw per-connection cost.
- **Join latency at 100 viewers**: `librtmp2-server` is fastest, 3.4 ms avg
  and 7.1 ms p95 against 7.3 / 12.6 ms for MediaMTX and 15.3 / 28.1 ms for
  LiveForge. SRS 8.0 (69.3 ms) and nginx-rtmp (89.9 ms) trail well behind.
- Aggregate throughput scales linearly with viewer count for all five, as
  expected for a simple relay (no transcoding) — 100 viewers at ~1.1 Mbps
  each is ~108-110 Mbps served, consistent across all five implementations.

## 500 and 1000 viewers: join latency, CPU and memory

One live publisher, then 500 or 1000 `bench_relay` viewers join it at once
and stay for 20 s (first 5 s of each viewer's reception discarded).
`librtmp2-server` gets 200 play keys, at most 5 viewers each. CPU is the
server's user + system time over a 10 s steady-state window, in percent
of one core (400% is the whole box); peak RSS is sampled once a second in
the same window, summed over the server's processes (nginx's master and
worker). Same three sweeps:

| Server | Viewers | Join latency avg / p95 / max | Steady throughput | Steady fps/viewer | Server CPU | Peak RSS |
|---|---|---|---|---|---|---|
| librtmp2-server | 500 | 33.80 / 71.69 / 82.18 ms | 547.78 Mbps | 73.1 | 34.3% | 35.7 MiB |
| librtmp2-server | 1000 | **42.75** / 98.68 / 126.08 ms | 1095.84 Mbps | 73.1 | 64.2% | 59.3 MiB |
| nginx-rtmp (1 worker) | 500 | 100.79 / 122.47 / 134.57 ms | 547.53 Mbps | 73.1 | 46.1% | **14.1 MiB** |
| nginx-rtmp (1 worker) | 1000 | 111.24 / 135.10 / 159.61 ms | 1094.82 Mbps | 73.1 | 76.3% | **20.1 MiB** |
| MediaMTX | 500 | **27.66** / **44.53** / **59.77** ms | 547.68 Mbps | 73.1 | 67.7% | 97.6 MiB |
| MediaMTX | 1000 | 49.45 / **96.67** / **109.47** ms | 1095.43 Mbps | 73.1 | 137.0% | 144.6 MiB |
| SRS 8.0 | 500 | 229.83 / 273.55 / 286.69 ms | 544.40 Mbps | 72.7 | **7.6%** | 106.2 MiB |
| SRS 8.0 | 1000 | 484.65 / 965.61 / 994.95 ms | 1095.40 Mbps | 73.1 | **14.0%** | 152.8 MiB |
| LiveForge | 500 | 28.61 / 70.01 / 105.50 ms | 547.68 Mbps | 73.1 | 41.3% | 91.3 MiB |
| LiveForge | 1000 | 91.63 / 202.32 / 315.14 ms | 1095.99 Mbps | 73.1 | 78.7% | 153.6 MiB |

- **Every server delivered the full stream to every one of the 1000
  viewers**, about 1.1 Gbps in total.
- **Join latency**: at 500 viewers MediaMTX (27.7 ms avg) and LiveForge
  (28.6 ms) are ahead of `librtmp2-server` (33.8 ms); at 1000 viewers
  `librtmp2-server` has the lowest average (42.8 ms against 49.5 ms for
  MediaMTX and 91.6 ms for LiveForge), with a p95 level with MediaMTX's
  (98.7 against 96.7 ms). nginx-rtmp stays near its usual ~100 ms and SRS
  8.0 climbs to 230 and 485 ms.
- **CPU**: `librtmp2-server` needs 34% of one core for 500 viewers and 64%
  for 1000, half of what MediaMTX uses (68% and 137%) and less than
  LiveForge (41% / 79%) and nginx-rtmp (46% / 76%). SRS 8.0 uses the least
  (8% / 14%): its merged-write buffering (see above) batches sends into
  far fewer syscalls, which is also where its join latency goes.
- **Memory**: nginx-rtmp is the leanest (14 / 20 MiB), then
  `librtmp2-server` (36 / 59 MiB); MediaMTX, LiveForge and SRS 8.0 all sit
  around 90-105 MiB at 500 viewers and 145-155 MiB at 1000.

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
| connect+publish, sequential (`bench_handshake --concurrency 1`, 3 × 100), avg / p50 | **0.32 / 0.26 ms** | 0.39 / 0.35 ms | 0.46 / 0.42 ms |
| single-viewer join (3 × 20 sequential `bench_relay --players 1` joins), avg / p50 | **0.94 / 0.88 ms** | 1.09 / 1.03 ms | 1.17 / 1.18 ms |
| 100-viewer join (3 rounds), avg / p50 / p95 | **4.4 / 4.5 / 7.7 ms** | 16.7 / 15.6 / 32.3 ms | 5.5 / 5.1 / 9.8 ms |
| connect+publish, 30 concurrent (3 × 120), avg / p50 / p95 | **2.2 / 1.8 / 4.8 ms** | 3.0 / 2.6 / 6.1 ms | 2.6 / 2.4 / 5.5 ms |
| steady fps per viewer at 100 viewers | 73.1 | 73.1 | 73.0 |

`librtmp2-server` is the only one of the three that authenticates every
publish and play here (per-stream keys looked up and session rows written in
SQLite); the other two accept any stream name. It is fastest on every
latency row, and all three hold the same frame rate:
sequential connect+publish (0.32 ms against 0.39 and 0.46 ms),
single-viewer joins (0.9 ms against 1.1 and 1.2 ms), 100-viewer joins (4.4 ms avg
against 5.5 ms for MediaMTX and 16.7 ms for LiveForge) and 30 concurrent
connect+publish (2.2 ms against 2.6 and 3.0 ms). Publishes and plays are answered from an in-memory
snapshot of the stream and play keys as soon as the request arrives, and
the session row is written by the auth worker right after, so neither
waits on SQLite; one active publisher per stream and the per-key viewer
limit are still enforced, the former by the database itself.

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
NGINX_BIN=/path/to/nginx NGINX_RTMP_MODULE=/path/to/ngx_rtmp_module.so \
MEDIAMTX_BIN=/path/to/mediamtx SRS_BIN=/path/to/srs LIVEFORGE_BIN=/path/to/liveforge \
  scripts/run_rtmp_benchmarks.sh
```

`NGINX_BIN` and `NGINX_RTMP_MODULE` default to the distribution package
(`nginx` on `PATH` and `/usr/lib/nginx/modules/ngx_rtmp_module.so`). To
reproduce the nginx 1.31.6 numbers above, build nginx from
[github.com/nginx/nginx](https://github.com/nginx/nginx) at the
`release-1.31.6` tag with the module from
[github.com/arut/nginx-rtmp-module](https://github.com/arut/nginx-rtmp-module):
`auto/configure --prefix=$PWD/inst --with-compat --add-dynamic-module=../nginx-rtmp-module && make && make install`,
then point `NGINX_BIN` at `inst/sbin/nginx` and `NGINX_RTMP_MODULE` at
`inst/modules/ngx_rtmp_module.so`. Use MediaMTX's prebuilt release binary
from [github.com/bluenviron/mediamtx](https://github.com/bluenviron/mediamtx/releases).

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
