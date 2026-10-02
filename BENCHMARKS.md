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
| Version | 0.6.1, built against librtmp2 0.10.2 | nginx 1.31.6 + nginx-rtmp-module `master` @ 6c7719d | v1.21.1 | v8.0.48 (`v8.0-d0`, the SRS 8.0 release; bundled FFmpeg) | `main` @ 4e70fb3 (built with Go 1.26) |
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
- Date: 2026-09-28

## Handshake latency (connect + publish, count=120, concurrency=30)

Mean of three full sweeps of `scripts/run_rtmp_benchmarks.sh`,
`librtmp2-server` 0.6.1 on librtmp2 0.10.2 (default sharding, 4 poll
threads on this box):

| Server | Success rate | Handshakes/s | avg | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| librtmp2-server | 100% | **9486.0/s** | **2.41 ms** | **2.15 ms** | **5.53 ms** | **6.56 ms** | **6.99 ms** |
| nginx-rtmp | 100% | 633.7/s | 44.30 ms | 44.04 ms | 46.53 ms | 48.51 ms | 48.84 ms |
| MediaMTX | 100% | 6894.7/s | 3.51 ms | 3.05 ms | 6.66 ms | 7.39 ms | 8.03 ms |
| SRS 8.0 | 100% | 512.7/s | 54.42 ms | 54.73 ms | 63.06 ms | 65.20 ms | 65.47 ms |
| LiveForge | 100% | 7042.5/s | 3.32 ms | 3.07 ms | 6.57 ms | 7.98 ms | 8.69 ms |

`librtmp2-server` leads on throughput and every latency column (all five
servers complete every handshake), ahead of LiveForge and
MediaMTX and 18x faster than nginx-rtmp and 23x faster than SRS on average, even though it
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
nginx-rtmp and the three low-latency servers at ~54 ms.

## Play handshake latency (connect + play, count=120, concurrency=30)

The player-side counterpart: `bench_handshake --play` against a stream
that is already live, timing each connect up to `NetStream.Play.Start`.
Same three sweeps:

| Server | Success rate | Handshakes/s | avg | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| librtmp2-server | 100% | **10073.2/s** | **1.95 ms** | **1.58 ms** | **4.32 ms** | **5.82 ms** | **6.39 ms** |
| nginx-rtmp | 100% | 331.4/s | 89.47 ms | 89.79 ms | 92.61 ms | 93.19 ms | 94.22 ms |
| MediaMTX | 100% | 5415.4/s | 4.61 ms | 4.11 ms | 8.76 ms | 9.69 ms | 10.79 ms |
| SRS 8.0 | 100% | 556.0/s | 51.20 ms | 50.60 ms | 58.23 ms | 60.55 ms | 60.66 ms |
| LiveForge | 100% | 7176.8/s | 3.00 ms | 2.62 ms | 6.67 ms | 8.52 ms | 8.71 ms |

`librtmp2-server` is fastest here too: 1.95 ms on average against 3.0 ms
for LiveForge, 4.6 ms for MediaMTX, 51 ms for SRS 8.0 and 89 ms for
nginx-rtmp, and it answers about 10,100 play requests per second against
about 7,200 for LiveForge and 5,400 for MediaMTX. As with publishing, it is the only server here
that checks every play against a key (one `play_key` per viewer, answered
from the in-memory key snapshot); the others accept any stream name.

## Concurrent-viewer relay throughput and join latency

Combined audio+video frame rate for this source is ~73 tags/sec/viewer
(30 fps video + ~43 fps audio); "steady fps/player" close to 73 means every
viewer received the full stream with no drops. Mean of the same three sweeps as above.

| Server | Players | Join latency avg / p95 / max | Steady throughput | Steady fps/player |
|---|---|---|---|---|
| librtmp2-server | 1 | **0.91** / **0.91** / **0.91** ms | 1.10 Mbps | 73.1 |
| librtmp2-server | 25 | **1.50** / **2.34** / **2.81** ms | 27.38 Mbps | 73.1 |
| librtmp2-server | 100 | **2.94** / **6.49** / **7.77** ms | 109.53 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 1 | 85.82 / 85.82 / 85.82 ms | 1.10 Mbps | 73.2 |
| nginx-rtmp (1 worker) | 25 | 87.87 / 91.46 / 92.32 ms | 27.44 Mbps | 73.1 |
| nginx-rtmp (1 worker) | 100 | 89.83 / 96.00 / 96.48 ms | 109.75 Mbps | 73.1 |
| MediaMTX | 1 | 1.35 / 1.35 / 1.35 ms | 1.10 Mbps | 73.1 |
| MediaMTX | 25 | 2.09 / 3.40 / 3.49 ms | 27.38 Mbps | 73.0 |
| MediaMTX | 100 | 5.51 / 10.86 / 12.45 ms | 109.55 Mbps | 73.1 |
| SRS 8.0 | 1 | 46.89 / 46.89 / 46.89 ms | 1.07 Mbps | 71.9 |
| SRS 8.0 | 25 | 49.88 / 53.20 / 53.80 ms | 26.86 Mbps | 71.9 |
| SRS 8.0 | 100 | 73.22 / 85.16 / 86.43 ms | 108.72 Mbps | 72.4 |
| LiveForge | 1 | 1.01 / 1.01 / 1.01 ms | 1.09 Mbps | 73.0 |
| LiveForge | 25 | 4.50 / 7.72 / 8.33 ms | 27.38 Mbps | 73.1 |
| LiveForge | 100 | 13.00 / 26.22 / 32.60 ms | 109.64 Mbps | 73.1 |

Takeaways:

- **All five relayed every frame to every viewer with zero loss** at up to
  100 concurrent viewers of one stream on this 4-vCPU box (steady fps/player
  in the 72-73 range across the board), and at 500 and 1000 viewers too
  (see below).
- **Join latency at 1 viewer**: `librtmp2-server` (0.91 ms), LiveForge
  (1.01 ms) and MediaMTX (1.35 ms) are within half a millisecond of each
  other, far ahead of SRS 8.0 (46.9 ms) and nginx-rtmp (85.8 ms). A single join is
  one sample per sweep, so the repeated rounds below (60 joins per server)
  are the better comparison for this row.
- **Join latency at 25 viewers**: `librtmp2-server` (1.5 ms avg, 2.3 ms
  p95) is ahead of MediaMTX (2.1 / 3.4 ms) and LiveForge (4.5 / 7.7 ms)
  and far ahead of SRS 8.0 (49.9 ms) and nginx-rtmp (87.9 ms). SRS's
  default merged-write buffering (see above) is most of what separates it
  here rather than raw per-connection cost.
- **Join latency at 100 viewers**: `librtmp2-server` is fastest, 2.9 ms avg
  and 6.5 ms p95 against 5.5 / 10.9 ms for MediaMTX and 13.0 / 26.2 ms for
  LiveForge. SRS 8.0 (73.2 ms) and nginx-rtmp (89.8 ms) trail well behind.
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
| librtmp2-server | 500 | 32.69 / 64.82 / **72.01** ms | 547.52 Mbps | 73.1 | 31.1% | 22.1 MiB |
| librtmp2-server | 1000 | **32.62** / **93.75** / 135.06 ms | 1095.64 Mbps | 73.1 | 60.3% | 31.8 MiB |
| nginx-rtmp (1 worker) | 500 | 97.29 / 110.56 / 116.23 ms | 547.43 Mbps | 73.1 | 46.3% | **14.0 MiB** |
| nginx-rtmp (1 worker) | 1000 | 120.61 / 152.38 / 489.88 ms | 1095.35 Mbps | 73.1 | 77.6% | **20.0 MiB** |
| MediaMTX | 500 | **31.47** / **62.60** / 76.15 ms | 547.61 Mbps | 73.1 | 67.6% | 98.7 MiB |
| MediaMTX | 1000 | 58.75 / 107.45 / **126.94** ms | 1095.44 Mbps | 73.1 | 135.4% | 144.3 MiB |
| SRS 8.0 | 500 | 204.45 / 238.71 / 246.63 ms | 548.23 Mbps | 73.2 | **7.4%** | 106.2 MiB |
| SRS 8.0 | 1000 | 519.38 / 1182.86 / 1202.89 ms | 1087.31 Mbps | 72.5 | **13.4%** | 149.4 MiB |
| LiveForge | 500 | 37.03 / 84.94 / 124.28 ms | 547.71 Mbps | 73.1 | 40.2% | 91.9 MiB |
| LiveForge | 1000 | 109.89 / 240.27 / 294.31 ms | 1095.97 Mbps | 73.1 | 74.7% | 147.8 MiB |

- **Every server delivered the full stream to every one of the 1000
  viewers**, about 1.1 Gbps in total.
- **Join latency**: at 500 viewers MediaMTX (31.5 ms avg, 62.6 ms p95)
  and `librtmp2-server` (32.7 / 64.8 ms) are level, ahead of LiveForge
  (37.0 / 84.9 ms). At 1000 viewers `librtmp2-server` has both the lowest
  average and the lowest p95 (32.6 / 93.8 ms against 58.8 / 107.5 ms for
  MediaMTX and 109.9 / 240.3 ms for LiveForge). nginx-rtmp stays near its
  usual ~100 ms and SRS 8.0 climbs to 204 and 519 ms.
- **CPU**: `librtmp2-server` needs 31% of one core for 500 viewers and 60%
  for 1000, less than LiveForge (40% / 75%) and nginx-rtmp (46% / 78%) and
  under half of what MediaMTX uses (68% / 135%). SRS 8.0 uses the least
  (7% / 13%): its merged-write buffering (see above) batches sends into
  far fewer syscalls, which is also where its join latency goes.
- **Memory**: nginx-rtmp is the leanest (14 / 20 MiB), then
  `librtmp2-server` (22 / 32 MiB), which relays each frame to its viewers
  without copying it per viewer. MediaMTX, LiveForge and SRS 8.0 all sit
  around 90-105 MiB at 500 viewers and 145-150 MiB at 1000.

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

## Fan-out CPU profile: where the time goes, and what the allocation/sort work changed

These numbers come from a **separate, later session** than the five-server
tables above (same 4-vCPU shared VM, kernel 6.18, same ffmpeg publisher:
1280x720@30, libx264 veryfast/zerolatency 2500k, GOP 60 frames = 2 s, AAC
128k), measuring only `librtmp2-server` with `BENCH_PHASES=load
BENCH_SERVERS=lrtmp2-server`. They do not redefine the older tables. The
measurement window is the 10 s starting 8 s after the viewers were launched
(20 s for 2000/5000, see below); CPU is `utime+stime` of the server process
only (never ffmpeg or `bench_relay`). "CPU/Gbit" is core-seconds per
delivered Gbit = `cpu_pct/100 / delivered_gbps`.

**A/B of the librtmp2 fan-out changes** (vectored sends from a stack iovec
array instead of two `Vec`s per player and `sendmsg`; per-connection message
lists instead of sorting `frames x players` messages), two interleaved
rounds, `SERVER_BIN` = before/after build of the same server:

| Viewers | CPU % before → after | CPU/Gbit before → after | Gbit/s | join p95 ms before → after | join p99 ms before → after | peak RSS MiB |
|---|---|---|---|---|---|---|
| 500 | 35.0 → 34.8 | 0.640 → 0.636 | 0.547 | 67.9 → 61.3 | 75.6 → 65.6 | 21.2 → 21.7 |
| 1000 | 68.1 → 65.9 | 0.621 → 0.601 | 1.096 | 144.3 → 81.7 | 178.2 → 102.4 | 31.2 → 31.2 |
| 2000 (1 run) | 93.1 → 90.1 | 0.425 → 0.411 | 2.19 | 318 → 254 | 375 → 320 | 52.0 → 51.4 |

Every viewer received every frame in both builds (1097 frames per viewer in
the steady window at 500/1000, ~2560 at 2000). **Read the CPU columns as "no
regression, at best a few percent gain"**: round-to-round noise on this VM is
about ±5 % (the 500-viewer baseline ranged 33.2–35.7 %), larger than the
difference. The join-latency columns are noisier still (single-run p95 at
1000 viewers ranged 52–177 ms for the *same* build), so no latency claim is
made either way.

**5000 viewers are not measurable on this host.** `bench_relay` runs one
thread per viewer on the same 4 vCPUs as the server; at 5000 the benchmark
client, not the server, is the bottleneck (the server uses ~67 % of one core
while viewers receive only ~915–965 of the ~2560 frames they should get), so
the 5000 step is useful only as a smoke test that the server accepts and
serves 5000 concurrent viewers (RSS 105–108 MiB). Run it against a separate
load generator host for capacity numbers.

**Profile.** `perf record -e cpu-clock -g` of the server at 1000 viewers
(`PERF_RECORD=1 PERF_RECORD_EVENT=cpu-clock`; this VM exposes no hardware
counters, so cycles/instructions/branches are `<not supported>` here) —
share of samples, before → after:

| Category | before | after |
|---|---|---|
| kernel (TCP send path, scheduler, wakeups) | 72.8 % | 74.6 % |
| `librtmp2-server` binary (user) | 18.3 % | 17.7 % |
| libc | 6.9 % | 5.6 % |
| `send_and_export_relay_frames` (incl. inlined per-frame connection scan) | 0.96 % | 1.04 % |
| `Conn::send_staged_media` | 0.53 % | 0.48 % |
| iovec/window building in `try_send_vectored` | 0.24 % | ~0 % |
| sorting (`sort_by_key` over all staged messages) | 0.22 % | 0.06 % |
| allocator (`malloc`/`free`/`alloc::`) | 2.7 % | 1.7 % |
| `__libc_sendmsg` wrapper | 0.78 % | 0.76 % |
| TCP transmit symbols (`tcp_sendmsg`, `tcp_write_xmit`, `ip_queue_xmit`, …) | 4.5 % | 4.4 % |

Findings: (1) the library's user-space fan-out is a small slice; most of the
CPU is the kernel TCP path and scheduler/wakeup cost (about 57,000 context
switches per 10 s at 1000 viewers), which payload-neutral allocation changes
cannot move. (2) The per-frame scan of `connections`
(`conn_will_receive_relay_frame`) is ~1 % of samples at 1000 viewers, so a
subscriber index was **not** built (it would have to be kept consistent with
play start/stop, pause, `receiveAudio/Video`, multitrack negotiation, route
renames, authorization and teardown for at most that 1 %). (3) The remaining
user-space cost sits in the server's own per-connection bookkeeping
(`update_rtt`, `stream_ids_for_conn`, `has_authorized_session`, hashing and
string clones in `rtmp_bridge`, each ~0.5–1.8 %), a better next target than
the fan-out itself. (4) A `strace -c` sample (slows the server) shows
`sendmsg` as >90 % of socket syscalls; since the same bytes must traverse the
kernel TCP path either way, bounded micro-batching to reduce syscalls was
**not** implemented: with no hardware counters here there is no evidence that
syscall entry, rather than per-byte TCP work and wakeups, is the cost, and
batching would trade join latency for it. `io_uring` was likewise left out.

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

Fan-out runs on the server alone, with more viewer steps and CPU metrics:

```bash
BENCH_SERVERS=lrtmp2-server BENCH_PHASES=load \
LOAD_VIEWERS="500 1000 2000 5000" \
  scripts/run_rtmp_benchmarks.sh                      # load-summary per step
# A/B two builds, plus optional perf (software events work in a VM):
SERVER_BIN=/path/to/other-build PERF_STAT=auto PERF_RECORD=1 \
PERF_RECORD_EVENT=cpu-clock STRACE_SAMPLE=1 \
BENCH_SERVERS=lrtmp2-server BENCH_PHASES=load LOAD_VIEWERS="1000" \
  scripts/run_rtmp_benchmarks.sh
# flamegraph (FlameGraph scripts are not a dependency of this repo):
perf script -i <work_dir>/perf/load-1000.data | stackcollapse-perf.pl | flamegraph.pl > fanout.svg
```

The options and exactly what is measured (which process, when the window
starts and how long it is, publisher bitrate/GOP/fps) are documented at the
top of the script. `perf` is optional; without it the benchmark runs as
before.

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
