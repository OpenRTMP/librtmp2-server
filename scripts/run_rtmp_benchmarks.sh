#!/usr/bin/env bash
# Reproduces the cross-server numbers in BENCHMARKS.md: librtmp2-server vs
# nginx-rtmp vs MediaMTX vs SRS vs LiveForge, using librtmp2's
# bench_handshake/bench_relay tools (a real RTMP client, so all servers are
# driven identically) plus a real ffmpeg-encoded source stream. Per server:
# connect+publish handshakes, joins at 1/25/100 viewers, connect+play
# handshakes, and a 500/1000-viewer load test with the server's CPU and
# memory use.
#
# Requirements (all optional pieces are skipped with a warning if missing):
#   - a sibling ../librtmp2 checkout with `cargo build --release --examples`
#     already run (for target/release/examples/bench_handshake, bench_relay)
#   - this crate built in release mode
#   - ffmpeg
#   - nginx with the nginx-rtmp module (Debian/Ubuntu: `libnginx-mod-rtmp`),
#     or a source build pointed at with NGINX_BIN and NGINX_RTMP_MODULE
#   - a MediaMTX binary (set MEDIAMTX_BIN to its path; skipped if unset)
#   - an SRS binary (set SRS_BIN to its path; skipped if unset) — build from
#     https://github.com/ossrs/srs (`trunk/configure && make`) or use a
#     packaged binary; not vendored here for the same reason MediaMTX isn't
#   - a LiveForge binary (set LIVEFORGE_BIN to its path; skipped if unset) —
#     build from https://github.com/im-pingo/liveforge (`go build ./cmd/liveforge`)
#
# Usage: scripts/run_rtmp_benchmarks.sh [work_dir]
#
# Optional environment knobs (defaults reproduce the historic behaviour, so
# existing results keep their meaning):
#   BENCH_SERVERS   servers to run, space separated, from
#                   "lrtmp2-server nginx mediamtx srs liveforge"
#                   (default: all that are available).
#   BENCH_PHASES    phases to run, from "handshake relay play load"
#                   (default: all). `BENCH_PHASES=load` is the quick way to
#                   iterate on fan-out CPU.
#   LOAD_VIEWERS    viewer counts of the many-viewer load test (default
#                   "500 1000"; e.g. "500 1000 2000 5000"). Higher steps need
#                   more RAM/threads: bench_relay uses one thread per viewer
#                   and shares the host with the server under test.
#   LOAD_RUN_SECS / LOAD_WARMUP_MS   bench_relay --run-secs/--warmup-ms of the
#                   load test (default 20 / 5000).
#   LOAD_MEASURE_DELAY / LOAD_MEASURE_SECS   the server CPU/RSS window starts
#                   LOAD_MEASURE_DELAY s after the viewers were launched
#                   (default 8) and lasts LOAD_MEASURE_SECS s (default 10);
#                   raise the delay when many viewers need longer to join, and
#                   keep delay + measure <= run-secs.
#   PERF_STAT=auto|1|0   wrap the measurement window in `perf stat -p <server>`
#                   (task-clock, cycles, instructions, branches, branch-misses,
#                   context-switches, cpu-migrations, page-faults). `auto`
#                   (default) uses perf only if installed and permitted;
#                   hardware counters that the host (e.g. a VM) does not
#                   expose are reported as "<not supported>".
#   PERF_RECORD=1   additionally record a CPU profile of the librtmp2-server
#                   during the same window (`perf record -g`); writes
#                   <work_dir>/perf/load-<N>.data. Turn it into a flamegraph
#                   with e.g. `perf script -i ... | stackcollapse-perf.pl |
#                   flamegraph.pl > out.svg` (FlameGraph scripts are NOT a
#                   dependency of this repository). On hosts without hardware
#                   counters set PERF_RECORD_EVENT=cpu-clock.
#   STRACE_SAMPLE=1   after the timed window, attach `strace -c -f` for
#                   STRACE_SECS (default 5) s to count sendmsg/write syscalls
#                   and derive bytes per send syscall. strace slows the
#                   server, so this is a separate, untimed sample.
#   PERF_BIN        path of the perf binary (default: perf from PATH).
#   SERVER_BIN      librtmp2-server binary to benchmark (default:
#                   ./target/release/librtmp2-server), e.g. to A/B two builds.
#
# What is measured (load test): only the server process under test (and its
# children, for nginx) -- never the ffmpeg publisher or bench_relay. The
# publisher is a real ffmpeg 1280x720@30 testsrc + sine, libx264 veryfast
# zerolatency 2500k video, GOP 60 frames (2 s), AAC 128k. CPU measurement
# begins LOAD_MEASURE_DELAY s after the viewers are launched (the first
# bench_relay warmup, LOAD_WARMUP_MS, is a per-viewer window relative to its
# first frame) and lasts LOAD_MEASURE_SECS s. The printed "load-summary" line
# carries: viewers, cpu_pct (of one core), cpu_seconds, peak_rss_mib,
# delivered_gbps / frames_per_viewer (from bench_relay's steady window) and
# core_s_per_gbit = (cpu_pct/100) / delivered_gbps, i.e. CPU core-seconds
# spent per delivered Gbit.
#
# This starts and stops its own nginx/MediaMTX/SRS/LiveForge/librtmp2-server
# instances on non-default ports (1935-1939) so it doesn't collide with
# anything already running; it does not touch system nginx config.

set -euo pipefail

# The 1000-viewer load test needs well over 1024 sockets on both ends.
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)" || true

WORK_DIR="${1:-$(mktemp -d)}"
LIBRTMP2_DIR="${LIBRTMP2_DIR:-../librtmp2}"
SERVER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MEDIAMTX_BIN="${MEDIAMTX_BIN:-}"
SRS_BIN="${SRS_BIN:-}"
LIVEFORGE_BIN="${LIVEFORGE_BIN:-}"
LOAD_VIEWERS="${LOAD_VIEWERS:-500 1000}"
LOAD_RUN_SECS="${LOAD_RUN_SECS:-20}"
LOAD_WARMUP_MS="${LOAD_WARMUP_MS:-5000}"
LOAD_MEASURE_DELAY="${LOAD_MEASURE_DELAY:-8}"
LOAD_MEASURE_SECS="${LOAD_MEASURE_SECS:-10}"
PERF_STAT="${PERF_STAT:-auto}"
PERF_RECORD="${PERF_RECORD:-0}"
PERF_RECORD_EVENT="${PERF_RECORD_EVENT:-}"
STRACE_SAMPLE="${STRACE_SAMPLE:-0}"
STRACE_SECS="${STRACE_SECS:-5}"
BENCH_SERVERS="${BENCH_SERVERS:-lrtmp2-server nginx mediamtx srs liveforge}"
BENCH_PHASES="${BENCH_PHASES:-handshake relay play load}"
want_server() { [[ " $BENCH_SERVERS " == *" $1 "* ]]; }
want_phase() { [[ " $BENCH_PHASES " == *" $1 "* ]]; }
# Largest load step, to size connection limits and viewer key pools.
MAX_LOAD_VIEWERS=0
for _n in $LOAD_VIEWERS; do ((_n > MAX_LOAD_VIEWERS)) && MAX_LOAD_VIEWERS=$_n; done
# nginx and its RTMP module: the system package by default, or a source build
# (e.g. the latest nginx release with nginx-rtmp-module as a dynamic module).
NGINX_BIN="${NGINX_BIN:-nginx}"
NGINX_RTMP_MODULE="${NGINX_RTMP_MODULE:-/usr/lib/nginx/modules/ngx_rtmp_module.so}"
# nginx resolves a relative load_module path against its own prefix, not
# this script's working directory, so pin it to an absolute path here.
[[ "$NGINX_RTMP_MODULE" == /* ]] || NGINX_RTMP_MODULE="$PWD/$NGINX_RTMP_MODULE"
# Each server is started from its own work directory, so resolve relative
# binary paths against the caller's directory first.
for var in MEDIAMTX_BIN SRS_BIN LIVEFORGE_BIN; do
  if [[ -n "${!var}" && -e "${!var}" ]]; then
    printf -v "$var" '%s' "$(realpath "${!var}")"
  fi
done

BENCH_HANDSHAKE="$LIBRTMP2_DIR/target/release/examples/bench_handshake"
BENCH_RELAY="$LIBRTMP2_DIR/target/release/examples/bench_relay"

mkdir -p "$WORK_DIR"/{logs,lrtmp2-server,nginx,mediamtx,srs,liveforge,perf}
echo "Work dir: $WORK_DIR"

for bin in "$BENCH_HANDSHAKE" "$BENCH_RELAY"; do
  if [[ ! -x "$bin" ]]; then
    echo "error: $bin not found — build librtmp2 first:" >&2
    echo "  (cd $LIBRTMP2_DIR && cargo build --release --example bench_handshake --example bench_relay)" >&2
    exit 1
  fi
done

command -v ffmpeg >/dev/null || { echo "error: ffmpeg not found" >&2; exit 1; }

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

# Servers are launched as `(cd dir || exit 1; exec server ...) &`: the
# subshell execs into the server, so `$!` is the server's own PID and the
# server stays a child of this script. Backgrounding inside the subshell
# instead (`(cd dir && server ... &)`) gives `$!` the subshell's PID, so
# killing it left the server running into the next server's leg, and it
# detaches the server from this script, so `wait` can't reap it.
stop_server() {
  local pid
  pid="$(cat "$1")"
  kill "$pid" >/dev/null 2>&1 || true
  for _ in $(seq 1 50); do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 0.1
  done
  kill -9 "$pid" >/dev/null 2>&1 || true
}

# Waits until something listens on 127.0.0.1:$1 (up to 30 s), so a server
# that is slow to come up is not benchmarked before it accepts connections.
wait_port() {
  local port="$1" log="$2"
  for _ in $(seq 1 300); do
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && return 0
    sleep 0.1
  done
  echo "error: nothing listening on :$port after 30 s; server log:" >&2
  cat "$log" >&2 || true
  exit 1
}

# Starts the ffmpeg publisher in the background (a child of this script, not
# of a `$(...)` subshell) and leaves its PID in PUBLISH_PID.
publish() {
  local url="$1" logfile="$2"
  ffmpeg -hide_banner -loglevel error -re \
    -f lavfi -i testsrc=size=1280x720:rate=30 \
    -f lavfi -i sine=frequency=440 \
    -c:v libx264 -preset veryfast -tune zerolatency -b:v 2500k -g 60 \
    -c:a aac -b:a 128k -f flv "$url" >"$logfile" 2>&1 &
  PUBLISH_PID=$!
  PIDS+=("$PUBLISH_PID")
}

relay_sweep() {
  local label="$1" pub_url="$2" play_arg_kind="$3" play_arg="$4"
  for n in 1 25 100; do
    echo "=== $label relay, players=$n ==="
    local pid target_url
    if [[ "$play_arg_kind" = "list" ]]; then
      # pub_url is one exact, pre-provisioned publish key (e.g.
      # librtmp2-server, which validates it exactly) — reuse it as-is rather
      # than appending $n, which would turn it into a key nothing provisioned.
      target_url="$pub_url"
    else
      target_url="${pub_url}${n}"
    fi
    publish "$target_url" "$WORK_DIR/logs/pub-$label-$n.log"
    pid=$PUBLISH_PID
    sleep 3
    if [[ "$play_arg_kind" = "list" ]]; then
      "$BENCH_RELAY" --url-list "$play_arg" --players "$n" --run-secs 15 --warmup-ms 3000
    else
      "$BENCH_RELAY" "${play_arg}${n}" --players "$n" --run-secs 15 --warmup-ms 3000
    fi
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
    sleep 2
  done
}

# Connect + play handshake: one live publisher, then `count` players join it
# at `concurrency` and disconnect as soon as NetStream.Play.Start arrives.
play_handshake() {
  local label="$1" pub_url="$2" play_arg_kind="$3" play_arg="$4" pid
  echo "=== $label play handshake (count=120, concurrency=30) ==="
  publish "$pub_url" "$WORK_DIR/logs/pub-$label-play-handshake.log"
  pid=$PUBLISH_PID
  sleep 3
  if [[ "$play_arg_kind" = "list" ]]; then
    "$BENCH_HANDSHAKE" --play --url-list "$play_arg" --count 120 --concurrency 30
  else
    "$BENCH_HANDSHAKE" --play "$play_arg" --count 120 --concurrency 30
  fi
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" 2>/dev/null || true
  sleep 2
}

# Prints the server's total CPU ticks (utime + stime) and resident memory in
# KiB, summed over the process and its children (nginx runs its worker as a
# child of the master whose PID the pid file holds).
proc_tree_usage() {
  local root="$1" ticks=0 rss=0 p stat
  for p in "$root" $(pgrep -P "$root" 2>/dev/null || true); do
    [[ -r "/proc/$p/stat" ]] || continue
    stat="$(cat "/proc/$p/stat")"
    stat="${stat##*) }"
    # Fields after the command: utime is the 12th, stime the 13th.
    read -r -a f <<<"$stat"
    ticks=$((ticks + f[11] + f[12]))
    rss=$((rss + $(awk '/^VmRSS:/ {print $2}' "/proc/$p/status" 2>/dev/null || echo 0)))
  done
  echo "$ticks $rss"
}

# Many-viewer load: one live stream, LOAD_VIEWERS players (default 500 and
# 1000). Reports the usual bench_relay join/throughput lines plus the server's
# average CPU (in percent of one core), CPU seconds and peak resident memory
# over the steady-state window, and a one-line "load-summary" (see the header
# comment for the exact definitions). The first lines keep their historic
# format: "server resources: cpu_pct=.. peak_rss_mib=..".
perf_usable() {
  PERF="${PERF_BIN:-perf}"
  command -v "$PERF" >/dev/null 2>&1 && "$PERF" stat -e task-clock -- true >/dev/null 2>&1
}

load_test() {
  local label="$1" server_pid_file="$2" pub_url="$3" play_arg_kind="$4" play_arg="$5"
  local hz server_pid n pid bench_pid t0 t1 ticks0 ticks1 rss peak_rss
  local relay_out perf_pid="" rec_pid="" use_perf=0 gbps fpv cpu_pct cpu_s peak_mib
  hz="$(getconf CLK_TCK)"
  server_pid="$(cat "$server_pid_file")"
  if [[ "$PERF_STAT" = "1" ]] || { [[ "$PERF_STAT" = "auto" ]] && perf_usable; }; then
    use_perf=1
  fi
  for n in $LOAD_VIEWERS; do
    echo "=== $label load, players=$n ==="
    publish "$pub_url" "$WORK_DIR/logs/pub-$label-load-$n.log"
    pid=$PUBLISH_PID
    sleep 3
    relay_out="$WORK_DIR/logs/relay-$label-load-$n.out"
    if [[ "$play_arg_kind" = "list" ]]; then
      "$BENCH_RELAY" --url-list "$play_arg" --players "$n" --run-secs "$LOAD_RUN_SECS" --warmup-ms "$LOAD_WARMUP_MS" >"$relay_out" &
    else
      "$BENCH_RELAY" "$play_arg" --players "$n" --run-secs "$LOAD_RUN_SECS" --warmup-ms "$LOAD_WARMUP_MS" >"$relay_out" &
    fi
    bench_pid=$!
    sleep "$LOAD_MEASURE_DELAY"
    read -r ticks0 peak_rss < <(proc_tree_usage "$server_pid")
    if ((use_perf)); then
      "${PERF:-perf}" stat -x, -o "$WORK_DIR/logs/perfstat-$label-load-$n.csv" \
        -e task-clock,cycles,instructions,branches,branch-misses,context-switches,cpu-migrations,page-faults \
        -p "$server_pid" -- sleep "$LOAD_MEASURE_SECS" >/dev/null 2>&1 &
      perf_pid=$!
    fi
    if [[ "$PERF_RECORD" = "1" ]] && perf_usable; then
      "${PERF:-perf}" record -g ${PERF_RECORD_EVENT:+-e "$PERF_RECORD_EVENT"} \
        -o "$WORK_DIR/perf/load-$n.data" -p "$server_pid" -- sleep "$LOAD_MEASURE_SECS" \
        >"$WORK_DIR/logs/perfrecord-$n.log" 2>&1 &
      rec_pid=$!
    fi
    t0="$(date +%s.%N)"
    for _ in $(seq 1 "$LOAD_MEASURE_SECS"); do
      sleep 1
      read -r _ rss < <(proc_tree_usage "$server_pid")
      ((rss > peak_rss)) && peak_rss=$rss
    done
    read -r ticks1 rss < <(proc_tree_usage "$server_pid")
    t1="$(date +%s.%N)"
    ((rss > peak_rss)) && peak_rss=$rss
    [[ -n "$perf_pid" ]] && { wait "$perf_pid" 2>/dev/null || true; perf_pid=""; }
    [[ -n "$rec_pid" ]] && { wait "$rec_pid" 2>/dev/null || true; rec_pid=""; }
    if [[ "$STRACE_SAMPLE" = "1" ]] && command -v strace >/dev/null; then
      # Untimed, separate sample: strace slows the server noticeably.
      timeout "$STRACE_SECS" strace -c -f -p "$server_pid" \
        -e trace=sendmsg,sendto,write,writev,send -o "$WORK_DIR/logs/strace-$label-load-$n.txt" || true
    fi
    wait "$bench_pid" || true
    cat "$relay_out"
    cpu_pct="$(awk -v d="$((ticks1 - ticks0))" -v hz="$hz" -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.1f", 100 * d / hz / (t1 - t0) }')"
    cpu_s="$(awk -v d="$((ticks1 - ticks0))" -v hz="$hz" 'BEGIN { printf "%.2f", d / hz }')"
    peak_mib="$(awk -v rss="$peak_rss" 'BEGIN { printf "%.1f", rss / 1024 }')"
    echo "server resources: cpu_pct=$cpu_pct peak_rss_mib=$peak_mib"
    gbps="$(sed -n 's/^delivered:.*aggregate_gbps=\([0-9.]*\).*/\1/p' "$relay_out")"
    fpv="$(sed -n 's/^delivered:.*steady_frames_per_viewer=\([0-9.]*\).*/\1/p' "$relay_out")"
    awk -v n="$n" -v pct="$cpu_pct" -v cs="$cpu_s" -v rss="$peak_mib" -v g="${gbps:-0}" -v f="${fpv:-0}" \
      -v win="$LOAD_MEASURE_SECS" 'BEGIN {
        printf "load-summary: viewers=%d cpu_pct=%s cpu_seconds=%s (window %ss) peak_rss_mib=%s delivered_gbps=%s frames_per_viewer=%s core_s_per_gbit=%s\n",
          n, pct, cs, win, rss, g, f, (g > 0 ? sprintf("%.3f", pct / 100 / g) : "n/a") }'
    if [[ -s "$WORK_DIR/logs/perfstat-$label-load-$n.csv" ]]; then
      echo "perf stat (server pid $server_pid, ${LOAD_MEASURE_SECS}s window, value,unit,event):"
      grep -v '^#' "$WORK_DIR/logs/perfstat-$label-load-$n.csv" | cut -d, -f1-3 | sed 's/^/  /'
    fi
    if [[ -s "$WORK_DIR/logs/strace-$label-load-$n.txt" ]]; then
      echo "strace -c sample (${STRACE_SECS}s, slows the server):"
      sed 's/^/  /' "$WORK_DIR/logs/strace-$label-load-$n.txt"
    fi
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
    sleep 3
  done
}

### 1. librtmp2-server ###
if want_server lrtmp2-server; then
echo "--- starting librtmp2-server on :1935 (HTTP :8080) ---"
(
  cd "$SERVER_DIR" || exit 1
  LRTMP2_DB="$WORK_DIR/lrtmp2-server/server.db" \
  LRTMP2_RTMP_BIND=127.0.0.1:1935 LRTMP2_HTTP_BIND=127.0.0.1:8080 \
  LRTMP2_RTMP_MAX_CONNECTIONS=$((MAX_LOAD_VIEWERS + 200 > 1200 ? MAX_LOAD_VIEWERS + 200 : 1200)) LRTMP2_LOG_LEVEL=1 \
  LRTMP2_HTTP_RATE_LIMIT_API=10000 LRTMP2_HTTP_RATE_LIMIT_DEFAULT=10000 \
  exec "${SERVER_BIN:-./target/release/librtmp2-server}"
) >"$WORK_DIR/logs/lrtmp2-server.log" 2>&1 &
echo $! > "$WORK_DIR/lrtmp2-server.pid"
PIDS+=("$(cat "$WORK_DIR/lrtmp2-server.pid")")
wait_port 1935 "$WORK_DIR/logs/lrtmp2-server.log"
wait_port 8080 "$WORK_DIR/logs/lrtmp2-server.log"
TOKEN=$(sed -n '/Generated API token/{n;p;q}' "$WORK_DIR/logs/lrtmp2-server.log")

# One stream for the publisher, plus 24 extra viewer play_keys: this server
# caps concurrent connections per play_key at 5 by design (see BENCHMARKS.md),
# so a 100-viewer test needs >= 20 distinct keys, one per up-to-5 viewers.
STREAM=$(curl -sS -X POST http://127.0.0.1:8080/api/v1/streams \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"id":"bench","name":"Bench","app":"live"}')
PUBLISH_KEY=$(python3 -c "import json,sys;print(json.load(sys.stdin)['publish_key'])" <<<"$STREAM")
PLAY_URLS="$WORK_DIR/lrtmp2-server-play-urls.txt"
python3 -c "import json,sys;print('rtmp://127.0.0.1:1935/live/'+json.load(sys.stdin)['play_key'])" <<<"$STREAM" > "$PLAY_URLS"
for i in $(seq 1 24); do
  RESP=$(curl -sS -X POST "http://127.0.0.1:8080/api/v1/streams/bench/players" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"name\":\"Viewer $i\"}")
  python3 -c "import json,sys;print('rtmp://127.0.0.1:1935/live/'+json.load(sys.stdin)['play_key'])" <<<"$RESP" >> "$PLAY_URLS"
done

if want_phase handshake; then
echo "=== librtmp2-server handshake (count=120, concurrency=30) ==="
HS_URLS="$WORK_DIR/lrtmp2-server-handshake-urls.txt"
> "$HS_URLS"
for i in $(seq 1 120); do
  RESP=$(curl -sS -X POST http://127.0.0.1:8080/api/v1/streams \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"id\":\"hs$i\",\"name\":\"HS $i\",\"app\":\"live\"}")
  python3 -c "import json,sys;print('rtmp://127.0.0.1:1935/live/'+json.load(sys.stdin)['publish_key'])" <<<"$RESP" >> "$HS_URLS" || true
done
"$BENCH_HANDSHAKE" --url-list "$HS_URLS" --count 120 --concurrency 30
fi

want_phase relay && relay_sweep "lrtmp2-server" "rtmp://127.0.0.1:1935/live/$PUBLISH_KEY" list "$PLAY_URLS"
want_phase play && play_handshake "lrtmp2-server" "rtmp://127.0.0.1:1935/live/$PUBLISH_KEY" list "$PLAY_URLS"

# N viewers at up to 5 per play_key need ceil(N/5) keys (200 for 1000
# viewers, 1000 for 5000): the 25 above plus the rest.
LOAD_URLS="$WORK_DIR/lrtmp2-server-load-urls.txt"
cp "$PLAY_URLS" "$LOAD_URLS"
LOAD_KEYS=$(((MAX_LOAD_VIEWERS + 4) / 5))
((LOAD_KEYS < 200)) && LOAD_KEYS=200
if want_phase load; then
for i in $(seq 25 $((LOAD_KEYS - 1))); do
  RESP=$(curl -sS -X POST "http://127.0.0.1:8080/api/v1/streams/bench/players" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"name\":\"Viewer $i\"}")
  python3 -c "import json,sys;print('rtmp://127.0.0.1:1935/live/'+json.load(sys.stdin)['play_key'])" <<<"$RESP" >> "$LOAD_URLS"
done
load_test "lrtmp2-server" "$WORK_DIR/lrtmp2-server.pid" "rtmp://127.0.0.1:1935/live/$PUBLISH_KEY" list "$LOAD_URLS"
fi

stop_server "$WORK_DIR/lrtmp2-server.pid"
fi

### 2. nginx-rtmp ###
if want_server nginx && command -v "$NGINX_BIN" >/dev/null && [[ -e "$NGINX_RTMP_MODULE" ]]; then
  echo "--- starting nginx-rtmp on :1936 ---"
  cat > "$WORK_DIR/nginx/nginx.conf" <<EOF
load_module $NGINX_RTMP_MODULE;
worker_processes 1; # see BENCHMARKS.md: nginx-rtmp relay state is per worker
error_log $WORK_DIR/logs/nginx-error.log info;
pid $WORK_DIR/nginx/nginx.pid;
events { worker_connections 4096; }
rtmp {
    server {
        listen 127.0.0.1:1936;
        chunk_size 4096;
        application live { live on; record off; }
    }
}
EOF
  "$NGINX_BIN" -c "$WORK_DIR/nginx/nginx.conf"
  wait_port 1936 "$WORK_DIR/logs/nginx-error.log"
  if want_phase handshake; then
  echo "=== nginx-rtmp handshake (count=120, concurrency=30) ==="
  "$BENCH_HANDSHAKE" rtmp://127.0.0.1:1936/live/hsbench --count 120 --concurrency 30
  fi
  want_phase relay && relay_sweep "nginx" "rtmp://127.0.0.1:1936/live/bench" prefix "rtmp://127.0.0.1:1936/live/bench"
  want_phase play && play_handshake "nginx" "rtmp://127.0.0.1:1936/live/playbench" prefix "rtmp://127.0.0.1:1936/live/playbench"
  want_phase load && load_test "nginx" "$WORK_DIR/nginx/nginx.pid" "rtmp://127.0.0.1:1936/live/loadbench" prefix "rtmp://127.0.0.1:1936/live/loadbench"
  "$NGINX_BIN" -c "$WORK_DIR/nginx/nginx.conf" -s stop || true
else
  echo "skipping nginx-rtmp: $NGINX_BIN or $NGINX_RTMP_MODULE not found"
fi

### 3. MediaMTX ###
if want_server mediamtx && [[ -n "$MEDIAMTX_BIN" ]] && [[ -x "$MEDIAMTX_BIN" ]]; then
  echo "--- starting MediaMTX on :1937 ---"
  cat > "$WORK_DIR/mediamtx/mediamtx.yml" <<EOF
logLevel: info
logDestinations: [stdout, file]
logFile: $WORK_DIR/logs/mediamtx.log
api: false
metrics: false
pprof: false
playback: false
rtsp: no
rtmp: yes
rtmpAddress: 127.0.0.1:1937
rtmpEncryption: "no"
hls: no
webrtc: no
srt: no
paths:
  all_others:
EOF
  (cd "$WORK_DIR/mediamtx" || exit 1; exec "$MEDIAMTX_BIN" ./mediamtx.yml) \
    >"$WORK_DIR/logs/mediamtx-stdout.log" 2>&1 &
  echo $! > "$WORK_DIR/mediamtx.pid"
  PIDS+=("$(cat "$WORK_DIR/mediamtx.pid")")
  wait_port 1937 "$WORK_DIR/logs/mediamtx.log"
  if want_phase handshake; then
  echo "=== MediaMTX handshake (count=120, concurrency=30) ==="
  "$BENCH_HANDSHAKE" rtmp://127.0.0.1:1937/live/hsbench --count 120 --concurrency 30
  fi
  want_phase relay && relay_sweep "mediamtx" "rtmp://127.0.0.1:1937/live/bench" prefix "rtmp://127.0.0.1:1937/live/bench"
  want_phase play && play_handshake "mediamtx" "rtmp://127.0.0.1:1937/live/playbench" prefix "rtmp://127.0.0.1:1937/live/playbench"
  want_phase load && load_test "mediamtx" "$WORK_DIR/mediamtx.pid" "rtmp://127.0.0.1:1937/live/loadbench" prefix "rtmp://127.0.0.1:1937/live/loadbench"
  stop_server "$WORK_DIR/mediamtx.pid"
else
  echo "skipping MediaMTX: set MEDIAMTX_BIN to a built binary to include it"
fi

### 4. SRS ###
if want_server srs && [[ -n "$SRS_BIN" ]] && [[ -x "$SRS_BIN" ]]; then
  echo "--- starting SRS on :1938 ---"
  cat > "$WORK_DIR/srs/srs.conf" <<EOF
max_connections     $((MAX_LOAD_VIEWERS + 500 > 2000 ? MAX_LOAD_VIEWERS + 500 : 2000));
daemon              off;
pid                 $WORK_DIR/srs/srs.pid;
srs_log_tank        file;
srs_log_file        $WORK_DIR/logs/srs.log;
rtmp {
    listen          1938;
}
http_api {
    enabled off;
}
http_server {
    enabled off;
}
vhost __defaultVhost__ {
}
EOF
  (cd "$WORK_DIR/srs" || exit 1; exec "$SRS_BIN" -c ./srs.conf) \
    >"$WORK_DIR/logs/srs-stdout.log" 2>&1 &
  echo $! > "$WORK_DIR/srs.pid"
  PIDS+=("$(cat "$WORK_DIR/srs.pid")")
  wait_port 1938 "$WORK_DIR/logs/srs-stdout.log"
  if want_phase handshake; then
  echo "=== SRS handshake (count=120, concurrency=30) ==="
  "$BENCH_HANDSHAKE" rtmp://127.0.0.1:1938/live/hsbench --count 120 --concurrency 30
  fi
  want_phase relay && relay_sweep "srs" "rtmp://127.0.0.1:1938/live/bench" prefix "rtmp://127.0.0.1:1938/live/bench"
  want_phase play && play_handshake "srs" "rtmp://127.0.0.1:1938/live/playbench" prefix "rtmp://127.0.0.1:1938/live/playbench"
  want_phase load && load_test "srs" "$WORK_DIR/srs.pid" "rtmp://127.0.0.1:1938/live/loadbench" prefix "rtmp://127.0.0.1:1938/live/loadbench"
  stop_server "$WORK_DIR/srs.pid"
else
  echo "skipping SRS: set SRS_BIN to a built binary to include it"
fi

### 5. LiveForge ###
if want_server liveforge && [[ -n "$LIVEFORGE_BIN" ]] && [[ -x "$LIVEFORGE_BIN" ]]; then
  echo "--- starting LiveForge on :1939 ---"
  # RTMP only, like the other servers here: every other protocol listener,
  # the admin API and recording (on in LiveForge's sample config) are off.
  # The stream block mirrors the sample config's defaults; LiveForge does
  # not fill them in on its own, and without them it drops publishers.
  cat > "$WORK_DIR/liveforge/liveforge.yaml" <<EOF
server:
  name: bench
  log_level: warn
rtmp:
  enabled: true
  listen: "127.0.0.1:1939"
  chunk_size: 4096
stream:
  gop_cache: true
  gop_cache_num: 1
  gop_cache_max_frames: 300
  gop_cache_max_duration: 10s
  gop_cache_max_bytes: 33554432
  ring_buffer_size: 1024
  idle_timeout: 30s
  no_publisher_timeout: 15s
rtsp: {enabled: false}
http_stream: {enabled: false}
websocket: {enabled: false}
webrtc: {enabled: false}
srt: {enabled: false}
sip: {enabled: false, gateway: {enabled: false}}
gb28181: {enabled: false}
auth: {enabled: false}
record: {enabled: false}
dvr: {enabled: false}
metrics: {enabled: false}
api: {enabled: false}
EOF
  (cd "$WORK_DIR/liveforge" || exit 1; exec "$LIVEFORGE_BIN" -c ./liveforge.yaml) \
    >"$WORK_DIR/logs/liveforge.log" 2>&1 &
  echo $! > "$WORK_DIR/liveforge.pid"
  PIDS+=("$(cat "$WORK_DIR/liveforge.pid")")
  wait_port 1939 "$WORK_DIR/logs/liveforge.log"
  if want_phase handshake; then
  echo "=== LiveForge handshake (count=120, concurrency=30) ==="
  "$BENCH_HANDSHAKE" rtmp://127.0.0.1:1939/live/hsbench --count 120 --concurrency 30
  fi
  want_phase relay && relay_sweep "liveforge" "rtmp://127.0.0.1:1939/live/bench" prefix "rtmp://127.0.0.1:1939/live/bench"
  want_phase play && play_handshake "liveforge" "rtmp://127.0.0.1:1939/live/playbench" prefix "rtmp://127.0.0.1:1939/live/playbench"
  want_phase load && load_test "liveforge" "$WORK_DIR/liveforge.pid" "rtmp://127.0.0.1:1939/live/loadbench" prefix "rtmp://127.0.0.1:1939/live/loadbench"
  stop_server "$WORK_DIR/liveforge.pid"
else
  echo "skipping LiveForge: set LIVEFORGE_BIN to a built binary to include it"
fi

echo "Done. Logs in $WORK_DIR/logs"
