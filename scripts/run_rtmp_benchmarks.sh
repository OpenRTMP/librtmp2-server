#!/usr/bin/env bash
# Reproduces the cross-server numbers in BENCHMARKS.md: librtmp2-server vs
# nginx-rtmp vs MediaMTX vs SRS, using librtmp2's bench_handshake/bench_relay
# tools (a real RTMP client, so all servers are driven identically) plus a
# real ffmpeg-encoded source stream.
#
# Requirements (all optional pieces are skipped with a warning if missing):
#   - a sibling ../librtmp2 checkout with `cargo build --release --examples`
#     already run (for target/release/examples/bench_handshake, bench_relay)
#   - this crate built in release mode
#   - ffmpeg
#   - nginx with the nginx-rtmp module (Debian/Ubuntu: `libnginx-mod-rtmp`)
#   - a MediaMTX binary (set MEDIAMTX_BIN to its path; skipped if unset)
#   - an SRS binary (set SRS_BIN to its path; skipped if unset) — build from
#     https://github.com/ossrs/srs (`trunk/configure && make`) or use a
#     packaged binary; not vendored here for the same reason MediaMTX isn't
#   - a LiveForge binary (set LIVEFORGE_BIN to its path; skipped if unset) —
#     build from https://github.com/im-pingo/liveforge (`go build ./cmd/liveforge`)
#
# Usage: scripts/run_rtmp_benchmarks.sh [work_dir]
#
# This starts and stops its own nginx/MediaMTX/SRS/LiveForge/librtmp2-server
# instances on non-default ports (1935-1939) so it doesn't collide with
# anything already running; it does not touch system nginx config.

set -euo pipefail

WORK_DIR="${1:-$(mktemp -d)}"
LIBRTMP2_DIR="${LIBRTMP2_DIR:-../librtmp2}"
SERVER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MEDIAMTX_BIN="${MEDIAMTX_BIN:-}"
SRS_BIN="${SRS_BIN:-}"
LIVEFORGE_BIN="${LIVEFORGE_BIN:-}"

BENCH_HANDSHAKE="$LIBRTMP2_DIR/target/release/examples/bench_handshake"
BENCH_RELAY="$LIBRTMP2_DIR/target/release/examples/bench_relay"

mkdir -p "$WORK_DIR"/{logs,lrtmp2-server,nginx,mediamtx,srs,liveforge}
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

publish() {
  local url="$1" logfile="$2"
  ffmpeg -hide_banner -loglevel error -re \
    -f lavfi -i testsrc=size=1280x720:rate=30 \
    -f lavfi -i sine=frequency=440 \
    -c:v libx264 -preset veryfast -tune zerolatency -b:v 2500k -g 60 \
    -c:a aac -b:a 128k -f flv "$url" >"$logfile" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  echo "$pid"
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
    pid=$(publish "$target_url" "$WORK_DIR/logs/pub-$label-$n.log")
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

### 1. librtmp2-server ###
echo "--- starting librtmp2-server on :1935 (HTTP :8080) ---"
(
  cd "$SERVER_DIR"
  LRTMP2_DB="$WORK_DIR/lrtmp2-server/server.db" \
  LRTMP2_RTMP_BIND=127.0.0.1:1935 LRTMP2_HTTP_BIND=127.0.0.1:8080 \
  LRTMP2_RTMP_MAX_CONNECTIONS=500 LRTMP2_LOG_LEVEL=1 \
  LRTMP2_HTTP_RATE_LIMIT_API=10000 LRTMP2_HTTP_RATE_LIMIT_DEFAULT=10000 \
  ./target/release/librtmp2-server >"$WORK_DIR/logs/lrtmp2-server.log" 2>&1 &
  echo $! > "$WORK_DIR/lrtmp2-server.pid"
)
PIDS+=("$(cat "$WORK_DIR/lrtmp2-server.pid")")
sleep 2
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

relay_sweep "lrtmp2-server" "rtmp://127.0.0.1:1935/live/$PUBLISH_KEY" list "$PLAY_URLS"

kill "$(cat "$WORK_DIR/lrtmp2-server.pid")" >/dev/null 2>&1 || true

### 2. nginx-rtmp ###
if command -v nginx >/dev/null && [[ -e /usr/lib/nginx/modules/ngx_rtmp_module.so ]]; then
  echo "--- starting nginx-rtmp on :1936 ---"
  cat > "$WORK_DIR/nginx/nginx.conf" <<EOF
load_module /usr/lib/nginx/modules/ngx_rtmp_module.so;
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
  nginx -c "$WORK_DIR/nginx/nginx.conf"
  echo "=== nginx-rtmp handshake (count=120, concurrency=30) ==="
  "$BENCH_HANDSHAKE" rtmp://127.0.0.1:1936/live/hsbench --count 120 --concurrency 30
  relay_sweep "nginx" "rtmp://127.0.0.1:1936/live/bench" prefix "rtmp://127.0.0.1:1936/live/bench"
  nginx -c "$WORK_DIR/nginx/nginx.conf" -s stop || true
else
  echo "skipping nginx-rtmp: nginx or ngx_rtmp_module.so not found"
fi

### 3. MediaMTX ###
if [[ -n "$MEDIAMTX_BIN" ]] && [[ -x "$MEDIAMTX_BIN" ]]; then
  echo "--- starting MediaMTX on :1937 ---"
  cat > "$WORK_DIR/mediamtx/mediamtx.yml" <<EOF
logLevel: info
logDestinations: [file]
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
  (cd "$WORK_DIR/mediamtx" && "$MEDIAMTX_BIN" ./mediamtx.yml >"$WORK_DIR/logs/mediamtx-stdout.log" 2>&1 &
   echo $! > "$WORK_DIR/mediamtx.pid")
  PIDS+=("$(cat "$WORK_DIR/mediamtx.pid")")
  sleep 2
  echo "=== MediaMTX handshake (count=120, concurrency=30) ==="
  "$BENCH_HANDSHAKE" rtmp://127.0.0.1:1937/live/hsbench --count 120 --concurrency 30
  relay_sweep "mediamtx" "rtmp://127.0.0.1:1937/live/bench" prefix "rtmp://127.0.0.1:1937/live/bench"
  kill "$(cat "$WORK_DIR/mediamtx.pid")" >/dev/null 2>&1 || true
else
  echo "skipping MediaMTX: set MEDIAMTX_BIN to a built binary to include it"
fi

### 4. SRS ###
if [[ -n "$SRS_BIN" ]] && [[ -x "$SRS_BIN" ]]; then
  echo "--- starting SRS on :1938 ---"
  cat > "$WORK_DIR/srs/srs.conf" <<EOF
max_connections     1000;
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
  (cd "$WORK_DIR/srs" && "$SRS_BIN" -c ./srs.conf >"$WORK_DIR/logs/srs-stdout.log" 2>&1 &
   echo $! > "$WORK_DIR/srs.pid")
  PIDS+=("$(cat "$WORK_DIR/srs.pid")")
  sleep 2
  echo "=== SRS handshake (count=120, concurrency=30) ==="
  "$BENCH_HANDSHAKE" rtmp://127.0.0.1:1938/live/hsbench --count 120 --concurrency 30
  relay_sweep "srs" "rtmp://127.0.0.1:1938/live/bench" prefix "rtmp://127.0.0.1:1938/live/bench"
  kill "$(cat "$WORK_DIR/srs.pid")" >/dev/null 2>&1 || true
else
  echo "skipping SRS: set SRS_BIN to a built binary to include it"
fi

### 5. LiveForge ###
if [[ -n "$LIVEFORGE_BIN" ]] && [[ -x "$LIVEFORGE_BIN" ]]; then
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
  (cd "$WORK_DIR/liveforge" && "$LIVEFORGE_BIN" -c ./liveforge.yaml >"$WORK_DIR/logs/liveforge.log" 2>&1 &
   echo $! > "$WORK_DIR/liveforge.pid")
  PIDS+=("$(cat "$WORK_DIR/liveforge.pid")")
  sleep 2
  echo "=== LiveForge handshake (count=120, concurrency=30) ==="
  "$BENCH_HANDSHAKE" rtmp://127.0.0.1:1939/live/hsbench --count 120 --concurrency 30
  relay_sweep "liveforge" "rtmp://127.0.0.1:1939/live/bench" prefix "rtmp://127.0.0.1:1939/live/bench"
  kill "$(cat "$WORK_DIR/liveforge.pid")" >/dev/null 2>&1 || true
else
  echo "skipping LiveForge: set LIVEFORGE_BIN to a built binary to include it"
fi

echo "Done. Logs in $WORK_DIR/logs"
