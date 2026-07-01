#!/usr/bin/env bash
#
# play_interop.sh — Play (pull) interop test against a real RTMP server.
#
# Starts mediamtx as a real RTMP server, has ffmpeg publish a looping
# H.264 + AAC stream into it, then uses the run_play_pull example (built on
# top of the librtmp2 client) to play (pull) that stream. The test exits 0
# once it has pulled both a video and an audio frame.
#
# Requires: ffmpeg on PATH, a Rust toolchain, and a mediamtx binary (set
# MEDIAMTX to its path, or have `mediamtx` on PATH).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PORT="${PORT:-11940}"
ADDR="127.0.0.1:${PORT}"
URL="rtmp://${ADDR}/live/test"
MEDIAMTX="${MEDIAMTX:-mediamtx}"
BIN_NAME="play_pull"

command -v ffmpeg >/dev/null 2>&1 || { echo "ffmpeg not found on PATH"; exit 1; }
command -v "$MEDIAMTX" >/dev/null 2>&1 || [ -x "$MEDIAMTX" ] || { echo "mediamtx not found (set MEDIAMTX)"; exit 1; }

echo "== building $BIN_NAME =="
cargo build --example "$BIN_NAME" --all-features
BIN="$(find target -type f -name "$BIN_NAME" -path '*/examples/*' | head -n1)"

PUB=""; MTX=""
cleanup() {
    [ -n "$PUB" ] && kill "$PUB" 2>/dev/null || true
    [ -n "$MTX" ] && kill "$MTX" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

echo "== starting mediamtx RTMP server on :$PORT =="
# Minimal config: accept any publish/read path (catch-all).
MTX_CFG="$(mktemp /tmp/mediamtx.XXXXXX.yml)"
MTX_LOG="$(mktemp /tmp/mediamtx.XXXXXX.log)"
printf 'paths:\n  all_others:\n' > "$MTX_CFG"
# Only RTMP is needed; disable the other listeners so they can't fail to bind.
MTX_RTMPADDRESS=":$PORT" MTX_HLS=no MTX_WEBRTC=no MTX_RTSP=no MTX_SRT=no \
    "$MEDIAMTX" "$MTX_CFG" >"$MTX_LOG" 2>&1 &
MTX=$!

# Wait for mediamtx's RTMP port to actually accept connections instead of a
# fixed sleep, which is either too short (flaky) or too long (slow) depending
# on machine load. Also bail out early if mediamtx exits during startup
# instead of waiting out the full poll loop.
for _ in $(seq 1 50); do
    if ! kill -0 "$MTX" 2>/dev/null; then
        echo "mediamtx exited during startup"
        cat "$MTX_LOG" || true
        exit 1
    fi
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        exec 3>&-
        break
    fi
    sleep 0.2
done

echo "== publishing looping test stream with ffmpeg =="
timeout 40 ffmpeg -hide_banner -loglevel error -re -stream_loop -1 \
    -f lavfi -i "testsrc=size=640x480:rate=20" \
    -f lavfi -i "sine=frequency=1000" \
    -c:v libx264 -preset ultrafast -pix_fmt yuv420p -g 20 \
    -c:a aac -b:a 64k \
    -f flv "$URL" >"$(mktemp /tmp/play_publish.XXXXXX.log)" 2>&1 &
PUB=$!

# Give the publisher a moment to register the stream.
sleep 3

echo "== pulling stream with the librtmp2 client example =="
set +e
"$BIN" "$URL" 20
RC=$?
set -e

echo "== mediamtx log (tail) =="
tail -n 8 "$MTX_LOG" || true

if [ "$RC" -ne 0 ]; then
    echo "PLAY INTEROP FAILED (play client exit=$RC)"
    exit 1
fi
echo "PLAY INTEROP OK"
