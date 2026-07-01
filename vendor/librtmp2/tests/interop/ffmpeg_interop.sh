#!/usr/bin/env bash
#
# ffmpeg_interop.sh — Interop smoke test against the real ffmpeg RTMP client.
#
# Builds the run_ffmpeg_ingest example, starts it as an RTMP server, then
# publishes a short generated H.264 + AAC stream to it with ffmpeg. The
# example exits 0 once it has ingested both a video and an audio frame.
#
# Requires: ffmpeg on PATH and a Rust toolchain.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PORT="${PORT:-11935}"
ADDR="127.0.0.1:${PORT}"
BIN_NAME="ffmpeg_ingest"

command -v ffmpeg >/dev/null 2>&1 || { echo "ffmpeg not found on PATH"; exit 1; }

echo "== building $BIN_NAME =="
cargo build --example "$BIN_NAME" --all-features
BIN="$(find target -type f -name "$BIN_NAME" -path '*/examples/*' | head -n1)"

echo "== starting ingest server on $ADDR =="
LOG="$(mktemp /tmp/interop_server.XXXXXX.log)"
# Require several frames and at least one >=8 KiB frame, so a video keyframe
# that spans multiple chunks exercises multi-chunk reassembly.
"$BIN" "$ADDR" 25 5 8192 >"$LOG" 2>&1 &
SRV=$!

cleanup() { kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true; }
trap cleanup EXIT

# Give the listener a moment to bind.
sleep 1

echo "== publishing test stream with ffmpeg =="
set +e
timeout 30 ffmpeg -hide_banner -loglevel error \
    -f lavfi -i "testsrc=size=1280x720:rate=25:duration=4" \
    -f lavfi -i "sine=frequency=1000:duration=4" \
    -c:v libx264 -preset ultrafast -pix_fmt yuv420p -g 25 -b:v 6M -maxrate 6M -bufsize 6M \
    -c:a aac -b:a 128k \
    -f flv "rtmp://${ADDR}/live/test"
FF_RC=$?
set -e
echo "ffmpeg exit=$FF_RC"

# A non-zero ffmpeg exit is expected when the ingest server is satisfied and
# disconnects before ffmpeg finishes its full duration (it sees "Connection
# reset by peer"). Only treat it as a real failure if the server is still
# running, i.e. ffmpeg died before the server ever got enough data.
if [ "$FF_RC" -ne 0 ] && kill -0 "$SRV" 2>/dev/null; then
    echo "INTEROP FAILED (ffmpeg publish exit=$FF_RC, ingest server still running)"
    exit 1
fi

# Wait for the ingest server to finish (it exits 0 on success).
wait "$SRV"
SRV_RC=$?
trap - EXIT

echo "== ingest server log =="
cat "$LOG"

if [ "$SRV_RC" -ne 0 ]; then
    echo "INTEROP FAILED (ingest server exit=$SRV_RC)"
    exit 1
fi
echo "INTEROP OK"
