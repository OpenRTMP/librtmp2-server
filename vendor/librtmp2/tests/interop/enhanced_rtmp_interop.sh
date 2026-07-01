#!/usr/bin/env bash
#
# enhanced_rtmp_interop.sh — Enhanced-RTMP (FourCC) ingest interop test.
#
# Publishes HEVC and AV1 streams with ffmpeg. ffmpeg muxes these using the
# Enhanced-RTMP extended video tag (FourCC "hvc1" / "av01") rather than a legacy
# FLV codec id, which is exactly the path OBS uses for HEVC/AV1. This exercises
# librtmp2's E-RTMP parsing against a real encoder.
#
# Each codec is tested only if its ffmpeg encoder is available; the test fails
# only if a present codec fails to ingest.
#
# Requires: ffmpeg on PATH and a Rust toolchain.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BASE_PORT="${PORT:-11945}"
BIN_NAME="ffmpeg_ingest"

command -v ffmpeg >/dev/null 2>&1 || { echo "ffmpeg not found on PATH"; exit 1; }

# Capture the encoder list once (piping into `grep -q` under `set -o pipefail`
# would report failure when grep closes the pipe early and ffmpeg gets SIGPIPE).
ENCODERS="$(ffmpeg -hide_banner -encoders 2>/dev/null || true)"
have_enc() { printf '%s\n' "$ENCODERS" | grep -q " $1 "; }

echo "== building $BIN_NAME =="
cargo build --example "$BIN_NAME" --all-features
BIN="$(find target -type f -name "$BIN_NAME" -path '*/examples/*' | head -n1)"

# run_one <label> <encoder> <extra ffmpeg args> <port>
run_one() {
    local label="$1" enc="$2" extra="$3" port="$4"
    local addr="127.0.0.1:${port}"
    echo "== [$label] ingest server on $addr =="
    local log
    log="$(mktemp "/tmp/eR_${label}.XXXXXX.log")"
    "$BIN" "$addr" 40 1 0 >"$log" 2>&1 &
    local srv=$!
    sleep 1
    echo "== [$label] publishing with ffmpeg ($enc) =="
    set +e
    # shellcheck disable=SC2086
    timeout 50 ffmpeg -hide_banner -loglevel error \
        -f lavfi -i "testsrc=size=320x240:rate=10:duration=2" \
        -f lavfi -i "sine=frequency=1000:duration=2" \
        -c:v "$enc" $extra -pix_fmt yuv420p -g 10 \
        -c:a aac -b:a 64k \
        -f flv "rtmp://${addr}/live/test"
    local ff_rc=$?
    set -e
    echo "[$label] ffmpeg exit=$ff_rc"

    # A non-zero ffmpeg exit is expected when the ingest server is satisfied
    # and disconnects before ffmpeg finishes (it sees "Connection reset by
    # peer"). Only treat it as a real failure if the server is still running,
    # i.e. ffmpeg died before the server ever got enough data.
    if [ "$ff_rc" -ne 0 ] && kill -0 "$srv" 2>/dev/null; then
        echo "[$label] ENHANCED-RTMP INTEROP FAILED (ffmpeg publish exit=$ff_rc, ingest server still running)"
        kill "$srv" 2>/dev/null || true
        wait "$srv" 2>/dev/null || true
        return 1
    fi

    wait "$srv"; local rc=$?
    echo "== [$label] ingest log =="; cat "$log"
    if [ "$rc" -ne 0 ]; then
        echo "[$label] ENHANCED-RTMP INTEROP FAILED (ingest exit=$rc)"
        return 1
    fi
    echo "[$label] ENHANCED-RTMP INTEROP OK"
    return 0
}

tested=0
if have_enc libx265; then
    tested=1
    run_one hevc libx265 "-preset ultrafast" "$BASE_PORT"
fi
AV1_ENC=""
for enc in libaom-av1 libsvtav1 librav1e; do
    if have_enc "$enc"; then AV1_ENC="$enc"; break; fi
done
if [ -n "$AV1_ENC" ]; then
    tested=1
    extra=""
    [ "$AV1_ENC" = "libaom-av1" ] && extra="-cpu-used 8"
    [ "$AV1_ENC" = "libsvtav1" ] && extra="-preset 12"
    run_one av1 "$AV1_ENC" "$extra" "$((BASE_PORT + 1))"
fi

if [ "$tested" -eq 0 ]; then
    echo "No HEVC or AV1 encoder available in ffmpeg; skipping Enhanced-RTMP interop test."
    exit 0
fi
echo "ENHANCED-RTMP INTEROP: all available codecs passed"
