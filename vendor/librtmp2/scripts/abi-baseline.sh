#!/usr/bin/env bash
# abi-baseline.sh — Generate or compare ABI dumps for librtmp2
#
# Usage:
#   ./scripts/abi-baseline.sh dump          # Generate baseline ABI dump
#   ./scripts/abi-baseline.sh compare HEAD  # Compare current vs HEAD
#   ./scripts/abi-baseline.sh compare v0.1.0 # Compare current vs tag
#
# Requires (install on Ubuntu):
#   sudo apt-get install -y abigail-tools libabigail-dev abi-compliance-checker
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ABI_DIR="$PROJECT_DIR/abi-dumps"

mkdir -p "$ABI_DIR"

build_and_dump() {
    local label="$1"
    local install_prefix="$PROJECT_DIR/install-abi-$label"

    echo "=== Building ($label) ==="
    cd "$PROJECT_DIR"
    make clean
    make DEBUG=1 all
    make install PREFIX="$install_prefix"

    echo "=== Generating ABI dump ($label) ==="
    abidw \
        "$install_prefix/lib/liblibrtmp2.so" \
        --out-file "$ABI_DIR/librtmp2-${label}.xml"

    echo "✅ Dump saved: $ABI_DIR/librtmp2-${label}.xml"
}

case "${1:-}" in
    dump)
        build_and_dump "baseline"
        ;;
    compare)
        BASELINE_REF="${2:-HEAD}"
        BASELINE_TAG=$(git -C "$PROJECT_DIR" describe --tags --abbrev=0 "$BASELINE_REF" 2>/dev/null || echo "")

        if [ -z "$BASELINE_TAG" ]; then
            echo "No tag found for $BASELINE_REF, using HEAD"
            BASELINE_TAG="HEAD~1"
        fi

        echo "Baseline: $BASELINE_TAG"

        # Build baseline
        cd "$PROJECT_DIR"
        git stash || true
        git checkout "$BASELINE_TAG"
        build_and_dump "baseline"
        git checkout - || git checkout main
        git stash pop || true

        # Build current
        build_and_dump "current"

        # Compare
        echo "=== Running ABI compliance check ==="
        abi-compliance-checker \
            -l librtmp2 \
            -old "$ABI_DIR/librtmp2-baseline.xml" \
            -new "$ABI_DIR/librtmp2-current.xml" \
            -report-path "$ABI_DIR/abi-report.html" \
            -xml 2>&1 | tee "$ABI_DIR/abi-check-result.txt"

        if grep -q "Binary compatibility: Incompatible" "$ABI_DIR/abi-check-result.txt"; then
            echo "❌ ABI BREAKING CHANGES DETECTED!"
            exit 1
        fi

        echo "✅ ABI check passed"
        ;;
    *)
        echo "Usage: $0 {dump|compare [ref]}"
        exit 1
        ;;
esac
