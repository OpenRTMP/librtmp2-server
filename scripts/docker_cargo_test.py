#!/usr/bin/env python3
"""Run cargo test suites for librtmp2-server inside rust:latest.

The cargo test suites run against the staged ``librtmp2-server`` and
``librtmp2`` checkouts; see ``openrtmp_docker_paths`` for the source and
build directories.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

# The shared helpers live next to this script: make them importable whether
# the script is run as a file (``python scripts/docker_cargo_test.py``) or as
# a module (``python -m scripts.docker_cargo_test``).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from openrtmp_docker_paths import resolve_dest, resolve_src_root, stage_sources

SRC_ROOT = resolve_src_root()
DEST = resolve_dest("openrtmp-build")
LOG_NAME = "cargo-test-full.log"


def refresh_copy() -> None:
    stage_sources(DEST, SRC_ROOT, ("librtmp2", "librtmp2-server"))


def main() -> int:
    DEST.mkdir(parents=True, exist_ok=True)
    refresh_copy()
    mount = str(DEST).replace("\\", "/")
    bash = r"""
set -euxo pipefail
export PATH=/usr/local/cargo/bin:$PATH
export CARGO_TERM_COLOR=never
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq pkg-config libssl-dev >/dev/null
status=0
{
  echo '=== cargo test (default) ==='
  if ! cargo test --all-targets 2>&1; then
    echo DEFAULT_FAIL=1
    status=1
  fi
  echo '=== cargo test --features cluster,test-support ==='
  if ! cargo test --features cluster,test-support --all-targets 2>&1; then
    echo CLUSTER_FAIL=1
    status=1
  fi
  echo '=== cargo clippy --features cluster ==='
  if ! cargo clippy --all-targets --features cluster -- -D warnings 2>&1; then
    echo CLIPPY_FAIL=1
    status=1
  fi
  echo OVERALL_STATUS=$status
} | tee /src/cargo-test-full.log
tail -n 80 /src/cargo-test-full.log
# `tee` masks pipeline status without pipefail; also refuse success when the
# log records an explicit suite failure marker.
if grep -qE 'DEFAULT_FAIL=|CLUSTER_FAIL=|CLIPPY_FAIL=' /src/cargo-test-full.log; then
  exit 1
fi
exit 0
"""
    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{mount}:/src",
        "-w",
        "/src/librtmp2-server",
        "rust:latest",
        "bash",
        "-c",
        bash,
    ]
    stale_log = DEST / LOG_NAME
    if stale_log.exists():
        stale_log.unlink()
    print("running docker cargo test...", flush=True)
    r = subprocess.run(cmd)
    src_log = DEST / LOG_NAME
    out_log = SRC_ROOT / "librtmp2-server" / LOG_NAME
    if src_log.exists():
        shutil.copy(src_log, out_log)
        print(f"copied log bytes={out_log.stat().st_size}", flush=True)
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
