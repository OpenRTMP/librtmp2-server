#!/usr/bin/env python3
"""Run cargo test suites for librtmp2-server inside rust:latest."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys

SRC_ROOT = r"X:\AWDev\GitHub\OpenRTMP"
DEST = r"C:\Users\alexg\AppData\Local\Temp\openrtmp-build"
LOG_NAME = "cargo-test-full.log"


def refresh_copy() -> None:
    for name in ("librtmp2", "librtmp2-server"):
        src = os.path.join(SRC_ROOT, name)
        dst = os.path.join(DEST, name)
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(
            src,
            dst,
            ignore=shutil.ignore_patterns("target", ".git", "*.log"),
        )


def main() -> int:
    os.makedirs(DEST, exist_ok=True)
    refresh_copy()
    mount = DEST.replace("\\", "/")
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
    print("running docker cargo test...", flush=True)
    r = subprocess.run(cmd)
    src_log = os.path.join(DEST, LOG_NAME)
    out_log = os.path.join(SRC_ROOT, "librtmp2-server", LOG_NAME)
    if os.path.exists(src_log):
        shutil.copy(src_log, out_log)
        print(f"copied log bytes={os.path.getsize(out_log)}", flush=True)
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
