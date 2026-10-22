#!/usr/bin/env python3
"""Run cargo test suites for librtmp2-server inside rust:latest.

``SRC_ROOT`` is the parent directory that contains the ``librtmp2-server`` and
``librtmp2`` source trees; the script resolves both beneath it. Set
``OPENRTMP_SRC_ROOT`` to point at another parent directory and ``OPENRTMP_DEST``
to use a different build directory (defaults to a folder in the system temp
dir).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_SRC_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = Path(os.environ.get("OPENRTMP_SRC_ROOT") or DEFAULT_SRC_ROOT).resolve()
DEST = Path(
    os.environ.get("OPENRTMP_DEST") or Path(tempfile.gettempdir()) / "openrtmp-build"
).resolve()
LOG_NAME = "cargo-test-full.log"
CARGO_MANIFEST = "Cargo.toml"


def require_source_tree(name: str) -> Path:
    """Return ``SRC_ROOT/name``, failing loudly when the checkout is missing.

    ``shutil.copytree`` over a missing directory raises deep inside the copy,
    so without this check a wrong ``SRC_ROOT`` dies with a confusing error
    before Docker is even started.
    """
    path = SRC_ROOT / name
    if not (path / CARGO_MANIFEST).is_file():
        raise SystemExit(
            f"source tree missing or incomplete: {path}\n"
            "set OPENRTMP_SRC_ROOT to the OpenRTMP checkout "
            f"(default is {DEFAULT_SRC_ROOT})"
        )
    return path


def refresh_copy() -> None:
    for name in ("librtmp2", "librtmp2-server"):
        src = require_source_tree(name)
        dst = DEST / name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(
            src,
            dst,
            ignore=shutil.ignore_patterns("target", ".git", "*.log"),
        )


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
