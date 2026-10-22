#!/usr/bin/env python3
"""Run cargo check --features cluster using rust:latest (PATH fixed).

The source and build directories come from ``OPENRTMP_SRC_ROOT`` and
``OPENRTMP_DEST``; see ``openrtmp_docker_paths`` for the defaults.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

# The shared helpers live next to this script: make them importable whether
# the script is run as a file (``python scripts/docker_cargo_check.py``) or as
# a module (``python -m scripts.docker_cargo_check``).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from openrtmp_docker_paths import resolve_dest, resolve_src_root, stage_sources

SRC_ROOT = resolve_src_root()
DEST = resolve_dest("openrtmp-build")
LOG_NAME = "cargo-check-full.log"


def refresh_copy() -> None:
    stage_sources(DEST, SRC_ROOT, ("librtmp2", "librtmp2-server"))


def main() -> int:
    DEST.mkdir(parents=True, exist_ok=True)
    refresh_copy()
    mount = str(DEST).replace("\\", "/")
    bash = (
        "export PATH=/usr/local/cargo/bin:$PATH; "
        "which cargo; rustc --version; "
        "apt-get update -qq && DEBIAN_FRONTEND=noninteractive "
        "apt-get install -y -qq pkg-config libssl-dev >/dev/null; "
        "export CARGO_TERM_COLOR=never; "
        "cargo check --features cluster > /src/cargo-check-full.log 2>&1; "
        "CARGO_EXIT=$?; "
        "echo CARGO_EXIT=$CARGO_EXIT >> /src/cargo-check-full.log; "
        "grep -E '^error(\\[|:)' /src/cargo-check-full.log | head -n 80; "
        "tail -n 40 /src/cargo-check-full.log; "
        "exit $CARGO_EXIT"
    )
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
    print("running docker cargo check...", flush=True)
    try:
        r = subprocess.run(cmd, shell=False, check=True)
    except subprocess.CalledProcessError as exc:
        r = exc
    src_log = DEST / LOG_NAME
    out_log = SRC_ROOT / "librtmp2-server" / LOG_NAME
    if src_log.exists():
        shutil.copy(src_log, out_log)
        print(f"copied log bytes={out_log.stat().st_size}", flush=True)
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
