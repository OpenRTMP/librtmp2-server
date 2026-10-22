#!/usr/bin/env python3
"""Run cargo check --features cluster using rust:latest (PATH fixed).

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
LOG_NAME = "cargo-check-full.log"
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
