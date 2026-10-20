#!/usr/bin/env python3
"""Re-run cluster_ha tests only after leader-forward fix.

The source root defaults to the checkout that contains this script. Set
``OPENRTMP_SRC_ROOT`` to test another checkout and ``OPENRTMP_DEST`` to use a
different build directory (defaults to a folder in the system temp dir).
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
    os.environ.get("OPENRTMP_DEST") or Path(tempfile.gettempdir()) / "openrtmp-build2"
).resolve()
LOG_NAME = "cargo-cluster-ha.log"


def require_source_tree(name: str) -> Path:
    """Return ``SRC_ROOT/name``, failing loudly when the checkout is missing.

    ``os.walk`` over a missing directory silently yields nothing, so without
    this check a wrong ``SRC_ROOT`` would run the suite against whatever stale
    copy already lives in ``DEST`` and report that result as the verdict on the
    current tree.
    """
    path = SRC_ROOT / name
    if not (path / "Cargo.toml").is_file():
        raise SystemExit(
            f"source tree missing or incomplete: {path}\n"
            "set OPENRTMP_SRC_ROOT to the OpenRTMP checkout "
            f"(default is {DEFAULT_SRC_ROOT})"
        )
    return path


def sync_server() -> None:
    src = require_source_tree("librtmp2-server")
    dst = DEST / "librtmp2-server"
    # Sync only changed source (keep target cache).
    for root, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if d not in (".git", "target")]
        rel = Path(root).relative_to(src)
        out = dst / rel
        out.mkdir(parents=True, exist_ok=True)
        for f in files:
            if f.endswith(".log"):
                continue
            shutil.copy2(Path(root) / f, out / f)
    # Ensure librtmp2 present
    lib_src = require_source_tree("librtmp2")
    lib_dst = DEST / "librtmp2"
    if not (lib_dst / "Cargo.toml").is_file():
        shutil.copytree(
            lib_src,
            lib_dst,
            ignore=shutil.ignore_patterns("target", ".git", "*.log"),
        )


def main() -> int:
    # Validate both trees before either branch so a moved/renamed checkout
    # cannot silently run the suite against a stale destination copy.
    server_src = require_source_tree("librtmp2-server")
    lib_src = require_source_tree("librtmp2")
    DEST.mkdir(parents=True, exist_ok=True)
    if not (DEST / "librtmp2-server" / "Cargo.toml").is_file():
        for src, name in ((server_src, "librtmp2-server"), (lib_src, "librtmp2")):
            d = DEST / name
            if d.exists():
                shutil.rmtree(d)
            shutil.copytree(src, d, ignore=shutil.ignore_patterns("target", ".git", "*.log"))
    else:
        sync_server()

    mount = str(DEST).replace("\\", "/")
    bash = r"""
export PATH=/usr/local/cargo/bin:$PATH
export CARGO_TERM_COLOR=never
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq pkg-config libssl-dev >/dev/null
cargo test --features cluster,test-support --test cluster_ha -- --nocapture > /src/cargo-cluster-ha.log 2>&1
EXIT=$?
echo EXIT=$EXIT >> /src/cargo-cluster-ha.log
grep -E '^(test |failures:|error|EXIT=|thread )' /src/cargo-cluster-ha.log | tail -n 80
tail -n 30 /src/cargo-cluster-ha.log
exit $EXIT
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
    print("running cluster_ha...", flush=True)
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
