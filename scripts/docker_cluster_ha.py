#!/usr/bin/env python3
"""Re-run cluster_ha tests only after leader-forward fix.

The cluster_ha suite runs against the staged checkouts; see
``openrtmp_docker_paths`` for the source and build directories.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

# The shared helpers live next to this script: make them importable whether
# the script is run as a file (``python scripts/docker_cluster_ha.py``) or as
# a module (``python -m scripts.docker_cluster_ha``).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from openrtmp_docker_paths import (
    CARGO_MANIFEST,
    require_source_tree,
    resolve_dest,
    resolve_src_root,
)

SRC_ROOT = resolve_src_root()
DEST = resolve_dest("openrtmp-build2")
LOG_NAME = "cargo-cluster-ha.log"


def source_files(src: Path) -> set[Path]:
    """Relative paths of the files the build copy must contain."""
    out: set[Path] = set()
    for root, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if d not in (".git", "target")]
        rel = Path(root).relative_to(src)
        for f in files:
            if f.endswith(".log"):
                continue
            out.add(rel / f)
    return out


def sync_server() -> None:
    src = require_source_tree(SRC_ROOT, "librtmp2-server")
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
    # Drop the files that no longer exist in the source, so the build copy
    # matches the checkout: a deleted or renamed module must not survive here
    # and be compiled (or reported on) as if it were still part of the tree.
    keep = source_files(src)
    for root, dirs, files in os.walk(dst):
        dirs[:] = [d for d in dirs if d not in (".git", "target")]
        for f in files:
            path = Path(root) / f
            if path.relative_to(dst) not in keep:
                path.unlink()
    # Ensure librtmp2 present and current — the sibling checkout may have
    # changed since the last copy.
    lib_src = require_source_tree(SRC_ROOT, "librtmp2")
    lib_dst = DEST / "librtmp2"
    if lib_dst.exists():
        shutil.rmtree(lib_dst)
    shutil.copytree(
        lib_src,
        lib_dst,
        ignore=shutil.ignore_patterns("target", ".git", "*.log"),
    )


def main() -> int:
    # Validate both trees before either branch so a moved/renamed checkout
    # cannot silently run the suite against a stale destination copy.
    server_src = require_source_tree(SRC_ROOT, "librtmp2-server")
    lib_src = require_source_tree(SRC_ROOT, "librtmp2")
    DEST.mkdir(parents=True, exist_ok=True)
    if not (DEST / "librtmp2-server" / CARGO_MANIFEST).is_file():
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
