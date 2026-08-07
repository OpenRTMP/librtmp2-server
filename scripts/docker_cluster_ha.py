#!/usr/bin/env python3
"""Re-run cluster_ha tests only after leader-forward fix."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys

SRC_ROOT = r"X:\AWDev\GitHub\OpenRTMP"
DEST = r"C:\Users\alexg\AppData\Local\Temp\openrtmp-build2"
LOG_NAME = "cargo-cluster-ha.log"


def sync_server() -> None:
    src = os.path.join(SRC_ROOT, "librtmp2-server")
    dst = os.path.join(DEST, "librtmp2-server")
    # Sync only changed source (keep target cache).
    for root, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if d not in (".git", "target")]
        rel = os.path.relpath(root, src)
        out = dst if rel == "." else os.path.join(dst, rel)
        os.makedirs(out, exist_ok=True)
        for f in files:
            if f.endswith(".log"):
                continue
            s = os.path.join(root, f)
            d = os.path.join(out, f)
            shutil.copy2(s, d)
    # Ensure librtmp2 present
    lib_src = os.path.join(SRC_ROOT, "librtmp2")
    lib_dst = os.path.join(DEST, "librtmp2")
    if not os.path.exists(os.path.join(lib_dst, "Cargo.toml")):
        shutil.copytree(
            lib_src,
            lib_dst,
            ignore=shutil.ignore_patterns("target", ".git", "*.log"),
        )


def main() -> int:
    os.makedirs(DEST, exist_ok=True)
    if not os.path.exists(os.path.join(DEST, "librtmp2-server", "Cargo.toml")):
        for name in ("librtmp2", "librtmp2-server"):
            s = os.path.join(SRC_ROOT, name)
            d = os.path.join(DEST, name)
            if os.path.exists(d):
                shutil.rmtree(d)
            shutil.copytree(s, d, ignore=shutil.ignore_patterns("target", ".git", "*.log"))
    else:
        sync_server()

    mount = DEST.replace("\\", "/")
    bash = r"""
export PATH=/usr/local/cargo/bin:$PATH
export CARGO_TERM_COLOR=never
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq pkg-config libssl-dev >/dev/null
cargo test --features cluster,test-support --test cluster_ha -- --nocapture > /src/cargo-cluster-ha.log 2>&1
echo EXIT=$? >> /src/cargo-cluster-ha.log
grep -E '^(test |failures:|error|EXIT=|thread )' /src/cargo-cluster-ha.log | tail -n 80
tail -n 30 /src/cargo-cluster-ha.log
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
    print("running cluster_ha...", flush=True)
    r = subprocess.run(cmd)
    src_log = os.path.join(DEST, LOG_NAME)
    out_log = os.path.join(SRC_ROOT, "librtmp2-server", LOG_NAME)
    if os.path.exists(src_log):
        shutil.copy(src_log, out_log)
        print(f"copied log bytes={os.path.getsize(out_log)}", flush=True)
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
