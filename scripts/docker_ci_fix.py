#!/usr/bin/env python3
"""fmt + cargo update lock for librtmp2-server inside rust:latest.

The staged checkout is formatted and locked inside the container; see
``openrtmp_docker_paths`` for the source and build directories.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

# The shared helpers live next to this script: make them importable whether
# the script is run as a file (``python scripts/docker_ci_fix.py``) or as
# a module (``python -m scripts.docker_ci_fix``).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from openrtmp_docker_paths import (
    require_disjoint_dest,
    require_source_tree,
    resolve_dest,
    resolve_src_root,
)

SRC_ROOT = resolve_src_root()
SERVER_DIR = "librtmp2-server"
SRC = SRC_ROOT / SERVER_DIR
DEST = resolve_dest("openrtmp-server-ci-fix")


def main() -> int:
    require_source_tree(SRC_ROOT, SERVER_DIR)
    require_disjoint_dest(DEST, [SRC_ROOT, SRC])
    if DEST.exists():
        shutil.rmtree(DEST, ignore_errors=True)
    DEST.mkdir(parents=True)
    # copy sources without target/logs
    for root, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d not in (".git", "target")]
        rel = os.path.relpath(root, SRC)
        out = DEST if rel == "." else DEST / rel
        out.mkdir(parents=True, exist_ok=True)
        for f in files:
            if f.endswith(".log") or f == "check-out.txt":
                continue
            shutil.copy2(Path(root) / f, out / f)

    mount = str(DEST).replace("\\", "/")
    bash = r"""
set -eux
export PATH=/usr/local/cargo/bin:$PATH
export CARGO_TERM_COLOR=never
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq pkg-config libssl-dev git >/dev/null
rustup component add rustfmt
cd /src
cargo fmt
cargo generate-lockfile
cargo check --features test-support 2>&1 | tee /src/ci-fix.log | tail -n 60
EXIT=${PIPESTATUS[0]}
echo EXIT=$EXIT >> /src/ci-fix.log
exit $EXIT
"""
    r = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{mount}:/src",
            "-w",
            "/src",
            "rust:latest",
            "bash",
            "-c",
            bash,
        ]
    )
    # copy back fmt + lock + toml results
    for name in ("Cargo.lock", "Cargo.toml", "ci-fix.log"):
        s = DEST / name
        if s.exists():
            shutil.copy2(s, SRC / name)
    # copy formatted sources
    for sub in ("src", "tests", "benches"):
        for root, dirs, files in os.walk(DEST / sub):
            dirs[:] = [d for d in dirs if d != "target"]
            rel = os.path.relpath(root, DEST)
            for f in files:
                if f.endswith(".rs"):
                    s = Path(root) / f
                    d = SRC / rel / f
                    d.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(s, d)
    print("docker exit", r.returncode)
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
