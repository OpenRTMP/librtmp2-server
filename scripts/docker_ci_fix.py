#!/usr/bin/env python3
"""fmt + cargo update lock for librtmp2-server inside rust:latest."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys

SRC = r"X:\AWDev\GitHub\OpenRTMP\librtmp2-server"
DEST = r"C:\Users\alexg\AppData\Local\Temp\openrtmp-server-ci-fix"


def main() -> int:
    if os.path.exists(DEST):
        shutil.rmtree(DEST, ignore_errors=True)
    os.makedirs(DEST)
    # copy sources without target/logs
    for root, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d not in (".git", "target")]
        rel = os.path.relpath(root, SRC)
        out = DEST if rel == "." else os.path.join(DEST, rel)
        os.makedirs(out, exist_ok=True)
        for f in files:
            if f.endswith(".log") or f == "check-out.txt":
                continue
            shutil.copy2(os.path.join(root, f), os.path.join(out, f))

    mount = DEST.replace("\\", "/")
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
        s = os.path.join(DEST, name)
        if os.path.exists(s):
            shutil.copy2(s, os.path.join(SRC, name))
    # copy formatted sources
    for sub in ("src", "tests", "benches"):
        for root, dirs, files in os.walk(os.path.join(DEST, sub)):
            dirs[:] = [d for d in dirs if d != "target"]
            rel = os.path.relpath(root, DEST)
            for f in files:
                if f.endswith(".rs"):
                    s = os.path.join(root, f)
                    d = os.path.join(SRC, rel, f)
                    shutil.copy2(s, d)
    print("docker exit", r.returncode)
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
