#!/usr/bin/env python3
"""fmt + cargo update lock for librtmp2-server inside rust:latest.

``SRC_ROOT`` is the parent directory that contains the ``librtmp2-server``
checkout. Set ``OPENRTMP_SRC_ROOT`` to point at another parent directory and
``OPENRTMP_DEST`` to use a different build directory (defaults to a folder in
the system temp dir).
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
SERVER_DIR = "librtmp2-server"
SRC = SRC_ROOT / SERVER_DIR
DEST = Path(
    os.environ.get("OPENRTMP_DEST") or Path(tempfile.gettempdir()) / "openrtmp-server-ci-fix"
).resolve()
CARGO_MANIFEST = "Cargo.toml"


def require_server_tree() -> Path:
    """Return the librtmp2-server checkout, failing loudly when it is missing.

    ``os.walk`` over a missing directory silently yields nothing, so without
    this check the script would stage an empty tree and fail later inside the
    container.
    """
    if not (SRC / CARGO_MANIFEST).is_file():
        raise SystemExit(
            f"source tree missing or incomplete: {SRC}\n"
            "set OPENRTMP_SRC_ROOT to the OpenRTMP checkout "
            f"(default is {DEFAULT_SRC_ROOT})"
        )
    return SRC


def main() -> int:
    require_server_tree()
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
