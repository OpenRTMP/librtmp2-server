#!/usr/bin/env python3
"""Shared path helpers for the OpenRTMP docker helper scripts.

The helper scripts stage the ``librtmp2-server`` (and, for the cargo scripts,
``librtmp2``) checkouts into a build directory before they start a container.
``OPENRTMP_SRC_ROOT`` points them at another parent directory that holds
those checkouts, ``OPENRTMP_DEST`` at another build directory; both default
to the checkout this script lives in and to a folder in the system temp dir.
Resolving the source checkout, refusing a destination that overlaps it and
staging the sources are the same in every script, so they live here once and
are imported by them.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

# The parent directory that holds the `librtmp2-server` and `librtmp2` source
# trees.
DEFAULT_SRC_ROOT = Path(__file__).resolve().parents[2]
CARGO_MANIFEST = "Cargo.toml"


def resolve_src_root() -> Path:
    """The checkout root: ``OPENRTMP_SRC_ROOT`` or the default."""
    return Path(os.environ.get("OPENRTMP_SRC_ROOT") or DEFAULT_SRC_ROOT).resolve()


def resolve_dest(default_name: str) -> Path:
    """The build directory: ``OPENRTMP_DEST`` or a folder in the temp dir."""
    default = Path(tempfile.gettempdir()) / default_name
    return Path(os.environ.get("OPENRTMP_DEST") or default).resolve()


def require_source_tree(src_root: Path, name: str) -> Path:
    """Return ``src_root/name``, failing loudly when the checkout is missing.

    ``shutil.copytree`` over a missing directory raises deep inside the copy,
    so without this check a wrong ``SRC_ROOT`` dies with a confusing error
    before Docker is even started.
    """
    path = src_root / name
    if not (path / CARGO_MANIFEST).is_file():
        raise SystemExit(
            f"source tree missing or incomplete: {path}\n"
            "set OPENRTMP_SRC_ROOT to the OpenRTMP checkout "
            f"(default is {DEFAULT_SRC_ROOT})"
        )
    return path


def require_disjoint_dest(dest: Path, sources: list[Path]) -> None:
    """Refuse a destination that overlaps one of the source checkouts.

    The refresh step deletes the destination before staging the sources into
    it, so a destination that equals or contains a checkout would delete the
    very sources it is about to copy — and one inside a checkout would stage
    the tree into itself.
    """
    dest = dest.resolve()
    for src in sources:
        src = src.resolve()
        if dest == src or dest in src.parents or src in dest.parents:
            raise SystemExit(
                f"destination overlaps the source checkout: DEST={dest} SRC={src}\n"
                "set OPENRTMP_DEST to a directory outside the OpenRTMP checkout"
            )


def stage_sources(dest: Path, src_root: Path, names: tuple[str, ...]) -> None:
    """Stage the named checkouts into ``dest``, replacing what is there."""
    require_disjoint_dest(dest, [src_root / name for name in names])
    for name in names:
        src = require_source_tree(src_root, name)
        dst = dest / name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(
            src,
            dst,
            ignore=shutil.ignore_patterns("target", ".git", "*.log"),
        )
