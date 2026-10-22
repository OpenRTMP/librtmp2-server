"""Unit tests for the path validation in docker_cluster_ha.py."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

_SCRIPT = Path(__file__).with_name("docker_cluster_ha.py")


def _load():
    spec = importlib.util.spec_from_file_location("docker_cluster_ha", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SourceTreeValidationTests(unittest.TestCase):
    def test_src_root_defaults_to_repo_checkout(self) -> None:
        module = _load()
        self.assertEqual(module.SRC_ROOT, _SCRIPT.resolve().parents[2])

    def test_require_source_tree_rejects_missing_tree(self) -> None:
        module = _load()
        with tempfile.TemporaryDirectory() as tmp:
            module.SRC_ROOT = Path(tmp)
            with self.assertRaises(SystemExit) as ctx:
                module.require_source_tree(module.SRC_ROOT, "librtmp2-server")
            self.assertIn("source tree missing", str(ctx.exception))

            tree = Path(tmp) / "librtmp2-server"
            tree.mkdir()
            (tree / "Cargo.toml").write_text("", encoding="utf-8")
            self.assertEqual(
                module.require_source_tree(module.SRC_ROOT, "librtmp2-server"), tree
            )

    def test_sync_server_fails_loudly_for_missing_source(self) -> None:
        module = _load()
        with tempfile.TemporaryDirectory() as tmp:
            module.SRC_ROOT = Path(tmp) / "missing"
            module.DEST = Path(tmp) / "dest"
            with self.assertRaises(SystemExit):
                module.sync_server()

    def test_main_validates_source_before_touching_destination(self) -> None:
        module = _load()
        with tempfile.TemporaryDirectory() as tmp:
            module.SRC_ROOT = Path(tmp) / "missing"
            module.DEST = Path(tmp) / "dest"
            with self.assertRaises(SystemExit):
                module.main()


if __name__ == "__main__":
    unittest.main()
