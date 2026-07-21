from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dure.host_setup import (
    HostSetupLockError,
    acquire_host_setup_lock,
    release_host_setup_lock,
)


class HostSetupLockTests(unittest.TestCase):
    def test_lock_is_exclusive_and_reusable_after_release(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "host-setup.lock"
            descriptor = acquire_host_setup_lock(path, require_root_owner=False)
            try:
                with self.assertRaisesRegex(HostSetupLockError, "already running"):
                    acquire_host_setup_lock(path, require_root_owner=False)
            finally:
                release_host_setup_lock(descriptor)

            second = acquire_host_setup_lock(path, require_root_owner=False)
            release_host_setup_lock(second)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_symbolic_or_hard_link_lock_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target"
            target.write_text("unchanged", encoding="utf-8")
            symbolic = root / "symbolic.lock"
            symbolic.symlink_to(target)

            with self.assertRaises(HostSetupLockError):
                acquire_host_setup_lock(symbolic, require_root_owner=False)
            self.assertEqual(target.read_text(encoding="utf-8"), "unchanged")

            hard_link = root / "hard-link.lock"
            os.link(target, hard_link)
            with self.assertRaisesRegex(HostSetupLockError, "unsafe"):
                acquire_host_setup_lock(hard_link, require_root_owner=False)

    def test_non_root_owned_metadata_is_rejected_when_required(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "host-setup.lock"
            metadata = SimpleNamespace(
                st_mode=stat.S_IFREG | 0o600,
                st_nlink=1,
                st_uid=1234,
            )
            with patch("dure.host_setup.os.fstat", return_value=metadata):
                with self.assertRaisesRegex(HostSetupLockError, "not root-owned"):
                    acquire_host_setup_lock(path, require_root_owner=True)


if __name__ == "__main__":
    unittest.main()
