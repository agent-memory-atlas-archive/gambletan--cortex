"""Installed-wheel acceptance requiring a real foreign-owned Linux fixture.

CI must create a regular file, chown it to another uid (for example nobody),
chmod it 0666, and set CORTEX_FOREIGN_OWNED_DB to its absolute path. Run this
script with the installed-wheel interpreter and -I. Missing prerequisites fail;
this test neither fabricates foreign ownership nor skips the security boundary.
"""
import gc
import hashlib
import os
from pathlib import Path
import stat
import unittest

import cortex_ai_memory as memory


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(65536), b""):
            digest.update(block)
    metadata = path.lstat()
    return (metadata.st_dev, metadata.st_ino, metadata.st_uid, metadata.st_gid,
            stat.S_IMODE(metadata.st_mode), metadata.st_size, digest.hexdigest())


class ForeignOwnedDatabaseAcceptance(unittest.TestCase):
    def test_personal_open_refuses_foreign_owned_world_writable_file_unchanged(self):
        configured = os.environ.get("CORTEX_FOREIGN_OWNED_DB")
        self.assertTrue(configured, "CI must supply CORTEX_FOREIGN_OWNED_DB")
        path = Path(configured)
        self.assertTrue(path.is_absolute(), "fixture path must be absolute")
        metadata = path.lstat()
        self.assertTrue(stat.S_ISREG(metadata.st_mode), "fixture must be a regular file")
        self.assertNotEqual(metadata.st_uid, os.geteuid(), "fixture must have a genuinely foreign uid")
        self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o666,
                         "fixture must start world-writable, without special mode bits")
        checkout = Path(__file__).resolve().parents[2]
        self.assertNotIn(checkout, Path(memory.__file__).resolve().parents,
                         "run against the installed wheel, not adjacent source")
        before = fingerprint(path)
        with self.assertRaises(RuntimeError):
            instance = memory.Cortex(str(path.resolve()))
            del instance
            gc.collect()
        self.assertEqual(fingerprint(path), before,
                         "refused open must preserve foreign file identity, owner, mode and content")


if __name__ == "__main__":
    unittest.main()
