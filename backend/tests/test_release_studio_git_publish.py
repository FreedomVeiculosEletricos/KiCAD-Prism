from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from pathlib import Path

from app.release_studio.git_publish import GitPublishError, run_git
from app.services.job_runtime import JobCancelled


class GitPublishTransportTests(unittest.TestCase):
    def test_a_blocked_command_times_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = {**os.environ, "PATH": f"{_sleeping_git_bin(root)}{os.pathsep}{os.environ['PATH']}"}
            with self.assertRaisesRegex(GitPublishError, "timed out"):
                run_git("status", cwd=root, env=env, timeout_seconds=0.4)

    def test_cancellation_kills_a_blocked_command(self) -> None:
        calls = {"n": 0}

        def cancel() -> None:
            calls["n"] += 1
            if calls["n"] > 1:
                raise JobCancelled("stop")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = {**os.environ, "PATH": f"{_sleeping_git_bin(root)}{os.pathsep}{os.environ['PATH']}"}
            with self.assertRaises(JobCancelled):
                run_git("status", cwd=root, env=env, timeout_seconds=10, check_cancelled=cancel)


def _sleeping_git_bin(root: Path) -> Path:
    directory = root / "bin"
    directory.mkdir()
    real_git = shutil.which("git")
    if not real_git:
        raise AssertionError("git is required for publication tests")
    wrapper = directory / "git"
    wrapper.write_text(
        "#!/bin/sh\nsleep 30\nexit 1\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return directory


if __name__ == "__main__":
    unittest.main()
