"""git_utils.py — shared git working-tree validation."""

import subprocess
import sys
from pathlib import Path


def assert_git_clean(project_root: Path) -> None:
    """Fail fast when the repository has uncommitted changes.

    Metadata embeds ``git_commit``. To keep provenance strict, we refuse to write if the working
    tree is dirty. Skip this call entirely when the caller permits a dirty tree.
    """
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        sys.exit("git executable was not found; cannot verify clean working tree")
    except subprocess.CalledProcessError as e:
        sys.exit(f"failed to check git status: {e}")

    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if lines:
        preview = "\n".join(lines[:20])
        suffix = "\n..." if len(lines) > 20 else ""
        sys.exit(
            "Refusing to start because git working tree is dirty. "
            "Commit or stash changes first.\n"
            f"Dirty entries:\n{preview}{suffix}"
        )
