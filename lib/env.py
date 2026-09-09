"""Startup environment-variable validation shared across producers.

All environment variables in this project are required and validated at startup
(see the project conventions). ``require_env`` is the single fail-fast accessor
so every program reports a missing variable the same way.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def require_env(name: str) -> str:
    """Return the value of environment variable ``name`` or exit fail-fast.

    Empty strings are treated as unset. On failure the process exits with a clear
    message instructing the operator to add the variable to ``.env``.
    """
    val = os.environ.get(name, "")
    if not val:
        sys.exit(f"{name} is not set. Add it to .env.")
    return val


def require_directory_env(name: str) -> str:
    """Return a configured directory after creating it and checking write access."""
    directory = require_env(name)
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
    except OSError as error:
        sys.exit(f"Could not create {name}: {directory}\n{error}")
    if not os.access(directory, os.W_OK):
        sys.exit(f"{name} is not writable: {directory}")
    return directory
