"""Startup environment-variable validation shared across producers.

All environment variables in this project are required and validated at startup
(see the project conventions). ``require_env`` is the single fail-fast accessor
so every program reports a missing variable the same way.
"""

from __future__ import annotations

import os
import sys


def require_env(name: str) -> str:
    """Return the value of environment variable ``name`` or exit fail-fast.

    Empty strings are treated as unset. On failure the process exits with a clear
    message instructing the operator to add the variable to ``.env``.
    """
    val = os.environ.get(name, "")
    if not val:
        sys.exit(f"{name} is not set. Add it to .env.")
    return val
