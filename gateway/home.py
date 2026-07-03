"""Replaces hermes ``get_hermes_home()`` for the standalone channels repo."""

from __future__ import annotations

import os
from pathlib import Path


def channels_home() -> Path:
    env_value = os.environ.get("CHANNELS_HOME")
    if env_value:
        return Path(env_value)
    return Path(__file__).resolve().parent.parent / "data"
