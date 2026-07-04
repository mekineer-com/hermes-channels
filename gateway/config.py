"""Channels daemon config.

Defaults live in ``channels_home()/config.json`` and can be overridden by env:
CHANNELS_MEMU_BASE_URL, CHANNELS_SOUL_ID, CHANNELS_USER_ID,
CHANNELS_BRIDGE_PORT, CHANNELS_POLL_INTERVAL_SECONDS,
CHANNELS_DRAIN_INTERVAL_SECONDS, CHANNELS_TIMEOUT_SECONDS.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from gateway.home import channels_home

logger = logging.getLogger(__name__)


DEFAULT_CONFIG: dict[str, Any] = {
    "memu_base_url": "http://127.0.0.1:8099",
    "soul_id": "default",
    "user_id": "marcos",
    "bridge_port": 3000,
    "timeout_seconds": 90.0,
    "poll_interval_seconds": 1.0,
    "drain_interval_seconds": 2.0,
    "max_message_age_seconds": 300,
    "text_batch_delay_seconds": 5.0,
    "text_batch_split_delay_seconds": 10.0,
    "mode": "",
    "web_source_enabled": True,
    "web_source_headful": False,
    "web_source_chromium_path": "",
    "web_source_disable_service_workers": False,
    "web_source_resource_block": True,
    "reply_prefix": None,
    "session_reset_mode": "both",
    "session_reset_at_hour": 4,
    "session_reset_idle_minutes": 1440,
}


@dataclass
class DaemonSettings:
    memu_base_url: str
    soul_id: str
    user_id: str
    bridge_port: int
    timeout_seconds: float
    poll_interval_seconds: float
    drain_interval_seconds: float
    max_message_age_seconds: int
    text_batch_delay_seconds: float
    text_batch_split_delay_seconds: float
    web_source_enabled: bool
    web_source_headful: bool
    # "bot" | "self-chat"; empty = env WHATSAPP_MODE, then self-chat.
    mode: str = ""
    web_source_chromium_path: str = ""
    web_source_disable_service_workers: bool = False
    web_source_resource_block: bool = True
    # None = let the bridge use its built-in default prefix.
    reply_prefix: str | None = None
    # Session reset policy (hermes SessionResetPolicy defaults):
    # "daily", "idle", "both" (whichever triggers first), or "none".
    session_reset_mode: str = "both"
    session_reset_at_hour: int = 4
    session_reset_idle_minutes: int = 1440


def _coerce_float(value: Any, default: float, key: str) -> float:
    """Coerce numeric config values, falling back on malformed input."""
    try:
        return float(value)
    except (TypeError, ValueError):
        logger.warning("Ignoring invalid %s=%r (expected a number); using default %r", key, value, default)
        return default


def _coerce_int(value: Any, default: int, key: str) -> int:
    """Coerce integer config values, falling back on malformed input."""
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning("Ignoring invalid %s=%r (expected an integer); using default %r", key, value, default)
        return default


def load_config() -> DaemonSettings:
    data = dict(DEFAULT_CONFIG)
    path = channels_home() / "config.json"
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        parsed = {}
    if isinstance(parsed, dict):
        data.update(parsed)

    env_map = {
        "CHANNELS_MEMU_BASE_URL": "memu_base_url",
        "CHANNELS_SOUL_ID": "soul_id",
        "CHANNELS_USER_ID": "user_id",
        "CHANNELS_BRIDGE_PORT": "bridge_port",
        "CHANNELS_TIMEOUT_SECONDS": "timeout_seconds",
        "CHANNELS_POLL_INTERVAL_SECONDS": "poll_interval_seconds",
        "CHANNELS_DRAIN_INTERVAL_SECONDS": "drain_interval_seconds",
        "CHANNELS_MAX_MESSAGE_AGE_SECONDS": "max_message_age_seconds",
    }
    for env_key, config_key in env_map.items():
        if os.environ.get(env_key) is not None:
            data[config_key] = os.environ[env_key]

    return DaemonSettings(
        memu_base_url=str(data["memu_base_url"]).rstrip("/"),
        soul_id=str(data["soul_id"]),
        user_id=str(data["user_id"]),
        bridge_port=_coerce_int(data["bridge_port"], DEFAULT_CONFIG["bridge_port"], "bridge_port"),
        timeout_seconds=_coerce_float(data["timeout_seconds"], DEFAULT_CONFIG["timeout_seconds"], "timeout_seconds"),
        poll_interval_seconds=_coerce_float(data["poll_interval_seconds"], DEFAULT_CONFIG["poll_interval_seconds"], "poll_interval_seconds"),
        drain_interval_seconds=_coerce_float(data["drain_interval_seconds"], DEFAULT_CONFIG["drain_interval_seconds"], "drain_interval_seconds"),
        max_message_age_seconds=_coerce_int(data["max_message_age_seconds"], DEFAULT_CONFIG["max_message_age_seconds"], "max_message_age_seconds"),
        text_batch_delay_seconds=_coerce_float(data["text_batch_delay_seconds"], DEFAULT_CONFIG["text_batch_delay_seconds"], "text_batch_delay_seconds"),
        text_batch_split_delay_seconds=_coerce_float(data["text_batch_split_delay_seconds"], DEFAULT_CONFIG["text_batch_split_delay_seconds"], "text_batch_split_delay_seconds"),
        mode=str(data["mode"] or "").strip().lower(),
        web_source_enabled=_coerce_bool(data["web_source_enabled"], True),
        web_source_headful=_coerce_bool(data["web_source_headful"], False),
        web_source_chromium_path=str(data["web_source_chromium_path"] or "").strip(),
        web_source_disable_service_workers=_coerce_bool(data["web_source_disable_service_workers"], False),
        web_source_resource_block=_coerce_bool(data["web_source_resource_block"], True),
        reply_prefix=None if data["reply_prefix"] is None else str(data["reply_prefix"]),
        session_reset_mode=str(data["session_reset_mode"]).strip().lower(),
        session_reset_at_hour=_coerce_int(data["session_reset_at_hour"], DEFAULT_CONFIG["session_reset_at_hour"], "session_reset_at_hour"),
        session_reset_idle_minutes=_coerce_int(data["session_reset_idle_minutes"], DEFAULT_CONFIG["session_reset_idle_minutes"], "session_reset_idle_minutes"),
    )


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        return default
    return bool(value)
