"""Policy lookups read from ``~/.hermes/memu.json``.

Today this file holds per-WhatsApp-channel routing policy. It will likely grow
to carry other memU-adjacent operator settings (the launcher is the intended
editor). Read fresh on every event so a hand-edit or launcher write takes
effect without a Hermes restart.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Literal

from gateway.whatsapp_identity import expand_whatsapp_aliases, normalize_whatsapp_identifier
from gateway.home import channels_home

logger = logging.getLogger(__name__)

WhatsAppChannelPolicy = Literal["full", "listen_only", "excluded"]


def _memu_json_path():
    return channels_home() / "memu.json"


def _read_memu_config() -> dict:
    path = _memu_json_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("memu_policy: failed to read %s: %s", path, exc)
        return {}
    return data if isinstance(data, dict) else {}


def _default_memorize_for_policy(policy: WhatsAppChannelPolicy) -> bool:
    return policy != "excluded"


def _read_whatsapp_channel_entries(chat_id: str) -> list[dict]:
    raw = str(chat_id or "").strip()
    if not raw:
        return []
    config = _read_memu_config()
    channels = (
        config.get("whatsapp", {}).get("channels", {})
        if isinstance(config.get("whatsapp"), dict)
        else {}
    )
    if not isinstance(channels, dict) or not channels:
        return []

    aliases = {alias for alias in expand_whatsapp_aliases(raw) if alias}
    if not aliases:
        return []
    return [
        entry
        for key, entry in channels.items()
        if isinstance(entry, dict) and normalize_whatsapp_identifier(str(key)) in aliases
    ]


def whatsapp_channel_settings(chat_id: str) -> tuple[WhatsAppChannelPolicy, bool]:
    """Return (policy, memorize) for a WhatsApp chat from ``~/.hermes/memu.json``."""
    entries = _read_whatsapp_channel_entries(chat_id)
    policy: WhatsAppChannelPolicy = "full"
    memorize: bool | None = None
    for entry in entries:
        raw_policy = str(entry.get("policy") or "").strip().lower()
        if raw_policy == "excluded":
            policy = "excluded"
        elif raw_policy == "listen_only" and policy != "excluded":
            policy = "listen_only"
        elif raw_policy == "full" and policy not in {"excluded", "listen_only"}:
            policy = "full"
        if isinstance(entry.get("memorize"), bool):
            memorize = bool(entry.get("memorize")) if memorize is not True else True
    if policy == "excluded":
        return policy, False
    return policy, memorize if memorize is not None else _default_memorize_for_policy(policy)
