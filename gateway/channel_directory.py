"""Platform-keyed channel_directory.json writer."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from gateway.home import channels_home
from gateway.util import atomic_json_write
from gateway.whatsapp_known_contacts import is_placeholder_whatsapp_name
from gateway.whatsapp_seam import canonical_whatsapp_jid


def write_channel_directory(
    *,
    sessions_index_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    home = channels_home()
    sessions_index_path = sessions_index_path or home / "sessions" / "sessions.json"
    output_path = output_path or home / "channel_directory.json"

    # Hermes parity (_build_from_sessions): the directory lists only chats
    # with session history, never the raw contact store.
    entries_by_id: dict[str, dict[str, str]] = {}
    for row in _whatsapp_sessions(sessions_index_path):
        existing = entries_by_id.get(row["id"])
        entries_by_id[row["id"]] = _better_session_entry(existing, row) if existing else row
    channels = list(entries_by_id.values())

    directory = {
        "updated_at": datetime.now().isoformat(),
        "platforms": {"whatsapp": channels},
    }
    atomic_json_write(output_path, directory)
    return directory


def _better_session_entry(
    existing: dict[str, str],
    candidate: dict[str, str],
) -> dict[str, str]:
    merged = dict(existing)
    existing_name = existing.get("name")
    candidate_name = candidate.get("name")
    if (
        is_placeholder_whatsapp_name(existing_name)
        and candidate_name
        and not is_placeholder_whatsapp_name(candidate_name)
    ):
        merged["name"] = candidate_name
    for key, value in candidate.items():
        if key == "name":
            continue
        if merged.get(key) in (None, "") and value not in (None, ""):
            merged[key] = value
    return merged


def _whatsapp_sessions(path: Path) -> list[dict[str, str]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return []
    if not isinstance(data, dict):
        return []
    rows = []
    for entry in data.values():
        origin = entry.get("origin") if isinstance(entry, dict) else None
        if not isinstance(origin, dict) or origin.get("platform") != "whatsapp":
            continue
        chat_id = str(origin.get("chat_id") or "").strip()
        if not chat_id:
            continue
        chat_id = canonical_whatsapp_jid(chat_id) or chat_id
        name = str(origin.get("chat_name") or origin.get("user_name") or chat_id).strip()
        rows.append({"id": chat_id, "name": name, "type": str(origin.get("chat_type") or "dm")})
    return rows
