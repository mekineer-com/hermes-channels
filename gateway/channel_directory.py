"""Platform-keyed channel_directory.json writer."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from gateway.home import channels_home
from gateway.util import atomic_json_write


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
    channels: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in _whatsapp_sessions(sessions_index_path):
        if row["id"] not in seen:
            channels.append(row)
            seen.add(row["id"])

    directory = {
        "updated_at": datetime.now().isoformat(),
        "platforms": {"whatsapp": channels},
    }
    atomic_json_write(output_path, directory)
    return directory


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
        name = str(origin.get("chat_name") or origin.get("user_name") or chat_id).strip()
        rows.append({"id": chat_id, "name": name, "type": str(origin.get("chat_type") or "dm")})
    return rows
