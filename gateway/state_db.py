"""Small raw-SQL state.db layer for Channels."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar


T = TypeVar("T")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    user_id TEXT,
    model TEXT,
    model_config TEXT,
    system_prompt TEXT,
    parent_session_id TEXT,
    started_at REAL NOT NULL,
    ended_at REAL,
    end_reason TEXT,
    message_count INTEGER DEFAULT 0,
    tool_call_count INTEGER DEFAULT 0,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cache_read_tokens INTEGER DEFAULT 0,
    cache_write_tokens INTEGER DEFAULT 0,
    reasoning_tokens INTEGER DEFAULT 0,
    cwd TEXT,
    billing_provider TEXT,
    billing_base_url TEXT,
    billing_mode TEXT,
    estimated_cost_usd REAL,
    actual_cost_usd REAL,
    cost_status TEXT,
    cost_source TEXT,
    pricing_version TEXT,
    title TEXT,
    api_call_count INTEGER DEFAULT 0,
    handoff_state TEXT,
    handoff_platform TEXT,
    handoff_error TEXT,
    rewind_count INTEGER NOT NULL DEFAULT 0,
    archived INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (parent_session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    role TEXT NOT NULL,
    content TEXT,
    sender_id TEXT,
    sender_name TEXT,
    source_chat_id TEXT,
    source_message_id TEXT,
    tool_call_id TEXT,
    tool_calls TEXT,
    tool_name TEXT,
    timestamp REAL NOT NULL,
    token_count INTEGER,
    finish_reason TEXT,
    reasoning TEXT,
    reasoning_content TEXT,
    reasoning_details TEXT,
    codex_reasoning_items TEXT,
    codex_message_items TEXT,
    platform_message_id TEXT,
    observed INTEGER DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_sessions_source ON sessions(source);
CREATE INDEX IF NOT EXISTS idx_sessions_source_id ON sessions(source, id);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_session_active
    ON messages(session_id, active, timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_source_key
    ON messages(source_chat_id, source_message_id)
    WHERE source_chat_id IS NOT NULL AND source_message_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS processed_source_keys (
    source_chat_id TEXT NOT NULL,
    source_message_id TEXT NOT NULL,
    processed_at REAL NOT NULL,
    PRIMARY KEY (source_chat_id, source_message_id)
);

CREATE TABLE IF NOT EXISTS whatsapp_message_arrivals (
    source_chat_id TEXT NOT NULL,
    source_message_id TEXT NOT NULL,
    seen_history_at REAL,
    seen_live_at REAL,
    first_seen_mode TEXT,
    PRIMARY KEY (source_chat_id, source_message_id)
);

CREATE TABLE IF NOT EXISTS souls (
    soul_id TEXT PRIMARY KEY,
    active_since REAL NOT NULL
);
"""


class ChannelsStateDB:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _execute_write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = fn(self._conn)
                self._conn.commit()
                return result
            except BaseException:
                self._conn.rollback()
                raise

    def create_session(
        self,
        session_id: str,
        source: str,
        *,
        user_id: str | None = None,
        parent_session_id: str | None = None,
    ) -> str:
        def _do(conn):
            conn.execute(
                """INSERT OR IGNORE INTO sessions (id, source, user_id, parent_session_id, started_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (session_id, source, user_id, parent_session_id, time.time()),
            )

        self._execute_write(_do)
        return session_id

    def end_session(self, session_id: str, end_reason: str) -> None:
        def _do(conn):
            conn.execute(
                "UPDATE sessions SET ended_at = ?, end_reason = ? WHERE id = ? AND ended_at IS NULL",
                (time.time(), end_reason, session_id),
            )

        self._execute_write(_do)

    def append_message(
        self,
        session_id: str,
        role: str,
        content: Any = None,
        sender_id: str | None = None,
        sender_name: str | None = None,
        source_chat_id: str | None = None,
        source_message_id: str | None = None,
        platform_message_id: str | None = None,
        observed: bool = False,
        timestamp: Any = None,
    ) -> int:
        stored_content = self._encode_content(content)
        message_timestamp = _coerce_message_timestamp(timestamp)
        if message_timestamp is None:
            message_timestamp = time.time()

        def _do(conn):
            cursor = conn.execute(
                """INSERT OR IGNORE INTO messages (session_id, role, content, sender_id, sender_name,
                   source_chat_id, source_message_id, timestamp, platform_message_id, observed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    role,
                    stored_content,
                    sender_id,
                    sender_name,
                    source_chat_id,
                    source_message_id,
                    message_timestamp,
                    platform_message_id,
                    1 if observed else 0,
                ),
            )
            inserted = bool(cursor.rowcount)
            if inserted:
                msg_id = cursor.lastrowid
            elif source_chat_id and source_message_id:
                row = conn.execute(
                    "SELECT id FROM messages WHERE source_chat_id = ? AND source_message_id = ? LIMIT 1",
                    (source_chat_id, source_message_id),
                ).fetchone()
                msg_id = int(row[0]) if row else 0
            else:
                msg_id = 0
            if inserted:
                conn.execute(
                    "UPDATE sessions SET message_count = message_count + 1 WHERE id = ?",
                    (session_id,),
                )
            return msg_id

        return self._execute_write(_do)

    def get_messages(self, session_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT role, content, sender_id, sender_name, source_chat_id, source_message_id, timestamp "
                "FROM messages WHERE session_id = ? AND active = 1 ORDER BY timestamp, id",
                (session_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def set_latest_user_sender(
        self,
        session_id: str,
        *,
        sender_id: str | None = None,
        sender_name: str | None = None,
    ) -> None:
        if sender_id is None and sender_name is None:
            return

        def _do(conn):
            row = conn.execute(
                "SELECT id FROM messages WHERE session_id = ? AND role = 'user' ORDER BY id DESC LIMIT 1",
                (session_id,),
            ).fetchone()
            if not row:
                return
            conn.execute(
                "UPDATE messages SET sender_id = ?, sender_name = ? WHERE id = ?",
                (sender_id, sender_name, row[0]),
            )

        self._execute_write(_do)

    def delete_message_by_source_key(
        self,
        *,
        source_chat_id: str,
        source_message_id: str,
    ) -> int:
        chat_key = str(source_chat_id or "").strip()
        message_key = str(source_message_id or "").strip()
        if not chat_key or not message_key:
            return 0

        def _do(conn):
            rows = conn.execute(
                "SELECT session_id FROM messages WHERE source_chat_id = ? AND source_message_id = ?",
                (chat_key, message_key),
            ).fetchall()
            if not rows:
                return 0
            session_counts: dict[str, int] = {}
            for row in rows:
                sid = str(row[0] or "").strip()
                if sid:
                    session_counts[sid] = session_counts.get(sid, 0) + 1

            cursor = conn.execute(
                "DELETE FROM messages WHERE source_chat_id = ? AND source_message_id = ?",
                (chat_key, message_key),
            )
            deleted = int(cursor.rowcount or 0)
            conn.execute(
                "DELETE FROM processed_source_keys WHERE source_chat_id = ? AND source_message_id = ?",
                (chat_key, message_key),
            )
            for sid, count in session_counts.items():
                conn.execute(
                    "UPDATE sessions SET message_count = MAX(message_count - ?, 0) WHERE id = ?",
                    (count, sid),
                )
            return deleted

        return int(self._execute_write(_do))

    def message_source_key_is_processed(
        self,
        *,
        source_chat_id: str,
        source_message_id: str,
    ) -> bool:
        chat_key = str(source_chat_id or "").strip()
        message_key = str(source_message_id or "").strip()
        if not chat_key or not message_key:
            return False
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1
                  FROM processed_source_keys
                 WHERE source_chat_id = ?
                   AND source_message_id = ?
                 LIMIT 1
                """,
                (chat_key, message_key),
            ).fetchone()
        return row is not None

    def message_source_key_exists(
        self,
        *,
        source_chat_id: str,
        source_message_id: str,
    ) -> bool:
        chat_key = str(source_chat_id or "").strip()
        message_key = str(source_message_id or "").strip()
        if not chat_key or not message_key:
            return False
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1
                  FROM messages
                 WHERE source_chat_id = ?
                   AND source_message_id = ?
                 LIMIT 1
                """,
                (chat_key, message_key),
            ).fetchone()
        return row is not None

    def mark_message_source_key_processed(
        self,
        *,
        source_chat_id: str,
        source_message_id: str,
        processed_at: Any = None,
    ) -> bool:
        chat_key = str(source_chat_id or "").strip()
        message_key = str(source_message_id or "").strip()
        if not chat_key or not message_key:
            return False

        processed_ts = _coerce_message_timestamp(processed_at)
        if processed_ts is None:
            processed_ts = time.time()

        def _do(conn):
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO processed_source_keys(
                    source_chat_id,
                    source_message_id,
                    processed_at
                ) VALUES (?, ?, ?)
                """,
                (chat_key, message_key, processed_ts),
            )
            return bool(cursor.rowcount)

        return bool(self._execute_write(_do))

    def record_whatsapp_arrival(
        self,
        source_chat_id: str,
        source_message_id: str,
        mode: str,
        seen_at: Any = None,
    ) -> bool:
        chat_key = str(source_chat_id or "").strip()
        message_key = str(source_message_id or "").strip()
        arrival_mode = str(mode or "").strip().lower()
        if not chat_key or not message_key or arrival_mode not in {"live", "persist_only"}:
            return False

        seen_ts = _coerce_message_timestamp(seen_at)
        if seen_ts is None:
            seen_ts = time.time()
        column = "seen_live_at" if arrival_mode == "live" else "seen_history_at"

        def _do(conn):
            insert_cursor = conn.execute(
                f"""
                INSERT OR IGNORE INTO whatsapp_message_arrivals(
                    source_chat_id,
                    source_message_id,
                    {column},
                    first_seen_mode
                ) VALUES (?, ?, ?, ?)
                """,
                (chat_key, message_key, seen_ts, arrival_mode),
            )
            if insert_cursor.rowcount:
                return True
            cursor = conn.execute(
                f"""
                UPDATE whatsapp_message_arrivals
                   SET {column} = ?
                 WHERE source_chat_id = ?
                   AND source_message_id = ?
                   AND {column} IS NULL
                """,
                (seen_ts, chat_key, message_key),
            )
            return bool(cursor.rowcount)

        return bool(self._execute_write(_do))

    def get_whatsapp_arrival(self, source_chat_id: str, source_message_id: str) -> dict[str, Any] | None:
        chat_key = str(source_chat_id or "").strip()
        message_key = str(source_message_id or "").strip()
        if not chat_key or not message_key:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT source_chat_id, source_message_id, seen_history_at, seen_live_at, first_seen_mode
                  FROM whatsapp_message_arrivals
                 WHERE source_chat_id = ?
                   AND source_message_id = ?
                 LIMIT 1
                """,
                (chat_key, message_key),
            ).fetchone()
        return dict(row) if row else None

    def message_source_key_has_response(
        self,
        *,
        source_chat_id: str,
        source_message_id: str,
    ) -> bool:
        chat_key = str(source_chat_id or "").strip()
        message_key = str(source_message_id or "").strip()
        if not chat_key or not message_key:
            return False
        with self._lock:
            response = self._conn.execute(
                """
                SELECT 1
                  FROM messages source
                 WHERE source.source_chat_id = ?
                   AND source.source_message_id = ?
                   AND EXISTS (
                       SELECT 1
                         FROM messages assistant
                        WHERE assistant.session_id = source.session_id
                          AND assistant.id > source.id
                          AND assistant.role = 'assistant'
                        LIMIT 1
                   )
                 LIMIT 1
                """,
                (chat_key, message_key),
            ).fetchone()
        return response is not None

    def stamp_latest_assistant_source_key(
        self,
        *,
        session_id: str,
        source_chat_id: str,
        source_message_id: str,
        content: str,
    ) -> int:
        session_key = str(session_id or "").strip()
        chat_key = str(source_chat_id or "").strip()
        message_key = str(source_message_id or "").strip()
        delivered_content = str(content or "").strip()
        if not session_key or not chat_key or not message_key or not delivered_content:
            return 0

        def _do(conn):
            existing = conn.execute(
                "SELECT id FROM messages WHERE source_chat_id = ? AND source_message_id = ? LIMIT 1",
                (chat_key, message_key),
            ).fetchone()
            if existing:
                return int(existing[0])
            row = conn.execute(
                """
                SELECT id FROM messages
                 WHERE session_id = ?
                   AND role = 'assistant'
                   AND (source_chat_id IS NULL OR source_chat_id = '')
                   AND (source_message_id IS NULL OR source_message_id = '')
                   AND content IS NOT NULL
                   AND TRIM(content) != ''
                   AND TRIM(content) = ?
                 ORDER BY id DESC
                 LIMIT 1
                """,
                (session_key, delivered_content),
            ).fetchone()
            if not row:
                return 0
            msg_id = int(row[0])
            conn.execute(
                """
                UPDATE messages
                   SET source_chat_id = ?, source_message_id = ?
                 WHERE id = ?
                """,
                (chat_key, message_key, msg_id),
            )
            return msg_id

        return int(self._execute_write(_do) or 0)

    def get_soul_active_since(self, soul_id: str) -> Optional[float]:
        selected = str(soul_id or "").strip()
        if not selected:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT active_since FROM souls WHERE soul_id = ?",
                (selected,),
            ).fetchone()
        if not row:
            return None
        timestamp = _coerce_message_timestamp(row[0])
        if timestamp is None:
            raise ValueError(f"invalid active_since for soul {selected!r}: {row[0]!r}")
        return timestamp

    @staticmethod
    def _encode_content(content: Any) -> str | None:
        if content is None or isinstance(content, str):
            return content
        return json.dumps(content, ensure_ascii=False)


def _coerce_message_timestamp(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            timestamp = float(text)
        except ValueError:
            try:
                return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None
    else:
        return None
    return timestamp / 1000.0 if timestamp > 10_000_000_000 else timestamp
