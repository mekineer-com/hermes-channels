"""Channels gateway daemon.

HERMES_HUNK_RANGES documents the copied/adapted source anchors for audit.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from gateway.channel_directory import write_channel_directory
from gateway.config import DaemonSettings, load_config
from gateway.contact_store import WhatsAppContactStore
from gateway.home import channels_home
from gateway.memu_client import MemuClientError, MemuHttpClient
from gateway.memu_policy import whatsapp_channel_settings
from gateway.state_db import ChannelsStateDB
from gateway.util import atomic_json_write
from gateway.whatsapp_bridge_client import read_self_dm_jid, send_text
from gateway.whatsapp_seam import (
    canonical_whatsapp_jid,
    chat_id_from_whatsapp_conversation_id,
)
from gateway.whatsapp_wal import WhatsAppGatewayWal

logger = logging.getLogger(__name__)

SUPPORTED_DOCUMENT_TYPES = {
    ".pdf": "application/pdf",
    ".md": "text/markdown",
    ".txt": "text/plain",
    ".csv": "text/csv",
    ".log": "text/plain",
    ".json": "application/json",
    ".xml": "application/xml",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
    ".toml": "application/toml",
    ".ini": "text/plain",
    ".cfg": "text/plain",
    ".zip": "application/zip",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".ppt": "application/vnd.ms-powerpoint",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".ts": "text/plain",
    ".py": "text/plain",
    ".sh": "text/plain",
}
_IS_WINDOWS = os.name == "nt"

HERMES_HUNK_RANGES = {
    "bridge_lifecycle": "gateway/platforms/whatsapp.py:520-1210",
    "poll_wal_replay": "gateway/platforms/whatsapp.py:1546-1623,1947-1962",
    "message_control": "gateway/platforms/base.py:1614-1679,3741-3822,3970-4799",
    "staleness_dedup": "gateway/platforms/whatsapp.py:1634-1648; gateway/run.py:10831-10893",
    "text_batching": "gateway/platforms/whatsapp.py:1663-1733,1969-1989",
    "turn_routing": "agent/soul_mode.py:148-191,287-336,444-580",
    "outbound_drain": "gateway/run.py:5758-6003,14753-14758,10903-10925",
    "transcript_persistence": "gateway/run.py:10927-11046; gateway/session.py:791-812,1018-1031,1289-1296",
    "state_db": "hermes_state.py:539-641,1390-1441,2532-2652,2857-3016,4087-4102",
    "directory_writer": "gateway/channel_directory.py:104-110",
    "inbound_preprocessing": "gateway/platforms/whatsapp.py:1800-1913; gateway/platforms/whatsapp_common.py:121-127,222-228,262-273; gateway/platforms/base.py:1124-1148",
    "revoke": "gateway/platforms/whatsapp.py:1748-1780; gateway/run.py:7482-7496,10770-10830; hermes_state.py:2815-2855",
}


class MessageType(Enum):
    TEXT = "text"
    LOCATION = "location"
    PHOTO = "photo"
    VIDEO = "video"
    AUDIO = "audio"
    VOICE = "voice"
    DOCUMENT = "document"
    STICKER = "sticker"
    COMMAND = "command"


class ProcessingOutcome(Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"


@dataclass
class SessionSource:
    platform: str
    chat_id: str
    chat_name: Optional[str] = None
    chat_type: str = "dm"
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    thread_id: Optional[str] = None
    chat_topic: Optional[str] = None
    message_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d = {
            "platform": self.platform,
            "chat_id": self.chat_id,
            "chat_name": self.chat_name,
            "chat_type": self.chat_type,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "thread_id": self.thread_id,
            "chat_topic": self.chat_topic,
        }
        if self.message_id:
            d["message_id"] = self.message_id
        return d


@dataclass
class MessageEvent:
    text: str
    message_type: MessageType = MessageType.TEXT
    source: SessionSource | None = None
    raw_message: Any = None
    message_id: Optional[str] = None
    media_urls: list[str] = field(default_factory=list)
    media_types: list[str] = field(default_factory=list)
    internal: bool = False
    timestamp: datetime = field(default_factory=datetime.now)

    def is_command(self) -> bool:
        return self.text.startswith("/")

    def get_command(self) -> Optional[str]:
        if not self.is_command():
            return None
        parts = self.text.split(maxsplit=1)
        raw = parts[0][1:].lower() if parts else None
        if raw and "@" in raw:
            raw = raw.split("@", 1)[0]
        if raw and "/" in raw:
            return None
        return raw


@dataclass
class SendResult:
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None
    raw_response: Any = None


@dataclass
class SessionEntry:
    session_id: str
    origin: SessionSource
    parent_session_id: str | None = None
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        out = {
            "session_id": self.session_id,
            "origin": self.origin.to_dict(),
            "updated_at": self.updated_at.isoformat(),
        }
        if self.parent_session_id:
            out["parent_session_id"] = self.parent_session_id
        return out


def build_session_key(
    source: SessionSource,
    group_sessions_per_user: bool = True,
    thread_sessions_per_user: bool = False,
) -> str:
    ns = "agent:main"
    platform = source.platform
    if source.chat_type == "dm":
        dm_chat_id = source.chat_id
        if source.platform == "whatsapp":
            dm_chat_id = canonical_whatsapp_jid(source.chat_id)
        if dm_chat_id:
            if source.thread_id:
                return f"{ns}:{platform}:dm:{dm_chat_id}:{source.thread_id}"
            return f"{ns}:{platform}:dm:{dm_chat_id}"
        dm_participant_id = source.user_id
        if dm_participant_id and source.platform == "whatsapp":
            dm_participant_id = canonical_whatsapp_jid(str(dm_participant_id)) or dm_participant_id
        if dm_participant_id:
            if source.thread_id:
                return f"{ns}:{platform}:dm:{dm_participant_id}:{source.thread_id}"
            return f"{ns}:{platform}:dm:{dm_participant_id}"
        if source.thread_id:
            return f"{ns}:{platform}:dm:{source.thread_id}"
        return f"{ns}:{platform}:dm"

    participant_id = source.user_id
    if participant_id and source.platform == "whatsapp":
        participant_id = canonical_whatsapp_jid(str(participant_id)) or participant_id
    key_parts = [ns, platform, source.chat_type]
    if source.chat_id:
        key_parts.append(source.chat_id)
    if source.thread_id:
        key_parts.append(source.thread_id)
    isolate_user = group_sessions_per_user
    if source.thread_id and not thread_sessions_per_user:
        isolate_user = False
    if isolate_user and participant_id:
        key_parts.append(str(participant_id))
    return ":".join(key_parts)


def build_conversation_id(
    *,
    platform: str,
    chat_id: str,
    thread_id: str = "",
    chat_type: str = "",
    gateway_session_key: str = "",
    session_id: str = "",
    canonical_whatsapp_fn: Any = None,
) -> str:
    platform = str(platform or "unknown").strip().lower() or "unknown"
    chat_id = str(chat_id or "").strip()
    thread_id = str(thread_id or "").strip()
    chat_type = str(chat_type or "").strip().lower()

    if platform == "cron":
        if chat_id:
            return f"cron:{chat_id}"
        if gateway_session_key:
            return f"cron:{gateway_session_key}"
        return f"cron:{session_id}"

    if platform == "whatsapp":
        session_key = str(gateway_session_key or "").strip()
        if session_key:
            parts = session_key.split(":")
            if len(parts) >= 5 and parts[0] == "agent" and parts[1] == "main" and parts[2] == "whatsapp":
                if parts[3] == "dm" and canonical_whatsapp_fn is not None:
                    canonical = canonical_whatsapp_fn(parts[4])
                    if canonical:
                        parts[4] = canonical
                return "whatsapp:" + ":".join(parts[3:])
        if chat_id and chat_type == "dm" and canonical_whatsapp_fn is not None:
            canonical = canonical_whatsapp_fn(chat_id)
            if canonical:
                chat_id = canonical

    if chat_id:
        if thread_id:
            return f"{platform}:{chat_id}:{thread_id}"
        return f"{platform}:{chat_id}"
    if gateway_session_key:
        return str(gateway_session_key)
    return f"{platform}:{session_id}"


def merge_pending_message_event(
    pending_messages: dict[str, MessageEvent],
    session_key: str,
    event: MessageEvent,
    *,
    merge_text: bool = False,
) -> None:
    existing = pending_messages.get(session_key)
    if existing:
        if event.source and event.source.platform == "whatsapp":
            pending_messages[session_key] = event
            return
        if (
            merge_text
            and getattr(existing, "message_type", None) == MessageType.TEXT
            and event.message_type == MessageType.TEXT
        ):
            if event.text:
                existing.text = f"{existing.text}\n{event.text}" if existing.text else event.text
            return
    pending_messages[session_key] = event


def _is_duplicate_whatsapp_followup(current_event: MessageEvent, queued_event: MessageEvent) -> bool:
    """True when a queued WhatsApp follow-up is a replay of the current turn."""
    if current_event.source is None or queued_event.source is None:
        return False
    if current_event.source.platform != "whatsapp":
        return False
    if queued_event.source.platform != "whatsapp":
        return False
    current_id = str(current_event.message_id or "").strip()
    queued_id = str(queued_event.message_id or "").strip()
    return bool(current_id and queued_id and current_id == queued_id)


def _file_content_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except OSError:
        return ""


def _coerce_gateway_timestamp(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric / 1000.0 if numeric > 10_000_000_000 else numeric
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            try:
                return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None
        return numeric / 1000.0 if numeric > 10_000_000_000 else numeric
    return None


class ChannelsDaemon:
    _SPLIT_THRESHOLD = 6000

    def __init__(self, settings: DaemonSettings | None = None, *, memu_client: Any = None):
        self.settings = settings or load_config()
        self.home = channels_home()
        self.whatsapp_home = self.home / "whatsapp"
        self._bridge_port = self.settings.bridge_port
        self._bridge_script = Path(__file__).resolve().parent.parent / "bridge" / "bridge.js"
        self._web_source_script = Path(__file__).resolve().parent.parent / "web-source" / "source-daemon.js"
        self._session_path = self.whatsapp_home / "session"
        self._web_source_db = self.whatsapp_home / "web_source.db"
        self._web_source_status_path = self.whatsapp_home / "web_source_status.json"
        self._web_source_auth_path = self.whatsapp_home / "wwebjs_auth"
        self._web_source_pid_path = self._web_source_status_path.with_name("web_source.pid")
        self._web_source_pairing_headful = False
        self._web_source_process: subprocess.Popen | None = None
        self._bridge_process: subprocess.Popen | None = None
        self._session_lock_fh = None
        self._bridge_log_fh = None
        self._web_source_log_fh = None
        self._running = False
        self._stop_requested = asyncio.Event()
        self._poll_task: asyncio.Task | None = None
        self._drain_task: asyncio.Task | None = None
        self._background_tasks: set[asyncio.Task] = set()
        self._session_interrupts: dict[str, asyncio.Event] = {}
        self._pending_messages: dict[str, MessageEvent] = {}
        self._session_tasks: dict[str, asyncio.Task] = {}
        self._pending_text_batches: dict[str, MessageEvent] = {}
        self._pending_text_batch_tasks: dict[str, asyncio.Task] = {}
        self._active_source_keys: set[tuple[str, str]] = set()
        self._gateway_wal = WhatsAppGatewayWal(
            wal_path=self.whatsapp_home / "gateway_wal.jsonl",
            offset_path=self.whatsapp_home / "gateway_wal.offset",
            compact_every=int(os.getenv("WHATSAPP_GATEWAY_WAL_COMPACT_EVERY", "100")),
        )
        self._contact_store = WhatsAppContactStore(store_path=self.whatsapp_home / "contact_store.json")
        self._db = ChannelsStateDB(self.home / "state.db")
        self._sessions_dir = self.home / "sessions"
        self._sessions_index = self._sessions_dir / "sessions.json"
        self._session_entries: dict[str, SessionEntry] = {}
        self._sessions_loaded = False
        self._memu_client = memu_client or MemuHttpClient(
            base_url=self.settings.memu_base_url,
            timeout_seconds=self.settings.timeout_seconds,
        )
        self._outbound_sent_path = self.whatsapp_home / "outbound_sent.json"
        self._outbound_sent_ids: set[str] | None = None

    async def start(self) -> None:
        ok = await self.connect()
        if not ok:
            raise RuntimeError("failed to start WhatsApp bridge")
        self._drain_task = asyncio.create_task(self._outbound_watcher())

    def request_stop(self) -> None:
        self._stop_requested.set()

    async def run_forever(self) -> None:
        await self.start()
        try:
            await self._stop_requested.wait()
        finally:
            await self.disconnect()

    def _whatsapp_mode(self) -> str:
        configured = str(self.settings.mode or "").strip().lower()
        if configured in {"bot", "self-chat"}:
            return configured
        env_mode = os.getenv("WHATSAPP_MODE", "").strip().lower()
        if env_mode in {"bot", "self-chat"}:
            return env_mode
        return "self-chat"

    async def connect(self) -> bool:
        if not shutil.which("node"):
            logger.warning("[whatsapp] Node.js not found. WhatsApp requires Node.js.")
            return False
        bridge_path = self._bridge_script
        if not bridge_path.exists():
            logger.warning("[whatsapp] Bridge script not found: %s", bridge_path)
            return False
        self._session_path.mkdir(parents=True, exist_ok=True)
        if not self._acquire_session_lock():
            return False

        creds_path = self._session_path / "creds.json"
        had_creds = creds_path.exists()
        if not had_creds:
            logger.warning("[whatsapp] WhatsApp enabled but not paired (no creds.json at %s).", creds_path)
        bridge_dir = bridge_path.parent
        pkg_json = bridge_dir / "package.json"
        dep_stamp = bridge_dir / "node_modules" / ".channels-pkg-hash"
        pkg_hash = _file_content_hash(pkg_json)
        deps_fresh = False
        if (bridge_dir / "node_modules").exists():
            try:
                deps_fresh = (dep_stamp.read_text().strip() == pkg_hash) and bool(pkg_hash)
            except OSError:
                deps_fresh = False
        if not deps_fresh:
            npm_bin = shutil.which("npm") or "npm"
            install_result = await asyncio.to_thread(
                subprocess.run,
                [npm_bin, "install", "--silent"],
                cwd=str(bridge_dir),
                capture_output=True,
                text=True,
                timeout=int(os.environ.get("WHATSAPP_NPM_INSTALL_TIMEOUT", "300")),
            )
            if install_result.returncode != 0:
                logger.warning("[whatsapp] npm install failed: %s", install_result.stderr)
                return False
            if pkg_hash:
                try:
                    dep_stamp.parent.mkdir(parents=True, exist_ok=True)
                    dep_stamp.write_text(pkg_hash)
                except OSError:
                    pass

        health = await self._bridge_health()
        if health.get("status") == "connected":
            running_hash = health.get("scriptHash", "")
            disk_hash = _file_content_hash(bridge_path)
            if running_hash and disk_hash and running_hash == disk_hash:
                self._running = True
                self._start_web_source()
                await self._replay_gateway_wal()
                self._poll_task = asyncio.create_task(self._poll_messages())
                return True
            logger.info("[whatsapp] Running bridge is stale, restarting")

        _kill_stale_bridge_by_pidfile(self._session_path, bridge_path)
        _kill_port_process(self._bridge_port)
        await asyncio.sleep(1)

        bridge_log = self.whatsapp_home / "bridge.log"
        self._bridge_log_fh = open(bridge_log, "a", encoding="utf-8")
        env = os.environ.copy()
        # Pass the reply prefix from config so the Node bridge can use it
        # without the user needing to set a separate env var.
        if self.settings.reply_prefix is not None:
            env["WHATSAPP_REPLY_PREFIX"] = self.settings.reply_prefix
        env["CHANNELS_IMAGE_CACHE_DIR"] = str(self.whatsapp_home / "image_cache")
        env["CHANNELS_AUDIO_CACHE_DIR"] = str(self.whatsapp_home / "audio_cache")
        env["CHANNELS_DOCUMENT_CACHE_DIR"] = str(self.whatsapp_home / "document_cache")
        self._bridge_process = subprocess.Popen(
            ["node", str(bridge_path), "--port", str(self._bridge_port), "--session", str(self._session_path), "--mode", self._whatsapp_mode()],
            stdout=self._bridge_log_fh,
            stderr=self._bridge_log_fh,
            preexec_fn=None if _IS_WINDOWS else os.setsid,
            env=env,
        )
        _write_bridge_pidfile(self._session_path, self._bridge_process.pid)

        http_ready = False
        data: dict[str, Any] = {}
        for _ in range(15):
            await asyncio.sleep(1)
            if self._bridge_process.poll() is not None:
                self._close_bridge_log()
                return False
            data = await self._bridge_health()
            if data:
                http_ready = True
                if data.get("status") == "connected":
                    break
        if not http_ready:
            self._close_bridge_log()
            return False

        if not had_creds and data.get("status") != "connected":
            self._running = True
            if (
                self.settings.web_source_enabled
                and not self.settings.web_source_headful
                and self.settings.web_source_auto_headful
            ):
                self._web_source_pairing_headful = True
            self._start_web_source()
            self._poll_task = asyncio.create_task(self._monitor_web_source_setup())
            return True

        if data.get("status") != "connected":
            for _ in range(15):
                await asyncio.sleep(1)
                if self._bridge_process.poll() is not None:
                    self._close_bridge_log()
                    return False
                data = await self._bridge_health()
                if data.get("status") == "connected":
                    break

        self._running = True
        self._start_web_source()
        await self._replay_gateway_wal()
        self._poll_task = asyncio.create_task(self._poll_messages())
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._stop_web_source()
        for task in [self._poll_task, self._drain_task, *list(self._background_tasks), *self._pending_text_batch_tasks.values()]:
            if task and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in [self._poll_task, self._drain_task, *list(self._background_tasks), *self._pending_text_batch_tasks.values()] if task),
            return_exceptions=True,
        )
        if self._bridge_process:
            try:
                _terminate_bridge_process(self._bridge_process, force=False)
                await asyncio.sleep(1)
                if self._bridge_process.poll() is None:
                    _terminate_bridge_process(self._bridge_process, force=True)
            except Exception:
                pass
        try:
            (self._session_path / "bridge.pid").unlink(missing_ok=True)
        except OSError:
            pass
        self._close_bridge_log()
        self._close_web_source_log()
        self._release_session_lock()
        self._db.close()

    async def _poll_messages(self) -> None:
        wal = self._gateway_wal
        while self._running:
            self._check_web_source_exit()
            if self._bridge_process and self._bridge_process.poll() is not None:
                break
            try:
                drained = False
                while self._running and not drained:
                    messages = await self._bridge_get_json("/messages", {"limit": 100}, timeout=30)
                    if not isinstance(messages, list):
                        break
                    if not messages:
                        drained = True
                        break
                    for msg_data in messages:
                        if not isinstance(msg_data, dict):
                            continue
                        wal_row = wal.append(msg_data)
                        self._record_whatsapp_arrival_raw(msg_data)
                        if wal_row is None:
                            if msg_data.get("seq") is None:
                                continue
                            await self._ack_bridge_message(msg_data.get("seq"))
                            self._update_contact_store_from_event(msg_data)
                            continue
                        await self._ack_bridge_message(msg_data.get("seq"))
                        self._update_contact_store_from_event(msg_data)
                        event = await self._build_message_event(msg_data)
                        if event:
                            event.raw_message = dict(event.raw_message)
                            event.raw_message["wal_seq"] = wal_row["wal_seq"]
                            await self._dispatch_built_message_event(event)
                        else:
                            wal.mark_processed(wal_row["wal_seq"])
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("[whatsapp] Poll error: %s", exc)
                await asyncio.sleep(5)
            await asyncio.sleep(self.settings.poll_interval_seconds)

    async def _ack_bridge_message(self, seq: Any) -> None:
        seq_int = int(seq)
        if seq_int < 0:
            raise ValueError(f"Invalid bridge seq for ack: {seq!r}")
        try:
            await self._bridge_post_json("/ack", {"up_to_seq": seq_int}, timeout=10)
        except Exception as exc:
            logger.warning("[whatsapp] Bridge ack request failed for seq=%s: %s", seq_int, exc)

    async def _dispatch_built_message_event(self, event: MessageEvent) -> None:
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        delivery_mode = self._bridge_delivery_mode(raw)
        source_key = self._whatsapp_source_key(raw)
        if delivery_mode == "live" and self.settings.max_message_age_seconds > 0:
            timestamp = _coerce_gateway_timestamp(raw.get("timestamp"))
            if timestamp is not None:
                age_seconds = time.time() - timestamp
                if age_seconds > self.settings.max_message_age_seconds:
                    logger.info(
                        "[whatsapp] Dropping stale WhatsApp live message age=%ss chat=%r message=%r",
                        int(age_seconds),
                        raw.get("chatId"),
                        raw.get("messageId"),
                    )
                    wal_seq = raw.get("wal_seq")
                    if wal_seq is not None:
                        self._gateway_wal.mark_processed(wal_seq)
                    return
        if delivery_mode == "live":
            if self._source_key_is_active(source_key) or self._is_duplicate_source_message(raw):
                self._gateway_wal.mark_processed(raw.get("wal_seq"))
                return
            self._mark_source_key_active(source_key)
            if event.message_type == MessageType.TEXT:
                self._enqueue_text_event(event)
            else:
                await self.handle_message(event)
            return
        wal_seq = raw.get("wal_seq")
        if wal_seq is None:
            raise ValueError("WhatsApp WAL invariant break: missing wal_seq on persist-only event")
        if delivery_mode == "revoke":
            self._apply_whatsapp_revoke(event.source, raw)
            self._gateway_wal.mark_processed(wal_seq)
            return
        if self._history_event_can_trigger_turn(event):
            if self._source_key_is_active(source_key):
                self._gateway_wal.mark_processed(wal_seq)
                return
            self._mark_source_key_active(source_key)
            if event.message_type == MessageType.TEXT:
                self._enqueue_text_event(event)
            else:
                await self.handle_message(event)
            return
        self._persist_history_event(event)
        self._gateway_wal.mark_processed(wal_seq)

    def _enqueue_text_event(self, event: MessageEvent) -> None:
        key = self._text_batch_key(event)
        existing = self._pending_text_batches.get(key)
        chunk_len = len(event.text or "")
        if existing is None:
            event._last_chunk_len = chunk_len  # type: ignore[attr-defined]
            self._pending_text_batches[key] = event
        else:
            if self._is_history_live_text_copy(existing, event):
                if isinstance(existing.raw_message, dict) and isinstance(event.raw_message, dict):
                    self._merge_whatsapp_batch_metadata(existing.raw_message, event.raw_message)
                return
            if event.text:
                existing.text = f"{existing.text}\n{event.text}" if existing.text else event.text
            if isinstance(existing.raw_message, dict) and isinstance(event.raw_message, dict):
                self._merge_whatsapp_batch_metadata(existing.raw_message, event.raw_message)
            existing._last_chunk_len = chunk_len  # type: ignore[attr-defined]
            if event.media_urls:
                existing.media_urls.extend(event.media_urls)
                existing.media_types.extend(event.media_types)

        prior_task = self._pending_text_batch_tasks.get(key)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        self._pending_text_batch_tasks[key] = asyncio.create_task(self._flush_text_batch(key))

    def _is_history_live_text_copy(self, existing: MessageEvent, event: MessageEvent) -> bool:
        existing_raw = existing.raw_message if isinstance(existing.raw_message, dict) else {}
        event_raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        modes = {
            self._bridge_delivery_mode(existing_raw),
            self._bridge_delivery_mode(event_raw),
        }
        return (
            modes == {"live", "persist_only"}
            and str(existing_raw.get("chatId") or "") == str(event_raw.get("chatId") or "")
            and str(existing_raw.get("senderId") or "") == str(event_raw.get("senderId") or "")
            and str(existing.text or "").strip() == str(event.text or "").strip()
        )

    @staticmethod
    def _merge_whatsapp_batch_metadata(existing_raw: dict[str, Any], event_raw: dict[str, Any]) -> None:
        existing_raw["_wal_seqs"] = [
            *(existing_raw.get("_wal_seqs") or [existing_raw.get("wal_seq")]),
            event_raw.get("wal_seq"),
        ]
        existing_raw["_source_keys"] = [
            *(existing_raw.get("_source_keys") or [(existing_raw.get("chatId"), existing_raw.get("messageId"))]),
            (event_raw.get("chatId"), event_raw.get("messageId")),
        ]

    async def _flush_text_batch(self, key: str) -> None:
        current_task = asyncio.current_task()
        try:
            pending = self._pending_text_batches.get(key)
            last_len = getattr(pending, "_last_chunk_len", 0) if pending else 0
            if last_len >= self._SPLIT_THRESHOLD:
                delay = self.settings.text_batch_split_delay_seconds
            else:
                delay = self.settings.text_batch_delay_seconds
            await asyncio.sleep(delay)
            event = self._pending_text_batches.pop(key, None)
            if not event:
                return
            await self.handle_message(event)
        finally:
            if self._pending_text_batch_tasks.get(key) is current_task:
                self._pending_text_batch_tasks.pop(key, None)

    async def handle_message(self, event: MessageEvent) -> None:
        if not event.source:
            return
        session_key = build_session_key(event.source)
        if self._session_is_busy(session_key):
            old_pending = self._pending_messages.get(session_key)
            merge_pending_message_event(
                self._pending_messages,
                session_key,
                event,
                merge_text=event.message_type == MessageType.TEXT,
            )
            new_pending = self._pending_messages.get(session_key)
            if old_pending is not None and old_pending is not new_pending:
                self._mark_event_wal_processed(old_pending)
                self._clear_event_source_keys(old_pending)
            return
        self._drop_orphan_pending(session_key)
        self._start_session_processing(event, session_key)

    def _start_session_processing(self, event: MessageEvent, session_key: str) -> bool:
        interrupt_event = asyncio.Event()
        self._session_interrupts[session_key] = interrupt_event
        task = asyncio.create_task(self._process_message_background(event, session_key))
        self._session_tasks[session_key] = task
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return True

    async def _process_message_background(self, event: MessageEvent, session_key: str) -> None:
        interrupt_event = self._session_interrupts.get(session_key) or asyncio.Event()
        self._session_interrupts[session_key] = interrupt_event
        typing_task = asyncio.create_task(self._keep_typing(event.source.chat_id, stop_event=interrupt_event))
        outcome = ProcessingOutcome.FAILURE
        try:
            response = await self._handle_turn(event, session_key)
            if response:
                result = await self.send(event.source.chat_id, response)
                if result.success:
                    await self._handle_response_delivery(event, result, response)
                    outcome = ProcessingOutcome.SUCCESS
                else:
                    outcome = ProcessingOutcome.FAILURE
            else:
                outcome = ProcessingOutcome.SUCCESS
            await self.on_processing_complete(event, outcome)

            if session_key in self._pending_messages:
                pending_event = self._pending_messages.pop(session_key)
                interrupt_event.clear()
                await self._stop_typing_task(typing_task)
                drain_task = asyncio.create_task(self._process_message_background(pending_event, session_key))
                self._session_tasks[session_key] = drain_task
                try:
                    self._background_tasks.add(drain_task)
                    drain_task.add_done_callback(self._background_tasks.discard)
                except TypeError:
                    # Tests stub create_task() with non-hashable sentinels; tolerate.
                    pass
                return
        except asyncio.CancelledError:
            await self.on_processing_complete(event, ProcessingOutcome.CANCELLED)
            raise
        except Exception as exc:
            logger.error("[whatsapp] Error handling message: %s", exc, exc_info=True)
            await self.on_processing_complete(event, ProcessingOutcome.FAILURE)
            try:
                await self.send(
                    event.source.chat_id,
                    f"Sorry, I encountered an error ({type(exc).__name__}).\n{str(exc)[:300]}",
                )
            except Exception:
                pass
        finally:
            await self._stop_typing_task(typing_task)
            late_pending = self._pending_messages.pop(session_key, None)
            if late_pending is not None and _is_duplicate_whatsapp_followup(event, late_pending):
                logger.info(
                    "[whatsapp] Dropping duplicate WhatsApp late-arrival replay (message_id=%s) for %s",
                    str(late_pending.message_id or ""),
                    session_key,
                )
                late_pending = None
            if late_pending is not None:
                current_task = asyncio.current_task()
                existing_task = self._session_tasks.get(session_key)
                if existing_task is not None and existing_task is not current_task:
                    # The in-band drain (or an earlier late-arrival drain) already
                    # spawned a follow-up task that owns this session. Re-queue the
                    # late-arrival event so that task picks it up instead of spawning
                    # a second concurrent _process_message_background for the same key.
                    self._pending_messages[session_key] = late_pending
                else:
                    interrupt_event.clear()
                    drain_task = asyncio.create_task(self._process_message_background(late_pending, session_key))
                    self._session_tasks[session_key] = drain_task
                    try:
                        self._background_tasks.add(drain_task)
                        drain_task.add_done_callback(self._background_tasks.discard)
                    except TypeError:
                        # Tests stub create_task() with non-hashable sentinels; tolerate.
                        pass
            else:
                current_task = asyncio.current_task()
                if current_task is not None and self._session_tasks.get(session_key) is current_task:
                    del self._session_tasks[session_key]
                    if self._session_interrupts.get(session_key) is interrupt_event:
                        del self._session_interrupts[session_key]

    async def _handle_turn(self, event: MessageEvent, session_key: str) -> str:
        source = event.source
        entry = self.get_or_create_session(source)
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        source_chat_id = str(raw.get("chatId") or "").strip() or None
        source_message_id = str(raw.get("messageId") or "").strip() or None
        sender_id = str(raw.get("senderId") or source.user_id or "").strip() or None
        sender_name = str(raw.get("senderName") or source.user_name or "").strip() or None
        message_timestamp = _coerce_gateway_timestamp(raw.get("timestamp"))
        already_persisted = bool(
            source_chat_id
            and source_message_id
            and self._db.message_source_key_exists(
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
            )
        )
        if not already_persisted:
            self._db.append_message(
                session_id=entry.session_id,
                role="user",
                content=event.text,
                sender_id=sender_id,
                sender_name=sender_name,
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
                timestamp=message_timestamp,
            )
        if sender_id or sender_name:
            self._db.set_latest_user_sender(entry.session_id, sender_id=sender_id, sender_name=sender_name)

        conversation_id = build_conversation_id(
            platform=source.platform,
            chat_id=source.chat_id,
            thread_id=source.thread_id or "",
            chat_type=source.chat_type,
            gateway_session_key=session_key,
            session_id=entry.session_id,
            canonical_whatsapp_fn=canonical_whatsapp_jid,
        )
        chat_type = str(source.chat_type or "").strip().lower()
        channel_mode = "group" if (source.platform == "whatsapp" and chat_type != "dm") else "direct"
        policy, memorize_chat = whatsapp_channel_settings(source.chat_id)
        allow_public_response = True
        if policy == "excluded":
            logger.info("Soul excluded for %s (memu.json policy)", conversation_id)
            return ""
        if policy == "listen_only":
            logger.info("Soul listen_only for %s (memu.json policy)", conversation_id)
            allow_public_response = False

        history = [] if source.platform == "whatsapp" else self._db.get_messages(entry.session_id)
        try:
            turn_out = await asyncio.to_thread(
                self._memu_client.memu_turn,
                conversation_id=conversation_id,
                user_id=self.settings.user_id,
                soul_id=self.settings.soul_id,
                message=str(event.text or "").strip(),
                history=history,
                history_user_name=sender_name,
                user_name=sender_name,
                debug=False,
                channel_mode=channel_mode,
                chat_name=source.chat_name,
                chat_type=chat_type,
                memorize_chat=memorize_chat,
                external_message_id=source_message_id,
                allow_public_response=allow_public_response,
            )
            turn_ok = turn_out.get("ok", True)
            if isinstance(turn_ok, str):
                turn_ok = turn_ok.strip().lower() not in {"false", "0", "no", "off"}
            if not bool(turn_ok):
                raise MemuClientError("memU turn returned ok=false", response_body=json.dumps(turn_out, default=str))
            if not turn_out.get("should_respond", True):
                logger.info("Soul chose LISTEN for %s (channel_mode=%s)", conversation_id, channel_mode)
                return ""
            response_target = str(turn_out.get("response_target") or "respond").strip().lower()
            response_text = str(turn_out.get("response") or "").strip()
            if response_target in {"listen", "observe"}:
                logger.info("Soul chose response_target=%s for %s", response_target, conversation_id)
                return ""
            if response_target == "private" and source.platform == "whatsapp" and response_text:
                self._route_whatsapp_notice_to_self_dm(response_text, conversation_id, "PRIVATE reply")
                self._db.append_message(entry.session_id, "assistant", "")
                return ""
            if not response_text:
                raise MemuClientError("memU turn returned empty response", response_body=json.dumps(turn_out, default=str))
            self._db.append_message(entry.session_id, "assistant", response_text)
            return response_text
        except MemuClientError as exc:
            error_msg = (
                f"memU turn failed: {exc}"
                if getattr(exc, "status_code", None) is None
                else f"memU turn failed (HTTP {exc.status_code}): {exc}"
            )
            if source.platform == "whatsapp":
                self._route_whatsapp_notice_to_self_dm(error_msg, conversation_id, "memU failure notice")
            self._persist_exception_turn(entry, source, raw, event.text, error_msg)
            return ""
        except Exception as exc:
            error_msg = f"memU turn failed: {type(exc).__name__}: {exc}"
            if source.platform == "whatsapp":
                self._route_whatsapp_notice_to_self_dm(error_msg, conversation_id, "memU failure notice")
            self._persist_exception_turn(entry, source, raw, event.text, error_msg)
            return ""

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        wal_seqs = raw.get("_wal_seqs") or [raw.get("wal_seq")]
        if any(wal_seq is None for wal_seq in wal_seqs):
            raise ValueError("WhatsApp WAL invariant break: missing wal_seq on processing completion")
        for wal_seq in wal_seqs:
            self._gateway_wal.mark_processed(wal_seq)
        source_keys = raw.get("_source_keys") or [(raw.get("chatId"), raw.get("messageId"))]
        if outcome != ProcessingOutcome.SUCCESS:
            self._clear_source_keys(source_keys)
            return
        for chat_id, message_id in source_keys:
            chat_key = str(chat_id or "").strip()
            message_key = str(message_id or "").strip()
            if chat_key and message_key:
                self._db.mark_message_source_key_processed(
                    source_chat_id=chat_key,
                    source_message_id=message_key,
                )
        self._clear_source_keys(source_keys)

    async def _replay_gateway_wal(self) -> None:
        wal = self._gateway_wal
        for row in wal.pending():
            wal_seq = row.get("wal_seq")
            event_data = row.get("event")
            if not isinstance(event_data, dict):
                raise ValueError(f"Invalid WhatsApp WAL row payload at wal_seq={wal_seq!r}")
            self._record_whatsapp_arrival_raw(event_data)
            self._update_contact_store_from_event(event_data, source="gateway_wal_replay")
            event = await self._build_message_event(event_data)
            if event:
                event.raw_message = dict(event.raw_message)
                event.raw_message["wal_seq"] = wal_seq
                await self._dispatch_built_message_event(event)
            else:
                wal.mark_processed(wal_seq)

    async def _build_message_event(self, data: dict[str, Any]) -> Optional[MessageEvent]:
        delivery_mode = self._bridge_delivery_mode(data)
        if delivery_mode == "revoke":
            chat_id = str(data.get("chatId") or "").strip()
            if not chat_id:
                return None
            is_group = bool(data.get("isGroup")) or chat_id.endswith("@g.us")
            chat_type = "group" if is_group else "dm"
            chat_name = self._resolve_event_chat_name(data, is_group=is_group)
            source_user_id = str(data.get("senderId") or "").strip()
            source_user_name = str(data.get("senderName") or "").strip()
            if not is_group:
                source_user_id = chat_id or source_user_id
                source_user_name = chat_name or source_user_name
            raw_message = dict(data)
            raw_message["chatName"] = chat_name
            return MessageEvent(
                text="",
                message_type=MessageType.TEXT,
                source=SessionSource(
                    platform="whatsapp",
                    chat_id=chat_id,
                    chat_name=chat_name,
                    chat_type=chat_type,
                    user_id=source_user_id,
                    user_name=source_user_name,
                    message_id=str(data.get("messageId") or "").strip() or None,
                ),
                raw_message=raw_message,
                message_id=str(data.get("messageId") or "").strip() or None,
                internal=True,
            )
        persist_only = delivery_mode != "live"
        if persist_only and not self._should_persist_bridge_event(data):
            return None
        if not persist_only and not self._should_process_message(data):
            return None
        msg_type = MessageType.TEXT
        if data.get("hasMedia"):
            media_type = data.get("mediaType", "")
            if "image" in media_type:
                msg_type = MessageType.PHOTO
            elif "video" in media_type:
                msg_type = MessageType.VIDEO
            elif "audio" in media_type or "ptt" in media_type:
                msg_type = MessageType.VOICE
            else:
                msg_type = MessageType.DOCUMENT
        is_group = bool(data.get("isGroup"))
        chat_type = "group" if is_group else "dm"
        chat_name = self._resolve_event_chat_name(data, is_group=is_group)
        source_user_id = data.get("senderId")
        source_user_name = data.get("senderName")
        if not is_group:
            source_user_id = data.get("chatId") or source_user_id
            source_user_name = chat_name or source_user_name
        source = SessionSource(
            platform="whatsapp",
            chat_id=str(data.get("chatId", "")),
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=source_user_id,
            user_name=source_user_name,
            message_id=str(data.get("messageId") or "").strip() or None,
        )
        # The bridge downloads media itself and always emits local cache
        # paths in mediaUrls (message_ingest.js), so hermes's URL-caching
        # branches are dropped here; unrecognized entries pass through.
        raw_urls = data.get("mediaUrls", [])
        cached_urls = []
        media_types = []
        for url in raw_urls:
            if msg_type == MessageType.PHOTO and os.path.isabs(url):
                # Local file path — bridge already downloaded the image
                cached_urls.append(url)
                media_types.append("image/jpeg")
                logger.info("[whatsapp] Using bridge-cached image: %s", url)
            elif msg_type == MessageType.VOICE and os.path.isabs(url):
                # Local file path — bridge already downloaded the audio
                cached_urls.append(url)
                media_types.append("audio/ogg")
                logger.info("[whatsapp] Using bridge-cached audio: %s", url)
            elif msg_type == MessageType.DOCUMENT and os.path.isabs(url):
                # Local file path — bridge already downloaded the document
                cached_urls.append(url)
                ext = Path(url).suffix.lower()
                mime = SUPPORTED_DOCUMENT_TYPES.get(ext, "application/octet-stream")
                media_types.append(mime)
                logger.info("[whatsapp] Using bridge-cached document: %s", url)
            elif msg_type == MessageType.VIDEO and os.path.isabs(url):
                cached_urls.append(url)
                media_types.append("video/mp4")
                logger.info("[whatsapp] Using bridge-cached video: %s", url)
            else:
                cached_urls.append(url)
                media_types.append("unknown")

        # For text-readable documents, inject file content directly into
        # the message text so the agent can read it inline.
        # Cap at 100KB to match Telegram/Discord/Slack behaviour.
        body = data.get("body", "")
        if data.get("isGroup"):
            body = self._clean_bot_mention_text(body, data)

        MAX_TEXT_INJECT_BYTES = 100 * 1024
        if msg_type == MessageType.DOCUMENT and cached_urls:
            for doc_path in cached_urls:
                ext = Path(doc_path).suffix.lower()
                if ext in {".txt", ".md", ".csv", ".json", ".xml", ".yaml", ".yml", ".log", ".py", ".js", ".ts", ".html", ".css"}:
                    try:
                        file_size = Path(doc_path).stat().st_size
                        if file_size > MAX_TEXT_INJECT_BYTES:
                            logger.info("[whatsapp] Skipping text injection for %s (%s bytes > %s)", doc_path, file_size, MAX_TEXT_INJECT_BYTES)
                            continue
                        content = Path(doc_path).read_text(encoding="utf-8", errors="replace")
                        fname = Path(doc_path).name
                        # Remove the doc_<hex>_ prefix for display
                        display_name = fname
                        if "_" in fname:
                            parts = fname.split("_", 2)
                            if len(parts) >= 3:
                                display_name = parts[2]
                        injection = f"[Content of {display_name}]:\n{content}"
                        if body:
                            body = f"{injection}\n\n{body}"
                        else:
                            body = injection
                        logger.info("[whatsapp] Injected text content from: %s", doc_path)
                    except Exception as e:
                        logger.warning("[whatsapp] Failed to read document text: %s", e)

        raw_message = dict(data)
        raw_message["chatName"] = chat_name
        return MessageEvent(
            text=body,
            message_type=msg_type,
            source=source,
            raw_message=raw_message,
            message_id=data.get("messageId"),
            media_urls=cached_urls,
            media_types=media_types,
            internal=persist_only,
        )

    async def send(self, chat_id: str, content: str, **_: Any) -> SendResult:
        if not content or not content.strip():
            return SendResult(success=True, message_id=None)
        try:
            data = await self._bridge_post_json("/send", {"chatId": chat_id, "message": content}, timeout=30)
            message_id = str(data.get("messageId") or data.get("id") or "").strip() if isinstance(data, dict) else ""
            return SendResult(success=True, message_id=message_id or None, raw_response=data)
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_document(self, chat_id: str, file_path: str, caption: str | None = None) -> SendResult:
        payload = {"chatId": chat_id, "filePath": file_path}
        if caption:
            payload["caption"] = caption
        try:
            data = await self._bridge_post_json("/send-media", payload, timeout=60)
            message_id = str(data.get("messageId") or data.get("id") or "").strip() if isinstance(data, dict) else ""
            return SendResult(success=True, message_id=message_id or None, raw_response=data)
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_typing(self, chat_id: str, **_: Any) -> None:
        await self._bridge_post_json("/typing", {"chatId": chat_id}, timeout=2)

    async def _keep_typing(self, chat_id: str, interval: float = 2.0, stop_event: asyncio.Event | None = None) -> None:
        send_typing_timeout = max(0.25, min(1.5, interval - 0.25))
        try:
            while True:
                if stop_event is not None and stop_event.is_set():
                    return
                try:
                    await asyncio.wait_for(self.send_typing(chat_id), timeout=send_typing_timeout)
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    raise
                except Exception as typing_err:
                    logger.debug("[whatsapp] send_typing error (non-fatal): %s", typing_err)
                if stop_event is None:
                    await asyncio.sleep(interval)
                    continue
                loop = asyncio.get_running_loop()
                deadline = loop.time() + interval
                while not stop_event.is_set():
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    # Poll instead of wait_for(stop_event.wait()).  Cancelling
                    # wait_for while it owns the inner Event.wait task can leave
                    # shutdown paths stuck awaiting the typing task on Python
                    # 3.11/pytest-asyncio; sleep cancellation is immediate.
                    await asyncio.sleep(min(0.25, remaining))
                if stop_event.is_set():
                    return
        except asyncio.CancelledError:
            pass  # Normal cancellation when handler completes

    async def _outbound_watcher(self) -> None:
        while self._running:
            try:
                await self.drain_outbounds()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("WhatsApp memU outbound watcher failed")
            await asyncio.sleep(self.settings.drain_interval_seconds)

    async def drain_outbounds(self) -> int:
        rows = await asyncio.to_thread(
            self._memu_client.claim_whatsapp_outbounds,
            user_id=self.settings.user_id,
            soul_id=self.settings.soul_id,
            claimed_by="channels",
            limit=10,
        )
        for row in rows:
            if isinstance(row, dict):
                await self._deliver_outbound(row)
        return len([row for row in rows if isinstance(row, dict)])

    async def _deliver_outbound(self, row: dict[str, Any]) -> None:
        out_id = str(row.get("id") or "").strip()
        target = str(row.get("target") or "").strip().lower()
        text = str(row.get("response_text") or "").strip()
        media_path = str(row.get("media_path") or "").strip()
        origin = str(row.get("origin_conversation_id") or "").strip()
        if not out_id or (not text and not media_path):
            return

        async def _mark(status: str, *, provider_message_id: str | None = None, error: str | None = None) -> None:
            await asyncio.to_thread(
                self._memu_client.mark_whatsapp_outbound,
                user_id=self.settings.user_id,
                soul_id=self.settings.soul_id,
                outbound_id=out_id,
                status=status,
                provider_message_id=provider_message_id,
                error=error,
            )

        if out_id in self._load_outbound_sent():
            await _mark("sent")
            return
        if media_path and not os.access(media_path, os.R_OK):
            await _mark("failed", error="attachment missing")
            return

        try:
            if target == "private":
                if media_path:
                    self_dm = await asyncio.to_thread(read_self_dm_jid)
                    if not self_dm:
                        await _mark("failed", error="self-DM delivery failed")
                        return
                    result = await self.send_document(self_dm, media_path, text or None)
                    if isinstance(result, SendResult) and not result.success:
                        await _mark("failed", error=result.error or "adapter send failed")
                        return
                    self._record_outbound_sent(out_id)
                    await _mark("sent", provider_message_id=getattr(result, "message_id", None))
                    return
                ok = await asyncio.to_thread(
                    self._route_whatsapp_notice_to_self_dm,
                    text,
                    origin,
                    "free-turn PRIVATE reply",
                )
                if not ok:
                    await _mark("failed", error="self-DM delivery failed")
                    return
                self._record_outbound_sent(out_id)
                await _mark("sent")
                return
            if target != "respond":
                await _mark("failed", error=f"unsupported target {target!r}")
                return
            chat_id = chat_id_from_whatsapp_conversation_id(str(row.get("target_conversation_id") or origin))
            if not chat_id:
                await _mark("failed", error="target chat missing")
                return
            await self.send_typing(chat_id)
            result = await self.send_document(chat_id, media_path, text or None) if media_path else await self.send(chat_id, text)
            if not result.success:
                await _mark("failed", error=result.error or "adapter send failed")
                return
            self._record_outbound_sent(out_id)
            await _mark("sent", provider_message_id=result.message_id)
            if result.message_id and text:
                session_id = self._session_id_for_conversation_id(origin)
                if session_id:
                    self._db.stamp_latest_assistant_source_key(
                        session_id=session_id,
                        source_chat_id=chat_id,
                        source_message_id=result.message_id,
                        content=text,
                    )
        except Exception as exc:
            logger.exception("WhatsApp memU outbound delivery failed for %s", out_id)
            await _mark("failed", error=f"{type(exc).__name__}: {str(exc)[:220]}")

    def get_or_create_session(self, source: SessionSource) -> SessionEntry:
        self._ensure_sessions_loaded()
        key = build_session_key(source)
        entry = self._session_entries.get(key)
        db_end_session_id = None
        if entry:
            reset_reason = self._should_reset(entry)
            if not reset_reason:
                entry.updated_at = datetime.now()
                self._save_sessions()
                return entry
            # Session is being auto-reset; the server chains history across
            # rotations via parent_session_id.
            logger.info("Session auto-reset (%s) for %s", reset_reason, key)
            db_end_session_id = entry.session_id
        session_id = f"session_{uuid.uuid4().hex}"
        entry = SessionEntry(
            session_id=session_id,
            origin=source,
            parent_session_id=db_end_session_id,
        )
        self._session_entries[key] = entry
        self._save_sessions()
        if db_end_session_id:
            self._db.end_session(db_end_session_id, "session_reset")
        self._db.create_session(
            session_id,
            source.platform,
            user_id=source.user_id,
            parent_session_id=db_end_session_id,
        )
        write_channel_directory()
        return entry

    def get_or_create_history_session(self, source: SessionSource) -> SessionEntry:
        """Resolve a session for persisted history without touching activity state."""
        self._ensure_sessions_loaded()
        key = build_session_key(source)
        entry = self._session_entries.get(key)
        if entry:
            return entry
        session_id = f"session_{uuid.uuid4().hex}"
        entry = SessionEntry(session_id=session_id, origin=source)
        self._session_entries[key] = entry
        self._save_sessions()
        self._db.create_session(session_id, source.platform, user_id=source.user_id)
        write_channel_directory()
        return entry

    def _should_reset(self, entry: SessionEntry) -> Optional[str]:
        """Check if a session should be reset based on policy.

        Returns the reset reason ("idle" or "daily") if a reset is needed,
        or None if the session is still valid.
        """
        mode = self.settings.session_reset_mode
        if mode == "none":
            return None

        now = datetime.now()

        if mode in {"idle", "both"}:
            idle_deadline = entry.updated_at + timedelta(minutes=self.settings.session_reset_idle_minutes)
            if now > idle_deadline:
                return "idle"

        if mode in {"daily", "both"}:
            today_reset = now.replace(
                hour=self.settings.session_reset_at_hour,
                minute=0,
                second=0,
                microsecond=0,
            )
            if now.hour < self.settings.session_reset_at_hour:
                today_reset -= timedelta(days=1)

            if entry.updated_at < today_reset:
                return "daily"

        return None

    def _ensure_sessions_loaded(self) -> None:
        if self._sessions_loaded:
            return
        self._sessions_dir.mkdir(parents=True, exist_ok=True)
        if self._sessions_index.exists():
            try:
                data = json.loads(self._sessions_index.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                data = {}
            if isinstance(data, dict):
                for key, entry_data in data.items():
                    try:
                        origin_data = entry_data["origin"]
                        origin = SessionSource(**origin_data)
                        try:
                            updated_at = datetime.fromisoformat(entry_data["updated_at"])
                        except (KeyError, TypeError, ValueError):
                            updated_at = datetime.now()
                        self._session_entries[key] = SessionEntry(
                            session_id=entry_data["session_id"],
                            origin=origin,
                            parent_session_id=entry_data.get("parent_session_id"),
                            updated_at=updated_at,
                        )
                    except (KeyError, TypeError):
                        continue
        self._sessions_loaded = True

    def _save_sessions(self) -> None:
        self._sessions_dir.mkdir(parents=True, exist_ok=True)
        data = {key: entry.to_dict() for key, entry in self._session_entries.items()}
        atomic_json_write(self._sessions_index, data)

    def _apply_whatsapp_revoke(self, source: SessionSource, raw: dict[str, Any]) -> None:
        source_chat_id = str(raw.get("chatId") or "").strip()
        source_message_id = str(raw.get("messageId") or "").strip()
        if not source_chat_id or not source_message_id:
            return

        deleted = 0
        try:
            deleted = int(
                self._db.delete_message_by_source_key(
                    source_chat_id=source_chat_id,
                    source_message_id=source_message_id,
                )
            )
        except Exception as exc:
            logger.warning(
                "Failed to apply WhatsApp revoke in state.db chat=%s message=%s: %s",
                source_chat_id,
                source_message_id,
                exc,
            )

        logger.info(
            "Applied WhatsApp revoke chat=%s message=%s deleted_rows=%d",
            source_chat_id,
            source_message_id,
            deleted,
        )

    def _persist_history_event(self, event: MessageEvent) -> None:
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        source = event.source
        source_chat_id = str(raw.get("chatId") or "").strip()
        source_message_id = str(raw.get("messageId") or "").strip()
        if not source_chat_id or not source_message_id:
            raise ValueError("WhatsApp history event missing source key")
        content = str(event.text or "").strip()
        if not content and not raw.get("hasMedia"):
            return
        message_timestamp = _coerce_gateway_timestamp(raw.get("timestamp"))
        if message_timestamp is None:
            return
        active_since = self._db.get_soul_active_since(self.settings.soul_id)
        if active_since is not None and message_timestamp < active_since:
            return
        role_hint = str(raw.get("speakerRoleHint") or "").strip().lower()
        role = "assistant" if role_hint == "assistant" else "user"
        if role == "assistant":
            speaker_name = self.settings.soul_id or str(raw.get("speakerNameHint") or raw.get("senderName") or "").strip()
            sender_name = speaker_name or None
            sender_id = f"soul:{speaker_name}" if speaker_name else None
        else:
            sender_id = str(raw.get("senderId") or "").strip() or None
            sender_name = str(raw.get("senderName") or "").strip() or None
        session_entry = self.get_or_create_history_session(source)
        self._db.append_message(
            session_id=session_entry.session_id,
            role=role,
            content=content,
            sender_id=sender_id,
            sender_name=sender_name,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            timestamp=message_timestamp,
        )
        if role == "user" and (sender_id or sender_name):
            self._db.set_latest_user_sender(session_entry.session_id, sender_id=sender_id, sender_name=sender_name)

    def _persist_exception_turn(
        self,
        session_entry: SessionEntry,
        source: SessionSource,
        raw_message: dict[str, Any],
        message_text: str,
        error_response: str,
    ) -> None:
        raw = raw_message if isinstance(raw_message, dict) else {}
        source_chat_id = str(raw.get("chatId") or "").strip() or None
        source_message_id = str(raw.get("messageId") or "").strip() or None
        sender_id = str(raw.get("senderId") or "").strip() or None
        sender_name = str(raw.get("senderName") or "").strip() or None
        message_timestamp = _coerce_gateway_timestamp(raw.get("timestamp"))
        already_persisted = bool(
            source_chat_id
            and source_message_id
            and self._db.message_source_key_exists(
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
            )
        )
        if not already_persisted:
            self._db.append_message(
                session_entry.session_id,
                "user",
                message_text,
                sender_id=sender_id,
                sender_name=sender_name,
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
                timestamp=message_timestamp,
            )
            if sender_id or sender_name:
                self._db.set_latest_user_sender(session_entry.session_id, sender_id=sender_id, sender_name=sender_name)
        self._db.append_message(session_entry.session_id, "assistant", error_response, source_chat_id=source.chat_id)

    def _route_whatsapp_notice_to_self_dm(self, text: str, conversation_id: str, notice_kind: str) -> bool:
        try:
            self_dm = read_self_dm_jid()
        except Exception as exc:
            logger.warning("Soul %s not routed (self-DM lookup failed) for %s: %s", notice_kind, conversation_id, exc)
            return False
        if not self_dm:
            logger.warning("Soul %s not routed (self_dm missing) for %s", notice_kind, conversation_id)
            return False
        try:
            ok = bool(send_text(self_dm, text))
        except Exception as exc:
            logger.warning("Soul %s not routed (send failed self_dm=%r) for %s: %s", notice_kind, self_dm, conversation_id, exc)
            return False
        if ok:
            logger.info("Soul routed %s to self-DM %s (from %s)", notice_kind, self_dm, conversation_id)
            return True
        logger.warning("Soul %s not routed (self_dm=%r); silent exit for %s", notice_kind, self_dm, conversation_id)
        return False

    @staticmethod
    def _whatsapp_source_key(raw: dict[str, Any]) -> tuple[str, str] | None:
        source_chat_id = str(raw.get("chatId") or "").strip()
        source_message_id = str(raw.get("messageId") or "").strip()
        if not source_chat_id or not source_message_id:
            return None
        return source_chat_id, source_message_id

    def _record_whatsapp_arrival_raw(self, raw: dict[str, Any]) -> None:
        delivery_mode = self._bridge_delivery_mode(raw)
        if delivery_mode not in {"live", "persist_only"}:
            return
        chat_id = str(raw.get("chatId") or "").strip().lower()
        if not chat_id or chat_id == "status@broadcast" or chat_id.endswith("@newsletter"):
            return
        source_key = self._whatsapp_source_key(raw)
        if source_key is None:
            return
        self._db.record_whatsapp_arrival(
            source_chat_id=source_key[0],
            source_message_id=source_key[1],
            mode=delivery_mode,
        )

    def _source_key_is_active(self, source_key: tuple[str, str] | None) -> bool:
        return bool(source_key and source_key in self._active_source_keys)

    def _mark_source_key_active(self, source_key: tuple[str, str] | None) -> None:
        if source_key:
            self._active_source_keys.add(source_key)

    def _clear_source_keys(self, source_keys: Any) -> None:
        for chat_id, message_id in source_keys or []:
            chat_key = str(chat_id or "").strip()
            message_key = str(message_id or "").strip()
            if chat_key and message_key:
                self._active_source_keys.discard((chat_key, message_key))

    def _clear_event_source_keys(self, event: MessageEvent) -> None:
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        self._clear_source_keys(raw.get("_source_keys") or [(raw.get("chatId"), raw.get("messageId"))])

    def _mark_event_wal_processed(self, event: MessageEvent) -> None:
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        for wal_seq in raw.get("_wal_seqs") or [raw.get("wal_seq")]:
            if wal_seq is not None:
                self._gateway_wal.mark_processed(wal_seq)

    def _history_event_can_trigger_turn(self, event: MessageEvent) -> bool:
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        if not self._whatsapp_source_key(raw):
            return False
        if raw.get("fromMe") or str(raw.get("speakerRoleHint") or "").strip().lower() == "assistant":
            return False
        if not str(event.text or "").strip() and not raw.get("hasMedia"):
            return False
        message_timestamp = _coerce_gateway_timestamp(raw.get("timestamp"))
        if message_timestamp is None:
            return False
        active_since = self._db.get_soul_active_since(self.settings.soul_id)
        if active_since is not None and message_timestamp < active_since:
            return False
        return not self._is_duplicate_source_message(raw)

    def _is_duplicate_source_message(self, raw: dict[str, Any]) -> bool:
        source_chat_id = str(raw.get("chatId") or "").strip()
        source_message_id = str(raw.get("messageId") or "").strip()
        if not source_chat_id or not source_message_id:
            return False
        handled = self._db.message_source_key_is_processed(
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
        )
        if not handled:
            # A bare persisted row is NOT handled: history-sync copies are
            # persist-only (stored, never answered), so the live copy must
            # still get its turn. Skip only when a response exists or the
            # key was marked processed (covers listen-only outcomes).
            handled = self._db.message_source_key_has_response(
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
            )
            if handled:
                self._db.mark_message_source_key_processed(
                    source_chat_id=source_chat_id,
                    source_message_id=source_message_id,
                )
        if handled:
            logger.info("Skipped already-handled WhatsApp source message chat=%s message=%s", source_chat_id, source_message_id)
        return handled

    async def _handle_response_delivery(self, event: MessageEvent, result: Any, content: str) -> None:
        source = getattr(event, "source", None)
        message_id = str(getattr(result, "message_id", "") or "").strip()
        chat_id = str(getattr(source, "chat_id", "") or "").strip()
        if not source or source.platform != "whatsapp" or not message_id or not chat_id:
            return
        session_entry = self.get_or_create_history_session(source)
        self._db.stamp_latest_assistant_source_key(
            session_id=session_entry.session_id,
            source_chat_id=chat_id,
            source_message_id=message_id,
            content=content,
        )

    def _session_is_busy(self, session_key: str) -> bool:
        task = self._session_tasks.get(session_key)
        return bool(task and not task.done())

    def _drop_orphan_pending(self, session_key: str) -> None:
        pending_event = self._pending_messages.pop(session_key, None)
        if pending_event is not None:
            logger.warning(
                "[whatsapp] Dropping orphaned pending message for %s (abnormal task exit)",
                session_key,
            )
            self._clear_event_source_keys(pending_event)

    async def _stop_typing_task(self, typing_task: asyncio.Task) -> None:
        if not typing_task.done():
            typing_task.cancel()
            try:
                await typing_task
            except (asyncio.CancelledError, Exception):
                pass

    def _text_batch_key(self, event: MessageEvent) -> str:
        return build_session_key(event.source)

    def _resolve_event_chat_name(self, data: dict[str, Any], *, is_group: bool) -> str:
        chat_name = str(data.get("chatName") or "").strip()
        if chat_name:
            return chat_name
        chat_id = str(data.get("chatId") or "").strip()
        sender_name = str(data.get("senderName") or "").strip()
        if not is_group and sender_name:
            return sender_name
        if chat_id:
            return chat_id.split("@", 1)[0] or chat_id
        return "unknown-chat"

    @staticmethod
    def _normalize_whatsapp_id(value: Optional[str]) -> str:
        if not value:
            return ""
        normalized = str(value).strip()
        if ":" in normalized and "@" in normalized:
            normalized = re.sub(r":.*@", "@", normalized, count=1)
        return normalized

    def _bot_ids_from_message(self, data: dict[str, Any]) -> set[str]:
        bot_ids = set()
        for candidate in data.get("botIds") or []:
            normalized = self._normalize_whatsapp_id(candidate)
            if normalized:
                bot_ids.add(normalized)
        return bot_ids

    def _clean_bot_mention_text(self, text: str, data: dict[str, Any]) -> str:
        if not text:
            return text
        bot_ids = self._bot_ids_from_message(data)
        cleaned = text
        for bot_id in bot_ids:
            bare_id = bot_id.split("@", 1)[0]
            if bare_id:
                cleaned = re.sub(
                    rf"@{re.escape(bare_id)}\b[,:\-]*\s*", "", cleaned
                )
        return cleaned.strip() or text

    @staticmethod
    def _bridge_delivery_mode(data: dict[str, Any]) -> str:
        delivery_mode = str(data.get("deliveryMode") or "").strip().lower()
        if delivery_mode in {"live", "persist_only", "revoke"}:
            return delivery_mode
        if str(data.get("eventType") or "").strip().lower() == "revoke":
            return "revoke"
        return "persist_only"

    @staticmethod
    def _should_persist_bridge_event(data: dict[str, Any]) -> bool:
        chat_id = str(data.get("chatId") or "").strip().lower()
        if not chat_id or chat_id == "status@broadcast" or chat_id.endswith("@newsletter"):
            return False
        body = str(data.get("body") or "").strip()
        return bool(body or data.get("hasMedia"))

    @staticmethod
    def _should_process_message(data: dict[str, Any]) -> bool:
        chat_id = str(data.get("chatId") or "").strip().lower()
        if not chat_id or chat_id == "status@broadcast" or chat_id.endswith("@newsletter"):
            return False
        return bool(str(data.get("body") or "").strip() or data.get("hasMedia"))

    def _update_contact_store_from_event(self, event_data: dict[str, Any], *, source: str = "gateway_wal") -> None:
        try:
            self._contact_store.update_from_event(event_data, source=source)
        except Exception:
            logger.warning("Failed to update WhatsApp contact store", exc_info=True)

    def _session_id_for_conversation_id(self, conversation_id: str) -> str:
        chat_id = chat_id_from_whatsapp_conversation_id(conversation_id)
        if not chat_id:
            return ""
        self._ensure_sessions_loaded()
        for entry in self._session_entries.values():
            if entry.origin.platform == "whatsapp" and entry.origin.chat_id == chat_id:
                return entry.session_id
        return ""

    def _load_outbound_sent(self) -> set[str]:
        if self._outbound_sent_ids is not None:
            return self._outbound_sent_ids
        try:
            data = json.loads(self._outbound_sent_path.read_text(encoding="utf-8"))
            ids: set[str] = set(data) if isinstance(data, list) else set()
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            ids = set()
        self._outbound_sent_ids = ids
        return ids

    def _record_outbound_sent(self, out_id: str) -> None:
        ids = self._load_outbound_sent()
        if out_id in ids:
            return
        ids.add(out_id)
        existing = []
        try:
            parsed = json.loads(self._outbound_sent_path.read_text(encoding="utf-8"))
            if isinstance(parsed, list):
                existing = [str(item) for item in parsed if str(item).strip()]
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass
        existing = [item for item in existing if item != out_id]
        existing.append(out_id)
        existing = existing[-500:]
        self._outbound_sent_ids = set(existing)
        atomic_json_write(self._outbound_sent_path, existing)

    async def _bridge_health(self) -> dict[str, Any]:
        try:
            data = await self._bridge_get_json("/health", timeout=2)
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    async def _bridge_get_json(self, path: str, params: dict[str, Any] | None = None, *, timeout: float = 10) -> Any:
        return await asyncio.to_thread(self._request_json, "GET", path, None, params, timeout)

    async def _bridge_post_json(self, path: str, payload: dict[str, Any], *, timeout: float = 10) -> Any:
        return await asyncio.to_thread(self._request_json, "POST", path, payload, None, timeout)

    def _request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        params: dict[str, Any] | None,
        timeout: float,
    ) -> Any:
        query = f"?{urllib.parse.urlencode(params)}" if params else ""
        url = f"http://127.0.0.1:{self._bridge_port}{path}{query}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method=method,
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw) if raw else {}

    def _web_source_command(self) -> list[str]:
        command = [
            "node",
            str(self._web_source_script),
            "--db", str(self._web_source_db),
            "--status", str(self._web_source_status_path),
            "--auth", str(self._web_source_auth_path),
            "--client-id", "memu-web-source",
            "--backfill-limit", "100",
            "--contact-snapshot-interval", "900",
            "--memory-diagnostics-interval", "60",
        ]
        active_since = self._db.get_soul_active_since(self.settings.soul_id)
        if active_since is not None:
            command.extend(["--backfill-since", str(int(active_since))])
            command.extend(["--active-since", str(int(active_since))])
        if self.settings.web_source_disable_service_workers:
            command.append("--disable-service-workers")
        if not self.settings.web_source_resource_block:
            command.append("--no-resource-block")
        if self.settings.web_source_headful or self._web_source_pairing_headful:
            command.append("--headful")
        return command

    def _read_web_source_status(self) -> dict[str, Any]:
        try:
            data = json.loads(self._web_source_status_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _start_web_source(self) -> bool:
        if not self.settings.web_source_enabled:
            return True
        process = self._web_source_process
        if process and process.poll() is None:
            return True
        if not self._web_source_script.exists():
            logger.warning("[whatsapp] WhatsApp web-source script not found: %s", self._web_source_script)
            return False
        web_source_dir = self._web_source_script.parent
        if not (web_source_dir / "node_modules").exists():
            logger.warning("[whatsapp] WhatsApp web-source dependencies missing - run `npm install` in %s", web_source_dir)
            return False
        self._web_source_db.parent.mkdir(parents=True, exist_ok=True)
        self._web_source_status_path.parent.mkdir(parents=True, exist_ok=True)
        self._web_source_auth_path.mkdir(parents=True, exist_ok=True)
        _kill_stale_web_source_by_pidfile(
            self._web_source_pid_path,
            script_path=self._web_source_script,
            db_path=self._web_source_db,
            status_path=self._web_source_status_path,
            auth_path=self._web_source_auth_path,
        )
        self._web_source_log_fh = open(self._web_source_status_path.with_suffix(".log"), "a", encoding="utf-8")
        try:
            self._web_source_status_path.unlink()
        except OSError:
            pass
        env = os.environ.copy()
        if self.settings.web_source_chromium_path:
            env["PUPPETEER_EXECUTABLE_PATH"] = self.settings.web_source_chromium_path
        self._web_source_process = subprocess.Popen(
            self._web_source_command(),
            cwd=str(web_source_dir),
            stdout=self._web_source_log_fh,
            stderr=self._web_source_log_fh,
            preexec_fn=None if _IS_WINDOWS else os.setsid,
            env=env,
        )
        try:
            self._web_source_pid_path.write_text(str(self._web_source_process.pid))
        except OSError:
            pass
        return True

    def _check_web_source_exit(self) -> None:
        if not self.settings.web_source_enabled or not self._web_source_process:
            return
        returncode = self._web_source_process.poll()
        if returncode is not None:
            logger.warning("[whatsapp] WhatsApp web-source exited unexpectedly with code %s", returncode)
            self._web_source_process = None
            try:
                self._web_source_pid_path.unlink(missing_ok=True)
            except OSError:
                pass
            self._close_web_source_log()
            return
        status = self._read_web_source_status()
        if (
            status.get("state") == "pairing"
            and not self.settings.web_source_headful
            and not self._web_source_pairing_headful
            and self.settings.web_source_auto_headful
        ):
            logger.info("[whatsapp] WhatsApp web-source needs pairing; opening Chromium window")
            if self._stop_web_source():
                self._web_source_pairing_headful = True
                self._start_web_source()
        elif (
            status.get("state") == "ready"
            and self._web_source_pairing_headful
            and not self.settings.web_source_headful
        ):
            logger.info("[whatsapp] WhatsApp web-source paired; returning Chromium to headless mode")
            if self._stop_web_source():
                self._web_source_pairing_headful = False
                self._start_web_source()

    def _stop_web_source(self) -> bool:
        proc = self._web_source_process
        stopped = True
        if proc and proc.poll() is None:
            try:
                _terminate_bridge_process(proc, force=False)
                if proc.poll() is None:
                    proc.wait(timeout=2)
            except Exception:
                try:
                    _terminate_bridge_process(proc, force=True)
                    if proc.poll() is None:
                        proc.wait(timeout=2)
                except Exception:
                    pass
            stopped = proc.poll() is not None
        if stopped:
            self._web_source_process = None
            try:
                self._web_source_pid_path.unlink(missing_ok=True)
            except OSError:
                pass
            self._close_web_source_log()
        return stopped

    async def _monitor_web_source_setup(self) -> None:
        while self._running:
            if (self._session_path / "creds.json").exists():
                logger.info("[whatsapp] WhatsApp pairing detected; starting reply bridge")
                if await self.connect():
                    return
            self._check_web_source_exit()
            await asyncio.sleep(2)

    def _close_bridge_log(self) -> None:
        if self._bridge_log_fh:
            try:
                self._bridge_log_fh.close()
            except Exception:
                pass
            self._bridge_log_fh = None

    def _close_web_source_log(self) -> None:
        if self._web_source_log_fh:
            try:
                self._web_source_log_fh.close()
            except Exception:
                pass
            self._web_source_log_fh = None

    def _acquire_session_lock(self) -> bool:
        if self._session_lock_fh is not None:
            return True
        lock_path = self._session_path / "channels-session.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+", encoding="utf-8")
        if _IS_WINDOWS:
            self._session_lock_fh = handle
            return True
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            logger.warning("[whatsapp] WhatsApp session is already locked by another gateway")
            return False
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        self._session_lock_fh = handle
        return True

    def _release_session_lock(self) -> None:
        handle = self._session_lock_fh
        if handle is None:
            return
        if not _IS_WINDOWS:
            try:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        try:
            handle.close()
        finally:
            self._session_lock_fh = None


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_pidfile(pid_file: Path) -> Optional[int]:
    if not pid_file.exists():
        return None
    try:
        pid = int(pid_file.read_text().strip())
    except (ValueError, OSError, TypeError):
        try:
            pid_file.unlink()
        except OSError:
            pass
        return None
    return pid


def _pid_cmdline(pid: int) -> str:
    try:
        return " ".join(Path(f"/proc/{pid}/cmdline").read_text(encoding="utf-8", errors="ignore").split("\0"))
    except OSError:
        return ""


def _cmdline_contains_all(cmdline: str, markers: list[str]) -> bool:
    return bool(cmdline) and all(marker in cmdline for marker in markers)


def _kill_stale_pidfile_process(pid_file: Path, *, markers: list[str], label: str) -> None:
    pid = _read_pidfile(pid_file)
    if pid is None:
        return
    if _pid_exists(pid):
        cmdline = _pid_cmdline(pid)
        if _cmdline_contains_all(cmdline, markers):
            try:
                _terminate_pid_tree(pid, force=False)
                time.sleep(0.5)
                if _pid_exists(pid):
                    _terminate_pid_tree(pid, force=True)
                logger.info("[whatsapp] Killed stale %s PID %d from pidfile", label, pid)
            except (ProcessLookupError, PermissionError, OSError, subprocess.SubprocessError):
                pass
        else:
            logger.warning("[whatsapp] Ignoring stale %s pidfile for PID %d because command line did not match", label, pid)
    try:
        pid_file.unlink()
    except OSError:
        pass


def _kill_stale_bridge_by_pidfile(session_path: Path, bridge_script: Path) -> None:
    _kill_stale_pidfile_process(
        session_path / "bridge.pid",
        markers=[str(bridge_script), str(session_path)],
        label="bridge",
    )


def _write_bridge_pidfile(session_path: Path, pid: int) -> None:
    try:
        (session_path / "bridge.pid").write_text(str(pid))
    except OSError:
        pass


def _kill_stale_web_source_by_pidfile(
    pid_file: Path,
    *,
    script_path: Path,
    db_path: Path,
    status_path: Path,
    auth_path: Path,
) -> None:
    _kill_stale_pidfile_process(
        pid_file,
        markers=[str(script_path), str(db_path), str(status_path), str(auth_path)],
        label="web-source",
    )


def _terminate_pid_tree(pid: int, *, force: bool = False) -> None:
    if _IS_WINDOWS:
        cmd = ["taskkill", "/PID", str(pid), "/T"]
        if force:
            cmd.append("/F")
        subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return
    os.killpg(os.getpgid(pid), signal.SIGKILL if force else signal.SIGTERM)


def _terminate_bridge_process(proc, *, force: bool = False) -> None:
    if _IS_WINDOWS:
        cmd = ["taskkill", "/PID", str(proc.pid), "/T"]
        if force:
            cmd.append("/F")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            details = (result.stderr or result.stdout or "").strip()
            raise OSError(details or f"taskkill failed for PID {proc.pid}")
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL if force else signal.SIGTERM)
        return
    except (ProcessLookupError, PermissionError, OSError):
        pass
    if force:
        proc.kill()
    else:
        proc.terminate()


def _kill_port_process(port: int) -> None:
    if _IS_WINDOWS:
        return
    try:
        result = subprocess.run(["fuser", "-k", f"{int(port)}/tcp"], capture_output=True, text=True, timeout=5)
        if result.returncode not in {0, 1}:
            logger.debug("fuser returned %s for port %s: %s", result.returncode, port, result.stderr)
    except (FileNotFoundError, subprocess.SubprocessError, ValueError):
        pass


async def amain() -> None:
    logging.basicConfig(level=os.environ.get("CHANNELS_LOG_LEVEL", "INFO"))
    daemon = ChannelsDaemon()

    def shutdown_signal_handler(sig: int) -> None:
        logger.info("Received %s — initiating shutdown", signal.Signals(sig).name)
        daemon.request_stop()

    loop = asyncio.get_running_loop()
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown_signal_handler, sig)  # windows-footgun: ok — wrapped in try/except NotImplementedError for Windows
            except NotImplementedError:
                pass
    else:
        logger.info("Skipping signal handlers (not running in main thread).")

    await daemon.run_forever()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
