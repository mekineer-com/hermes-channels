import asyncio
import json
import sqlite3
import threading
import time
from dataclasses import dataclass

import pytest

import gateway.daemon as daemon_module
from gateway.config import DaemonSettings
from gateway.daemon import (
    ChannelsDaemon,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
    SessionSource,
    build_session_key,
)
from gateway.memu_client import MemuClientError


def settings() -> DaemonSettings:
    return DaemonSettings(
        memu_base_url="http://memu.invalid",
        soul_id="soul",
        user_id="user",
        bridge_port=3000,
        timeout_seconds=1,
        poll_interval_seconds=0.01,
        drain_interval_seconds=0.01,
        max_message_age_seconds=300,
        text_batch_delay_seconds=0.01,
        text_batch_split_delay_seconds=0.01,
        web_source_enabled=False,
        web_source_headful=False,
    )


class FakeMemu:
    def __init__(self, turn=None, rows=None):
        self.turn = turn or {"ok": True, "response": "pong", "response_target": "respond"}
        self.turn_calls = []
        self.claim_calls = []
        self.mark_calls = []
        self.rows = rows or []

    def memu_turn(self, **kwargs):
        self.turn_calls.append(kwargs)
        return dict(self.turn)

    def claim_whatsapp_outbounds(self, **kwargs):
        self.claim_calls.append(kwargs)
        return list(self.rows)

    def mark_whatsapp_outbound(self, **kwargs):
        self.mark_calls.append(kwargs)
        return {"ok": True}


def make_daemon(tmp_path, monkeypatch, memu=None):
    monkeypatch.setenv("CHANNELS_HOME", str(tmp_path))
    daemon = ChannelsDaemon(settings(), memu_client=memu or FakeMemu())
    daemon.send_typing = async_noop
    return daemon


async def async_noop(*_args, **_kwargs):
    return None


def event(text="hello", message_id="m1"):
    source = SessionSource(
        platform="whatsapp",
        chat_id="123@lid",
        chat_name="Ada",
        chat_type="dm",
        user_id="123@lid",
        user_name="Ada",
        message_id=message_id,
    )
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=source,
        raw_message={
            "deliveryMode": "live",
            "chatId": "123@lid",
            "messageId": message_id,
            "senderId": "123@lid",
            "senderName": "Ada",
            "timestamp": 100,
            "wal_seq": 1,
        },
        message_id=message_id,
    )


def history_event(text="hello", message_id="m1", *, wal_seq=1):
    ev = event(text, message_id)
    ev.raw_message["deliveryMode"] = "persist_only"
    ev.raw_message["timestamp"] = time.time()
    ev.raw_message["wal_seq"] = wal_seq
    ev.internal = True
    return ev


def test_batching_debounce_flushes_merged_text(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        seen = []

        async def handle(ev):
            seen.append(ev)

        daemon.handle_message = handle
        first = event("one", "m1")
        second = event("two", "m2")
        second.raw_message["wal_seq"] = 2
        daemon._enqueue_text_event(first)
        daemon._enqueue_text_event(second)
        await asyncio.sleep(0.04)
        assert len(seen) == 1
        assert seen[0].text == "one\ntwo"
        assert seen[0].raw_message["_wal_seqs"] == [1, 2]
        await daemon.disconnect()

    asyncio.run(run())


def test_dedup_gate_marks_wal_processed(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        daemon._db.mark_message_source_key_processed(source_chat_id="123@lid", source_message_id="m1")
        await daemon._dispatch_built_message_event(event())
        assert daemon._gateway_wal.processed_up_to == 1
        await daemon.disconnect()

    asyncio.run(run())


def test_connect_without_creds_starts_bridge_http_but_not_polling(tmp_path, monkeypatch):
    async def run():
        real_sleep = asyncio.sleep
        settings_obj = settings()
        settings_obj.web_source_enabled = True
        settings_obj.web_source_auto_headful = False
        daemon = ChannelsDaemon(settings_obj, memu_client=FakeMemu())
        daemon.whatsapp_home = tmp_path / "whatsapp"
        daemon._session_path = daemon.whatsapp_home / "session"
        daemon._bridge_script = tmp_path / "bridge" / "bridge.js"
        daemon._bridge_script.parent.mkdir(parents=True)
        daemon._bridge_script.write_text("// bridge\n", encoding="utf-8")
        daemon._bridge_port = 3123

        class FakeProcess:
            pid = 1234

            def poll(self):
                return None

        class FakeCompleted:
            returncode = 0
            stderr = ""

        popen_calls = []
        web_source_calls = []
        replay_calls = []
        poll_calls = []
        monitor_calls = []

        async def fast_sleep(_seconds):
            await real_sleep(0)

        async def bridge_health():
            return {"status": "connecting", "qr": "qr-1"}

        async def replay_gateway_wal():
            replay_calls.append(True)

        async def poll_messages():
            poll_calls.append(True)

        async def monitor_setup():
            monitor_calls.append(True)

        monkeypatch.setattr(daemon_module.shutil, "which", lambda _name: "/usr/bin/node")
        monkeypatch.setattr(daemon_module, "_file_content_hash", lambda _path: "hash")
        monkeypatch.setattr(daemon_module, "_kill_stale_bridge_by_pidfile", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(daemon_module, "_kill_port_process", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(daemon_module.asyncio, "sleep", fast_sleep)
        monkeypatch.setattr(
            daemon_module.subprocess,
            "Popen",
            lambda *args, **kwargs: popen_calls.append((args, kwargs)) or FakeProcess(),
        )
        monkeypatch.setattr(daemon_module.subprocess, "run", lambda *_args, **_kwargs: FakeCompleted())
        daemon._acquire_session_lock = lambda: True
        daemon._bridge_health = bridge_health
        daemon._start_web_source = lambda: web_source_calls.append(daemon._web_source_pairing_headful) or True
        daemon._replay_gateway_wal = replay_gateway_wal
        daemon._poll_messages = poll_messages
        daemon._monitor_web_source_setup = monitor_setup

        assert await daemon.connect() is True
        await real_sleep(0)

        assert popen_calls
        assert web_source_calls == [False]
        assert monitor_calls == [True]
        assert replay_calls == []
        assert poll_calls == []
        await daemon.disconnect()

    asyncio.run(run())


def test_turn_payload_and_respond_route(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu({"ok": True, "response": "pong", "response_target": "respond"})
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == "pong"
        assert memu.turn_calls[0]["conversation_id"] == "whatsapp:dm:123@lid"
        assert memu.turn_calls[0]["channel_mode"] == "direct"
        assert memu.turn_calls[0]["external_message_id"] == "m1"
        # Regression: WhatsApp turns never send DB transcript history to memU
        # (memU owns WhatsApp history itself).
        assert memu.turn_calls[0]["history"] == []

        await daemon.on_processing_complete(event(), ProcessingOutcome.SUCCESS)
        assert daemon._db.message_source_key_is_processed(source_chat_id="123@lid", source_message_id="m1")

        await daemon._handle_response_delivery(event(), SendResult(True, "provider-1"), "pong")
        session_id = next(iter(daemon._session_entries.values())).session_id
        rows = daemon._db.get_messages(session_id)
        assistant_row = next(row for row in rows if row["role"] == "assistant")
        assert assistant_row["source_chat_id"] == "123@lid"
        assert assistant_row["source_message_id"] == "provider-1"
        await daemon.disconnect()

    asyncio.run(run())


def test_private_route_goes_to_self_dm(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu({"ok": True, "response": "secret", "response_target": "private"})
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        routed = []
        monkeypatch.setattr("gateway.daemon.read_self_dm_jid", lambda: "self@s.whatsapp.net")
        monkeypatch.setattr("gateway.daemon.send_text", lambda chat, text: routed.append((chat, text)) or True)

        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == ""
        assert routed == [("self@s.whatsapp.net", "secret")]
        # Regression: PRIVATE replies must never leak the soul's response text
        # into the transcript row — only the self-DM send carries it.
        session_id = next(iter(daemon._session_entries.values())).session_id
        rows = daemon._db.get_messages(session_id)
        assert rows[-1]["role"] == "assistant"
        assert rows[-1]["content"] == ""
        await daemon.disconnect()

    asyncio.run(run())


def test_memu_failure_does_not_duplicate_user_row(tmp_path, monkeypatch):
    async def run():
        class FailingMemu(FakeMemu):
            def memu_turn(self, **kwargs):
                self.turn_calls.append(kwargs)
                raise MemuClientError("boom")

        daemon = make_daemon(tmp_path, monkeypatch, FailingMemu())
        monkeypatch.setattr("gateway.daemon.read_self_dm_jid", lambda: "self@s.whatsapp.net")
        monkeypatch.setattr("gateway.daemon.send_text", lambda *_args: True)

        # Retry the same message_id twice (as a real WAL redelivery would):
        # the unique (source_chat_id, source_message_id) index must keep the
        # user row singular even though _handle_turn is invoked twice.
        await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        session_id = next(iter(daemon._session_entries.values())).session_id
        rows = daemon._db.get_messages(session_id)
        assert [row["role"] for row in rows] == ["user", "assistant", "assistant"]
        assert sum(1 for row in rows if row["role"] == "user") == 1
        await daemon.disconnect()

    asyncio.run(run())


def test_outbound_drain_claims_as_channels_and_marks_sent(tmp_path, monkeypatch):
    async def run():
        media = tmp_path / "note.txt"
        media.write_text("x", encoding="utf-8")
        memu = FakeMemu(
            rows=[
                {
                    "id": "out-1",
                    "target": "respond",
                    "response_text": "hello",
                    "origin_conversation_id": "whatsapp:dm:123@lid",
                },
                {
                    "id": "out-2",
                    "target": "respond",
                    "media_path": str(media),
                    "origin_conversation_id": "whatsapp:dm:123@lid",
                },
            ]
        )
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        sent = []

        async def send(chat_id, text, **_kwargs):
            sent.append(("text", chat_id, text))
            return SendResult(True, "sent-text")

        async def send_document(chat_id, path, caption=None):
            sent.append(("media", chat_id, path, caption))
            return SendResult(True, "sent-media")

        daemon.send = send
        daemon.send_document = send_document
        assert await daemon.drain_outbounds() == 2
        assert memu.claim_calls[0]["claimed_by"] == "channels"
        assert [call["status"] for call in memu.mark_calls] == ["sent", "sent"]
        assert sent[0] == ("text", "123@lid", "hello")
        assert sent[1] == ("media", "123@lid", str(media), None)
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T1: session rotation
# ---------------------------------------------------------------------------


def test_session_rotates_on_idle_and_chains_parent(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        source = event().source
        first = daemon.get_or_create_session(source)
        first.updated_at = first.updated_at.replace(year=2000)  # force idle-aged
        second = daemon.get_or_create_session(source)

        assert second.session_id != first.session_id
        assert second.parent_session_id == first.session_id

        key = build_session_key(source)
        saved = json.loads((tmp_path / "sessions" / "sessions.json").read_text(encoding="utf-8"))
        assert saved[key]["parent_session_id"] == first.session_id

        with sqlite3.connect(tmp_path / "state.db") as conn:
            old_row = conn.execute(
                "SELECT ended_at, end_reason FROM sessions WHERE id = ?", (first.session_id,)
            ).fetchone()
            assert old_row[0] is not None
            assert old_row[1] == "session_reset"
            new_parent = conn.execute(
                "SELECT parent_session_id FROM sessions WHERE id = ?", (second.session_id,)
            ).fetchone()[0]
            assert new_parent == first.session_id
        await daemon.disconnect()

    asyncio.run(run())


def test_session_does_not_rotate_when_fresh(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        source = event().source
        first = daemon.get_or_create_session(source)
        second = daemon.get_or_create_session(source)

        assert second is first
        assert second.session_id == first.session_id

        with sqlite3.connect(tmp_path / "state.db") as conn:
            row = conn.execute(
                "SELECT ended_at, end_reason FROM sessions WHERE id = ?", (first.session_id,)
            ).fetchone()
            assert row[0] is None
            assert row[1] is None
        await daemon.disconnect()

    asyncio.run(run())


def test_history_session_never_rotates_or_bumps_activity(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        source = event().source
        entry = daemon.get_or_create_history_session(source)
        entry.updated_at = entry.updated_at.replace(year=2000)  # would trigger idle reset for get_or_create_session
        original = entry.updated_at

        again = daemon.get_or_create_history_session(source)

        assert again is entry
        assert again.updated_at == original
        with sqlite3.connect(tmp_path / "state.db") as conn:
            count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            assert count == 1
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T2: batched multi-message completion marks every source key + wal seq
# ---------------------------------------------------------------------------


def test_batched_completion_marks_every_source_key_and_wal_seq(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        completed = []
        processed = []
        daemon._gateway_wal.mark_processed = completed.append
        daemon._db.mark_message_source_key_processed = lambda **kwargs: processed.append(kwargs)

        async def handle(ev):
            await daemon.on_processing_complete(ev, ProcessingOutcome.SUCCESS)

        daemon.handle_message = handle
        for idx, wal_seq in enumerate((10, 11, 12), start=1):
            ev = event(f"m{idx}", f"msg-{wal_seq}")
            ev.raw_message["wal_seq"] = wal_seq
            daemon._enqueue_text_event(ev)
        await asyncio.sleep(0.05)

        assert completed == [10, 11, 12]
        assert processed == [
            {"source_chat_id": "123@lid", "source_message_id": f"msg-{seq}"} for seq in (10, 11, 12)
        ]
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T3: drain failure paths
# ---------------------------------------------------------------------------


def test_drain_missing_attachment_marks_failed(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu(
            rows=[
                {
                    "id": "out-1",
                    "target": "respond",
                    "media_path": str(tmp_path / "missing.bin"),
                    "origin_conversation_id": "whatsapp:dm:123@lid",
                }
            ]
        )
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        assert await daemon.drain_outbounds() == 1
        assert memu.mark_calls[0]["status"] == "failed"
        assert memu.mark_calls[0]["error"] == "attachment missing"
        await daemon.disconnect()

    asyncio.run(run())


def test_drain_unresolvable_chat_marks_failed(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu(
            rows=[
                {
                    "id": "out-1",
                    "target": "respond",
                    "response_text": "hi",
                    "origin_conversation_id": "not-a-conversation-id",
                }
            ]
        )
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        assert await daemon.drain_outbounds() == 1
        assert memu.mark_calls[0]["status"] == "failed"
        assert memu.mark_calls[0]["error"] == "target chat missing"
        await daemon.disconnect()

    asyncio.run(run())


def test_drain_send_failure_marks_failed_with_error_text(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu(
            rows=[
                {
                    "id": "out-1",
                    "target": "respond",
                    "response_text": "hi",
                    "origin_conversation_id": "whatsapp:dm:123@lid",
                }
            ]
        )
        daemon = make_daemon(tmp_path, monkeypatch, memu)

        async def send(chat_id, text, **_kwargs):
            return SendResult(False, error="boom")

        daemon.send = send
        assert await daemon.drain_outbounds() == 1
        assert memu.mark_calls[0]["status"] == "failed"
        assert memu.mark_calls[0]["error"] == "boom"
        await daemon.disconnect()

    asyncio.run(run())


def test_drain_skips_resend_when_out_id_already_sent(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu(
            rows=[
                {
                    "id": "out-1",
                    "target": "respond",
                    "response_text": "hi",
                    "origin_conversation_id": "whatsapp:dm:123@lid",
                }
            ]
        )
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        daemon._outbound_sent_path.parent.mkdir(parents=True, exist_ok=True)
        daemon._outbound_sent_path.write_text(json.dumps(["out-1"]), encoding="utf-8")
        sent = []

        async def send(chat_id, text, **_kwargs):
            sent.append((chat_id, text))
            return SendResult(True, "should-not-be-used")

        daemon.send = send
        assert await daemon.drain_outbounds() == 1
        assert sent == []
        assert memu.mark_calls[0]["status"] == "sent"
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T13 / T14: drain private routing + assistant source-key stamping
# ---------------------------------------------------------------------------


def test_drain_private_target_routes_to_self_dm_and_marks_sent(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu(
            rows=[
                {
                    "id": "out-1",
                    "target": "private",
                    "response_text": "secret",
                    "origin_conversation_id": "whatsapp:dm:123@lid",
                }
            ]
        )
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        routed = []
        monkeypatch.setattr("gateway.daemon.read_self_dm_jid", lambda: "self@s.whatsapp.net")
        monkeypatch.setattr("gateway.daemon.send_text", lambda chat, text: routed.append((chat, text)) or True)

        assert await daemon.drain_outbounds() == 1
        assert routed == [("self@s.whatsapp.net", "secret")]
        assert memu.mark_calls[0]["status"] == "sent"
        await daemon.disconnect()

    asyncio.run(run())


def test_drain_respond_stamps_assistant_source_key_when_id_and_text_present(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu(
            rows=[
                {
                    "id": "out-1",
                    "target": "respond",
                    "response_text": "hi there",
                    "origin_conversation_id": "whatsapp:dm:123@lid",
                }
            ]
        )
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        source = event().source
        entry = daemon.get_or_create_session(source)
        # Simulate the free-turn assistant row already persisted with no
        # source key, as _handle_turn does before the outbound is drained.
        daemon._db.append_message(entry.session_id, "assistant", "hi there")

        async def send(chat_id, text, **_kwargs):
            return SendResult(True, "wamid.999")

        daemon.send = send
        assert await daemon.drain_outbounds() == 1
        rows = daemon._db.get_messages(entry.session_id)
        assistant_row = next(row for row in rows if row["role"] == "assistant")
        assert assistant_row["source_chat_id"] == "123@lid"
        assert assistant_row["source_message_id"] == "wamid.999"
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T4: _handle_turn routing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("target", ["listen", "observe"])
def test_handle_turn_listen_observe_targets_return_empty_with_no_assistant_row(tmp_path, monkeypatch, target):
    async def run():
        memu = FakeMemu({"ok": True, "response": "should not send", "response_target": target})
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == ""
        session_id = next(iter(daemon._session_entries.values())).session_id
        rows = daemon._db.get_messages(session_id)
        assert all(row["role"] != "assistant" for row in rows)
        await daemon.disconnect()

    asyncio.run(run())


def test_handle_turn_should_respond_false_returns_empty(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu({"ok": True, "should_respond": False, "response": "unused"})
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == ""
        await daemon.disconnect()

    asyncio.run(run())


def test_handle_turn_policy_excluded_short_circuits_before_memu_call(tmp_path, monkeypatch):
    async def run():
        (tmp_path / "memu.json").write_text(
            json.dumps({"whatsapp": {"channels": {"123@lid": {"policy": "excluded"}}}}),
            encoding="utf-8",
        )
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == ""
        assert memu.turn_calls == []
        await daemon.disconnect()

    asyncio.run(run())


def test_handle_turn_policy_listen_only_disallows_public_response(tmp_path, monkeypatch):
    async def run():
        (tmp_path / "memu.json").write_text(
            json.dumps({"whatsapp": {"channels": {"123@lid": {"policy": "listen_only"}}}}),
            encoding="utf-8",
        )
        memu = FakeMemu({"ok": True, "response": "pong", "response_target": "respond"})
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == "pong"
        assert memu.turn_calls[0]["allow_public_response"] is False
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T9: memU failure notices
# ---------------------------------------------------------------------------


def test_memu_exception_notifies_self_dm_and_persists_error_rows(tmp_path, monkeypatch):
    async def run():
        class FailingMemu(FakeMemu):
            def memu_turn(self, **kwargs):
                self.turn_calls.append(kwargs)
                raise MemuClientError("boom", status_code=500)

        daemon = make_daemon(tmp_path, monkeypatch, FailingMemu())
        routed = []
        monkeypatch.setattr("gateway.daemon.read_self_dm_jid", lambda: "self@s.whatsapp.net")
        monkeypatch.setattr("gateway.daemon.send_text", lambda chat, text: routed.append((chat, text)) or True)

        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == ""
        assert len(routed) == 1
        assert routed[0][0] == "self@s.whatsapp.net"
        assert "memU turn failed" in routed[0][1]

        session_id = next(iter(daemon._session_entries.values())).session_id
        rows = daemon._db.get_messages(session_id)
        assert [row["role"] for row in rows] == ["user", "assistant"]
        assert rows[-1]["content"] == routed[0][1]
        await daemon.disconnect()

    asyncio.run(run())


def test_memu_ok_false_raises_and_notifies(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu({"ok": False, "response": "ignored"})
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        routed = []
        monkeypatch.setattr("gateway.daemon.read_self_dm_jid", lambda: "self@s.whatsapp.net")
        monkeypatch.setattr("gateway.daemon.send_text", lambda chat, text: routed.append((chat, text)) or True)

        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == ""
        assert len(routed) == 1
        await daemon.disconnect()

    asyncio.run(run())


def test_memu_empty_response_raises_and_notifies(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu({"ok": True, "response": "", "response_target": "respond"})
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        routed = []
        monkeypatch.setattr("gateway.daemon.read_self_dm_jid", lambda: "self@s.whatsapp.net")
        monkeypatch.setattr("gateway.daemon.send_text", lambda chat, text: routed.append((chat, text)) or True)

        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == ""
        assert len(routed) == 1
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T6: staleness drop
# ---------------------------------------------------------------------------


def test_stale_live_message_marked_processed_without_dispatch(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        dispatched = []

        async def handle(ev):
            dispatched.append(ev)

        daemon.handle_message = handle
        ev = event("old", "m1")
        ev.raw_message["timestamp"] = time.time() - (daemon.settings.max_message_age_seconds + 100)
        await daemon._dispatch_built_message_event(ev)

        assert dispatched == []
        assert daemon._gateway_wal.processed_up_to == 1
        assert memu.turn_calls == []
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T7: _persist_history_event
# ---------------------------------------------------------------------------


def test_persist_history_event_skips_message_older_than_active_since(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        daemon.settings.soul_id = "soul"
        with sqlite3.connect(tmp_path / "state.db") as conn:
            conn.execute("INSERT INTO souls(soul_id, active_since) VALUES(?, ?)", ("soul", 1000.0))

        ev = event("too old", "m1")
        ev.raw_message["timestamp"] = 500  # before active_since
        daemon._persist_history_event(ev)

        key = build_session_key(ev.source)
        assert key not in daemon._session_entries
        with sqlite3.connect(tmp_path / "state.db") as conn:
            count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            assert count == 0
        await daemon.disconnect()

    asyncio.run(run())


def test_persist_history_event_assistant_role_hint_shapes_soul_sender(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        daemon.settings.soul_id = "soul-x"
        ev = event("soul said hi", "m1")
        ev.raw_message["speakerRoleHint"] = "assistant"

        daemon._persist_history_event(ev)

        entry = daemon._session_entries[build_session_key(ev.source)]
        rows = daemon._db.get_messages(entry.session_id)
        assert rows[0]["role"] == "assistant"
        assert rows[0]["sender_name"] == "soul-x"
        assert rows[0]["sender_id"] == "soul:soul-x"
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T8: split-delay for large text chunks
# ---------------------------------------------------------------------------


def test_split_delay_used_for_chunk_at_or_above_threshold(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        daemon.settings.text_batch_delay_seconds = 5.0
        daemon.settings.text_batch_split_delay_seconds = 0.02
        dispatched = []

        async def handle(ev):
            dispatched.append(ev.text)

        daemon.handle_message = handle
        big = event("x" * ChannelsDaemon._SPLIT_THRESHOLD, "m1")
        daemon._enqueue_text_event(big)
        await asyncio.sleep(0.1)

        assert dispatched  # flushed on the short split delay, not the 5s base delay
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T10: _replay_gateway_wal
# ---------------------------------------------------------------------------


def test_replay_gateway_wal_redispatches_pending_and_marks_non_message_processed(tmp_path, monkeypatch):
    async def run():
        daemon = make_daemon(tmp_path, monkeypatch)
        dispatched = []

        async def handle(ev):
            dispatched.append(ev.text)
            await daemon.on_processing_complete(ev, ProcessingOutcome.SUCCESS)

        daemon.handle_message = handle
        daemon._gateway_wal.append(
            {
                "seq": 101,
                "chatId": "123@lid",
                "messageId": "m-101",
                "senderId": "123@lid",
                "senderName": "Ada",
                "body": "pic",
                "hasMedia": True,
                "mediaType": "image/jpeg",
                "deliveryMode": "live",
                "timestamp": time.time(),
            }
        )
        daemon._gateway_wal.append(
            {
                "seq": 102,
                "chatId": "123@lid",
                "messageId": "m-102",
                "deliveryMode": "persist_only",
                "body": "",
            }
        )

        await daemon._replay_gateway_wal()

        assert dispatched == ["pic"]
        assert daemon._gateway_wal.processed_up_to == 2
        await daemon.disconnect()

    asyncio.run(run())


def test_history_arrival_can_trigger_turn_once(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        daemon.send = lambda *_args, **_kwargs: asyncio.sleep(0, result=SendResult(True, "sent-1"))

        await daemon._dispatch_built_message_event(history_event("hello", "hist-1"))
        await asyncio.sleep(0.05)

        assert len(memu.turn_calls) == 1
        assert memu.turn_calls[0]["message"] == "hello"
        assert daemon._gateway_wal.processed_up_to == 1
        assert daemon._db.message_source_key_is_processed(source_chat_id="123@lid", source_message_id="hist-1")
        await daemon.disconnect()

    asyncio.run(run())


def test_history_then_live_records_both_arrivals_and_turns_once(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        daemon.send = lambda *_args, **_kwargs: asyncio.sleep(0, result=SendResult(True, "sent-1"))

        hist = history_event("hello", "same-1", wal_seq=1)
        live = event("hello", "same-1")
        live.raw_message["timestamp"] = time.time()
        live.raw_message["wal_seq"] = 2
        daemon._record_whatsapp_arrival_raw(hist.raw_message)
        await daemon._dispatch_built_message_event(hist)
        daemon._record_whatsapp_arrival_raw(live.raw_message)
        await daemon._dispatch_built_message_event(live)
        await asyncio.sleep(0.05)

        assert len(memu.turn_calls) == 1
        assert memu.turn_calls[0]["message"] == "hello"
        assert daemon._gateway_wal.processed_up_to == 2
        row = daemon._db.get_whatsapp_arrival("123@lid", "same-1")
        assert row["seen_history_at"] is not None
        assert row["seen_live_at"] is not None
        await daemon.disconnect()

    asyncio.run(run())


def test_live_then_history_records_both_arrivals_and_turns_once(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        daemon.send = lambda *_args, **_kwargs: asyncio.sleep(0, result=SendResult(True, "sent-1"))

        live = event("hello", "same-2")
        live.raw_message["timestamp"] = time.time()
        live.raw_message["wal_seq"] = 1
        hist = history_event("hello", "same-2", wal_seq=2)
        daemon._record_whatsapp_arrival_raw(live.raw_message)
        await daemon._dispatch_built_message_event(live)
        daemon._record_whatsapp_arrival_raw(hist.raw_message)
        await daemon._dispatch_built_message_event(hist)
        await asyncio.sleep(0.05)

        assert len(memu.turn_calls) == 1
        assert daemon._gateway_wal.processed_up_to == 2
        row = daemon._db.get_whatsapp_arrival("123@lid", "same-2")
        assert row["seen_history_at"] is not None
        assert row["seen_live_at"] is not None
        await daemon.disconnect()

    asyncio.run(run())


def test_from_me_and_empty_history_record_arrival_without_turn(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)

        from_me = history_event("sent by me", "from-me", wal_seq=1)
        from_me.raw_message["fromMe"] = True
        assistant = history_event("soul said it", "assistant-row", wal_seq=2)
        assistant.raw_message["speakerRoleHint"] = "assistant"
        empty = history_event("", "empty", wal_seq=3)
        daemon._record_whatsapp_arrival_raw(from_me.raw_message)
        await daemon._dispatch_built_message_event(from_me)
        daemon._record_whatsapp_arrival_raw(assistant.raw_message)
        await daemon._dispatch_built_message_event(assistant)
        daemon._record_whatsapp_arrival_raw(empty.raw_message)
        built = await daemon._build_message_event(empty.raw_message)
        if built:
            await daemon._dispatch_built_message_event(built)

        assert memu.turn_calls == []
        assert daemon._db.get_whatsapp_arrival("123@lid", "from-me") is not None
        assert daemon._db.get_whatsapp_arrival("123@lid", "assistant-row") is not None
        assert daemon._db.get_whatsapp_arrival("123@lid", "empty") is not None
        await daemon.disconnect()

    asyncio.run(run())


def test_history_recovery_ignores_live_max_age_gate(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        daemon.send = lambda *_args, **_kwargs: asyncio.sleep(0, result=SendResult(True, "sent-1"))
        hist = history_event("old but recoverable", "old-history", wal_seq=1)
        hist.raw_message["timestamp"] = time.time() - (daemon.settings.max_message_age_seconds + 100)

        await daemon._dispatch_built_message_event(hist)
        await asyncio.sleep(0.05)

        assert len(memu.turn_calls) == 1
        assert memu.turn_calls[0]["message"] == "old but recoverable"
        await daemon.disconnect()

    asyncio.run(run())


def test_replayed_history_arrival_still_processes_if_not_handled(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        daemon.send = lambda *_args, **_kwargs: asyncio.sleep(0, result=SendResult(True, "sent-1"))
        daemon._db.record_whatsapp_arrival("123@lid", "replay-1", "persist_only", seen_at=time.time())
        daemon._gateway_wal.append(
            {
                "seq": 10,
                "chatId": "123@lid",
                "messageId": "replay-1",
                "senderId": "123@lid",
                "senderName": "Ada",
                "body": "after crash",
                "deliveryMode": "persist_only",
                "timestamp": time.time(),
            }
        )

        await daemon._replay_gateway_wal()
        await asyncio.sleep(0.05)

        assert len(memu.turn_calls) == 1
        assert memu.turn_calls[0]["message"] == "after crash"
        assert daemon._gateway_wal.processed_up_to == 1
        await daemon.disconnect()

    asyncio.run(run())


def test_history_live_copies_do_not_merge_same_text(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu()
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        daemon.settings.text_batch_delay_seconds = 0.04
        daemon.send = lambda *_args, **_kwargs: asyncio.sleep(0, result=SendResult(True, "sent-1"))

        hist = history_event("hello", "same-3", wal_seq=1)
        live = event("hello", "same-3")
        live.raw_message["timestamp"] = time.time()
        live.raw_message["wal_seq"] = 2
        await daemon._dispatch_built_message_event(hist)
        await daemon._dispatch_built_message_event(live)
        await asyncio.sleep(0.08)

        assert len(memu.turn_calls) == 1
        assert memu.turn_calls[0]["message"] == "hello"
        await daemon.disconnect()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# T11: double-dispatch regression
# ---------------------------------------------------------------------------


def test_second_event_during_active_turn_queues_without_second_task(tmp_path, monkeypatch):
    async def run():
        release = threading.Event()
        started = threading.Event()

        class SlowMemu(FakeMemu):
            def memu_turn(self, **kwargs):
                self.turn_calls.append(kwargs)
                started.set()
                release.wait(timeout=2)
                return dict(self.turn)

        daemon = make_daemon(tmp_path, monkeypatch, SlowMemu())
        sent = []

        async def send(chat_id, text, **_kwargs):
            sent.append((chat_id, text))
            return SendResult(True, "p1")

        daemon.send = send
        session_key = "agent:main:whatsapp:dm:123@lid"

        first = event("first", "m1")
        await daemon.handle_message(first)
        await asyncio.to_thread(started.wait, 2)

        assert session_key in daemon._active_sessions
        first_task = daemon._session_tasks[session_key]
        assert first_task is not None

        second = event("second", "m2")
        second.raw_message["wal_seq"] = 2
        await daemon.handle_message(second)

        # Second event was queued, not dispatched as a competing task.
        assert daemon._pending_messages.get(session_key) is second
        assert daemon._session_tasks[session_key] is first_task
        assert len(daemon._session_tasks) == 1

        release.set()
        await asyncio.wait_for(first_task, timeout=2)
        drain_task = daemon._session_tasks.get(session_key)
        if drain_task is not None:
            await asyncio.wait_for(drain_task, timeout=2)

        assert len(daemon._memu_client.turn_calls) == 2
        assert sent[-1] == ("123@lid", "pong")
        await daemon.disconnect()

    asyncio.run(run())
