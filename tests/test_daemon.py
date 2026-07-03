import asyncio
import json
from dataclasses import dataclass

from gateway.config import DaemonSettings
from gateway.daemon import ChannelsDaemon, MessageEvent, MessageType, ProcessingOutcome, SendResult, SessionSource
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


def test_turn_payload_and_respond_route(tmp_path, monkeypatch):
    async def run():
        memu = FakeMemu({"ok": True, "response": "pong", "response_target": "respond"})
        daemon = make_daemon(tmp_path, monkeypatch, memu)
        sends = []

        async def send(chat_id, text, **_kwargs):
            sends.append((chat_id, text))
            return SendResult(True, "provider-1")

        daemon.send = send
        response = await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        assert response == "pong"
        assert memu.turn_calls[0]["conversation_id"] == "whatsapp:dm:123@lid"
        assert memu.turn_calls[0]["channel_mode"] == "direct"
        assert memu.turn_calls[0]["external_message_id"] == "m1"
        await daemon.on_processing_complete(event(), ProcessingOutcome.SUCCESS)
        await daemon._handle_response_delivery(event(), SendResult(True, "provider-1"), "pong")
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

        await daemon._handle_turn(event(), "agent:main:whatsapp:dm:123@lid")
        session_id = next(iter(daemon._session_entries.values())).session_id
        rows = daemon._db.get_messages(session_id)
        assert [row["role"] for row in rows] == ["user", "assistant"]
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
