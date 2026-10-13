import json

import pytest

from gateway.memu_client import MemuClientError, MemuHttpClient, normalize_history_for_memu


def test_read_owner_uses_mcp_owner(monkeypatch) -> None:
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return json.dumps({"user_id": "Fictional Owner"}).encode()

    seen = {}

    def fake_urlopen(request, timeout):
        seen.update(method=request.method, url=request.full_url, timeout=timeout)
        return Response()

    monkeypatch.setattr("gateway.memu_client.urllib.request.urlopen", fake_urlopen)

    assert MemuHttpClient(base_url="http://127.0.0.1:8099", timeout_seconds=2).read_owner() == "Fictional Owner"
    assert seen == {"method": "GET", "url": "http://127.0.0.1:8099/owner", "timeout": 2.0}


@pytest.mark.parametrize("payload", [{}, {"souls": "Siri"}, {"souls": ["Siri", 1]}, {"souls": [""]}])
def test_list_souls_rejects_malformed_response(monkeypatch, payload) -> None:
    client = MemuHttpClient(base_url="http://127.0.0.1:8099")
    monkeypatch.setattr(client, "_request", lambda path: payload)

    with pytest.raises(MemuClientError, match="invalid soul list"):
        client.list_souls()


def test_list_souls_preserves_exact_names(monkeypatch) -> None:
    client = MemuHttpClient(base_url="http://127.0.0.1:8099")
    seen = []
    monkeypatch.setattr(client, "_request", lambda path: seen.append(path) or {"souls": ["Siri", "siri"]})

    assert client.list_souls() == ["Siri", "siri"]
    assert seen == ["/souls"]


def test_normalize_history_for_memu_omits_incoming_human_role() -> None:
    out = normalize_history_for_memu(
        [
            {"role": "user", "content": "hello", "sender_name": "TestHuman"},
            {"role": "assistant", "content": "hi"},
        ],
        soul_name="TestSoul",
    )

    assert out == [
        {"content": "hello", "name": "TestHuman"},
        {"content": "hi"},
    ]


def test_turn_forwards_owner_evidence_without_changing_sender_name(monkeypatch):
    client = MemuHttpClient(base_url="http://memu.invalid")
    seen = []
    monkeypatch.setattr(client, "_post", lambda path, body: seen.append((path, body)) or {})
    for evidence in (None, "TestOwner"):
        client.memu_turn(conversation_id="whatsapp:dm:test", user_id="TestOwner", soul_id="TestSoul",
                         message="hello", user_name="WhatsApp Display", source_owner_id=evidence)
    assert "source_owner_id" not in seen[0][1]
    assert seen[1][1]["source_owner_id"] == "TestOwner"
    assert all(body["user_name"] == "WhatsApp Display" for _, body in seen)


def test_mark_outbound_requires_and_forwards_exact_claim_timestamp(monkeypatch):
    client = MemuHttpClient(base_url="http://memu.invalid")
    seen = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(request, timeout):
        seen.append((request.full_url, json.loads(request.data)))
        return Response()

    monkeypatch.setattr("gateway.memu_client.urllib.request.urlopen", fake_urlopen)
    kwargs = dict(user_id="TestOwner", soul_id="TestSoul", outbound_id="out-1", status="sent")
    with pytest.raises(TypeError, match="claimed_at"):
        client.mark_whatsapp_outbound(**kwargs)
    claimed_at = "2040-01-02T03:04:05.123456+00:00"
    assert client.mark_whatsapp_outbound(**kwargs, claimed_at=claimed_at) == {"ok": True}
    assert seen == [("http://memu.invalid/integration/whatsapp/outbounds/mark",
                     {**kwargs, "claimed_at": claimed_at})]


def test_foreign_soul_history_does_not_inherit_requesting_soul():
    rows = normalize_history_for_memu([
        {"role": "assistant", "content": "a", "sender_id": "soul:TestSoul", "sender_name": "TestSoul"},
        {"role": "assistant", "content": "b", "sender_id": "soul:OtherSoul", "sender_name": "OtherSoul"},
        {"role": "assistant", "content": "unknown"},
    ], soul_name="TestSoul")
    assert rows == [{"content": "a", "name": "TestSoul", "role": "assistant"},
                    {"content": "b", "name": "OtherSoul"}, {"content": "unknown"}]
