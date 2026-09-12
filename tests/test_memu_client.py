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
            {"role": "user", "content": "hello", "sender_name": "Michael"},
            {"role": "assistant", "content": "hi"},
        ],
        soul_name="Siri",
    )

    assert out == [
        {"content": "hello", "name": "Michael"},
        {"content": "hi", "role": "assistant", "name": "Siri"},
    ]
