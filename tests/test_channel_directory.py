import json

from gateway.channel_directory import write_channel_directory


def test_directory_writer_output_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("CHANNELS_HOME", str(tmp_path))
    contact_path = tmp_path / "whatsapp" / "contact_store.json"
    contact_path.parent.mkdir(parents=True)
    contact_path.write_text(
        json.dumps(
            {
                "contacts": {
                    "123@lid": {
                        "preferred_jid": "123@lid",
                        "display": {"chat_name": "Ada"},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    sessions_path = tmp_path / "sessions" / "sessions.json"
    sessions_path.parent.mkdir()
    sessions_path.write_text(
        json.dumps(
            {
                "agent:main:whatsapp:group:g@g.us": {
                    "session_id": "s1",
                    "origin": {
                        "platform": "whatsapp",
                        "chat_id": "g@g.us",
                        "chat_name": "Group",
                        "chat_type": "group",
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    out = write_channel_directory()

    assert set(out) == {"updated_at", "platforms"}
    assert out["platforms"]["whatsapp"] == [
        {"id": "123@lid", "name": "Ada", "type": "dm"},
        {"id": "g@g.us", "name": "Group", "type": "group"},
    ]
    assert json.loads((tmp_path / "channel_directory.json").read_text()) == out
