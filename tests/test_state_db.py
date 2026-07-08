import sqlite3

from gateway.state_db import ChannelsStateDB


def test_state_db_dedup_response_fallback_and_active_since(tmp_path):
    db = ChannelsStateDB(tmp_path / "state.db")
    db.create_session("s1", "whatsapp", user_id="u1")
    db.append_message(
        "s1",
        "user",
        "hello",
        source_chat_id="chat@lid",
        source_message_id="m1",
        timestamp=100,
    )
    assert db.message_source_key_exists(source_chat_id="chat@lid", source_message_id="m1")
    assert not db.message_source_key_has_response(source_chat_id="chat@lid", source_message_id="m1")

    db.append_message("s1", "assistant", "hi", timestamp=101)
    assert db.message_source_key_has_response(source_chat_id="chat@lid", source_message_id="m1")
    assert db.mark_message_source_key_processed(source_chat_id="chat@lid", source_message_id="m1")
    assert db.message_source_key_is_processed(source_chat_id="chat@lid", source_message_id="m1")

    db.create_session("s2", "whatsapp", user_id="u1", parent_session_id="s1")
    with sqlite3.connect(tmp_path / "state.db") as conn:
        assert conn.execute("SELECT parent_session_id FROM sessions WHERE id = 's2'").fetchone()[0] == "s1"
        conn.execute("INSERT INTO souls(soul_id, active_since) VALUES('soul', 1234.5)")
    assert db.get_soul_active_since("soul") == 1234.5
    db.close()


def test_append_message_duplicate_source_key_returns_existing_id(tmp_path):
    db = ChannelsStateDB(tmp_path / "state.db")
    db.create_session("s1", "whatsapp", user_id="u1")
    first_id = db.append_message(
        "s1", "user", "hello", source_chat_id="chat@lid", source_message_id="m1", timestamp=100
    )
    second_id = db.append_message(
        "s1", "user", "hello again", source_chat_id="chat@lid", source_message_id="m1", timestamp=200
    )
    assert second_id == first_id
    rows = db.get_messages("s1")
    assert len(rows) == 1
    assert rows[0]["content"] == "hello"
    db.close()


def test_whatsapp_arrival_records_history_and_live_without_processing(tmp_path):
    db = ChannelsStateDB(tmp_path / "state.db")

    assert db.record_whatsapp_arrival("chat@lid", "m1", "persist_only", seen_at=10)
    assert not db.record_whatsapp_arrival("chat@lid", "m1", "persist_only", seen_at=20)
    assert db.record_whatsapp_arrival("chat@lid", "m1", "live", seen_at=30)
    assert not db.record_whatsapp_arrival("chat@lid", "m1", "live", seen_at=40)

    row = db.get_whatsapp_arrival("chat@lid", "m1")
    assert row["seen_history_at"] == 10
    assert row["seen_live_at"] == 30
    assert row["first_seen_mode"] == "persist_only"
    assert not db.message_source_key_is_processed(source_chat_id="chat@lid", source_message_id="m1")
    db.close()
