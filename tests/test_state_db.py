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
