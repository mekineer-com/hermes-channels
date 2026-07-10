from gateway.memu_client import normalize_history_for_memu


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
