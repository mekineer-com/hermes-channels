import json
import logging

from gateway.config import _coerce_bool, load_config


def test_env_override_beats_config_json_beats_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("CHANNELS_HOME", str(tmp_path))
    (tmp_path / "config.json").write_text(
        json.dumps({"soul_id": "from-config", "bridge_port": 4000}), encoding="utf-8"
    )
    monkeypatch.setenv("CHANNELS_SOUL_ID", "from-env")

    settings = load_config()

    assert settings.soul_id == "from-env"  # env beats config.json
    assert settings.bridge_port == 4000  # config.json beats default
    assert settings.user_id == "marcos"  # untouched default


def test_coerce_bool_strings():
    assert _coerce_bool("true") is True
    assert _coerce_bool("YES") is True
    assert _coerce_bool("1") is True
    assert _coerce_bool("0") is False
    assert _coerce_bool("off") is False
    assert _coerce_bool("garbage", True) is True
    assert _coerce_bool(None, False) is False


def test_garbage_numeric_value_falls_back_to_default_with_warning(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("CHANNELS_HOME", str(tmp_path))
    (tmp_path / "config.json").write_text(json.dumps({"bridge_port": "not-a-number"}), encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        settings = load_config()

    assert settings.bridge_port == 3000  # default, not raised
    assert any("bridge_port" in record.message for record in caplog.records)
