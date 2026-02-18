from app.config import Settings


def test_accepts_channel_id_from_standard_env() -> None:
    settings = Settings.model_validate({"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890"})
    assert settings.channel_id == "-1001234567890"


def test_accepts_channel_id_from_alternative_env_name() -> None:
    settings = Settings.model_validate({"BOT_TOKEN": "token", "CHAT_ID": "@my_channel"})
    assert settings.channel_id == "@my_channel"


def test_recovers_channel_id_from_malformed_env_key() -> None:
    settings = Settings.model_validate({"BOT_TOKEN": "token", "-1003531603514": ""})
    assert settings.channel_id == "-1003531603514"
