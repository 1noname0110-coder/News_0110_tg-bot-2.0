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


def test_has_default_period_news_limits() -> None:
    settings = Settings.model_validate({"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890"})
    assert settings.max_period_news_daily == 350
    assert settings.max_period_news_weekly == 800


def test_accepts_custom_period_news_limits() -> None:
    settings = Settings.model_validate(
        {
            "BOT_TOKEN": "token",
            "CHANNEL_ID": "-1001234567890",
            "MAX_PERIOD_NEWS_DAILY": "120",
            "MAX_PERIOD_NEWS_WEEKLY": "240",
        }
    )
    assert settings.max_period_news_daily == 120
    assert settings.max_period_news_weekly == 240
