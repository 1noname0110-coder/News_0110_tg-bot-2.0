import pytest

from app.config import Settings


def test_accepts_channel_id_from_standard_env() -> None:
    settings = Settings.model_validate(
        {"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890", "ADMIN_USER_IDS": "12345"}
    )
    assert settings.channel_id == "-1001234567890"


def test_accepts_channel_id_from_alternative_env_name() -> None:
    settings = Settings.model_validate(
        {"BOT_TOKEN": "token", "CHAT_ID": "@my_channel", "ADMIN_USER_IDS": "12345"}
    )
    assert settings.channel_id == "@my_channel"


def test_recovers_channel_id_from_malformed_env_key() -> None:
    settings = Settings.model_validate(
        {"BOT_TOKEN": "token", "-1003531603514": "", "ADMIN_USER_IDS": "12345"}
    )
    assert settings.channel_id == "-1003531603514"


def test_requires_channel_id_when_missing() -> None:
    with pytest.raises(ValueError, match="CHANNEL_ID"):
        Settings.model_validate({"BOT_TOKEN": "token", "ADMIN_USER_IDS": "12345"})


def test_requires_channel_id_when_empty() -> None:
    with pytest.raises(ValueError, match="CHANNEL_ID"):
        Settings.model_validate({"BOT_TOKEN": "token", "CHANNEL_ID": "   ", "ADMIN_USER_IDS": "12345"})


def test_requires_admin_user_ids_when_missing() -> None:
    with pytest.raises(ValueError, match="ADMIN_USER_IDS"):
        Settings.model_validate({"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890"})


def test_requires_admin_user_ids_when_empty() -> None:
    with pytest.raises(ValueError, match="ADMIN_USER_IDS"):
        Settings.model_validate(
            {"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890", "ADMIN_USER_IDS": "   "}
        )


def test_has_default_period_news_limits() -> None:
    settings = Settings.model_validate(
        {"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890", "ADMIN_USER_IDS": "12345"}
    )
    assert settings.max_period_news_daily == 350
    assert settings.max_period_news_weekly == 800


def test_accepts_custom_period_news_limits() -> None:
    settings = Settings.model_validate(
        {
            "BOT_TOKEN": "token",
            "CHANNEL_ID": "-1001234567890",
            "ADMIN_USER_IDS": "12345",
            "MAX_PERIOD_NEWS_DAILY": "120",
            "MAX_PERIOD_NEWS_WEEKLY": "240",
        }
    )
    assert settings.max_period_news_daily == 120
    assert settings.max_period_news_weekly == 240


def test_parses_admin_ids_with_empty_values() -> None:
    settings = Settings.model_validate(
        {
            "BOT_TOKEN": "token",
            "CHANNEL_ID": "-1001234567890",
            "ADMIN_USER_IDS": "12345, ,67890,,",
        }
    )
    assert settings.admin_ids == {12345, 67890}


def test_rejects_non_numeric_admin_ids() -> None:
    with pytest.raises(ValueError, match="Некорректный формат ADMIN_USER_IDS"):
        Settings.model_validate(
            {
                "BOT_TOKEN": "token",
                "CHANNEL_ID": "-1001234567890",
                "ADMIN_USER_IDS": "12345,abc,67890",
            }
        )


def test_filter_profile_defaults_to_balanced() -> None:
    settings = Settings.model_validate(
        {"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890", "ADMIN_USER_IDS": "12345"}
    )
    assert settings.filter_threshold_profile == "balanced"


def test_accepts_custom_filter_profile() -> None:
    settings = Settings.model_validate(
        {
            "BOT_TOKEN": "token",
            "CHANNEL_ID": "-1001234567890",
            "ADMIN_USER_IDS": "12345",
            "FILTER_THRESHOLD_PROFILE": "strict",
        }
    )
    assert settings.filter_threshold_profile == "strict"
