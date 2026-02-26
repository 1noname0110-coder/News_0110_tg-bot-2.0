import re
import logging
from functools import lru_cache
from typing import Any

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    bot_token: str = Field(alias="BOT_TOKEN")
    channel_id: str = Field(alias="CHANNEL_ID")
    admin_user_ids: str = Field(alias="ADMIN_USER_IDS")

    database_url: str = Field(default="sqlite+aiosqlite:///./news_bot.db", alias="DATABASE_URL")
    timezone: str = Field(default="Asia/Vladivostok", alias="TIMEZONE")

    daily_publish_hour: int = Field(default=21, alias="DAILY_PUBLISH_HOUR")
    weekly_publish_hour: int = Field(default=21, alias="WEEKLY_PUBLISH_HOUR")

    llm_enabled: bool = Field(default=False, alias="LLM_ENABLED")
    llm_api_key: str | None = Field(default=None, alias="LLM_API_KEY")
    llm_model: str = Field(default="gpt-4o-mini", alias="LLM_MODEL")
    llm_base_url: str | None = Field(default=None, alias="LLM_BASE_URL")

    fetch_timeout_seconds: int = Field(default=20, alias="FETCH_TIMEOUT_SECONDS")

    dedup_similarity_threshold: float = Field(default=0.82, alias="DEDUP_SIMILARITY_THRESHOLD")
    dedup_similarity_threshold_same_source: float | None = Field(
        default=None,
        alias="DEDUP_SIMILARITY_THRESHOLD_SAME_SOURCE",
    )
    dedup_similarity_threshold_cross_source: float | None = Field(
        default=None,
        alias="DEDUP_SIMILARITY_THRESHOLD_CROSS_SOURCE",
    )
    per_topic_limit_daily: int = Field(default=3, alias="PER_TOPIC_LIMIT_DAILY")
    per_topic_limit_weekly: int = Field(default=4, alias="PER_TOPIC_LIMIT_WEEKLY")
    publish_all_important: bool = Field(default=True, alias="PUBLISH_ALL_IMPORTANT")
    max_period_news_daily: int = Field(default=350, alias="MAX_PERIOD_NEWS_DAILY")
    max_period_news_weekly: int = Field(default=800, alias="MAX_PERIOD_NEWS_WEEKLY")
    filter_threshold_profile: str = Field(default="balanced", alias="FILTER_THRESHOLD_PROFILE")

    @model_validator(mode="before")
    @classmethod
    def normalize_channel_id(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        if data.get("CHANNEL_ID"):
            return data

        for alt_key in ("CHAT_ID", "TELEGRAM_CHANNEL_ID"):
            if data.get(alt_key):
                data["CHANNEL_ID"] = str(data[alt_key]).strip()
                return data

        malformed_key = next(
            (
                key
                for key in data.keys()
                if isinstance(key, str) and re.fullmatch(r"-100\d{5,}", key)
            ),
            None,
        )
        if malformed_key:
            data["CHANNEL_ID"] = malformed_key
        return data

    @model_validator(mode="before")
    @classmethod
    def validate_admin_user_ids_format(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        raw_admin_ids = data.get("ADMIN_USER_IDS")
        if raw_admin_ids is None or not str(raw_admin_ids).strip():
            raise ValueError(
                "ADMIN_USER_IDS не задан. Укажите ADMIN_USER_IDS в переменных окружения "
                "(список числовых Telegram user id через запятую)."
            )

        invalid_values = [
            part.strip()
            for part in str(raw_admin_ids).split(",")
            if part.strip() and not part.strip().isdigit()
        ]

        if invalid_values:
            raise ValueError(
                "Некорректный формат ADMIN_USER_IDS: ожидается список числовых Telegram user id через запятую "
                f"(например, '123456789,987654321'). Неверные значения: {', '.join(invalid_values)}"
            )

        return data

    @model_validator(mode="after")
    def ensure_required_runtime_values(self) -> "Settings":
        if not self.channel_id or not self.channel_id.strip():
            raise ValueError(
                "CHANNEL_ID не задан. Укажите CHANNEL_ID, CHAT_ID или TELEGRAM_CHANNEL_ID в переменных окружения."
            )

        if not self.admin_user_ids or not self.admin_user_ids.strip():
            raise ValueError(
                "ADMIN_USER_IDS не задан. Укажите ADMIN_USER_IDS в переменных окружения "
                "(список числовых Telegram user id через запятую)."
            )
        return self

    @property
    def admin_ids(self) -> set[int]:
        if not self.admin_user_ids.strip():
            return set()

        parsed_ids: set[int] = set()
        for uid in self.admin_user_ids.split(","):
            normalized_uid = uid.strip()
            if not normalized_uid:
                continue
            if not normalized_uid.isdigit():
                logger.warning("Skipping invalid admin user id value: %r", normalized_uid)
                continue
            parsed_ids.add(int(normalized_uid))

        return parsed_ids


@lru_cache
def get_settings() -> Settings:
    return Settings()
