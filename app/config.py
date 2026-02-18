from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    bot_token: str = Field(alias="BOT_TOKEN")
    channel_id: str = Field(alias="-1003531603514")
    admin_user_ids: str = Field(default="", alias="5322247321")

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
    per_topic_limit_daily: int = Field(default=3, alias="PER_TOPIC_LIMIT_DAILY")
    per_topic_limit_weekly: int = Field(default=4, alias="PER_TOPIC_LIMIT_WEEKLY")

    @property
    def admin_ids(self) -> set[int]:
        if not self.admin_user_ids.strip():
            return set()
        return {int(uid.strip()) for uid in self.admin_user_ids.split(",") if uid.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()
