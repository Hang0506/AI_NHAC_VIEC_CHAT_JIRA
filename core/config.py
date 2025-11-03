from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import os
import logging


class Settings(BaseSettings):
    app_name: str = Field(default="AI_NHAC_VIEC_CHAT_JIRA")
    app_env: str = Field(default="development")
    log_level: str = Field(default="INFO")
    timezone: str = Field(default="Asia/Ho_Chi_Minh")

    database_url: str = Field(alias="DATABASE_URL", default="sqlite+pysqlite:///./app.db")

    jira_base_url: str = Field(alias="JIRA_BASE_URL", default="")
    jira_username: str = Field(alias="JIRA_USERNAME", default="")
    jira_api_token: str = Field(alias="JIRA_API_TOKEN", default="")

    chat_base_url: str = Field(alias="CHAT_BASE_URL", default="")
    chat_api_key: str = Field(alias="CHAT_API_KEY", default="")

    scheduler_enabled: bool = Field(alias="SCHEDULER_ENABLED", default=True)
    schedule_cron: str = Field(alias="SCHEDULE_CRON", default="*/5 * * * *")  # Legacy, backward compat
    schedule_cron_reminders: str = Field(alias="SCHEDULE_CRON_REMINDERS", default="*/5 * * * *")
    schedule_cron_legacy: str = Field(alias="SCHEDULE_CRON_LEGACY", default="*/10 * * * *")

    model_config = SettingsConfigDict(
        env_file=os.getenv("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow",
    )


settings = Settings()  # type: ignore

# Lightweight diagnostics to verify env loading (avoids circular import with core.logging)
_logger = logging.getLogger("core.config")
_env_file_used = os.getenv("ENV_FILE", ".env")
try:
    _logger.info(
        "Settings loaded | env_file=%s | app_env=%s | db=%s | scheduler_enabled=%s | cron_reminders=%s | cron_legacy=%s",
        _env_file_used,
        settings.app_env,
        settings.database_url,
        settings.scheduler_enabled,
        settings.schedule_cron_reminders,
        settings.schedule_cron_legacy,
    )
except Exception:
    # Fallback print if logging not configured yet
    print(
        f"[Settings] env_file={_env_file_used} app_env={settings.app_env} "
        f"db={settings.database_url} scheduler_enabled={settings.scheduler_enabled} "
        f"cron_reminders={settings.schedule_cron_reminders} cron_legacy={settings.schedule_cron_legacy}"
    )
