from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite:///./manga_manager.db"
    library_root: Path = Path("./storage/library")
    kavita_library_root: Path | None = None
    staging_root: Path = Path("./storage/staging")
    archive_root: Path = Path("./storage/archive")

    kavita_url: str = ""
    kavita_api_key: str = ""
    kavita_sync_want_to_read: bool = False
    kavita_sync_read_progress: bool = False
    kavita_series_url_template: str = "{base_url}/library/{library_id}/series/{series_id}"
    kavita_chapter_url_template: str = (
        "{base_url}/library/{library_id}/series/{series_id}/chapter/{chapter_id}"
    )

    enable_asura: bool = True
    enable_mangafire: bool = True
    enable_kingofshojo: bool = True

    asura_poll_minutes: int = Field(default=30, ge=5)
    mangafire_poll_minutes: int = Field(default=60, ge=5)
    kingofshojo_poll_minutes: int = Field(default=180, ge=15)
    asura_delay_hours: int = Field(default=6, ge=0)

    request_timeout_seconds: float = Field(default=25.0, ge=5.0)
    max_page_bytes: int = Field(default=25 * 1024 * 1024, ge=1)
    max_cover_bytes: int = Field(default=5 * 1024 * 1024, ge=1)
    asura_request_interval_seconds: float = Field(default=2.0, ge=0.0)
    mangafire_request_interval_seconds: float = Field(default=0.0, ge=0.0)
    kingofshojo_request_interval_seconds: float = Field(default=0.0, ge=0.0)
    user_agent: str = "MangaManager/0.1 personal archiver"
    downloads_enabled: bool = True
    discovery_page_size: int = Field(default=50, ge=10)
    first_import_chapters: int = Field(default=5, ge=1)
    source_pull_concurrency: int = Field(default=4, ge=1)
    download_concurrency: int = Field(default=2, ge=1)
    backfill_downloads_enabled: bool = True
    min_pages_per_chapter: int = Field(default=3, ge=1)
    max_download_attempts: int = Field(default=3, ge=1)
    download_retry_base_minutes: int = Field(default=15, ge=1)
    download_stale_minutes: int = Field(default=60, ge=1)
    rate_limit_cooldown_minutes: int = Field(default=30, ge=1)
    asura_rate_limit_cooldown_minutes: int = Field(default=60, ge=1)
    job_status_group_limit: int = Field(default=25, ge=5)
    keep_replaced_files: bool = True
    retention_replaced_days: int = Field(default=0, ge=0)
    retention_replaced_max_per_chapter: int = Field(default=0, ge=0)
    basic_auth_username: str = ""
    basic_auth_password: str = ""
    auth_protect_healthz: bool = False
    notification_webhook_url: str = ""
    notification_rss_enabled: bool = True
    notification_timeout_seconds: float = Field(default=5.0, ge=1.0)
    smtp_host: str = ""
    smtp_port: int = Field(default=587, ge=1)
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from: str = ""
    smtp_to: str = ""
    kingofshojo_timeout_seconds: float = Field(default=45.0, ge=10.0)
    kingofshojo_recent_pages: int = Field(default=3, ge=1)
    mangafire_recent_limit: int = Field(default=50, ge=1)
    mangafire_discovery_mode: Literal["new", "hot"] = "new"


settings = Settings()
