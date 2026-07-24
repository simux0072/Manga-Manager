from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    library_root: Path = Path("./storage-v2/library")
    kavita_library_root: Path | None = None

    kavita_url: str = ""
    kavita_api_key: str = ""
    kavita_series_url_template: str = "{base_url}/library/{library_id}/series/{series_id}"
    kavita_chapter_url_template: str = (
        "{base_url}/library/{library_id}/series/{series_id}/chapter/{chapter_id}"
    )

    asura_delay_hours: int = Field(default=6, ge=0)

    request_timeout_seconds: float = Field(default=25.0, ge=5.0)
    max_page_bytes: int = Field(default=25 * 1024 * 1024, ge=1)
    asura_request_interval_seconds: float = Field(default=2.0, ge=0.0)
    mangadex_request_interval_seconds: float = Field(default=0.25, ge=0.0)
    mangadex_at_home_interval_seconds: float = Field(default=1.55, ge=0.0)
    mangafire_request_interval_seconds: float = Field(default=0.0, ge=0.0)
    kingofshojo_request_interval_seconds: float = Field(default=0.0, ge=0.0)
    user_agent: str = "MangaManager/0.1 personal archiver"
    asura_page_concurrency: int = Field(default=1, ge=1)
    mangadex_page_concurrency: int = Field(default=4, ge=1)
    mangafire_page_concurrency: int = Field(default=4, ge=1)
    kingofshojo_page_concurrency: int = Field(default=4, ge=1)
    kingofshojo_timeout_seconds: float = Field(default=45.0, ge=10.0)
    source_frontier_required_hits: int = Field(default=3, ge=1)
    asura_recent_pages: int = Field(default=20, ge=1)
    mangadex_recent_pages: int = Field(default=20, ge=1)
    mangadex_recent_limit: int = Field(default=100, ge=1, le=100)
    mangadex_language: str = Field(default="en", min_length=2, max_length=8)
    kingofshojo_recent_pages: int = Field(default=20, ge=1)
    mangafire_recent_pages: int = Field(default=20, ge=1)
    mangafire_recent_limit: int = Field(default=50, ge=1)
    mangafire_discovery_mode: Literal["new", "hot"] = "new"


settings = Settings()
