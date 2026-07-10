from __future__ import annotations

import os
import socket
from datetime import timedelta
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class V2Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="V2_", extra="ignore")

    database_url: str = ""
    worker_id: str = Field(default_factory=default_worker_id, min_length=1, max_length=180)
    worker_concurrency: int = Field(default=1, ge=1, le=32)
    global_chapter_concurrency: int = Field(default=4, ge=1, le=32)
    asura_download_concurrency: int = Field(default=1, ge=1, le=2)
    mangafire_download_concurrency: int = Field(default=2, ge=1, le=8)
    kingofshojo_download_concurrency: int = Field(default=2, ge=1, le=8)
    worker_poll_seconds: float = Field(default=1.0, gt=0, le=60)
    worker_lease_seconds: int = Field(default=300, ge=30, le=86_400)
    worker_heartbeat_seconds: int = Field(default=30, ge=5, le=3_600)
    worker_shutdown_grace_seconds: int = Field(default=30, ge=1, le=3_600)
    retry_base_seconds: int = Field(default=30, ge=1, le=3_600)
    retry_cap_seconds: int = Field(default=3_600, ge=1, le=86_400)
    scheduler_check_seconds: int = Field(default=30, ge=5, le=3_600)
    enable_asura: bool = True
    enable_mangafire: bool = True
    enable_kingofshojo: bool = True
    asura_poll_minutes: int = Field(default=30, ge=5)
    mangafire_poll_minutes: int = Field(default=60, ge=5)
    kingofshojo_poll_minutes: int = Field(default=180, ge=15)
    storage_root: Path = Path("./storage-v2")
    max_page_bytes: int = Field(default=25 * 1024 * 1024, ge=1)
    max_chapter_bytes: int = Field(default=750 * 1024 * 1024, ge=1)
    max_pages_per_chapter: int = Field(default=2_000, ge=1)
    min_free_bytes: int = Field(default=5 * 1024 * 1024 * 1024, ge=0)

    def require_database_url(self) -> str:
        if not self.database_url:
            raise ValueError("V2_DATABASE_URL is required")
        if not self.database_url.startswith("postgresql+"):
            raise ValueError("V2_DATABASE_URL must use a postgresql+ SQLAlchemy driver")
        return self.database_url

    @property
    def lease_for(self) -> timedelta:
        return timedelta(seconds=self.worker_lease_seconds)

    @property
    def job_heartbeat_interval(self) -> timedelta:
        if self.worker_heartbeat_seconds >= self.worker_lease_seconds:
            raise ValueError("V2_WORKER_HEARTBEAT_SECONDS must be shorter than the lease")
        return timedelta(seconds=self.worker_heartbeat_seconds)

    def source_intervals(self) -> dict[str, timedelta]:
        intervals: dict[str, timedelta] = {}
        if self.enable_asura:
            intervals["asura"] = timedelta(minutes=self.asura_poll_minutes)
        if self.enable_mangafire:
            intervals["mangafire"] = timedelta(minutes=self.mangafire_poll_minutes)
        if self.enable_kingofshojo:
            intervals["kingofshojo"] = timedelta(minutes=self.kingofshojo_poll_minutes)
        return intervals

    def pool_limits(self) -> dict[str, int]:
        return {
            "source_pull": 1,
            "download:asura": self.asura_download_concurrency,
            "download:mangafire": self.mangafire_download_concurrency,
            "download:kingofshojo": self.kingofshojo_download_concurrency,
            "chapter_global": self.global_chapter_concurrency,
            "kavita": 1,
            "maintenance": 1,
            "notification": 1,
        }
