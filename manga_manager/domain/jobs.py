from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class JobKind(StrEnum):
    SOURCE_PULL = "source_pull"
    SOURCE_REFRESH = "source_refresh"
    CHAPTER_DOWNLOAD = "chapter_download"
    KAVITA_SYNC = "kavita_sync"
    LIBRARY_REPAIR = "library_repair"
    COVER_BACKFILL = "cover_backfill"
    MAINTENANCE = "maintenance"
    NOTIFICATION = "notification"


class JobState(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: int = Field(default=1, ge=1)


class SourcePullPayload(JobPayload):
    source: str = Field(min_length=1, max_length=50, pattern=r"^[a-z0-9_-]+$")
    workflow_key: str = Field(default="", max_length=100)


class SourceRefreshPayload(JobPayload):
    version: int = Field(default=2, ge=1)
    source: str = Field(min_length=1, max_length=50, pattern=r"^[a-z0-9_-]+$")
    source_id: str = Field(min_length=1, max_length=500)
    title: str = Field(min_length=1, max_length=500)
    url: str = Field(min_length=1, max_length=4096)
    aliases: tuple[str, ...] = ()
    description: str = ""
    cover_url: str = ""
    genres: tuple[str, ...] = ()
    popularity: float = 0
    external_ids: dict[str, str] = Field(default_factory=dict)
    metadata: dict = Field(default_factory=dict)
    workflow_key: str = Field(default="", max_length=100)
    observation_version: str = Field(default="", max_length=100)


class ChapterDownloadPayload(JobPayload):
    chapter_release_id: int = Field(gt=0)
    attempted_sources: tuple[str, ...] = ()
    preferred_only: bool = False


class CoverBackfillPayload(JobPayload):
    source_series_id: int = Field(gt=0)


class KavitaSyncPayload(JobPayload):
    series_id: int = Field(default=0, ge=0)
    series_ids: tuple[int, ...] = Field(default=(), max_length=100)
    folder_path: str = Field(default="", max_length=4096)
    reading_status: Literal["", "read", "unread"] = ""

    @model_validator(mode="after")
    def validate_targets(self) -> "KavitaSyncPayload":
        if self.series_id <= 0 and not self.series_ids:
            raise ValueError("Kavita sync requires at least one series")
        if any(series_id <= 0 for series_id in self.series_ids):
            raise ValueError("Kavita batch series IDs must be positive")
        if self.series_ids and (self.folder_path or self.reading_status):
            raise ValueError("Kavita batch sync cannot contain an outbound series mutation")
        return self


class LibraryRepairPayload(JobPayload):
    series_id: int = Field(gt=0)
    reason: str = Field(default="metadata", min_length=1, max_length=100)
    obsolete_storage_keys: tuple[str, ...] = ()


class MaintenancePayload(JobPayload):
    action: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9_-]+$")


class NotificationPayload(JobPayload):
    activity_event_id: int = Field(gt=0)


JOB_PAYLOAD_TYPES: dict[JobKind, type[JobPayload]] = {
    JobKind.SOURCE_PULL: SourcePullPayload,
    JobKind.SOURCE_REFRESH: SourceRefreshPayload,
    JobKind.CHAPTER_DOWNLOAD: ChapterDownloadPayload,
    JobKind.KAVITA_SYNC: KavitaSyncPayload,
    JobKind.LIBRARY_REPAIR: LibraryRepairPayload,
    JobKind.COVER_BACKFILL: CoverBackfillPayload,
    JobKind.MAINTENANCE: MaintenancePayload,
    JobKind.NOTIFICATION: NotificationPayload,
}


def parse_job_payload(kind: JobKind, payload: JobPayload | dict) -> JobPayload:
    payload_type = JOB_PAYLOAD_TYPES[kind]
    if isinstance(payload, payload_type):
        return payload
    raw = payload.model_dump(mode="json") if isinstance(payload, JobPayload) else payload
    return payload_type.model_validate(raw)


ACTIVE_JOB_STATES = frozenset(
    {
        JobState.QUEUED,
        JobState.LEASED,
        JobState.RETRY_WAIT,
    }
)
TERMINAL_JOB_STATES = frozenset(
    {
        JobState.SUCCEEDED,
        JobState.FAILED,
        JobState.CANCELLED,
    }
)


@dataclass(frozen=True, slots=True)
class JobLease:
    id: int
    kind: JobKind
    dedupe_key: str
    payload: JobPayload
    priority: int
    attempt: int
    max_attempts: int
    owner: str
    expires_at: datetime
    source: str = ""
    series_key: str = ""
    pool: str = "maintenance"
