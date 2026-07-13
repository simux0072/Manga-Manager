from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class JobKind(StrEnum):
    SOURCE_PULL = "source_pull"
    SOURCE_REFRESH = "source_refresh"
    CHAPTER_DOWNLOAD = "chapter_download"
    KAVITA_SYNC = "kavita_sync"
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


class SourceRefreshPayload(JobPayload):
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


class ChapterDownloadPayload(JobPayload):
    chapter_release_id: int = Field(gt=0)


class KavitaSyncPayload(JobPayload):
    series_id: int = Field(gt=0)
    folder_path: str = Field(default="", max_length=4096)


class MaintenancePayload(JobPayload):
    action: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9_-]+$")


class NotificationPayload(JobPayload):
    activity_event_id: int = Field(gt=0)


JOB_PAYLOAD_TYPES: dict[JobKind, type[JobPayload]] = {
    JobKind.SOURCE_PULL: SourcePullPayload,
    JobKind.SOURCE_REFRESH: SourceRefreshPayload,
    JobKind.CHAPTER_DOWNLOAD: ChapterDownloadPayload,
    JobKind.KAVITA_SYNC: KavitaSyncPayload,
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
