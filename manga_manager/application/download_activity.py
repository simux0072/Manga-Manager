from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.orm import Session

from manga_manager.domain.jobs import JobKind, JobState
from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    StorageState,
    WorkJob,
)


def has_runnable_or_leased_downloads(
    session: Session,
    *,
    now: datetime | None = None,
    series_key: str | None = None,
) -> bool:
    """Return whether foreground download work can use resources now.

    A future retry, disabled provider, provider cooldown, or storage pause must
    release background lanes. A live lease remains foreground work until normal
    lease recovery resolves it.
    """

    current = now or datetime.now(timezone.utc)
    provider_blocked = (
        select(CatalogSourceState.source)
        .where(CatalogSourceState.source == WorkJob.source)
        .where(
            or_(
                CatalogSourceState.manual_enabled.is_(False),
                CatalogSourceState.cooldown_until > current,
            )
        )
        .exists()
    )
    storage_paused = (
        select(StorageState.id)
        .where(StorageState.id == 1, StorageState.paused.is_(True))
        .exists()
    )
    runnable = and_(
        WorkJob.status.in_((JobState.QUEUED.value, JobState.RETRY_WAIT.value)),
        WorkJob.available_at <= current,
        WorkJob.attempts < WorkJob.max_attempts,
        ~provider_blocked,
        ~storage_paused,
    )
    live_lease = and_(
        WorkJob.status == JobState.LEASED.value,
        WorkJob.lease_expires_at.is_not(None),
        WorkJob.lease_expires_at > current,
    )
    active_download = exists().where(
        WorkJob.kind == JobKind.CHAPTER_DOWNLOAD.value,
        or_(live_lease, runnable),
    )
    if series_key is not None:
        active_download = active_download.where(WorkJob.series_key == series_key)
    return bool(session.scalar(select(active_download)))
