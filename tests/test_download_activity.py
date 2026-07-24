from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from manga_manager.application.download_activity import (
    has_runnable_or_leased_downloads,
)
from manga_manager.domain.jobs import ChapterDownloadPayload, JobKind
from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    JobBase,
    StorageState,
)
from manga_manager.infrastructure.job_queue import JobQueue


NOW = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as db_session:
        yield db_session


def enqueue_download(
    session: Session,
    *,
    source: str = "asura",
    available_at: datetime = NOW,
):
    job, _ = JobQueue().enqueue(
        session,
        kind=JobKind.CHAPTER_DOWNLOAD,
        dedupe_key=f"chapter:{source}:{available_at.isoformat()}",
        payload=ChapterDownloadPayload(chapter_release_id=1),
        source=source,
        series_key="1",
        available_at=available_at,
    )
    return job


def test_ready_download_is_runnable(session: Session) -> None:
    enqueue_download(session)

    assert has_runnable_or_leased_downloads(session, now=NOW) is True


def test_future_download_retry_releases_background_lanes(session: Session) -> None:
    enqueue_download(session, available_at=NOW + timedelta(minutes=10))

    assert has_runnable_or_leased_downloads(session, now=NOW) is False


def test_exhausted_download_releases_background_lanes(session: Session) -> None:
    job = enqueue_download(session)
    job.attempts = job.max_attempts
    session.flush()

    assert has_runnable_or_leased_downloads(session, now=NOW) is False


@pytest.mark.parametrize("manual_enabled", [False, True])
def test_blocked_provider_releases_background_lanes(
    session: Session, manual_enabled: bool
) -> None:
    enqueue_download(session, source="mangafire")
    session.add(
        CatalogSourceState(
            source="mangafire",
            manual_enabled=manual_enabled,
            cooldown_until=(
                NOW + timedelta(minutes=10) if manual_enabled else None
            ),
        )
    )
    session.flush()

    assert has_runnable_or_leased_downloads(session, now=NOW) is False


def test_storage_pause_releases_background_lanes(session: Session) -> None:
    enqueue_download(session)
    session.add(StorageState(id=1, paused=True))
    session.flush()

    assert has_runnable_or_leased_downloads(session, now=NOW) is False


def test_leased_download_remains_foreground_during_cooldown(session: Session) -> None:
    enqueue_download(session, source="asura")
    lease = JobQueue().claim(
        session,
        owner="worker-a",
        lease_for=timedelta(minutes=5),
        now=NOW,
    )
    assert lease is not None
    session.add(
        CatalogSourceState(
            source="asura",
            cooldown_until=NOW + timedelta(minutes=10),
        )
    )
    session.flush()

    assert has_runnable_or_leased_downloads(session, now=NOW) is True


def test_expired_download_lease_releases_background_lanes(session: Session) -> None:
    enqueue_download(
        session,
        source="asura",
        available_at=NOW - timedelta(minutes=10),
    )
    lease = JobQueue().claim(
        session,
        owner="worker-a",
        lease_for=timedelta(minutes=5),
        now=NOW - timedelta(minutes=10),
    )
    assert lease is not None

    assert has_runnable_or_leased_downloads(session, now=NOW) is False
