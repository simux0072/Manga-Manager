from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path

import httpx
from sqlalchemy.orm import Session

from app.adapters import adapter_for_source
from app.adapters.base import (
    ChapterTemporarilyUnavailable,
    SourceAdapter,
    SourceRateLimited,
)
from app.domain import ChapterItem
from manga_manager.application.job_handlers import JobContext, PermanentJobError, RetryableJobError
from manga_manager.domain.jobs import ChapterDownloadPayload, JobKind, KavitaSyncPayload
from manga_manager.infrastructure.artifact_repository import ArtifactRepository
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogSeries,
    CatalogSourceSeries,
    CatalogSourceState,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.storage import ContentAddressedStorage
from manga_manager.worker.runtime import SessionFactory


AdapterFactory = Callable[[str], SourceAdapter | None]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class DownloadSnapshot:
    release_id: int
    chapter_id: int
    series_id: int
    series_storage_key: str
    series_title: str
    series_description: str
    chapter_number: str
    chapter_title: str
    source: str
    source_series_id: str
    release_title: str
    release_url: str
    published_at: datetime | None


class ChapterDownloadHandler:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        storage: ContentAddressedStorage,
        adapter_factory: AdapterFactory = adapter_for_source,
        artifacts: ArtifactRepository | None = None,
        queue: JobQueue | None = None,
        cooldowns: dict[str, timedelta] | None = None,
        circuit_breaker_failures: int = 3,
    ) -> None:
        self.session_factory = session_factory
        self.storage = storage
        self.adapter_factory = adapter_factory
        self.artifacts = artifacts or ArtifactRepository()
        self.queue = queue or JobQueue()
        self.cooldowns = cooldowns or {
            "asura": timedelta(minutes=15),
            "default": timedelta(minutes=5),
        }
        self.circuit_breaker_failures = circuit_breaker_failures

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, ChapterDownloadPayload):
            raise RuntimeError("chapter download handler received the wrong payload")
        with self.session_factory() as session:
            snapshot = load_snapshot(session, payload.chapter_release_id)
        if snapshot is None:
            raise PermanentJobError("release_missing", "chapter release does not exist")

        adapter = self.adapter_factory(snapshot.source)
        if adapter is None:
            raise RetryableJobError(
                "source_unavailable", f"source {snapshot.source} is unavailable"
            )
        item = ChapterItem(
            source=snapshot.source,
            source_series_id=snapshot.source_series_id,
            number=snapshot.chapter_number,
            title=snapshot.release_title,
            url=snapshot.release_url,
            published_at=snapshot.published_at,
        )
        try:
            blob = await self.storage.store_pages(
                adapter.iter_chapter_pages(item),
                comic_info_xml=comic_info(snapshot),
                progress=lambda _count: context.ensure_lease(),
            )
        except SourceRateLimited as exc:
            retry_at = aware_datetime(exc.retry_after)
            retry_at = retry_at or utcnow() + self._cooldown(snapshot.source)
            self._record_source_failure(snapshot.source, str(exc), retry_at)
            delay = max(retry_at - utcnow(), timedelta(seconds=1)) if retry_at else None
            raise RetryableJobError("rate_limited", str(exc), retry_after=delay) from exc
        except ChapterTemporarilyUnavailable as exc:
            retry_at = aware_datetime(exc.retry_after)
            delay = max(retry_at - utcnow(), timedelta(seconds=1)) if retry_at else None
            raise RetryableJobError("temporarily_unavailable", str(exc), retry_after=delay) from exc
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            self._record_source_failure(snapshot.source, str(exc), None)
            raise RetryableJobError("source_network_error", str(exc)) from exc
        except ValueError as exc:
            raise RetryableJobError("invalid_content", str(exc)) from exc
        finally:
            await adapter.aclose()

        context.ensure_lease()
        projection_relative = self.storage.projection_path(
            snapshot.series_storage_key,
            snapshot.chapter_id,
            snapshot.chapter_number,
        ).as_posix()
        with self.session_factory() as session, session.begin():
            result = self.artifacts.activate(
                session,
                chapter_id=snapshot.chapter_id,
                chapter_release_id=snapshot.release_id,
                blob=blob,
                projection_relative_path=projection_relative,
                provenance="download",
                source=snapshot.source,
                replace=True,
            )
            self.queue.enqueue(
                session,
                kind=JobKind.KAVITA_SYNC,
                dedupe_key=f"series:{snapshot.series_id}",
                payload=KavitaSyncPayload(
                    series_id=snapshot.series_id,
                    folder_path=str(self.storage.library_root / Path(projection_relative).parent),
                ),
                priority=100,
                series_key=str(snapshot.series_id),
            )
        self.storage.materialize(blob.relative_path, result.projection_relative_path)

    def _cooldown(self, source: str) -> timedelta:
        return self.cooldowns.get(source, self.cooldowns["default"])

    def _record_source_failure(
        self, source: str, error: str, cooldown_until: datetime | None
    ) -> None:
        with self.session_factory() as session, session.begin():
            state = session.get(CatalogSourceState, source)
            if state is None:
                state = CatalogSourceState(source=source)
                session.add(state)
                session.flush()
            failures = state.consecutive_failures + 1
            if cooldown_until is None and failures >= self.circuit_breaker_failures:
                cooldown_until = utcnow() + self._cooldown(source)
            state.consecutive_failures = failures
            state.last_error = error[:4000]
            state.cooldown_until = cooldown_until
            state.health_status = "cooldown" if cooldown_until else "degraded"
            state.updated_at = utcnow()


def load_snapshot(session: Session, release_id: int) -> DownloadSnapshot | None:
    release = session.get(CatalogChapterRelease, release_id)
    if release is None:
        return None
    chapter = session.get(CatalogChapter, release.chapter_id)
    source_series = session.get(CatalogSourceSeries, release.source_series_id)
    if chapter is None or source_series is None:
        return None
    series = session.get(CatalogSeries, chapter.series_id)
    if series is None:
        return None
    return DownloadSnapshot(
        release_id=release.id,
        chapter_id=chapter.id,
        series_id=series.id,
        series_storage_key=series.storage_key,
        series_title=series.title,
        series_description=series.description,
        chapter_number=chapter.display_number,
        chapter_title=chapter.title,
        source=release.source,
        source_series_id=source_series.source_id,
        release_title=release.title,
        release_url=release.url,
        published_at=release.published_at,
    )


def comic_info(snapshot: DownloadSnapshot) -> str:
    return (
        "<?xml version='1.0' encoding='utf-8'?>"
        "<ComicInfo>"
        f"<Series>{escape(snapshot.series_title)}</Series>"
        f"<Number>{escape(snapshot.chapter_number)}</Number>"
        f"<Title>{escape(snapshot.chapter_title or snapshot.release_title)}</Title>"
        f"<Summary>{escape(snapshot.series_description)}</Summary>"
        f"<Web>{escape(snapshot.release_url)}</Web>"
        f"<Tags>{escape(snapshot.source)}</Tags>"
        "</ComicInfo>"
    )


def aware_datetime(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)
