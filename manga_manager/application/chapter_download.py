from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.adapters import adapter_for_source
from app.adapters.base import (
    ChapterTemporarilyUnavailable,
    SourceAdapter,
    SourceRateLimited,
)
from app.adapters.http import traffic_class_for_url
from app.domain import ChapterItem
from manga_manager.application.download_plans import DownloadPlanCoordinator
from manga_manager.application.job_handlers import (
    DeferredJobError,
    JobContext,
    PermanentJobError,
    ReroutedJobError,
    RetryableJobError,
    exception_message,
)
from manga_manager.domain.jobs import ChapterDownloadPayload, JobKind, LibraryRepairPayload
from manga_manager.infrastructure.artifact_repository import ArtifactRepository
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogSeries,
    CatalogSourceSeries,
    CatalogSourceState,
    ChapterReleaseAttempt,
    ProviderPolicy,
    ProviderEndpointState,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.storage import ContentAddressedStorage, StorageCapacityError
from manga_manager.infrastructure.storage_capacity import StorageCapacityCoordinator
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
        self.capacity = StorageCapacityCoordinator(storage.root, storage.min_free_bytes)

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, ChapterDownloadPayload):
            raise RuntimeError("chapter download handler received the wrong payload")
        with self.session_factory() as session:
            snapshot = load_snapshot(session, payload.chapter_release_id)
        if snapshot is None:
            raise PermanentJobError("release_missing", "chapter release does not exist")

        with self.session_factory() as session, session.begin():
            reserved = self.capacity.reserve(
                session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                requested_bytes=self.storage.max_chapter_bytes,
                lease_expires_at=context.lease.expires_at,
            )
        if not reserved:
            raise DeferredJobError(
                "storage_capacity",
                "download paused until the storage free-space reserve is restored",
                retry_after=timedelta(minutes=5),
            )

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
        with self.session_factory() as progress_session, progress_session.begin():
            self.queue.progress(
                progress_session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message=f"connecting to {snapshot.source}",
                details={"phase": "download", "processed": 0, "total": 0, "unit": "pages"},
            )

        def report_page(processed: int, total: int, byte_count: int) -> None:
            context.ensure_lease()
            with self.session_factory() as progress_session, progress_session.begin():
                if not self.queue.progress(
                    progress_session,
                    job_id=context.lease.id,
                    owner=context.lease.owner,
                    message=f"downloaded {processed}/{total} pages",
                    details={
                        "phase": "download",
                        "processed": processed,
                        "total": total,
                        "unit": "pages",
                        "bytes": byte_count,
                    },
                ):
                    context.lease_lost.set()
                    context.ensure_lease()

        try:
            try:
                pages = adapter.iter_chapter_pages(item, progress=report_page)
            except TypeError as exc:
                if "progress" not in str(exc):
                    raise
                pages = adapter.iter_chapter_pages(item)
            blob = await self.storage.store_pages(
                pages,
                comic_info_xml=comic_info(snapshot),
                progress=lambda _count: context.ensure_lease(),
            )
        except SourceRateLimited as exc:
            retry_at = aware_datetime(exc.retry_after)
            retry_at = retry_at or utcnow() + self._cooldown(snapshot.source)
            self._record_source_failure(
                snapshot.source,
                exception_message(exc),
                retry_at,
                traffic_class=getattr(exc, "traffic_class", "origin"),
            )
            if self._reroute(
                context,
                snapshot,
                code="rate_limited",
                message=exception_message(exc),
                retry_at=retry_at,
            ):
                raise ReroutedJobError(
                    f"chapter rerouted after {snapshot.source} rate limit"
                ) from exc
            delay = max(retry_at - utcnow(), timedelta(seconds=1)) if retry_at else None
            raise RetryableJobError(
                "rate_limited", exception_message(exc), retry_after=delay
            ) from exc
        except ChapterTemporarilyUnavailable as exc:
            retry_at = aware_datetime(exc.retry_after)
            retry_at = retry_at or utcnow() + timedelta(hours=6)
            if self._reroute(
                context,
                snapshot,
                code="temporarily_unavailable",
                message=exception_message(exc),
                retry_at=retry_at,
            ):
                raise ReroutedJobError("chapter rerouted after temporary failure") from exc
            delay = max(retry_at - utcnow(), timedelta(seconds=1)) if retry_at else None
            raise RetryableJobError(
                "temporarily_unavailable", exception_message(exc), retry_after=delay
            ) from exc
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            code = f"http_{status}"
            retry_at = utcnow() + (
                timedelta(hours=24) if status == 404 else self._cooldown(snapshot.source)
            )
            if status in {403, 429, 503}:
                self._record_source_failure(
                    snapshot.source,
                    exception_message(exc),
                    retry_at,
                    traffic_class=traffic_class_for_url(
                        adapter.base_url, str(exc.response.request.url)
                    ),
                )
            if status in {403, 404, 429, 503} and self._reroute(
                context,
                snapshot,
                code=code,
                message=exception_message(exc),
                retry_at=retry_at,
            ):
                raise ReroutedJobError(f"chapter rerouted after HTTP {status}") from exc
            raise RetryableJobError(
                code, exception_message(exc), retry_after=retry_at - utcnow()
            ) from exc
        except httpx.TransportError as exc:
            retry_at = utcnow() + self._cooldown(snapshot.source)
            self._record_source_failure(
                snapshot.source, exception_message(exc), None, traffic_class="cdn"
            )
            if context.lease.attempt >= 2 and self._reroute(
                context,
                snapshot,
                code="source_network_error",
                message=exception_message(exc),
                retry_at=retry_at,
            ):
                raise ReroutedJobError("chapter rerouted after repeated network errors") from exc
            delay = retry_at - utcnow() if context.lease.attempt >= 2 else None
            raise RetryableJobError(
                "source_network_error", exception_message(exc), retry_after=delay
            ) from exc
        except StorageCapacityError as exc:
            raise DeferredJobError(
                "storage_capacity",
                str(exc),
                retry_after=timedelta(minutes=5),
            ) from exc
        except ValueError as exc:
            retry_at = utcnow() + timedelta(hours=24)
            if self._reroute(
                context,
                snapshot,
                code="invalid_content",
                message=str(exc),
                retry_at=retry_at,
            ):
                raise ReroutedJobError("chapter rerouted after invalid content") from exc
            raise RetryableJobError(
                "invalid_content", str(exc), retry_after=retry_at - utcnow()
            ) from exc
        finally:
            await adapter.aclose()

        context.ensure_lease()
        projection_relative = self.storage.projection_path(
            snapshot.series_storage_key,
            snapshot.chapter_id,
            snapshot.chapter_number,
        ).as_posix()
        with self.session_factory() as session, session.begin():
            is_upgrade = context.lease.dedupe_key.startswith("upgrade:")
            is_fallback = bool(
                session.scalar(
                    select(ChapterReleaseAttempt.id).where(
                        ChapterReleaseAttempt.chapter_release_id == snapshot.release_id,
                        ChapterReleaseAttempt.job_id == context.lease.id,
                        ChapterReleaseAttempt.outcome == "fallback_queued",
                    )
                )
            )
            provenance = "upgrade" if is_upgrade else "fallback" if is_fallback else "download"
            result = self.artifacts.activate(
                session,
                chapter_id=snapshot.chapter_id,
                chapter_release_id=snapshot.release_id,
                blob=blob,
                projection_relative_path=projection_relative,
                provenance=provenance,
                source=snapshot.source,
                replace=True,
            )
            # Publish the projection before making repair work claimable. Otherwise a fast
            # maintenance worker can normalize the CBZ and the downloader can overwrite that
            # projection with the stale pre-repair blob afterward.
            self.storage.materialize(blob.relative_path, result.projection_relative_path)
            self.queue.enqueue(
                session,
                kind=JobKind.LIBRARY_REPAIR,
                dedupe_key=f"artifact:{result.artifact_id}",
                payload=LibraryRepairPayload(series_id=snapshot.series_id, reason="download"),
                priority=100,
                series_key=str(snapshot.series_id),
            )
            session.add(
                ChapterReleaseAttempt(
                    chapter_id=snapshot.chapter_id,
                    chapter_release_id=snapshot.release_id,
                    job_id=context.lease.id,
                    source=snapshot.source,
                    outcome="upgraded"
                    if is_upgrade
                    else "fallback_succeeded"
                    if is_fallback
                    else "succeeded",
                    details_json={"artifact_id": result.artifact_id},
                )
            )

    def _reroute(
        self,
        context: JobContext,
        snapshot: DownloadSnapshot,
        *,
        code: str,
        message: str,
        retry_at: datetime,
    ) -> bool:
        with self.session_factory() as session, session.begin():
            job = session.get(WorkJob, context.lease.id)
            release = session.get(CatalogChapterRelease, snapshot.release_id)
            if job is None or release is None or job.status != "leased":
                return False
            coordinator = DownloadPlanCoordinator(self.queue)
            payload = ChapterDownloadPayload.model_validate(job.payload)
            attempted = set(payload.attempted_sources) | {snapshot.source}
            if payload.preferred_only:
                release.downloadable_after = retry_at
                return False
            alternative = coordinator.fallback_release(
                session,
                snapshot.chapter_id,
                snapshot.release_id,
                excluded_sources=attempted,
            )
            if alternative is None:
                release.downloadable_after = retry_at
                job.payload = payload.model_copy(
                    update={"attempted_sources": tuple(sorted(attempted))}
                ).model_dump(mode="json")
                session.add(
                    ChapterReleaseAttempt(
                        chapter_id=snapshot.chapter_id,
                        chapter_release_id=snapshot.release_id,
                        job_id=job.id,
                        source=snapshot.source,
                        outcome="failed",
                        error_code=code,
                        error_message=message[:4000],
                        retry_after=retry_at,
                        details_json={"fallback_available": False},
                    )
                )
                return False
            coordinator.reroute(
                session,
                job=job,
                failed_release=release,
                alternative=alternative,
                error_code=code,
                error_message=message,
                retry_after=retry_at,
            )
            return True

    def _cooldown(self, source: str) -> timedelta:
        return self.cooldowns.get(source, self.cooldowns["default"])

    def _record_source_failure(
        self,
        source: str,
        error: str,
        cooldown_until: datetime | None,
        *,
        traffic_class: str = "origin",
    ) -> None:
        with self.session_factory() as session, session.begin():
            state = session.get(CatalogSourceState, source)
            if state is None:
                state = CatalogSourceState(source=source)
                session.add(state)
                session.flush()
            failures = (state.consecutive_failures or 0) + 1
            if cooldown_until is None and failures >= self.circuit_breaker_failures:
                cooldown_until = utcnow() + self._cooldown(source)
            state.consecutive_failures = failures
            state.last_error = error[:4000]
            state.cooldown_until = cooldown_until
            state.health_status = "cooldown" if cooldown_until else "degraded"
            state.updated_at = utcnow()
            policy = session.get(ProviderPolicy, source)
            if policy is not None and cooldown_until is not None:
                metadata = dict(policy.metadata_json or {})
                metadata.update(
                    {
                        "recovery_probe_step": 0,
                        "next_recovery_probe": (utcnow() + timedelta(minutes=1)).isoformat(),
                        "recovery_probe_successes": 0,
                    }
                )
                policy.metadata_json = metadata
                policy.clean_since = None
            endpoint = session.scalar(
                select(ProviderEndpointState).where(
                    ProviderEndpointState.source == source,
                    ProviderEndpointState.traffic_class == traffic_class,
                )
            )
            if endpoint is None:
                endpoint = ProviderEndpointState(source=source, traffic_class=traffic_class)
                session.add(endpoint)
            endpoint.consecutive_failures = (endpoint.consecutive_failures or 0) + 1
            endpoint.last_error = error[:4000]
            endpoint.cooldown_until = cooldown_until
            endpoint.updated_at = utcnow()
            if cooldown_until is not None:
                DownloadPlanCoordinator(self.queue).bypass_cooling_source(
                    session, source=source, retry_after=cooldown_until
                )


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
    published = snapshot.published_at
    date_fields = (
        f"<Year>{published.year}</Year><Month>{published.month}</Month><Day>{published.day}</Day>"
        if published
        else ""
    )
    return (
        "<?xml version='1.0' encoding='utf-8'?>"
        "<ComicInfo>"
        f"<Series>{escape(snapshot.series_title)}</Series>"
        f"<Number>{escape(snapshot.chapter_number)}</Number>"
        f"<Title>{escape(snapshot.chapter_title or snapshot.release_title)}</Title>"
        f"<Summary>{escape(snapshot.series_description)}</Summary>"
        f"{date_fields}"
        f"<Web>{escape(snapshot.release_url)}</Web>"
        f"<Tags>{escape(snapshot.source)}</Tags>"
        "<LanguageISO>en</LanguageISO>"
        "<Manga>YesAndRightToLeft</Manga>"
        "</ComicInfo>"
    )


def aware_datetime(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)
