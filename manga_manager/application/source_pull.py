from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import httpx
from sqlalchemy import select

from app.adapters import adapter_for_source
from app.adapters.base import FrontierSentinel, SourceAdapter, SourceRateLimited
from app.domain import SeriesItem
from manga_manager.application.job_handlers import (
    JobContext,
    PermanentJobError,
    RetryableJobError,
    exception_message,
)
from manga_manager.application.cover_evidence import CoverEvidenceService
from manga_manager.application.download_plans import DownloadPlanCoordinator
from manga_manager.domain.jobs import JobKind, SourcePullPayload, SourceRefreshPayload
from manga_manager.infrastructure.catalog_repository import CatalogRepository
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.db_models import CatalogSourceState, ProviderEndpointState
from manga_manager.worker.runtime import SessionFactory


AdapterFactory = Callable[[str], SourceAdapter | None]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SourcePullHandler:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        adapter_factory: AdapterFactory = adapter_for_source,
        catalog: CatalogRepository | None = None,
        queue: JobQueue | None = None,
        frontier_limit: int = 5,
    ) -> None:
        self.session_factory = session_factory
        self.adapter_factory = adapter_factory
        self.catalog = catalog or CatalogRepository()
        self.queue = queue or JobQueue()
        self.frontier_limit = frontier_limit

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, SourcePullPayload):
            raise RuntimeError("source pull handler received the wrong payload")
        source = payload.source
        frontier = self._load_frontier(source)
        adapter = self.adapter_factory(source)
        if adapter is None:
            raise RetryableJobError("source_unavailable", f"source {source} is unavailable")

        try:
            listed_items = await adapter.list_recent_frontier(frontier)
            observed_frontier = build_listing_frontier(listed_items, self.frontier_limit)
            candidates = changed_before_stable_frontier(listed_items, frontier, required_hits=3)
        except SourceRateLimited as exc:
            retry_at = aware_datetime(exc.retry_after)
            self._record_failure(
                source,
                exception_message(exc),
                retry_at,
                traffic_class=exc.traffic_class or "origin",
            )
            delay = max(retry_at - utcnow(), timedelta(seconds=1)) if retry_at else None
            raise RetryableJobError(
                "rate_limited", exception_message(exc), retry_after=delay
            ) from exc
        except httpx.TransportError as exc:
            self._record_failure(source, exception_message(exc), None)
            raise RetryableJobError("source_network_error", exception_message(exc)) from exc
        except httpx.HTTPStatusError as exc:
            self._record_failure(source, exception_message(exc), None)
            status = exc.response.status_code
            if 400 <= status < 500 and status not in {408, 429}:
                raise PermanentJobError("source_http_error", exception_message(exc)) from exc
            raise RetryableJobError("source_http_error", exception_message(exc)) from exc
        except Exception as exc:
            self._record_failure(source, exception_message(exc), None)
            raise PermanentJobError("source_processing_error", exception_message(exc)) from exc
        finally:
            await adapter.aclose()

        with self.session_factory() as session, session.begin():
            created = 0
            for item in candidates:
                _job, was_created = self.queue.enqueue(
                    session,
                    kind=JobKind.SOURCE_REFRESH,
                    dedupe_key=f"refresh:{source}:{item.source_id}",
                    payload=refresh_payload(item),
                    source=source,
                    priority=55,
                    max_attempts=4,
                )
                created += int(was_created)
            self.catalog.record_poll_success(
                session,
                source=source,
                frontier=observed_frontier,
                partial_failures=0,
                metrics={
                    "listed": len(listed_items),
                    "candidates": len(candidates),
                    "refresh_jobs_created": created,
                    "stable_sentinels_required": min(3, len(frontier)),
                },
            )

    def _load_frontier(self, source: str) -> list[FrontierSentinel]:
        with self.session_factory() as session:
            rows = self.catalog.source_frontier(session, source)
        return [
            FrontierSentinel(
                source_id=str(row.get("source_id") or ""),
                latest_chapter=str(row.get("latest_chapter") or ""),
            )
            for row in rows
            if row.get("source_id")
        ]

    def _record_failure(
        self,
        source: str,
        error: str,
        cooldown_until: datetime | None,
        *,
        traffic_class: str = "origin",
    ) -> None:
        with self.session_factory() as session, session.begin():
            self.catalog.record_poll_failure(
                session, source=source, error=error, cooldown_until=cooldown_until
            )
            state = session.get(CatalogSourceState, source)
            effective_cooldown = state.cooldown_until if state is not None else cooldown_until
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
            endpoint.cooldown_until = effective_cooldown
            endpoint.updated_at = utcnow()
            if effective_cooldown is not None:
                DownloadPlanCoordinator(self.queue).bypass_cooling_source(
                    session, source=source, retry_after=effective_cooldown
                )


class SourceRefreshHandler:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        adapter_factory: AdapterFactory = adapter_for_source,
        catalog: CatalogRepository | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.adapter_factory = adapter_factory
        self.catalog = catalog or CatalogRepository()

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, SourceRefreshPayload):
            raise RuntimeError("source refresh handler received the wrong payload")
        adapter = self.adapter_factory(payload.source)
        if adapter is None:
            raise RetryableJobError("source_unavailable", f"source {payload.source} unavailable")
        item = item_from_refresh(payload)
        try:
            detail = getattr(adapter, "get_series_detail", None)
            enriched = await detail(item) if detail is not None else item
            chapters = await adapter.get_chapters(enriched)
        except SourceRateLimited as exc:
            retry_at = aware_datetime(exc.retry_after)
            delay = max(retry_at - utcnow(), timedelta(seconds=1)) if retry_at else None
            raise RetryableJobError(
                "rate_limited", exception_message(exc), retry_after=delay
            ) from exc
        except httpx.TransportError as exc:
            raise RetryableJobError("source_network_error", exception_message(exc)) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {408, 429, 500, 502, 503, 504}:
                raise RetryableJobError("source_http_error", exception_message(exc)) from exc
            self._quarantine(payload, exception_message(exc))
            raise PermanentJobError("source_item_invalid", exception_message(exc)) from exc
        except Exception as exc:
            self._quarantine(payload, exception_message(exc))
            raise PermanentJobError("source_item_invalid", exception_message(exc)) from exc
        finally:
            await adapter.aclose()
        context.ensure_lease()
        with self.session_factory() as session, session.begin():
            source_series = self.catalog.ingest(session, enriched, chapters)
            source_series_id = source_series.id
        await CoverEvidenceService(self.session_factory).refresh_for_source_series(source_series_id)

    def _quarantine(self, payload: SourceRefreshPayload, reason: str) -> None:
        from manga_manager.infrastructure.db_models import CatalogObservation

        with self.session_factory() as session, session.begin():
            session.add(
                CatalogObservation(
                    source=payload.source,
                    observation_type="source_refresh_failure",
                    source_key=payload.source_id,
                    state="quarantined",
                    reason=reason[:4000],
                    payload_json=payload.model_dump(mode="json"),
                )
            )


def refresh_payload(item: SeriesItem) -> SourceRefreshPayload:
    return SourceRefreshPayload(
        source=item.source,
        source_id=item.source_id,
        title=item.title,
        url=item.url,
        aliases=item.aliases,
        description=item.description,
        cover_url=item.cover_url,
        genres=item.genres,
        popularity=item.popularity,
        external_ids=item.external_ids,
        metadata=item.metadata,
    )


def item_from_refresh(payload: SourceRefreshPayload) -> SeriesItem:
    return SeriesItem(
        source=payload.source,
        source_id=payload.source_id,
        title=payload.title,
        url=payload.url,
        aliases=payload.aliases,
        description=payload.description,
        cover_url=payload.cover_url,
        genres=payload.genres,
        popularity=payload.popularity,
        external_ids=payload.external_ids,
        metadata=payload.metadata,
    )


def build_listing_frontier(rows: list[SeriesItem], limit: int) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for item in rows:
        latest = latest_listing_chapter(item)
        if latest:
            result.append({"source_id": item.source_id, "latest_chapter": latest})
        if len(result) >= limit:
            break
    return result


def changed_before_stable_frontier(
    rows: list[SeriesItem], sentinels: list[FrontierSentinel], *, required_hits: int
) -> list[SeriesItem]:
    if not sentinels:
        return rows
    known = {row.source_id: row.latest_chapter for row in sentinels}
    needed = min(required_hits, len(known))
    stable = 0
    changed: list[SeriesItem] = []
    for item in rows:
        latest = latest_listing_chapter(item)
        previous = known.get(item.source_id)
        if latest and previous and chapter_key(latest) <= chapter_key(previous):
            stable += 1
            if stable >= needed:
                break
            continue
        changed.append(item)
    return changed


def latest_listing_chapter(item: SeriesItem) -> str:
    rows = item.metadata.get("recent_chapters")
    if not isinstance(rows, list):
        return ""
    values = [
        str(row.get("number") or "") for row in rows if isinstance(row, dict) and row.get("number")
    ]
    return max(values, key=chapter_key, default="")


def chapter_key(value: str) -> tuple[int, Decimal, str]:
    try:
        return (1, Decimal(value), value)
    except InvalidOperation:
        return (0, Decimal(0), value)


def aware_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
