from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import httpx

from app.adapters import adapter_for_source
from app.adapters.asura import asura_series_url, split_asura_source_id
from app.adapters.base import FrontierSentinel, SourceAdapter, SourceRateLimited
from app.domain import SeriesItem
from manga_manager.application.job_handlers import (
    DeferredJobError,
    JobContext,
    PermanentJobError,
    RetryableJobError,
    exception_message,
)
from manga_manager.application.cover_evidence import CoverEvidenceService
from manga_manager.application.provider_health import (
    is_cloudflare_origin_error,
    is_transient_provider_status,
    provider_cooldown_until,
    record_provider_failure,
)
from manga_manager.domain.jobs import JobKind, SourcePullPayload, SourceRefreshPayload
from manga_manager.infrastructure.catalog_repository import CatalogRepository
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.db_models import CatalogSourceState, WorkJob
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
        with self.session_factory() as progress_session, progress_session.begin():
            self.queue.progress(
                progress_session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message=f"reading the latest {source} listing",
                details={"phase": "listing", "processed": 0, "total": 2, "unit": "phases"},
            )

        try:
            listed_items = await adapter.list_recent_frontier(frontier)
            observed_frontier = build_listing_frontier(listed_items, self.frontier_limit)
            candidates = changed_before_stable_frontier(listed_items, frontier, required_hits=3)
        except SourceRateLimited as exc:
            retry_at = aware_datetime(exc.retry_after) or provider_cooldown_until(
                self.session_factory, source
            )
            effective_cooldown = self._record_failure(
                source,
                exception_message(exc),
                retry_at,
                traffic_class=exc.traffic_class or "origin",
            )
            retry_at = effective_cooldown or retry_at
            delay = max(retry_at - utcnow(), timedelta(seconds=1))
            raise DeferredJobError(
                "rate_limited", exception_message(exc), retry_after=delay
            ) from exc
        except httpx.TransportError as exc:
            effective_cooldown = self._record_failure(source, exception_message(exc), None)
            if effective_cooldown is not None:
                raise DeferredJobError(
                    "source_network_error",
                    exception_message(exc),
                    retry_after=max(
                        effective_cooldown - utcnow(), timedelta(seconds=1)
                    ),
                ) from exc
            raise RetryableJobError("source_network_error", exception_message(exc)) from exc
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            cooldown = (
                provider_cooldown_until(self.session_factory, source)
                if is_cloudflare_origin_error(status)
                else None
            )
            effective_cooldown = self._record_failure(
                source, exception_message(exc), cooldown
            )
            if 400 <= status < 500 and status not in {408, 429}:
                raise PermanentJobError("source_http_error", exception_message(exc)) from exc
            if is_cloudflare_origin_error(status):
                retry_at = effective_cooldown or cooldown
                delay = max(retry_at - utcnow(), timedelta(seconds=1))
                raise DeferredJobError(
                    "provider_origin_unavailable",
                    exception_message(exc),
                    retry_after=delay,
                ) from exc
            if effective_cooldown is not None:
                raise DeferredJobError(
                    "source_http_error",
                    exception_message(exc),
                    retry_after=max(
                        effective_cooldown - utcnow(), timedelta(seconds=1)
                    ),
                ) from exc
            raise RetryableJobError("source_http_error", exception_message(exc)) from exc
        except Exception as exc:
            self._record_failure(source, exception_message(exc), None)
            raise PermanentJobError("source_processing_error", exception_message(exc)) from exc
        finally:
            await adapter.aclose()

        with self.session_factory() as progress_session, progress_session.begin():
            self.queue.progress(
                progress_session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message=f"discovered {len(candidates)} changed series",
                details={"phase": "discovery", "processed": 1, "total": 2, "unit": "phases"},
            )

        with self.session_factory() as session, session.begin():
            workflow_key = payload.workflow_key or f"pull:{source}:{context.lease.id}"
            root_job = session.get(WorkJob, context.lease.id)
            if root_job is not None:
                root_job.workflow_key = workflow_key
                root_job.group_key = workflow_key
            if source == "asura":
                listed_items = self._apply_asura_revision_consensus(session, listed_items)
                candidate_ids = {item.source_id for item in candidates}
                candidates = [item for item in listed_items if item.source_id in candidate_ids]
            created = 0
            for item in candidates:
                _job, was_created = self.queue.enqueue(
                    session,
                    kind=JobKind.SOURCE_REFRESH,
                    dedupe_key=f"refresh:{source}:{item.source_id}",
                    payload=refresh_payload(item, workflow_key=workflow_key),
                    source=source,
                    priority=55,
                    max_attempts=4,
                    workflow_key=workflow_key,
                    group_key=workflow_key,
                    coalesce=True,
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
            self.queue.progress(
                session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message=f"queued {created} refresh jobs",
                details={"phase": "enqueue", "processed": 2, "total": 2, "unit": "phases"},
            )

    @staticmethod
    def _apply_asura_revision_consensus(session, items: list[SeriesItem]) -> list[SeriesItem]:
        counts: dict[str, set[str]] = {}
        for item in items:
            revision = str(item.metadata.get("asura_revision") or "")
            if revision:
                counts.setdefault(revision, set()).add(item.source_id)
        consensus = max(counts, key=lambda value: len(counts[value]), default="")
        state = session.get(CatalogSourceState, "asura")
        if state is None:
            state = CatalogSourceState(source="asura")
            session.add(state)
        cursor = dict(state.cursor_json or {})
        if consensus and len(counts[consensus]) >= 3:
            cursor["global_revision"] = consensus
            cursor["revision_consensus_count"] = len(counts[consensus])
            state.cursor_json = cursor
        selected = str(cursor.get("global_revision") or "")
        normalized: list[SeriesItem] = []
        for item in items:
            stable, embedded = split_asura_source_id(item.source_id)
            observed = embedded or str(item.metadata.get("asura_revision") or "")
            revision = selected or observed
            metadata = dict(item.metadata)
            metadata["asura_revision"] = observed
            if selected and observed and observed != selected:
                metadata["asura_revision_override"] = observed
                revision = observed
            normalized.append(
                replace(
                    item,
                    source_id=stable,
                    url=asura_series_url(stable, revision),
                    metadata=metadata,
                )
            )
        return normalized

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
    ) -> datetime | None:
        return record_provider_failure(
            self.session_factory,
            source=source,
            error=error,
            cooldown_until=cooldown_until,
            traffic_class=traffic_class,
            catalog=self.catalog,
            queue=self.queue,
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
        item = self._resolve_item(item_from_refresh(payload))
        with self.session_factory() as progress_session, progress_session.begin():
            JobQueue().progress(
                progress_session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message=f"fetching {payload.title} from {payload.source}",
                details={"phase": "detail", "processed": 0, "total": 3, "unit": "phases"},
            )
        try:
            detail = getattr(adapter, "get_series_detail", None)
            enriched = await detail(item) if detail is not None else item
            chapters = await adapter.get_chapters(enriched)
        except SourceRateLimited as exc:
            retry_at = aware_datetime(exc.retry_after) or provider_cooldown_until(
                self.session_factory, payload.source
            )
            effective_cooldown = record_provider_failure(
                self.session_factory,
                source=payload.source,
                error=exception_message(exc),
                cooldown_until=retry_at,
                catalog=self.catalog,
            )
            retry_at = effective_cooldown or retry_at
            delay = max(retry_at - utcnow(), timedelta(seconds=1))
            raise DeferredJobError(
                "rate_limited", exception_message(exc), retry_after=delay
            ) from exc
        except httpx.TransportError as exc:
            effective_cooldown = record_provider_failure(
                self.session_factory,
                source=payload.source,
                error=exception_message(exc),
                cooldown_until=None,
                catalog=self.catalog,
            )
            if effective_cooldown is not None:
                raise DeferredJobError(
                    "source_network_error",
                    exception_message(exc),
                    retry_after=max(
                        effective_cooldown - utcnow(), timedelta(seconds=1)
                    ),
                ) from exc
            raise RetryableJobError("source_network_error", exception_message(exc)) from exc
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if is_transient_provider_status(status):
                cooldown = (
                    provider_cooldown_until(self.session_factory, payload.source)
                    if is_cloudflare_origin_error(status)
                    else None
                )
                effective_cooldown = record_provider_failure(
                    self.session_factory,
                    source=payload.source,
                    error=exception_message(exc),
                    cooldown_until=cooldown,
                    catalog=self.catalog,
                )
                if is_cloudflare_origin_error(status):
                    retry_at = effective_cooldown or cooldown
                    delay = max(retry_at - utcnow(), timedelta(seconds=1))
                    raise DeferredJobError(
                        "provider_origin_unavailable",
                        exception_message(exc),
                        retry_after=delay,
                    ) from exc
                if effective_cooldown is not None:
                    raise DeferredJobError(
                        "source_http_error",
                        exception_message(exc),
                        retry_after=max(
                            effective_cooldown - utcnow(), timedelta(seconds=1)
                        ),
                    ) from exc
                raise RetryableJobError("source_http_error", exception_message(exc)) from exc
            self._quarantine(payload, exception_message(exc))
            raise PermanentJobError("source_item_invalid", exception_message(exc)) from exc
        except Exception as exc:
            self._quarantine(payload, exception_message(exc))
            raise PermanentJobError("source_item_invalid", exception_message(exc)) from exc
        finally:
            await adapter.aclose()
        context.ensure_lease()
        with self.session_factory() as progress_session, progress_session.begin():
            JobQueue().progress(
                progress_session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message=f"parsed {len(chapters)} chapters",
                details={"phase": "catalog", "processed": 2, "total": 3, "unit": "phases"},
            )
        with self.session_factory() as session, session.begin():
            source_series = self.catalog.ingest(session, enriched, chapters)
            source_series_id = source_series.id
        await CoverEvidenceService(self.session_factory).refresh_for_source_series(source_series_id)
        with self.session_factory() as progress_session, progress_session.begin():
            JobQueue().progress(
                progress_session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message="catalog and cover evidence refreshed",
                details={"phase": "cover", "processed": 3, "total": 3, "unit": "phases"},
            )

    def _resolve_item(self, item: SeriesItem) -> SeriesItem:
        if item.source != "asura":
            return item
        with self.session_factory() as session:
            state = session.get(CatalogSourceState, "asura")
            revision = str((state.cursor_json if state else {}).get("global_revision") or "")
        override = str(item.metadata.get("asura_revision_override") or "")
        return replace(item, url=asura_series_url(item.source_id, override or revision))

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


def refresh_payload(item: SeriesItem, *, workflow_key: str = "") -> SourceRefreshPayload:
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
        workflow_key=workflow_key,
        observation_version=str(item.metadata.get("latest_chapter") or latest_listing_chapter(item)),
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
