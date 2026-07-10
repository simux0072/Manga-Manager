from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import httpx

from app.adapters import adapter_for_source
from app.adapters.base import FrontierSentinel, SourceAdapter, SourceRateLimited
from app.domain import ChapterItem, SeriesItem
from manga_manager.application.job_handlers import JobContext, RetryableJobError
from manga_manager.domain.jobs import SourcePullPayload
from manga_manager.infrastructure.catalog_repository import CatalogRepository
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.worker.runtime import SessionFactory


logger = logging.getLogger(__name__)
AdapterFactory = Callable[[str], SourceAdapter | None]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class FetchedSeries:
    item: SeriesItem
    chapters: tuple[ChapterItem, ...]


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
            fetched, failures = await self._fetch(adapter, frontier, context)
        except SourceRateLimited as exc:
            retry_at = aware_datetime(exc.retry_after)
            self._record_failure(source, str(exc), retry_at)
            delay = max(retry_at - utcnow(), timedelta(seconds=1)) if retry_at else None
            raise RetryableJobError("rate_limited", str(exc), retry_after=delay) from exc
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            self._record_failure(source, str(exc), None)
            raise RetryableJobError("source_network_error", str(exc)) from exc
        except Exception as exc:
            self._record_failure(source, str(exc), None)
            raise
        finally:
            await adapter.aclose()

        if failures and not fetched:
            message = f"all {failures} discovered items failed"
            self._record_failure(source, message, None)
            raise RetryableJobError("all_items_failed", message)

        for index, row in enumerate(fetched, start=1):
            context.ensure_lease()
            with self.session_factory() as session, session.begin():
                self.catalog.ingest(session, row.item, row.chapters)
            if index == len(fetched) or index % 10 == 0:
                with self.session_factory() as session, session.begin():
                    if not self.queue.progress(
                        session,
                        job_id=context.lease.id,
                        owner=context.lease.owner,
                        message=f"ingested {index}/{len(fetched)} series",
                        details={"processed": index, "total": len(fetched), "source": source},
                    ):
                        context.lease_lost.set()
                        context.ensure_lease()

        with self.session_factory() as session, session.begin():
            self.catalog.record_poll_success(
                session,
                source=source,
                frontier=build_frontier(fetched, self.frontier_limit),
                partial_failures=failures,
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

    async def _fetch(
        self,
        adapter: SourceAdapter,
        frontier: list[FrontierSentinel],
        context: JobContext,
    ) -> tuple[list[FetchedSeries], int]:
        items = await adapter.list_recent_frontier(frontier)
        fetched: list[FetchedSeries] = []
        failures = 0
        for item in items:
            context.ensure_lease()
            try:
                detail = getattr(adapter, "get_series_detail", None)
                enriched = await detail(item) if detail is not None else item
                chapters = await adapter.get_chapters(enriched)
                fetched.append(FetchedSeries(item=enriched, chapters=tuple(chapters)))
            except SourceRateLimited:
                raise
            except Exception as exc:
                failures += 1
                logger.warning(
                    "source item failed source=%s url=%s: %s", item.source, item.url, exc
                )
        return fetched, failures

    def _record_failure(
        self,
        source: str,
        error: str,
        cooldown_until: datetime | None,
    ) -> None:
        with self.session_factory() as session, session.begin():
            self.catalog.record_poll_failure(
                session,
                source=source,
                error=error,
                cooldown_until=cooldown_until,
            )


def build_frontier(rows: list[FetchedSeries], limit: int) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for row in rows:
        latest = max((chapter.number for chapter in row.chapters), key=chapter_key, default="")
        result.append({"source_id": row.item.source_id, "latest_chapter": latest})
        if len(result) >= limit:
            break
    return result


def chapter_key(value: str) -> tuple[int, Decimal, str]:
    try:
        return (1, Decimal(value), value)
    except InvalidOperation:
        return (0, Decimal(0), value)


def aware_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
