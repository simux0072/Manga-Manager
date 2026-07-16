from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from manga_manager.application.download_plans import DownloadPlanCoordinator
from manga_manager.infrastructure.catalog_repository import CatalogRepository
from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    ProviderEndpointState,
    ProviderPolicy,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.worker.runtime import SessionFactory


CLOUDFLARE_ORIGIN_ERROR_STATUSES = frozenset(range(520, 528))
TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504}) | (
    CLOUDFLARE_ORIGIN_ERROR_STATUSES
)
_STATUS_IN_ERROR = re.compile(r"(?:error\s+['\"]|Status/)(52[0-7])\b", re.IGNORECASE)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def is_transient_provider_status(status: int) -> bool:
    return status in TRANSIENT_HTTP_STATUSES


def is_cloudflare_origin_error(status: int) -> bool:
    return status in CLOUDFLARE_ORIGIN_ERROR_STATUSES


def contains_cloudflare_origin_error(message: str) -> bool:
    return _STATUS_IN_ERROR.search(message) is not None


def provider_cooldown_until(
    session_factory: SessionFactory,
    source: str,
    *,
    now: datetime | None = None,
) -> datetime:
    current = now or utcnow()
    with session_factory() as session:
        policy = session.get(ProviderPolicy, source)
        seconds = max(int(policy.cooldown_seconds or 0), 60) if policy is not None else 300
    return current + timedelta(seconds=seconds)


def record_provider_failure(
    session_factory: SessionFactory,
    *,
    source: str,
    error: str,
    cooldown_until: datetime | None,
    traffic_class: str = "origin",
    catalog: CatalogRepository | None = None,
    queue: JobQueue | None = None,
) -> datetime | None:
    repository = catalog or CatalogRepository()
    job_queue = queue or JobQueue()
    with session_factory() as session, session.begin():
        repository.record_poll_failure(
            session,
            source=source,
            error=error,
            cooldown_until=cooldown_until,
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
            DownloadPlanCoordinator(job_queue).bypass_cooling_source(
                session,
                source=source,
                retry_after=effective_cooldown,
            )
            if effective_cooldown.tzinfo is None:
                effective_cooldown = effective_cooldown.replace(tzinfo=timezone.utc)
        return effective_cooldown
