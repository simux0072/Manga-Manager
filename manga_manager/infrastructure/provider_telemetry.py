from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

from sqlalchemy import delete, select

from manga_manager.domain.providers import KNOWN_SOURCES
from manga_manager.infrastructure.db_models import (
    ProviderBenchmarkRun,
    ProviderPolicy,
    ProviderRequestSample,
)
from manga_manager.worker.runtime import SessionFactory


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


class ProviderTelemetry:
    def __init__(self, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory

    def begin(self, source: str, tier: int) -> int:
        with self.session_factory() as session, session.begin():
            run = ProviderBenchmarkRun(
                source=source,
                requested_tier=tier,
                stable_tier=max(1, tier - 1),
                report_json={},
            )
            session.add(run)
            session.flush()
            return run.id

    def cleanup_samples(self, *, days: int = 7, now: datetime | None = None) -> int:
        cutoff = (now or utcnow()) - timedelta(days=max(1, days))
        with self.session_factory() as session, session.begin():
            result = session.execute(
                delete(ProviderRequestSample).where(ProviderRequestSample.created_at < cutoff)
            )
            return int(result.rowcount or 0)

    def observer(self, run_id: int):
        def observe(sample: dict) -> None:
            with self.session_factory() as session, session.begin():
                run = session.get(ProviderBenchmarkRun, run_id)
                if run is None or run.state != "running":
                    return
                failed = bool(
                    sample.get("error_code")
                    or int(sample.get("status_code") or 0) in {403, 429, 503}
                )
                run.request_count += 1
                run.failure_count += int(failed)
                run.success_count += int(not failed)
                session.add(
                    ProviderRequestSample(
                        run_id=run.id,
                        source=run.source,
                        host=str(sample.get("host") or ""),
                        status_code=int(sample.get("status_code") or 0),
                        latency_ms=int(sample.get("latency_ms") or 0),
                        byte_count=int(sample.get("byte_count") or 0),
                        error_code=str(sample.get("error_code") or ""),
                        retry_after_seconds=sample.get("retry_after_seconds"),
                        headers_json=dict(sample.get("headers") or {})
                        | {"traffic_class": str(sample.get("traffic_class") or "origin")},
                    )
                )
                if failed and not run.limiting_signal:
                    run.limiting_signal = (
                        f"HTTP {sample.get('status_code')} {sample.get('error_code') or ''}"
                    ).strip()

        return observe

    def record_samples(self, samples: list[dict[str, Any]]) -> int:
        """Persist request observations in one transaction.

        Runtime traffic can produce hundreds of observations per minute. Keeping the
        batching boundary here prevents request instrumentation from becoming a
        database transaction for every image and catalog request.
        """
        rows = [row for row in samples if str(row.get("source") or "") in KNOWN_SOURCES]
        if not rows:
            return 0
        sources = {str(row["source"]) for row in rows}
        with self.session_factory() as session, session.begin():
            active_runs: dict[str, ProviderBenchmarkRun] = {}
            for run in session.scalars(
                select(ProviderBenchmarkRun)
                .where(
                    ProviderBenchmarkRun.source.in_(sources),
                    ProviderBenchmarkRun.state == "running",
                )
                .order_by(ProviderBenchmarkRun.id)
            ):
                active_runs[run.source] = run

            for sample in rows:
                source = str(sample["source"])
                status_code = int(sample.get("status_code") or 0)
                failed = bool(sample.get("error_code") or status_code in {403, 429, 503})
                run = active_runs.get(source)
                if run is not None:
                    run.request_count += 1
                    run.failure_count += int(failed)
                    run.success_count += int(not failed)
                    if failed and not run.limiting_signal:
                        run.limiting_signal = (
                            f"HTTP {status_code} {sample.get('error_code') or ''}"
                        ).strip()
                session.add(
                    ProviderRequestSample(
                        run_id=run.id if run is not None else None,
                        source=source,
                        host=str(sample.get("host") or ""),
                        status_code=status_code,
                        latency_ms=int(sample.get("latency_ms") or 0),
                        byte_count=int(sample.get("byte_count") or 0),
                        error_code=str(sample.get("error_code") or "") if failed else "",
                        retry_after_seconds=sample.get("retry_after_seconds"),
                        headers_json=dict(sample.get("headers") or {})
                        | {"traffic_class": str(sample.get("traffic_class") or "origin")},
                    )
                )
        return len(rows)

    def active_observer(self, sample: dict) -> None:
        self.record_samples([sample])


    def ensure_policy(
        self,
        source: str,
        *,
        job_limit: int,
        page_limit: int,
        cooldown_seconds: int,
        request_interval_seconds: float = 0.0,
        poll_interval_seconds: int = 0,
    ) -> None:
        now = utcnow()
        with self.session_factory() as session, session.begin():
            policy = session.get(ProviderPolicy, source)
            if policy is None:
                session.add(
                    ProviderPolicy(
                        source=source,
                        learned_job_limit=job_limit,
                        learned_page_limit=page_limit,
                        cooldown_seconds=cooldown_seconds,
                        request_interval_seconds=request_interval_seconds,
                        clean_since=now,
                        next_exploration_at=now + timedelta(days=7),
                        expires_at=now + timedelta(days=30),
                        metadata_json=poll_cadence_metadata({}, poll_interval_seconds),
                    )
                )
            else:
                changed = False
                if policy.request_interval_seconds <= 0 < request_interval_seconds:
                    # Backfill the conservative pace for policies created before request pacing
                    # became durable. Learned non-zero values are never overwritten here.
                    policy.request_interval_seconds = request_interval_seconds
                    changed = True
                metadata = poll_cadence_metadata(
                    policy.metadata_json, poll_interval_seconds
                )
                if metadata != (policy.metadata_json or {}):
                    policy.metadata_json = metadata
                    changed = True
                if changed:
                    policy.updated_at = now

    def start_due_exploration(
        self,
        source: str,
        *,
        ceiling: int,
        now: datetime | None = None,
    ) -> int | None:
        current = now or utcnow()
        with self.session_factory() as session, session.begin():
            policy = session.get(ProviderPolicy, source)
            if (
                policy is None
                or policy.next_exploration_at is None
                or aware(policy.next_exploration_at) > current
                or policy.learned_job_limit >= ceiling
            ):
                return None
            active = session.scalar(
                select(ProviderBenchmarkRun.id).where(
                    ProviderBenchmarkRun.source == source,
                    ProviderBenchmarkRun.state == "running",
                )
            )
            if active is not None:
                return None
            tier = min(ceiling, policy.learned_job_limit + 1)
            run = ProviderBenchmarkRun(
                source=source,
                requested_tier=tier,
                stable_tier=policy.learned_job_limit,
                report_json={},
            )
            session.add(run)
            session.flush()
            metadata = dict(policy.metadata_json or {})
            metadata.update(
                {
                    "exploration_tier": tier,
                    "exploration_until": (current + timedelta(minutes=10)).isoformat(),
                    "benchmark_run_id": run.id,
                }
            )
            policy.metadata_json = metadata
            policy.updated_at = current
            return run.id

    def finalize_due(self, *, now: datetime | None = None) -> int:
        current = now or utcnow()
        with self.session_factory() as session:
            rows = session.scalars(
                select(ProviderBenchmarkRun).where(ProviderBenchmarkRun.state == "running")
            ).all()
            due = [
                row.id
                for row in rows
                if row.limiting_signal
                or row.request_count >= 500
                or aware(row.started_at) + timedelta(minutes=10) <= current
            ]
        for run_id in due:
            with self.session_factory() as session:
                row = session.get(ProviderBenchmarkRun, run_id)
                limited = bool(row and row.limiting_signal)
                report = {
                    "request_count": row.request_count if row else 0,
                    "success_count": row.success_count if row else 0,
                    "failure_count": row.failure_count if row else 0,
                    "limiting_signal": row.limiting_signal if row else "",
                }
            policy = self.finish(run_id, rate_limited=limited, report=report)
            with self.session_factory() as session, session.begin():
                stored = session.get(ProviderPolicy, policy.source)
                if stored:
                    metadata = dict(stored.metadata_json or {})
                    for key in ("exploration_tier", "exploration_until", "benchmark_run_id"):
                        metadata.pop(key, None)
                    stored.metadata_json = metadata
        return len(due)

    def finish(self, run_id: int, *, rate_limited: bool, report: dict) -> ProviderPolicy:
        now = utcnow()
        with self.session_factory() as session, session.begin():
            run = session.get(ProviderBenchmarkRun, run_id)
            if run is None:
                raise ValueError("benchmark run disappeared")
            limited = rate_limited or bool(run.limiting_signal)
            useful = run.request_count >= 100 or int(report.get("completed_jobs") or 0) >= 2
            run.state = "limited" if limited else "succeeded" if useful else "inconclusive"
            run.completed_at = now
            run.report_json = dict(report)
            policy = session.get(ProviderPolicy, run.source)
            if policy is None:
                policy = ProviderPolicy(source=run.source, metadata_json={})
                session.add(policy)
            if limited:
                policy.last_limited_at = now
                policy.successful_tier_runs = 0
                policy.learned_job_limit = max(1, run.requested_tier - 1)
                policy.learned_page_limit = max(
                    1,
                    min(
                        policy.learned_page_limit,
                        1 if run.source == "asura" else max(2, (run.requested_tier - 1) * 2),
                    ),
                )
                policy.request_interval_seconds = max(
                    0.5,
                    policy.request_interval_seconds * 2
                    if policy.request_interval_seconds > 0
                    else 0.5,
                )
                policy.next_exploration_at = now + timedelta(days=30)
                retry_values = session.scalars(
                    select(ProviderRequestSample.retry_after_seconds).where(
                        ProviderRequestSample.run_id == run.id,
                        ProviderRequestSample.retry_after_seconds.is_not(None),
                    )
                ).all()
                if retry_values:
                    policy.cooldown_seconds = max(policy.cooldown_seconds, max(retry_values))
            elif useful:
                policy.successful_tier_runs += 1
                required = 2 if run.source == "asura" and run.requested_tier >= 2 else 1
                if policy.successful_tier_runs >= required:
                    policy.learned_job_limit = max(policy.learned_job_limit, run.requested_tier)
                    policy.learned_page_limit = max(
                        policy.learned_page_limit,
                        2 if run.source == "asura" else run.requested_tier * 2,
                    )
                    floor = 0.5 if run.source == "asura" else 0.0
                    if policy.request_interval_seconds > floor:
                        policy.request_interval_seconds = max(
                            floor, round(policy.request_interval_seconds * 0.8, 3)
                        )
                policy.clean_since = policy.clean_since or now
                policy.next_exploration_at = now + timedelta(days=7)
            else:
                policy.successful_tier_runs = 0
                policy.next_exploration_at = now + timedelta(days=1)
            policy.expires_at = now + timedelta(days=30)
            policy.updated_at = now
            session.flush()
            return policy


class BufferedTelemetryObserver:
    """Thread-safe, bounded request telemetry buffer for worker runtime traffic."""

    def __init__(
        self,
        telemetry: ProviderTelemetry,
        *,
        flush_interval_seconds: float = 1.0,
        max_samples: int = 5_000,
        batch_size: int = 1_000,
    ) -> None:
        self.telemetry = telemetry
        self.flush_interval_seconds = max(0.1, flush_interval_seconds)
        self.batch_size = max(1, batch_size)
        self._samples: deque[dict[str, Any]] = deque(maxlen=max(1, max_samples))
        self._lock = Lock()

    def observe(self, sample: dict[str, Any]) -> None:
        # Copy mutable callback data before returning control to the HTTP client.
        with self._lock:
            self._samples.append(dict(sample))

    def flush(self) -> int:
        with self._lock:
            count = min(len(self._samples), self.batch_size)
            batch = [self._samples.popleft() for _ in range(count)]
        if not batch:
            return 0
        return self.telemetry.record_samples(batch)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.flush_interval_seconds)
            except TimeoutError:
                await asyncio.to_thread(self.flush)
        while await asyncio.to_thread(self.flush):
            pass


def poll_cadence_metadata(metadata: dict | None, base_seconds: int) -> dict:
    result = dict(metadata or {})
    if base_seconds > 0:
        result["base_poll_seconds"] = int(base_seconds)
        result.setdefault("adaptive_poll_seconds", int(base_seconds))
        result.setdefault("unchanged_poll_streak", 0)
    return result


def effective_poll_interval(policy: ProviderPolicy | None, fallback: timedelta) -> timedelta:
    fallback_seconds = max(int(fallback.total_seconds()), 60)
    if policy is None:
        return fallback
    metadata = policy.metadata_json or {}
    try:
        seconds = int(metadata.get("adaptive_poll_seconds") or fallback_seconds)
    except (TypeError, ValueError):
        seconds = fallback_seconds
    return timedelta(seconds=max(60, min(seconds, fallback_seconds * 4)))
