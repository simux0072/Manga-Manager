from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

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
                        headers_json=dict(sample.get("headers") or {}),
                    )
                )
                if failed and not run.limiting_signal:
                    run.limiting_signal = (
                        f"HTTP {sample.get('status_code')} {sample.get('error_code') or ''}"
                    ).strip()

        return observe

    def active_observer(self, sample: dict) -> None:
        source = str(sample.get("source") or "")
        with self.session_factory() as session:
            run_id = session.scalar(
                select(ProviderBenchmarkRun.id)
                .where(
                    ProviderBenchmarkRun.source == source,
                    ProviderBenchmarkRun.state == "running",
                )
                .order_by(ProviderBenchmarkRun.id.desc())
                .limit(1)
            )
        if run_id is not None:
            self.observer(run_id)(sample)

    def ensure_policy(
        self, source: str, *, job_limit: int, page_limit: int, cooldown_seconds: int
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
                        clean_since=now,
                        next_exploration_at=now + timedelta(days=7),
                        expires_at=now + timedelta(days=30),
                        metadata_json={},
                    )
                )

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
            useful = run.request_count >= 10 or int(report.get("completed_jobs") or 0) > 0
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
                    policy.learned_job_limit = max(
                        policy.learned_job_limit, run.requested_tier
                    )
                    policy.learned_page_limit = max(
                        policy.learned_page_limit, 2 if run.source == "asura" else run.requested_tier * 2
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
