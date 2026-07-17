from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collections import defaultdict

from sqlalchemy import delete, select, tuple_

from manga_manager.infrastructure.db_models import JobDailyAggregate, WorkJob, WorkloadCycle


class JobRetention:
    def prune(self, session, *, now: datetime | None = None, batch: int = 250) -> int:
        current = now or datetime.now(timezone.utc)
        success_cutoff = current - timedelta(days=14)
        failure_cutoff = current - timedelta(days=90)
        rows = session.scalars(
            select(WorkJob)
            .where(
                (
                    (WorkJob.status.in_(("succeeded", "cancelled")))
                    & (WorkJob.completed_at < success_cutoff)
                )
                | ((WorkJob.status == "failed") & (WorkJob.completed_at < failure_cutoff))
            )
            .order_by(WorkJob.completed_at, WorkJob.id)
            .limit(batch)
            .with_for_update(skip_locked=True)
        ).all()
        buckets: dict[tuple, list[int]] = defaultdict(lambda: [0, 0, 0])
        for job in rows:
            completed = job.completed_at or job.updated_at
            day = completed.astimezone(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            values = buckets[(day, job.kind, job.source, job.status, job.error_code)]
            values[0] += 1
            values[1] += job.attempts or 0
            values[2] += max(int((completed - job.created_at).total_seconds()), 0)
        keys = list(buckets)
        existing = {
            (row.day, row.kind, row.source, row.status, row.error_code): row
            for row in session.scalars(
                select(JobDailyAggregate).where(
                    tuple_(
                        JobDailyAggregate.day,
                        JobDailyAggregate.kind,
                        JobDailyAggregate.source,
                        JobDailyAggregate.status,
                        JobDailyAggregate.error_code,
                    ).in_(keys or [(current, "", "", "", "")])
                )
            )
        }
        for key, values in buckets.items():
            bucket = existing.get(key)
            if bucket is None:
                bucket = JobDailyAggregate(
                    day=key[0], kind=key[1], source=key[2], status=key[3], error_code=key[4]
                )
                session.add(bucket)
            bucket.job_count = (bucket.job_count or 0) + values[0]
            bucket.attempt_count = (bucket.attempt_count or 0) + values[1]
            bucket.duration_seconds = (bucket.duration_seconds or 0) + values[2]
            bucket.updated_at = current
        if rows:
            session.execute(delete(WorkJob).where(WorkJob.id.in_([row.id for row in rows])))
        aggregate_cutoff = current - timedelta(days=365)
        session.execute(delete(JobDailyAggregate).where(JobDailyAggregate.day < aggregate_cutoff))
        cycle_cutoff = current - timedelta(days=30)
        session.execute(
            delete(WorkloadCycle).where(
                WorkloadCycle.status == "settled",
                WorkloadCycle.settled_at < cycle_cutoff,
                ~select(WorkJob.id)
                .where(WorkJob.cycle_id == WorkloadCycle.id)
                .exists(),
            )
        )
        return len(rows)
