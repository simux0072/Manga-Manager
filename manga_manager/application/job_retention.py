from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select

from manga_manager.infrastructure.db_models import JobDailyAggregate, WorkJob


class JobRetention:
    def prune(self, session, *, now: datetime | None = None, batch: int = 250) -> int:
        current = now or datetime.now(timezone.utc)
        success_cutoff = current - timedelta(days=14)
        failure_cutoff = current - timedelta(days=90)
        rows = session.scalars(
            select(WorkJob)
            .where(
                ((WorkJob.status.in_(("succeeded", "cancelled")))
                 & (WorkJob.completed_at < success_cutoff))
                | ((WorkJob.status == "failed") & (WorkJob.completed_at < failure_cutoff))
            )
            .order_by(WorkJob.completed_at, WorkJob.id)
            .limit(batch)
            .with_for_update(skip_locked=True)
        ).all()
        for job in rows:
            completed = job.completed_at or job.updated_at
            day = completed.astimezone(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            bucket = session.scalar(
                select(JobDailyAggregate).where(
                    JobDailyAggregate.day == day,
                    JobDailyAggregate.kind == job.kind,
                    JobDailyAggregate.source == job.source,
                    JobDailyAggregate.status == job.status,
                    JobDailyAggregate.error_code == job.error_code,
                )
            )
            if bucket is None:
                bucket = JobDailyAggregate(
                    day=day,
                    kind=job.kind,
                    source=job.source,
                    status=job.status,
                    error_code=job.error_code,
                )
                session.add(bucket)
            bucket.job_count = (bucket.job_count or 0) + 1
            bucket.attempt_count = (bucket.attempt_count or 0) + (job.attempts or 0)
            bucket.duration_seconds = (bucket.duration_seconds or 0) + max(
                int((completed - job.created_at).total_seconds()), 0
            )
            bucket.updated_at = current
            session.delete(job)
        aggregate_cutoff = current - timedelta(days=365)
        session.execute(
            delete(JobDailyAggregate).where(JobDailyAggregate.day < aggregate_cutoff)
        )
        return len(rows)
