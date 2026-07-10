from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.adapters import enabled_source_names
from app.db import SessionLocal
from app.services import (
    create_pull_job,
    queue_pending_kavita_syncs,
    drain_download_queue,
    recover_stale_pull_jobs,
    run_next_kavita_sync,
    run_pull_job,
)
from app.settings import settings


logger = logging.getLogger(__name__)


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(
        event_loop=asyncio.get_running_loop(),
        job_defaults={"coalesce": True, "max_instances": 1},
    )
    download_drain_lock = asyncio.Lock()
    kavita_drain_lock = asyncio.Lock()

    async def poll(source: str) -> None:
        try:
            with SessionLocal() as session:
                recover_stale_pull_jobs(session)
                job, created = create_pull_job(session, source)
            if created:
                await run_pull_job(job.id)
            else:
                logger.info("scheduled poll skipped; active pull exists for %s", source)
        except Exception:
            logger.exception("scheduled poll failed for %s", source)
            raise

    async def drain_downloads() -> None:
        if download_drain_lock.locked():
            logger.info("scheduled download drain skipped; drain already running")
            return
        try:
            async with download_drain_lock:
                with SessionLocal() as session:
                    await drain_download_queue(session)
        except Exception:
            logger.exception("scheduled download drain failed")
            raise

    async def drain_kavita_sync() -> None:
        if kavita_drain_lock.locked():
            logger.info("scheduled Kavita sync drain skipped; drain already running")
            return
        try:
            async with kavita_drain_lock:
                with SessionLocal() as session:
                    queue_pending_kavita_syncs(session)
                    while await run_next_kavita_sync(session):
                        await asyncio.sleep(1)
        except Exception:
            logger.exception("scheduled Kavita sync drain failed")
            raise

    sources = enabled_source_names()
    if "asura" in sources:
        scheduler.add_job(poll, "interval", minutes=settings.asura_poll_minutes, args=["asura"])
    if "mangafire" in sources:
        scheduler.add_job(poll, "interval", minutes=settings.mangafire_poll_minutes, args=["mangafire"])
    if "kingofshojo" in sources:
        scheduler.add_job(
            poll, "interval", minutes=settings.kingofshojo_poll_minutes, args=["kingofshojo"]
        )
    scheduler.add_job(drain_downloads, "interval", minutes=settings.download_drain_interval_minutes)
    scheduler.add_job(drain_kavita_sync, "interval", minutes=10)
    return scheduler
