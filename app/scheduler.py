from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.adapters import enabled_source_names
from app.db import SessionLocal
from app.services import (
    poll_source,
    queue_downloads,
    queue_pending_kavita_syncs,
    run_next_download,
    run_next_kavita_sync,
)
from app.settings import settings


logger = logging.getLogger(__name__)


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(
        event_loop=asyncio.get_running_loop(),
        job_defaults={"coalesce": True, "max_instances": 1},
    )

    async def poll(source: str) -> None:
        try:
            with SessionLocal() as session:
                await poll_source(session, source)
                queue_downloads(session)
        except Exception:
            logger.exception("scheduled poll failed for %s", source)
            raise

    async def drain_downloads() -> None:
        try:
            with SessionLocal() as session:
                while await run_next_download(session):
                    await asyncio.sleep(1)
        except Exception:
            logger.exception("scheduled download drain failed")
            raise

    async def drain_kavita_sync() -> None:
        try:
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
    scheduler.add_job(drain_downloads, "interval", minutes=10)
    scheduler.add_job(drain_kavita_sync, "interval", minutes=10)
    return scheduler
