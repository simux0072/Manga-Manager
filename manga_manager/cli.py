from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
from collections.abc import Sequence
from pathlib import Path

from sqlalchemy import text

from manga_manager.application.source_pull import SourcePullHandler
from manga_manager.application.storage_reconcile import StorageReconciler
from manga_manager.application.legacy_repair import (
    LegacyRepair,
    sqlite_path,
    write_legacy_report,
)
from manga_manager.application.cbz_import import LegacyCbzImporter, write_report
from manga_manager.application.chapter_download import ChapterDownloadHandler
from manga_manager.application.kavita_sync import KavitaSyncHandler
from manga_manager.domain.jobs import (
    ChapterDownloadPayload,
    JobKind,
    KavitaSyncPayload,
    SourcePullPayload,
)
from manga_manager.infrastructure.database import (
    create_database_engine,
    create_session_factory,
    run_migrations,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
)
from manga_manager.infrastructure.storage import ContentAddressedStorage
from manga_manager.settings import V2Settings
from manga_manager.worker.service import WorkerService
from manga_manager.worker.scheduler import SourcePollScheduler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="manga-manager")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("migrate", help="upgrade the v2 PostgreSQL schema")
    subcommands.add_parser("worker", help="run the durable v2 worker")
    subcommands.add_parser("doctor", help="check v2 database connectivity and migration state")
    stage = subcommands.add_parser("stage-check", help="verify staged database and storage health")
    stage.add_argument("--json", action="store_true", dest="json_output")
    benchmark = subcommands.add_parser(
        "benchmark-workers", help="report safe worker-pool benchmark settings"
    )
    benchmark.add_argument("--asura-concurrency", type=int, choices=[1, 2], default=1)
    cleanup = subcommands.add_parser(
        "cleanup-repair-archives", help="delete repair archives older than the retention window"
    )
    cleanup.add_argument("storage_root", type=Path)
    cleanup.add_argument("--retain-days", type=int, default=30)
    rescan = subcommands.add_parser(
        "rescan-legacy", help="rescan legacy sources, covers, chapters, matches, and stale jobs"
    )
    rescan.add_argument("database", type=Path)
    rescan.add_argument("--limit", type=int)
    for name, help_text in (
        ("audit-legacy", "audit a legacy SQLite catalog without changing it"),
        ("repair-legacy", "repair safe legacy defects; defaults to dry-run"),
    ):
        legacy = subcommands.add_parser(name, help=help_text)
        legacy.add_argument("database", type=Path)
        legacy.add_argument("--storage-root", type=Path)
        legacy.add_argument("--report", type=Path, required=True)
        legacy.add_argument(
            "--manifest-file",
            type=Path,
            help="persistent manifest cache; reused on retry when present",
        )
        legacy.add_argument("--backup-dir", type=Path)
        if name == "repair-legacy":
            legacy.add_argument(
                "--apply", action="store_true", help="apply safe repairs after backup"
            )
    pull = subcommands.add_parser("enqueue-pull", help="enqueue one source discovery pull")
    pull.add_argument("source", choices=["asura", "mangafire", "kingofshojo"])
    importer = subcommands.add_parser("import-cbz", help="validate or import legacy CBZ files")
    importer.add_argument("source", type=Path)
    importer.add_argument("--dry-run", action="store_true")
    importer.add_argument("--report", type=Path, default=Path("cbz-import-report.json"))
    download = subcommands.add_parser("enqueue-download", help="enqueue one chapter release")
    download.add_argument("chapter_release_id", type=int)
    subcommands.add_parser("reconcile-storage", help="repair the projected library from blobs")
    kavita = subcommands.add_parser("enqueue-kavita", help="enqueue one series for Kavita sync")
    kavita.add_argument("series_id", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "cleanup-repair-archives":
        removed = LegacyRepair.cleanup_archives(args.storage_root, retain_days=args.retain_days)
        print(f"removed={len(removed)} retain_days={args.retain_days}")
        return 0
    if args.command == "rescan-legacy":
        if args.limit is not None and args.limit < 1:
            parser.error("--limit must be positive")
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        from app.services import repair_known_series

        database = sqlite_path(args.database)
        legacy_engine = create_engine(f"sqlite:///{database}", connect_args={"timeout": 30})
        with Session(legacy_engine, expire_on_commit=False) as session:
            result = asyncio.run(repair_known_series(session, limit=args.limit))
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.command in {"audit-legacy", "repair-legacy"}:
        repair = LegacyRepair(args.database, storage_root=args.storage_root)
        apply = args.command == "repair-legacy" and args.apply
        manifest_path = args.manifest_file or args.report.with_suffix(
            args.report.suffix + ".manifest.json"
        )
        manifest = repair.manifest(manifest_path)
        actions, backup = (
            repair.repair(apply=apply, backup_dir=args.backup_dir)
            if args.command == "repair-legacy"
            else (repair.audit(), None)
        )
        write_legacy_report(
            args.report,
            database=repair.database,
            dry_run=not apply,
            actions=actions,
            manifest=manifest,
            backup=backup,
        )
        print(
            f"observations={len(actions)} applied={sum(item.applied for item in actions)} dry_run={str(not apply).lower()} report={args.report}"
        )
        return 0
    settings = V2Settings()
    try:
        database_url = settings.require_database_url()
    except ValueError as exc:
        parser.error(str(exc))
    if args.command == "migrate":
        run_migrations(database_url)
        return 0

    engine = create_database_engine(database_url)
    if args.command == "doctor":
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            version = connection.scalar(text("SELECT version_num FROM alembic_version"))
        print(f"database=ok migration={version}")
        return 0
    if args.command == "stage-check":
        storage = create_storage(settings)
        storage.ensure_directories()
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            version = connection.scalar(text("SELECT version_num FROM alembic_version"))
            counts = dict(
                connection.execute(
                    text("SELECT status, count(*) FROM job GROUP BY status ORDER BY status")
                ).all()
            )
            invalid = connection.scalar(
                text(
                    "SELECT count(*) FROM chapter_artifact a "
                    "LEFT JOIN artifact_blob b ON b.checksum=a.blob_checksum "
                    "WHERE a.state='active' AND b.checksum IS NULL"
                )
            )
            missing_projection = connection.scalar(
                text(
                    "SELECT count(*) FROM chapter_artifact a "
                    "LEFT JOIN library_projection p ON p.artifact_id=a.id "
                    "WHERE a.state='active' AND p.artifact_id IS NULL"
                )
            )
            blobs = connection.execute(
                text(
                    "SELECT b.relative_path FROM chapter_artifact a "
                    "JOIN artifact_blob b ON b.checksum=a.blob_checksum "
                    "WHERE a.state='active'"
                )
            ).all()
            started = time.perf_counter()
            connection.execute(
                text(
                    "SELECT id FROM series_v2 "
                    "ORDER BY latest_release_at DESC NULLS LAST, id DESC LIMIT 25"
                )
            ).all()
            first_page_ms = round((time.perf_counter() - started) * 1000, 2)
        invalid_archives: list[str] = []
        for (relative_path,) in blobs:
            path = storage.root / relative_path
            try:
                storage.validate_cbz(path)
            except (OSError, ValueError) as exc:
                invalid_archives.append(f"{relative_path}: {exc}")
        payload = {
            "ok": (
                invalid == 0
                and missing_projection == 0
                and not invalid_archives
                and first_page_ms < 1_000
            ),
            "database": "ok",
            "migration": version,
            "jobs": counts,
            "active_artifacts_without_blob": invalid,
            "active_artifacts_without_projection": missing_projection,
            "invalid_active_archives": invalid_archives,
            "catalog_first_page_ms": first_page_ms,
            "storage_root": str(storage.root.resolve()),
        }
        if args.json_output:
            print(json.dumps(payload, sort_keys=True))
        else:
            print(" ".join(f"{key}={value}" for key, value in payload.items()))
        return 0 if payload["ok"] else 1
    if args.command == "benchmark-workers":
        requested = args.asura_concurrency
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT health_status, cooldown_until > now() AS cooling FROM source_state_v2 "
                    "WHERE source='asura'"
                )
            ).first()
        effective = requested
        abandoned = False
        if requested == 2 and row is not None and (row.health_status == "cooldown" or row.cooling):
            effective = 1
            abandoned = True
        payload = {
            "requested_asura_concurrency": requested,
            "effective_asura_concurrency": effective,
            "abandoned_on_rate_limit": abandoned,
            "global_chapter_ceiling": settings.global_chapter_concurrency,
            "pool_limits": {**settings.pool_limits(), "download:asura": effective},
        }
        print(json.dumps(payload, sort_keys=True))
        return 0
    if args.command == "enqueue-pull":
        sessions = create_session_factory(engine)
        with sessions() as session, session.begin():
            job, created = JobQueue().enqueue(
                session,
                kind=JobKind.SOURCE_PULL,
                dedupe_key=f"source:{args.source}",
                payload=SourcePullPayload(source=args.source),
                priority=10,
            )
        print(f"job_id={job.id} created={str(created).lower()}")
        return 0
    if args.command == "import-cbz":
        sessions = create_session_factory(engine)
        storage = create_storage(settings)
        records = LegacyCbzImporter(session_factory=sessions, storage=storage).import_tree(
            args.source,
            dry_run=args.dry_run,
        )
        write_report(args.report, records)
        counts: dict[str, int] = {}
        for record in records:
            counts[record.status] = counts.get(record.status, 0) + 1
        print(" ".join(f"{key}={value}" for key, value in sorted(counts.items())))
        return 0
    if args.command == "enqueue-download":
        if args.chapter_release_id < 1:
            parser.error("chapter_release_id must be positive")
        sessions = create_session_factory(engine)
        with sessions() as session, session.begin():
            release = session.get(CatalogChapterRelease, args.chapter_release_id)
            if release is None:
                parser.error("chapter_release_id does not exist")
            chapter = session.get(CatalogChapter, release.chapter_id)
            if chapter is None:
                parser.error("chapter release has no canonical chapter")
            job, created = JobQueue().enqueue(
                session,
                kind=JobKind.CHAPTER_DOWNLOAD,
                dedupe_key=f"release:{args.chapter_release_id}",
                payload=ChapterDownloadPayload(chapter_release_id=args.chapter_release_id),
                priority=10,
                source=release.source,
                series_key=str(chapter.series_id),
            )
        print(f"job_id={job.id} created={str(created).lower()}")
        return 0
    if args.command == "reconcile-storage":
        report = StorageReconciler(
            session_factory=create_session_factory(engine),
            storage=create_storage(settings),
        ).run()
        print(" ".join(f"{key}={value}" for key, value in report.as_dict().items()))
        return 0
    if args.command == "enqueue-kavita":
        if args.series_id < 1:
            parser.error("series_id must be positive")
        sessions = create_session_factory(engine)
        with sessions() as session, session.begin():
            job, created = JobQueue().enqueue(
                session,
                kind=JobKind.KAVITA_SYNC,
                dedupe_key=f"series:{args.series_id}",
                payload=KavitaSyncPayload(series_id=args.series_id),
                priority=10,
            )
        print(f"job_id={job.id} created={str(created).lower()}")
        return 0
    if args.command == "worker":
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
        return asyncio.run(run_worker(settings, engine))
    raise RuntimeError(f"unsupported command {args.command}")


async def run_worker(settings: V2Settings, engine) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with suppress_not_implemented():
            loop.add_signal_handler(signum, stop.set)
    sessions = create_session_factory(engine)
    source_pull = SourcePullHandler(session_factory=sessions)
    chapter_download = ChapterDownloadHandler(
        session_factory=sessions,
        storage=create_storage(settings),
        cooldowns={
            source: settings.source_cooldown(source)
            for source in ("asura", "mangafire", "kingofshojo")
        }
        | {"default": settings.source_cooldown("default")},
        circuit_breaker_failures=settings.circuit_breaker_failures,
    )
    kavita_sync = KavitaSyncHandler(
        session_factory=sessions,
        library_root=settings.storage_root / "library",
    )
    service = WorkerService(
        session_factory=sessions,
        handlers={
            JobKind.SOURCE_PULL: source_pull,
            JobKind.CHAPTER_DOWNLOAD: chapter_download,
            JobKind.KAVITA_SYNC: kavita_sync,
        },
        settings=settings,
    )
    scheduler = SourcePollScheduler(
        engine=engine,
        session_factory=sessions,
        settings=settings,
    )
    await asyncio.gather(service.run(stop), scheduler.run(stop))
    return 0


def create_storage(settings: V2Settings) -> ContentAddressedStorage:
    return ContentAddressedStorage(
        settings.storage_root,
        max_page_bytes=settings.max_page_bytes,
        max_chapter_bytes=settings.max_chapter_bytes,
        max_pages=settings.max_pages_per_chapter,
        min_free_bytes=settings.min_free_bytes,
    )


class suppress_not_implemented:
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, _traceback) -> bool:
        return exception_type is NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
