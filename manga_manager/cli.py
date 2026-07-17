from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
import zipfile
from dataclasses import asdict
from xml.etree import ElementTree
from collections.abc import Sequence
from pathlib import Path

from sqlalchemy import exists, select, text
from sqlalchemy.engine import Connection

from manga_manager.application.source_pull import SourcePullHandler, SourceRefreshHandler
from manga_manager.application.storage_reconcile import StorageReconciler
from manga_manager.application.legacy_repair import (
    LegacyRepair,
    write_legacy_report,
)
from manga_manager.application.cbz_import import LegacyCbzImporter, write_report
from manga_manager.application.chapter_download import ChapterDownloadHandler
from manga_manager.application.kavita_sync import KavitaSyncHandler, KavitaSyncPlanner
from manga_manager.application.library_repair import (
    LibraryRepairHandler,
    enqueue_library_repair,
)
from manga_manager.application.catalog_recovery import CatalogRecovery, write_recovery_report
from manga_manager.application.match_training import export_training_data
from manga_manager.application.maintenance import MaintenanceHandler
from manga_manager.application.diagnostics import build_diagnostic_bundle
from manga_manager.application.database_audit import (
    LATEST_MISMATCH_SQL,
    audit_database,
    write_database_audit,
)
from manga_manager.application.provider_identity_repair import ProviderIdentityRepair
from manga_manager.application.refresh_queue_reconcile import RefreshQueueReconciler
from manga_manager.domain.jobs import (
    ChapterDownloadPayload,
    JobKind,
    KavitaSyncPayload,
    MaintenancePayload,
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
    CatalogSeries,
    ChapterArtifact,
)
from manga_manager.infrastructure.storage import ContentAddressedStorage
from manga_manager.settings import V2Settings
from manga_manager.worker.service import WorkerService
from manga_manager.worker.scheduler import SourcePollScheduler


STAGE_MUTATING_JOB_KINDS = ("chapter_download", "library_repair", "kavita_sync")
STAGE_ACTIVE_JOB_STATES = ("queued", "leased", "retry_wait")


def stage_active_mutations(connection: Connection) -> list[dict[str, object]]:
    rows = connection.execute(
        text(
            "SELECT kind,status,count(*) AS job_count FROM job "
            "WHERE kind IN ('chapter_download','library_repair','kavita_sync') "
            "AND status IN ('queued','leased','retry_wait') "
            "GROUP BY kind,status ORDER BY kind,status"
        )
    ).all()
    return [
        {"kind": str(kind), "status": str(status), "count": int(count)}
        for kind, status, count in rows
    ]


def stage_check_details(values: list[str], *, limit: int, full: bool) -> list[str]:
    return values if full else values[:limit]


def print_stage_check(payload: dict[str, object], *, json_output: bool) -> None:
    if json_output:
        # PostgreSQL statistics expose timezone-aware datetime objects. Reports already use
        # ``default=str``; keep stdout JSON equally robust and machine-readable.
        print(json.dumps(payload, sort_keys=True, default=str))
    else:
        print(" ".join(f"{key}={value}" for key, value in payload.items()))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="manga-manager")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("migrate", help="upgrade the v2 PostgreSQL schema")
    subcommands.add_parser("worker", help="run the durable v2 worker")
    subcommands.add_parser("doctor", help="check v2 database connectivity and migration state")
    diagnostics = subcommands.add_parser(
        "diagnostic-bundle", help="write a bounded credential-redacted diagnostic snapshot"
    )
    diagnostics.add_argument("--output", type=Path, required=True)
    diagnostics.add_argument("--recent-failures", type=int, default=200)
    database_audit = subcommands.add_parser(
        "database-audit", help="run bounded read-only PostgreSQL integrity and growth checks"
    )
    database_audit.add_argument("--json", action="store_true", dest="json_output")
    database_audit.add_argument("--report", type=Path)
    database_audit.add_argument("--statement-timeout-ms", type=int, default=5_000)
    stage = subcommands.add_parser("stage-check", help="verify staged database and storage health")
    stage.add_argument("--json", action="store_true", dest="json_output")
    stage.add_argument(
        "--detail-limit",
        type=int,
        default=25,
        help="maximum archive paths included per failure category",
    )
    stage.add_argument(
        "--full-details",
        action="store_true",
        help="include every failing archive path (potentially very large)",
    )
    benchmark = subcommands.add_parser(
        "benchmark-workers", help="run a bounded worker-pool benchmark"
    )
    benchmark.add_argument(
        "--source", choices=["asura", "mangafire", "kingofshojo"], default="asura"
    )
    benchmark.add_argument("--concurrency", type=int, choices=[1, 2, 3, 4], default=1)
    benchmark.add_argument("--traffic", choices=["origin", "cdn", "both"], default="both")
    benchmark.add_argument("--duration", type=int, default=60, help="maximum seconds")
    benchmark.add_argument("--max-jobs", type=int, default=2)
    benchmark.add_argument("--report", type=Path)
    benchmark.add_argument("--dry-run", action="store_true")
    cleanup = subcommands.add_parser(
        "cleanup-repair-archives", help="delete repair archives older than the retention window"
    )
    cleanup.add_argument("storage_root", type=Path)
    cleanup.add_argument("--retain-days", type=int, default=30)
    validate = subcommands.add_parser(
        "validate-legacy", help="validate active legacy CBZ archives with a resumable cache"
    )
    validate.add_argument("database", type=Path)
    validate.add_argument("--storage-root", type=Path, required=True)
    validate.add_argument("--report", type=Path, required=True)
    validate.add_argument("--manifest-file", type=Path)
    validate.add_argument("--validation-cache", type=Path)
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
    legacy_import = subcommands.add_parser(
        "migrate-legacy-library", help="import active legacy CBZs using legacy identity evidence"
    )
    legacy_import.add_argument("database", type=Path)
    legacy_import.add_argument("--storage-root", type=Path, required=True)
    legacy_import.add_argument("--report", type=Path, default=Path("legacy-library-import.json"))
    legacy_import.add_argument("--apply", action="store_true")
    download = subcommands.add_parser("enqueue-download", help="enqueue one chapter release")
    download.add_argument("chapter_release_id", type=int)
    subcommands.add_parser("reconcile-storage", help="repair the projected library from blobs")
    kavita = subcommands.add_parser("enqueue-kavita", help="enqueue one series for Kavita sync")
    kavita.add_argument("series_id", type=int)
    kavita_pending = subcommands.add_parser(
        "enqueue-kavita-pending", help="enqueue tracked downloaded series not yet synchronized"
    )
    kavita_pending.add_argument("--limit", type=int, default=100)
    kavita_check = subcommands.add_parser(
        "kavita-check", help="verify Kavita auth and path mapping"
    )
    kavita_check.add_argument("--scan-test", action="store_true")
    subcommands.add_parser("enqueue-probe", help="enqueue a deterministic staging probe")
    for name in ("audit-catalog-recovery", "repair-catalog-recovery"):
        recovery = subcommands.add_parser(name, help="reconcile downloaded legacy tracking state")
        recovery.add_argument("legacy_database", type=Path)
        recovery.add_argument("--report", type=Path, required=True)
        if name == "repair-catalog-recovery":
            recovery.add_argument("--apply", action="store_true")
    training = subcommands.add_parser(
        "export-match-training", help="export reviewed match labels and cached covers"
    )
    training.add_argument("output", type=Path)
    library_repair = subcommands.add_parser(
        "enqueue-library-repair", help="queue canonical CBZ/Kavita repair"
    )
    library_repair.add_argument("series_id", type=int, nargs="?")
    library_repair.add_argument("--all-tracked", action="store_true")
    provider_repair = subcommands.add_parser(
        "repair-provider-identities", help="audit or repair normalized provider identities"
    )
    provider_repair.add_argument("--report", type=Path, required=True)
    provider_repair.add_argument("--apply", action="store_true")
    refresh_repair = subcommands.add_parser(
        "reconcile-refresh-queue",
        help="audit or repair queued provider refresh payload compatibility",
    )
    refresh_repair.add_argument("--report", type=Path, required=True)
    refresh_repair.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "cleanup-repair-archives":
        removed = LegacyRepair.cleanup_archives(args.storage_root, retain_days=args.retain_days)
        print(f"removed={len(removed)} retain_days={args.retain_days}")
        return 0
    if args.command == "validate-legacy":
        repair = LegacyRepair(args.database, storage_root=args.storage_root)
        manifest_path = args.manifest_file or args.report.with_suffix(".manifest.json")
        validation_path = args.validation_cache or args.report.with_suffix(".cache.json")
        manifest = repair.manifest(manifest_path)
        results = repair.validate_archives(manifest, validation_path)
        payload = {
            "database": str(repair.database),
            "storage_root": str(args.storage_root.resolve()),
            "summary": {
                "archives": len(results),
                "valid": sum(bool(row["valid"]) for row in results),
                "invalid": sum(not row["valid"] for row in results),
                "one_page_fractional_review": sum(
                    bool(row["review_one_page_fractional"]) for row in results
                ),
            },
            "archives": results,
        }
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        print(json.dumps(payload["summary"], sort_keys=True))
        return 0 if payload["summary"]["invalid"] == 0 else 1
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

    engine = create_database_engine(
        database_url, role="worker" if args.command == "worker" else "cli"
    )
    if args.command == "diagnostic-bundle":
        try:
            payload = build_diagnostic_bundle(
                engine,
                storage_root=settings.storage_root,
                recent_failure_limit=args.recent_failures,
            )
        except ValueError as exc:
            parser.error(str(exc))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_name(f".{args.output.name}.tmp")
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        temporary.chmod(0o600)
        temporary.replace(args.output)
        print(
            f"diagnostics={args.output} failures={len(payload['recent_failures'])} "
            f"database_bytes={payload['database_bytes']}"
        )
        return 0
    if args.command == "repair-provider-identities":
        sessions = create_session_factory(engine)
        service = ProviderIdentityRepair()
        if args.apply:
            with sessions() as session, session.begin():
                records = service.audit(session, lock=True)
                service.apply(session, records)
        else:
            with sessions() as session:
                records = service.audit(session)
        payload = {"applied": bool(args.apply), "records": [asdict(record) for record in records]}
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        print(f"observations={len(records)} applied={str(bool(args.apply)).lower()}")
        return 0
    if args.command == "reconcile-refresh-queue":
        sessions = create_session_factory(engine)
        service = RefreshQueueReconciler()
        if args.apply:
            with sessions() as session, session.begin():
                records = service.audit(session, lock=True)
                service.apply(session, records)
        else:
            with sessions() as session:
                records = service.audit(session)
        payload = {"applied": bool(args.apply), "records": [asdict(row) for row in records]}
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        print(f"observations={len(records)} applied={str(bool(args.apply)).lower()}")
        return 0
    if args.command == "doctor":
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            version = connection.scalar(text("SELECT version_num FROM alembic_version"))
        print(f"database=ok migration={version}")
        return 0
    if args.command == "database-audit":
        if args.statement_timeout_ms < 100:
            parser.error("--statement-timeout-ms must be at least 100")
        with engine.begin() as connection:
            payload = audit_database(connection, statement_timeout_ms=args.statement_timeout_ms)
        if args.report:
            write_database_audit(args.report, payload)
        print_stage_check(payload, json_output=args.json_output)
        return 0 if payload["ok"] else 1
    if args.command == "stage-check":
        if args.detail_limit < 0:
            parser.error("--detail-limit must not be negative")
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
            active_mutations = stage_active_mutations(connection)
            if active_mutations:
                payload = {
                    "ok": False,
                    "busy": True,
                    "database": "ok",
                    "migration": version,
                    "jobs": counts,
                    "active_storage_jobs": active_mutations,
                    "message": (
                        "storage validation deferred while chapter downloads, library repairs, "
                        "or Kavita synchronization jobs can change the validation result"
                    ),
                    "storage_root": str(storage.root.resolve()),
                }
                print_stage_check(payload, json_output=args.json_output)
                return 1
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
                    "SELECT b.relative_path,s.title,c.display_number FROM chapter_artifact a "
                    "JOIN artifact_blob b ON b.checksum=a.blob_checksum "
                    "JOIN chapter_v2 c ON c.id=a.chapter_id "
                    "JOIN series_v2 s ON s.id=c.series_id "
                    "WHERE a.state='active'"
                )
            ).all()
            duplicate_providers = connection.scalar(
                text(
                    "SELECT count(*) FROM (SELECT series_id,source FROM source_series_v2 "
                    "GROUP BY series_id,source HAVING count(*)>1) duplicates"
                )
            )
            missing_kavita = connection.scalar(
                text(
                    "SELECT count(*) FROM chapter_artifact a JOIN chapter_v2 c ON c.id=a.chapter_id "
                    "JOIN series_v2 s ON s.id=c.series_id LEFT JOIN kavita_projection k "
                    "ON k.artifact_id=a.id WHERE a.state='active' AND "
                    "s.status IN ('interested','reading','caught_up','paused') "
                    "AND k.artifact_id IS NULL"
                )
            )
            latest_mismatches = connection.scalar(text(LATEST_MISMATCH_SQL))
            expired_leases = connection.scalar(
                text(
                    "SELECT count(*) FROM job WHERE status='leased' "
                    "AND lease_expires_at < CURRENT_TIMESTAMP"
                )
            )
            started = time.perf_counter()
            connection.execute(
                text(
                    "SELECT id FROM series_v2 "
                    "ORDER BY latest_release_at DESC NULLS LAST, id DESC LIMIT 25"
                )
            ).all()
            first_page_ms = round((time.perf_counter() - started) * 1000, 2)
        invalid_archives: list[str] = []
        metadata_mismatches: list[str] = []
        archive_total = len(blobs)
        report_every = max(25, archive_total // 100)
        print(
            f"stage-check: validating {archive_total} active archives; "
            "the final result is printed after the storage scan",
            file=sys.stderr,
            flush=True,
        )
        for archive_index, (relative_path, expected_series, expected_number) in enumerate(blobs, 1):
            path = storage.root / relative_path
            try:
                storage.validate_cbz(path)
                with zipfile.ZipFile(path) as archive:
                    root = ElementTree.fromstring(archive.read("ComicInfo.xml"))
                actual_series = (root.findtext("Series") or "").strip()
                actual_number = (root.findtext("Number") or "").strip()
                if actual_series != expected_series or actual_number != expected_number:
                    metadata_mismatches.append(relative_path)
            except (OSError, ValueError, ElementTree.ParseError, zipfile.BadZipFile) as exc:
                invalid_archives.append(f"{relative_path}: {exc}")
            if archive_index % report_every == 0 or archive_index == archive_total:
                percent = round(archive_index / archive_total * 100, 1) if archive_total else 100.0
                print(
                    f"stage-check: {archive_index}/{archive_total} archives ({percent}%)",
                    file=sys.stderr,
                    flush=True,
                )
        payload = {
            "ok": (
                invalid == 0
                and missing_projection == 0
                and duplicate_providers == 0
                and missing_kavita == 0
                and latest_mismatches == 0
                and expired_leases == 0
                and not invalid_archives
                and not metadata_mismatches
                and first_page_ms < 1_000
            ),
            "database": "ok",
            "migration": version,
            "jobs": counts,
            "active_artifacts_without_blob": invalid,
            "active_artifacts_without_projection": missing_projection,
            "invalid_active_archive_count": len(invalid_archives),
            "invalid_active_archives": stage_check_details(
                invalid_archives,
                limit=args.detail_limit,
                full=args.full_details,
            ),
            "canonical_metadata_mismatch_count": len(metadata_mismatches),
            "canonical_metadata_mismatches": stage_check_details(
                metadata_mismatches,
                limit=args.detail_limit,
                full=args.full_details,
            ),
            "details_truncated": {
                "invalid_active_archives": max(
                    0,
                    len(invalid_archives)
                    - (len(invalid_archives) if args.full_details else args.detail_limit),
                ),
                "canonical_metadata_mismatches": max(
                    0,
                    len(metadata_mismatches)
                    - (len(metadata_mismatches) if args.full_details else args.detail_limit),
                ),
            },
            "duplicate_provider_identities": duplicate_providers,
            "tracked_artifacts_without_kavita_projection": missing_kavita,
            "latest_release_mismatches": latest_mismatches,
            "expired_job_leases": expired_leases,
            "catalog_first_page_ms": first_page_ms,
            "storage_root": str(storage.root.resolve()),
        }
        print_stage_check(payload, json_output=args.json_output)
        return 0 if payload["ok"] else 1
    if args.command == "benchmark-workers":
        if args.duration < 1 or args.max_jobs < 1:
            parser.error("--duration and --max-jobs must be positive")
        if args.source == "asura" and args.concurrency > 2:
            parser.error("Asura benchmarks are capped at concurrency two")
        requested = args.concurrency
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT health_status, cooldown_until > now() AS cooling FROM source_state_v2 "
                    "WHERE source=:source"
                ),
                {"source": args.source},
            ).first()
        effective = requested
        abandoned = False
        if requested > 1 and row is not None and (row.health_status == "cooldown" or row.cooling):
            effective = 1
            abandoned = True
        payload = {
            "source": args.source,
            "traffic": args.traffic,
            "requested_concurrency": requested,
            "effective_concurrency": effective,
            "abandoned_on_rate_limit": abandoned,
            "global_chapter_ceiling": settings.global_chapter_concurrency,
            "pool_limits": {
                **settings.pool_limits(),
                f"download:{args.source}": effective,
            },
        }
        if not args.dry_run and not abandoned:
            payload.update(
                asyncio.run(
                    run_worker_benchmark(
                        settings,
                        engine,
                        source=args.source,
                        concurrency=effective,
                        traffic=args.traffic,
                        duration=args.duration,
                        max_jobs=args.max_jobs,
                    )
                )
            )
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
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
    if args.command == "migrate-legacy-library":
        sessions = create_session_factory(engine)
        storage = create_storage(settings)
        completed_paths: set[str] = set()
        if args.apply and args.report.is_file():
            try:
                previous = json.loads(args.report.read_text(encoding="utf-8"))
                completed_paths = {
                    str(row["path"])
                    for row in previous
                    if row.get("status") in {"activated", "duplicate", "resumed"}
                }
            except (OSError, ValueError, TypeError, KeyError):
                completed_paths = set()
        records = LegacyCbzImporter(
            session_factory=sessions, storage=storage
        ).import_legacy_database(
            args.database,
            storage_root=args.storage_root,
            dry_run=not args.apply,
            completed_paths=completed_paths,
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
    if args.command == "enqueue-kavita-pending":
        if args.limit < 1:
            parser.error("--limit must be positive")
        sessions = create_session_factory(engine)
        with sessions() as session, session.begin():
            pending, created = KavitaSyncPlanner().enqueue_pending(session, limit=args.limit)
        print(f"pending={pending} created={created}")
        return 0
    if args.command == "kavita-check":
        from app.kavita import configured_kavita_client

        async def check_kavita():
            library_root = settings.storage_root / "kavita-library"
            client = configured_kavita_client(local_library_root=library_root)
            if not client.configured:
                return {"ok": False, "configured": False, "reason": "Kavita is not configured"}
            try:
                expires = await client.authkey_expires()
                series = await client.list_series()
                if args.scan_test:
                    await client.scan_folder_or_all(library_root)
                return {
                    "ok": True,
                    "configured": True,
                    "authkey_expires": expires,
                    "series_visible": len(series),
                    "mapped_root": str(client.kavita_path_for_local(library_root)),
                    "scan_test": bool(args.scan_test),
                }
            finally:
                await client.aclose()

        print(json.dumps(asyncio.run(check_kavita()), sort_keys=True))
        return 0
    if args.command == "enqueue-probe":
        sessions = create_session_factory(engine)
        with sessions() as session, session.begin():
            job, created = JobQueue().enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key="stage-probe",
                payload=MaintenancePayload(action="stage_probe"),
                priority=1,
                pool="health",
            )
        print(f"job_id={job.id} created={str(created).lower()}")
        return 0
    if args.command in {"audit-catalog-recovery", "repair-catalog-recovery"}:
        apply = args.command == "repair-catalog-recovery" and args.apply
        records = CatalogRecovery(create_session_factory(engine)).run(
            args.legacy_database, apply=apply
        )
        write_recovery_report(args.report, records, applied=apply)
        counts: dict[str, int] = {}
        for record in records:
            counts[record.action] = counts.get(record.action, 0) + 1
        print(
            " ".join(f"{key}={value}" for key, value in sorted(counts.items()))
            + f" applied={str(apply).lower()} report={args.report}"
        )
        return 0
    if args.command == "export-match-training":
        count = export_training_data(
            create_session_factory(engine), settings.storage_root, args.output
        )
        print(f"records={count} output={args.output}")
        return 0
    if args.command == "enqueue-library-repair":
        if (args.series_id is None and not args.all_tracked) or (
            args.series_id is not None and args.all_tracked
        ):
            parser.error("provide a series_id or --all-tracked")
        sessions = create_session_factory(engine)
        with sessions() as session, session.begin():
            if args.all_tracked:
                series_ids = session.scalars(
                    select(CatalogSeries.id)
                    .where(
                        CatalogSeries.status.in_(("interested", "reading", "caught_up", "paused"))
                    )
                    .where(
                        exists(
                            select(ChapterArtifact.id)
                            .join(
                                CatalogChapter,
                                CatalogChapter.id == ChapterArtifact.chapter_id,
                            )
                            .where(CatalogChapter.series_id == CatalogSeries.id)
                            .where(ChapterArtifact.state == "active")
                        )
                    )
                    .order_by(CatalogSeries.id)
                ).all()
            else:
                if args.series_id < 1 or session.get(CatalogSeries, args.series_id) is None:
                    parser.error("series_id does not exist")
                series_ids = [args.series_id]
            created = 0
            for series_id in series_ids:
                _, was_created = enqueue_library_repair(
                    session,
                    series_id=series_id,
                    reason="manual_repair",
                    priority=85,
                )
                created += int(was_created)
        print(f"eligible={len(series_ids)} created={created}")
        return 0
    if args.command == "worker":
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
        return asyncio.run(run_worker(settings, engine))
    raise RuntimeError(f"unsupported command {args.command}")


async def run_worker(settings: V2Settings, engine) -> int:
    from app.adapters import SourceAdapterPool
    from app.adapters.http import configure_provider_waiter, configure_request_observer
    from manga_manager.infrastructure.provider_scheduler import ProviderRequestScheduler
    from manga_manager.infrastructure.provider_telemetry import (
        BufferedTelemetryObserver,
        ProviderTelemetry,
    )
    from manga_manager.application.cover_backfill import CoverBackfillHandler

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with suppress_not_implemented():
            loop.add_signal_handler(signum, stop.set)
    sessions = create_session_factory(engine)
    request_scheduler = ProviderRequestScheduler(sessions)
    telemetry = ProviderTelemetry(sessions)
    telemetry_buffer = BufferedTelemetryObserver(telemetry)
    adapters = SourceAdapterPool()
    configure_provider_waiter(request_scheduler.wait)
    configure_request_observer(telemetry_buffer.observe)
    source_pull = SourcePullHandler(
        session_factory=sessions,
        adapter_factory=adapters.get,
        close_adapter=False,
    )
    source_refresh = SourceRefreshHandler(
        session_factory=sessions,
        adapter_factory=adapters.get,
        close_adapter=False,
    )
    storage = create_storage(settings)
    chapter_download = ChapterDownloadHandler(
        session_factory=sessions,
        storage=storage,
        adapter_factory=adapters.get,
        cooldowns={
            source: settings.source_cooldown(source)
            for source in ("asura", "mangafire", "kingofshojo")
        }
        | {"default": settings.source_cooldown("default")},
        circuit_breaker_failures=settings.circuit_breaker_failures,
        close_adapter=False,
    )
    kavita_sync = KavitaSyncHandler(
        session_factory=sessions,
        library_root=storage.kavita_root,
    )
    library_repair = LibraryRepairHandler(session_factory=sessions, storage=storage)
    maintenance = MaintenanceHandler(
        session_factory=sessions,
        adapter_factory=adapters.get,
        close_adapter=False,
    )
    cover_backfill = CoverBackfillHandler(session_factory=sessions)
    service = WorkerService(
        session_factory=sessions,
        handlers={
            JobKind.SOURCE_PULL: source_pull,
            JobKind.SOURCE_REFRESH: source_refresh,
            JobKind.CHAPTER_DOWNLOAD: chapter_download,
            JobKind.KAVITA_SYNC: kavita_sync,
            JobKind.LIBRARY_REPAIR: library_repair,
            JobKind.COVER_BACKFILL: cover_backfill,
            JobKind.MAINTENANCE: maintenance,
        },
        settings=settings,
    )
    scheduler = SourcePollScheduler(
        engine=engine,
        session_factory=sessions,
        settings=settings,
    )
    try:
        await asyncio.gather(
            service.run(stop),
            scheduler.run(stop),
            telemetry_buffer.run(stop),
        )
    finally:
        configure_provider_waiter(None)
        configure_request_observer(None)
        while telemetry_buffer.flush():
            pass
        telemetry_buffer.close()
        request_scheduler.close()
        await kavita_sync.aclose()
        await adapters.aclose()
        storage.close()
    return 0


async def run_worker_benchmark(
    settings: V2Settings,
    engine,
    *,
    source: str,
    concurrency: int,
    traffic: str,
    duration: int,
    max_jobs: int,
) -> dict[str, object]:
    from app.adapters.http import configure_provider_waiter, configure_request_observer
    from manga_manager.infrastructure.provider_scheduler import ProviderRequestScheduler
    from manga_manager.infrastructure.provider_telemetry import ProviderTelemetry

    field = f"{source}_download_concurrency"
    benchmark_settings = settings.model_copy(update={field: concurrency})
    sessions = create_session_factory(engine)
    scheduler = ProviderRequestScheduler(sessions)
    telemetry = ProviderTelemetry(sessions)
    run_id = telemetry.begin(source, concurrency)
    configure_provider_waiter(scheduler.wait)
    benchmark_observer = telemetry.observer(run_id)

    def observe_selected_traffic(sample: dict) -> None:
        if traffic == "both" or sample.get("traffic_class") == traffic:
            benchmark_observer(sample)

    configure_request_observer(observe_selected_traffic)
    handler = ChapterDownloadHandler(
        session_factory=sessions,
        storage=create_storage(benchmark_settings),
        cooldowns={
            name: benchmark_settings.source_cooldown(name)
            for name in ("asura", "mangafire", "kingofshojo")
        }
        | {"default": benchmark_settings.source_cooldown("default")},
        circuit_breaker_failures=benchmark_settings.circuit_breaker_failures,
    )
    service = WorkerService(
        session_factory=sessions,
        handlers={JobKind.CHAPTER_DOWNLOAD: handler},
        settings=benchmark_settings,
        pools={f"download:{source}"},
    )
    with engine.connect() as connection:
        start_event_id = connection.scalar(text("SELECT coalesce(max(id), 0) FROM job_event")) or 0
    stop = asyncio.Event()
    task = asyncio.create_task(service.run(stop))
    started = time.monotonic()
    completed = 0
    rate_limited = False
    try:
        while time.monotonic() - started < duration:
            await asyncio.sleep(0.5)
            with engine.connect() as connection:
                completed = int(
                    connection.scalar(
                        text(
                            "SELECT count(*) FROM job_event e JOIN job j ON j.id=e.job_id "
                            "WHERE e.id>:after AND e.event_type='succeeded' AND j.source=:source"
                        ),
                        {"after": start_event_id, "source": source},
                    )
                    or 0
                )
                state = connection.execute(
                    text(
                        "SELECT health_status, cooldown_until > now() AS cooling "
                        "FROM source_state_v2 WHERE source=:source"
                    ),
                    {"source": source},
                ).first()
            rate_limited = bool(
                state is not None and (state.health_status == "cooldown" or state.cooling)
            )
            if completed >= max_jobs or rate_limited:
                break
    finally:
        stop.set()
        await task
        configure_provider_waiter(None)
        configure_request_observer(None)
    result = {
        "completed_jobs": completed,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "rate_limited": rate_limited,
        "abandoned_on_rate_limit": rate_limited and concurrency > 1,
        "final_concurrency": 1 if rate_limited and concurrency > 1 else concurrency,
        "benchmark_run_id": run_id,
        "traffic": traffic,
    }
    policy = telemetry.finish(run_id, rate_limited=rate_limited, report=result)
    result["learned_policy"] = {
        "job_limit": policy.learned_job_limit,
        "page_limit": policy.learned_page_limit,
        "cooldown_seconds": policy.cooldown_seconds,
        "next_exploration_at": policy.next_exploration_at.isoformat()
        if policy.next_exploration_at
        else None,
    }
    return result


def create_storage(settings: V2Settings) -> ContentAddressedStorage:
    return ContentAddressedStorage(
        settings.storage_root,
        max_page_bytes=settings.max_page_bytes,
        max_chapter_bytes=settings.max_chapter_bytes,
        max_pages=settings.max_pages_per_chapter,
        min_download_pages=settings.min_download_pages,
        min_free_bytes=settings.min_free_bytes,
    )


class suppress_not_implemented:
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, _traceback) -> bool:
        return exception_type is NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
