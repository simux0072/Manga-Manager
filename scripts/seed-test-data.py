from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from PIL import Image, ImageDraw
from sqlalchemy import func, select, text

from manga_manager.application.cbz_import import LegacyCbzImporter
from manga_manager.application.cover_evidence import (
    ALGORITHM,
    LEGACY_ALGORITHM,
    cover_signature,
    fingerprint_cover,
)
from manga_manager.domain.catalog import normalize_title
from manga_manager.infrastructure.database import create_database_engine, create_session_factory
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogChapterRelease,
    CatalogCoverAsset,
    CatalogCoverFingerprint,
    CatalogCoverSignature,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    WorkJob,
    WorkloadCycle,
)
from manga_manager.infrastructure.storage import ContentAddressedStorage
from manga_manager.settings import V2Settings


SMALL_SERIES = (
    ("Synthetic Star Pilot", "asura", "star-pilot-asura", (49, 110, 246)),
    ("The Star Pilot", "mangafire", "star-pilot-mangafire", (49, 110, 246)),
    ("Synthetic Clockwork Garden", "kingofshojo", "clockwork-garden", (16, 185, 129)),
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="Seed an isolated Manga Manager test database")
    value.add_argument("--profile", choices=("small", "scale"), default="small")
    value.add_argument("--series-count", type=int, default=2_000)
    value.add_argument("--job-count", type=int, default=25_000)
    return value


def image_bytes(color: tuple[int, int, int], label: str, *, page: int = 0) -> bytes:
    image = Image.new("RGB", (240, 360), color)
    drawing = ImageDraw.Draw(image)
    drawing.rectangle((12, 12, 228, 348), outline=(235, 242, 255), width=4)
    drawing.text((24, 34), label, fill=(255, 255, 255))
    drawing.text((24, 310), f"Synthetic page {page}", fill=(255, 255, 255))
    output = io.BytesIO()
    image.save(output, format="JPEG", quality=82)
    return output.getvalue()


def write_cbz(path: Path, *, title: str, chapter: str, color: tuple[int, int, int]) -> None:
    comic_info = (
        "<?xml version='1.0' encoding='utf-8'?>"
        f"<ComicInfo><Series>{title}</Series><Number>{chapter}</Number>"
        "<Title>Generated integration fixture</Title><Writer>Manga Manager</Writer>"
        "<Summary>Generated synthetic content for deterministic testing.</Summary></ComicInfo>"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("ComicInfo.xml", comic_info)
        for page in range(1, 4):
            shade = tuple(min(255, component + page * 8) for component in color)
            archive.writestr(f"{page:04d}.jpg", image_bytes(shade, title, page=page))


def storage_for(settings: V2Settings) -> ContentAddressedStorage:
    return ContentAddressedStorage(
        settings.storage_root,
        max_page_bytes=settings.max_page_bytes,
        max_chapter_bytes=settings.max_chapter_bytes,
        max_pages=settings.max_pages_per_chapter,
        min_download_pages=settings.min_download_pages,
        min_free_bytes=0,
    )


def ensure_empty_or_seeded(session, profile: str, expected: int) -> bool:
    total = int(session.scalar(select(func.count()).select_from(CatalogSeries)) or 0)
    seeded = int(
        session.scalar(
            select(func.count())
            .select_from(CatalogSeries)
            .where(text("metadata_json->>'test_profile' = :profile"))
            .params(profile=profile)
        )
        or 0
    )
    if total == expected and seeded == expected:
        return True
    if total or seeded:
        raise RuntimeError(
            "test seeding requires an empty isolated database; refusing to alter existing data"
        )
    return False


def add_cover_evidence(
    session,
    *,
    source_series: CatalogSourceSeries,
    content: bytes,
    relative_path: str,
) -> None:
    legacy_hash, checksum, width, height = fingerprint_cover(content)
    features, keypoints, descriptors = cover_signature(content)
    session.add(
        CatalogCoverAsset(
            source_series_id=source_series.id,
            content_checksum=checksum,
            relative_path=relative_path,
            content_type="image/jpeg",
            source_url=source_series.cover_url,
            width=width,
            height=height,
        )
    )
    session.add(
        CatalogCoverSignature(
            source_series_id=source_series.id,
            algorithm_version=ALGORITHM,
            feature_json=features,
            keypoints_blob=keypoints,
            descriptors_blob=descriptors,
        )
    )
    session.add(
        CatalogCoverFingerprint(
            source_series_id=source_series.id,
            algorithm=LEGACY_ALGORITHM,
            hash_hex=legacy_hash,
            content_sha256=checksum,
            width=width,
            height=height,
        )
    )


def seed_small(settings: V2Settings, sessions) -> dict[str, int | str]:
    storage = storage_for(settings)
    storage.ensure_directories()
    with sessions() as session:
        if ensure_empty_or_seeded(session, "small", len(SMALL_SERIES)):
            source_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(CatalogSourceSeries)
                    .where(text("metadata_json->>'test_fixture' = 'true'"))
                )
                or 0
            )
            job_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(WorkJob)
                    .where(WorkJob.dedupe_key.like("test-seed:small:%"))
                )
                or 0
            )
            if source_count != len(SMALL_SERIES) or job_count != 4:
                raise RuntimeError("small test fixture is incomplete; reset its isolated database")
            return {"profile": "small", "status": "already_seeded", "series": len(SMALL_SERIES)}

    fixture_root = settings.storage_root / "seed-input"
    importer = LegacyCbzImporter(session_factory=sessions, storage=storage)
    for title, _source, _source_id, color in (SMALL_SERIES[0], SMALL_SERIES[2]):
        for chapter in ("1", "2"):
            archive = fixture_root / title / f"{title} Ch. {chapter}.cbz"
            write_cbz(archive, title=title, chapter=chapter, color=color)
            record = importer.import_file(archive, dry_run=False)
            if record.status not in {"activated", "duplicate", "reused"}:
                raise RuntimeError(f"fixture import failed: {record}")

    now = datetime.now(UTC)
    source_rows: list[CatalogSourceSeries] = []
    with sessions() as session, session.begin():
        for index, (title, source, source_id, color) in enumerate(SMALL_SERIES):
            series = session.scalar(
                select(CatalogSeries).where(CatalogSeries.normalized_title == normalize_title(title))
            )
            if series is None:
                series = CatalogSeries(title=title, normalized_title=normalize_title(title))
                session.add(series)
                session.flush()
            cover_label = SMALL_SERIES[0][0] if index in {0, 1} else title
            cover = image_bytes(color, cover_label)
            checksum = hashlib.sha256(cover).hexdigest()
            relative = Path("covers") / checksum[:2] / f"{checksum}.jpg"
            destination = settings.storage_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(cover)
            cover_data = "data:image/jpeg;base64," + base64.b64encode(cover).decode("ascii")
            series.status = "interested" if index in {0, 2} else "untracked"
            series.description = "Generated synthetic catalog fixture for integration testing."
            series.cover_url = cover_data
            series.cover_checksum = checksum
            series.cover_relative_path = relative.as_posix()
            series.metadata_json = {"test_profile": "small", "synthetic": True}
            series.latest_release_number = "2" if index in {0, 2} else ""
            series.latest_release_source = source
            series.latest_release_at = now if index in {0, 2} else None
            source_row = CatalogSourceSeries(
                series_id=series.id,
                source=source,
                source_id=source_id,
                normalized_source_id=source_id,
                title=title,
                normalized_title=normalize_title(title),
                url=f"https://fixtures.invalid/{source}/{source_id}",
                description=series.description,
                cover_url=cover_data,
                metadata_json={"test_fixture": True},
                detail_fetched_at=now,
            )
            session.add(source_row)
            session.flush()
            source_rows.append(source_row)
            session.add(
                CatalogSeriesAlias(
                    series_id=series.id,
                    source_series_id=source_row.id,
                    display_value=title,
                    normalized_value=normalize_title(title),
                )
            )
            add_cover_evidence(
                session,
                source_series=source_row,
                content=cover,
                relative_path=relative.as_posix(),
            )
            for chapter in session.scalars(
                select(CatalogChapter).where(CatalogChapter.series_id == series.id)
            ):
                session.add(
                    CatalogChapterRelease(
                        chapter_id=chapter.id,
                        source_series_id=source_row.id,
                        source=source,
                        source_release_id=chapter.canonical_number,
                        title=f"Chapter {chapter.display_number}",
                        url=f"https://fixtures.invalid/{source}/{source_id}/{chapter.canonical_number}",
                        published_at=now,
                    )
                )
                session.add(
                    CatalogChapterReadingState(
                        chapter_id=chapter.id,
                        status="read" if chapter.canonical_number == "1" else "unread",
                        read_at=now if chapter.canonical_number == "1" else None,
                    )
                )
        session.add(
            CatalogMatchDecision(
                left_source_series_id=source_rows[0].id,
                right_source_series_id=source_rows[1].id,
                confidence=0.91,
                evidence_json={
                    "score": 0.91,
                    "title": 0.86,
                    "cover": 1.0,
                    "description": 1.0,
                    "chapter_overlap": 0.0,
                    "test_fixture": True,
                },
                scorer_version="synthetic-v1",
                feature_vector_json={"test_fixture": True},
            )
        )
        cycle = WorkloadCycle(
            status="settled",
            total_units=4,
            successful_units=3,
            failed_units=1,
            settled_at=now,
        )
        session.add(cycle)
        session.flush()
        representative_jobs = (
            ("maintenance", "succeeded", "maintenance", "", ""),
            ("library_repair", "succeeded", "maintenance", "", ""),
            ("source_pull", "succeeded", "pull:asura", "asura", ""),
            ("chapter_download", "failed", "download:mangafire", "mangafire", "test_failure"),
        )
        for index, (kind, status, pool, source, error_code) in enumerate(representative_jobs):
            session.add(
                WorkJob(
                    kind=kind,
                    dedupe_key=f"test-seed:small:{index}",
                    payload={"test_fixture": True},
                    source=source,
                    pool=pool,
                    cycle_id=cycle.id,
                    workflow_key="test-seed:small",
                    group_key=f"cycle:{cycle.id}:test:{kind}",
                    status=status,
                    error_code=error_code,
                    error_message="Synthetic expected failure" if error_code else "",
                    completed_at=now,
                    progress_current=1,
                    progress_total=1,
                )
            )
    for path in sorted(fixture_root.rglob("*"), reverse=True):
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            path.rmdir()
    fixture_root.rmdir()
    return {"profile": "small", "status": "seeded", "series": len(SMALL_SERIES), "jobs": 4}


def seed_scale(settings: V2Settings, sessions, *, series_count: int, job_count: int) -> dict[str, int | str]:
    if series_count < 2_000 or job_count < 25_000:
        raise ValueError("scale profile requires at least 2000 series and 25000 jobs")
    with sessions() as session, session.begin():
        if ensure_empty_or_seeded(session, "scale", series_count):
            seeded_jobs = int(
                session.scalar(
                    select(func.count())
                    .select_from(WorkJob)
                    .where(WorkJob.dedupe_key.like("test-seed:scale:%"))
                )
                or 0
            )
            if seeded_jobs != job_count:
                raise RuntimeError("scale test fixture is incomplete; reset its isolated database")
            return {"profile": "scale", "status": "already_seeded", "series": series_count, "jobs": job_count}
        rows = [
            CatalogSeries(
                title=f"Synthetic Scale Series {index:05d}",
                normalized_title=f"synthetic scale series {index:05d}",
                description="Generated database-only scale fixture.",
                status="interested" if index % 5 == 0 else "untracked",
                metadata_json={"test_profile": "scale", "index": index},
            )
            for index in range(series_count)
        ]
        session.add_all(rows)
        session.flush()
        session.add_all(
            CatalogSourceSeries(
                series_id=row.id,
                source=("asura", "mangafire", "kingofshojo")[index % 3],
                source_id=f"scale-{index}",
                normalized_source_id=f"scale-{index}",
                title=row.title,
                normalized_title=row.normalized_title,
                url=f"https://fixtures.invalid/scale/{index}",
                metadata_json={"test_fixture": True},
            )
            for index, row in enumerate(rows)
        )
        queued = job_count // 4
        failed = job_count // 10
        cancelled = job_count // 10
        succeeded = job_count - queued - failed - cancelled
        cycle = WorkloadCycle(
            status="active",
            total_units=job_count,
            successful_units=succeeded,
            failed_units=failed,
            cancelled_units=cancelled,
            added_units=job_count,
        )
        session.add(cycle)
        session.flush()
        now = datetime.now(UTC)
        jobs: list[WorkJob] = []
        for index in range(job_count):
            if index < queued:
                status = "queued"
            elif index < queued + failed:
                status = "failed"
            elif index < queued + failed + cancelled:
                status = "cancelled"
            else:
                status = "succeeded"
            series_index = index % series_count
            kind = "library_repair" if index % 4 == 0 else "chapter_download"
            source = "" if kind == "library_repair" else ("asura", "mangafire", "kingofshojo")[index % 3]
            group_key = (
                f"cycle:{cycle.id}:maintenance:library_repair"
                if kind == "library_repair"
                else f"cycle:{cycle.id}:download:series:{series_index}"
            )
            jobs.append(
                WorkJob(
                    kind=kind,
                    dedupe_key=f"test-seed:scale:{index}",
                    payload={"series_id": rows[series_index].id, "test_fixture": True},
                    source=source,
                    series_key=f"series:{series_index}",
                    pool="maintenance" if kind == "library_repair" else f"download:{source}",
                    cycle_id=cycle.id,
                    workflow_key="test-seed:scale",
                    group_key=group_key,
                    status=status,
                    available_at=now,
                    error_code="synthetic_failure" if status == "failed" else "",
                    error_message="Synthetic scale failure" if status == "failed" else "",
                    completed_at=now if status in {"failed", "cancelled", "succeeded"} else None,
                    progress_current=1 if status in {"failed", "cancelled", "succeeded"} else 0,
                    progress_total=1,
                )
            )
            if len(jobs) == 2_000:
                session.bulk_save_objects(jobs)
                jobs.clear()
        if jobs:
            session.bulk_save_objects(jobs)
    return {"profile": "scale", "status": "seeded", "series": series_count, "jobs": job_count}


def main() -> int:
    args = parser().parse_args()
    if os.environ.get("MANGA_MANAGER_ALLOW_TEST_SEED") != "1":
        raise SystemExit("refusing to seed without MANGA_MANAGER_ALLOW_TEST_SEED=1")
    settings = V2Settings()
    engine = create_database_engine(settings.require_database_url())
    sessions = create_session_factory(engine)
    if args.profile == "small":
        result = seed_small(settings, sessions)
    else:
        result = seed_scale(
            settings,
            sessions,
            series_count=args.series_count,
            job_count=args.job_count,
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
