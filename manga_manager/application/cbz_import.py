from __future__ import annotations

import json
import re
import sqlite3
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from xml.etree import ElementTree

from sqlalchemy import select

from manga_manager.domain.catalog import (
    canonical_chapter_number,
    chapter_sort_number,
    normalize_title,
)
from manga_manager.infrastructure.artifact_repository import ArtifactRepository
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogSeries,
    CatalogSourceSeries,
)
from manga_manager.infrastructure.storage import ContentAddressedStorage
from manga_manager.worker.runtime import SessionFactory


@dataclass(frozen=True, slots=True)
class ImportRecord:
    path: str
    status: str
    series: str = ""
    chapter: str = ""
    checksum: str = ""
    reason: str = ""


class LegacyCbzImporter:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        storage: ContentAddressedStorage,
        artifacts: ArtifactRepository | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.storage = storage
        self.artifacts = artifacts or ArtifactRepository()

    def import_tree(self, source_root: Path, *, dry_run: bool) -> list[ImportRecord]:
        records: list[ImportRecord] = []
        for path in sorted(source_root.rglob("*.cbz")):
            records.append(self.import_file(path, dry_run=dry_run))
        return records

    def import_legacy_database(
        self,
        database: Path,
        *,
        storage_root: Path,
        dry_run: bool,
        completed_paths: set[str] | None = None,
    ) -> list[ImportRecord]:
        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                """
                SELECT df.path, df.source, ss.source_id, s.title AS series_title,
                       c.number AS chapter_number
                FROM downloaded_file df
                JOIN chapter c ON c.id=df.chapter_id
                JOIN series s ON s.id=c.series_id
                JOIN chapter_release cr ON cr.id=df.chapter_release_id
                JOIN source_series ss ON ss.id=cr.source_series_id
                WHERE df.active=1
                ORDER BY df.id
                """
            ).fetchall()
        finally:
            connection.close()
        completed = completed_paths or set()
        records: list[ImportRecord] = []
        for row in rows:
            resolved = self._legacy_path(row, storage_root)
            if str(resolved) in completed:
                records.append(
                    ImportRecord(
                        path=str(resolved),
                        status="resumed",
                        series=str(row["series_title"]),
                        chapter=str(row["chapter_number"]),
                    )
                )
            else:
                records.append(
                    self._import_legacy_row(row, storage_root=storage_root, dry_run=dry_run)
                )
        return records

    def _import_legacy_row(
        self, row: sqlite3.Row, *, storage_root: Path, dry_run: bool
    ) -> ImportRecord:
        path = self._legacy_path(row, storage_root)
        try:
            validated = self.storage.validate_cbz(path)
            if dry_run:
                return ImportRecord(
                    path=str(path),
                    status="valid",
                    series=str(row["series_title"]),
                    chapter=str(row["chapter_number"]),
                    checksum=validated.checksum,
                )
            blob = self.storage.store_existing(path)
            with self.session_factory() as session, session.begin():
                source_series = session.scalar(
                    select(CatalogSourceSeries).where(
                        CatalogSourceSeries.source == str(row["source"]),
                        CatalogSourceSeries.source_id == str(row["source_id"]),
                    )
                )
                series = (
                    session.get(CatalogSeries, source_series.series_id)
                    if source_series is not None
                    else self._series(session, str(row["series_title"]))
                )
                chapter = self._chapter(session, series.id, str(row["chapter_number"]))
                release = None
                if source_series is not None:
                    release = session.scalar(
                        select(CatalogChapterRelease).where(
                            CatalogChapterRelease.source_series_id == source_series.id,
                            CatalogChapterRelease.chapter_id == chapter.id,
                        )
                    )
                projection_path = self.storage.projection_path(
                    series.storage_key, chapter.id, chapter.display_number
                ).as_posix()
                result = self.artifacts.activate(
                    session,
                    chapter_id=chapter.id,
                    chapter_release_id=release.id if release else None,
                    blob=blob,
                    projection_relative_path=projection_path,
                    provenance="legacy_import",
                    source=str(row["source"]),
                )
            if result.status != "conflict":
                self.storage.materialize(blob.relative_path, result.projection_relative_path)
            return ImportRecord(
                path=str(path),
                status=result.status,
                series=series.title,
                chapter=chapter.display_number,
                checksum=blob.checksum,
                reason="different active artifact exists" if result.status == "conflict" else "",
            )
        except Exception as exc:
            return ImportRecord(path=str(path), status="invalid", reason=str(exc))

    @staticmethod
    def _legacy_path(row: sqlite3.Row, storage_root: Path) -> Path:
        raw_path = Path(str(row["path"]))
        candidates = [raw_path]
        if not raw_path.is_absolute():
            candidates.extend(
                [storage_root.parent / raw_path, storage_root / raw_path, storage_root / raw_path.name]
            )
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return candidates[-1]

    def import_file(self, path: Path, *, dry_run: bool) -> ImportRecord:
        try:
            validated = self.storage.validate_cbz(path)
            series_title, chapter_number = read_comic_info(path)
            if not series_title or not chapter_number:
                raise ValueError("ComicInfo.xml must contain Series and Number")
            if dry_run:
                return ImportRecord(
                    path=str(path),
                    status="valid",
                    series=series_title,
                    chapter=chapter_number,
                    checksum=validated.checksum,
                )

            blob = self.storage.store_existing(path)
            with self.session_factory() as session, session.begin():
                series = self._series(session, series_title)
                chapter = self._chapter(session, series.id, chapter_number)
                projection_path = self.storage.projection_path(
                    series.storage_key,
                    chapter.id,
                    chapter.display_number,
                ).as_posix()
                result = self.artifacts.activate(
                    session,
                    chapter_id=chapter.id,
                    chapter_release_id=None,
                    blob=blob,
                    projection_relative_path=projection_path,
                    provenance="legacy_import",
                )
            if result.status != "conflict":
                self.storage.materialize(blob.relative_path, result.projection_relative_path)
            return ImportRecord(
                path=str(path),
                status=result.status,
                series=series_title,
                chapter=chapter_number,
                checksum=blob.checksum,
                reason="different active artifact exists" if result.status == "conflict" else "",
            )
        except Exception as exc:
            return ImportRecord(path=str(path), status="invalid", reason=str(exc))

    def _series(self, session, title: str) -> CatalogSeries:
        normalized = normalize_title(title)
        candidates = session.scalars(
            select(CatalogSeries).where(CatalogSeries.normalized_title == normalized)
        ).all()
        if len(candidates) > 1:
            raise ValueError(f"ambiguous existing series title: {title}")
        if candidates:
            return candidates[0]
        series = CatalogSeries(title=title, normalized_title=normalized, status="untracked")
        session.add(series)
        session.flush()
        return series

    def _chapter(self, session, series_id: int, number: str) -> CatalogChapter:
        canonical = canonical_chapter_number(number)
        chapter = session.scalar(
            select(CatalogChapter).where(
                CatalogChapter.series_id == series_id,
                CatalogChapter.canonical_number == canonical,
            )
        )
        if chapter is not None:
            return chapter
        chapter = CatalogChapter(
            series_id=series_id,
            canonical_number=canonical,
            display_number=number,
            sort_number=chapter_sort_number(number),
        )
        session.add(chapter)
        session.flush()
        return chapter


def read_comic_info(path: Path) -> tuple[str, str]:
    with zipfile.ZipFile(path) as archive:
        names = {name.casefold(): name for name in archive.namelist()}
        member = names.get("comicinfo.xml")
        if member is None:
            return fallback_metadata(path)
        root = ElementTree.fromstring(archive.read(member))
    return (
        root.findtext("Series", default="").strip(),
        root.findtext("Number", default="").strip(),
    )


def fallback_metadata(path: Path) -> tuple[str, str]:
    match = re.match(r"(.+?)\s+Ch\.\s*([0-9.]+)", path.stem, flags=re.IGNORECASE)
    if not match:
        return "", ""
    return match.group(1).strip(), match.group(2).strip()


def write_report(path: Path, records: list[ImportRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps([asdict(record) for record in records], indent=2, sort_keys=True),
        encoding="utf-8",
    )
