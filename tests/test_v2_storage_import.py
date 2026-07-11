from __future__ import annotations

import io
import asyncio
import zipfile
from pathlib import Path

from PIL import Image
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from manga_manager.application.cbz_import import LegacyCbzImporter
from manga_manager.application.storage_reconcile import StorageReconciler
from manga_manager.infrastructure.db_models import (
    ArtifactBlob,
    CatalogChapter,
    CatalogSeries,
    ChapterArtifact,
    JobBase,
    LibraryProjection,
)
from manga_manager.infrastructure.storage import ContentAddressedStorage


def png_bytes(color: str) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (16, 16), color=color).save(output, format="PNG")
    return output.getvalue()


def make_cbz(path: Path, *, color: str = "red", number: str = "1") -> None:
    comic_info = (
        "<?xml version='1.0' encoding='utf-8'?>"
        f"<ComicInfo><Series>Example Series</Series><Number>{number}</Number></ComicInfo>"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("ComicInfo.xml", comic_info)
        archive.writestr("0001.png", png_bytes(color))


def storage(tmp_path: Path) -> ContentAddressedStorage:
    return ContentAddressedStorage(
        tmp_path / "storage-v2",
        max_page_bytes=1024 * 1024,
        max_chapter_bytes=10 * 1024 * 1024,
        max_pages=100,
        min_free_bytes=0,
    )


def test_content_addressed_storage_is_idempotent_and_materializes_projection(
    tmp_path: Path,
) -> None:
    source = tmp_path / "chapter.cbz"
    make_cbz(source)
    store = storage(tmp_path)
    first = store.store_existing(source)
    second = store.store_existing(source)
    assert first == second
    assert first.image_count == 1
    blob_path = store.root / first.relative_path
    assert blob_path.is_file()

    projection = store.materialize(first.relative_path, "Manga/series-key/ch-1-1.cbz")
    assert projection.is_file()
    assert projection.read_bytes() == source.read_bytes()


def test_storage_rejects_unsafe_zip_member(tmp_path: Path) -> None:
    source = tmp_path / "unsafe.cbz"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("../page.png", png_bytes("red"))
    try:
        storage(tmp_path).validate_cbz(source)
    except ValueError as exc:
        assert "unsafe zip member" in str(exc)
    else:
        raise AssertionError("unsafe archive was accepted")


def test_storage_requires_comic_info(tmp_path: Path) -> None:
    source = tmp_path / "missing-info.cbz"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("0001.png", png_bytes("red"))
    try:
        storage(tmp_path).validate_cbz(source)
    except ValueError as exc:
        assert "ComicInfo.xml" in str(exc)
    else:
        raise AssertionError("archive without ComicInfo.xml was accepted")


def test_download_storage_rejects_cover_only_page(tmp_path: Path) -> None:
    store = ContentAddressedStorage(
        tmp_path / "storage-v2",
        max_page_bytes=1024 * 1024,
        max_chapter_bytes=10 * 1024 * 1024,
        max_pages=100,
        min_download_pages=3,
    )

    async def pages():
        yield png_bytes("red")

    async def run() -> None:
        try:
            await store.store_pages(pages(), comic_info_xml="<ComicInfo/>")
        except ValueError as exc:
            assert "minimum is 3" in str(exc)
        else:
            raise AssertionError("cover-only download was accepted")

    asyncio.run(run())


def test_cbz_import_dry_run_full_duplicate_and_conflict(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    store = storage(tmp_path)
    importer = LegacyCbzImporter(session_factory=sessions, storage=store)
    first = tmp_path / "Example Series Ch. 1.cbz"
    make_cbz(first, color="red")

    dry_run = importer.import_file(first, dry_run=True)
    assert dry_run.status == "valid"
    assert dry_run.series == "Example Series"
    assert list((tmp_path / "storage-v2").rglob("*.cbz")) == []

    imported = importer.import_file(first, dry_run=False)
    duplicate = importer.import_file(first, dry_run=False)
    assert imported.status == "activated"
    assert duplicate.status == "duplicate"

    conflicting = tmp_path / "replacement.cbz"
    make_cbz(conflicting, color="blue")
    conflict = importer.import_file(conflicting, dry_run=False)
    assert conflict.status == "conflict"

    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(CatalogSeries)) == 1
        assert session.scalar(select(func.count()).select_from(CatalogChapter)) == 1
        assert session.scalar(select(func.count()).select_from(ArtifactBlob)) == 2
        assert session.scalar(select(func.count()).select_from(ChapterArtifact)) == 2
        assert session.scalar(
            select(func.count())
            .select_from(ChapterArtifact)
            .where(ChapterArtifact.state == "quarantined")
        ) == 1
        projection = session.scalar(select(LibraryProjection))
        assert projection is not None
        assert (store.library_root / projection.relative_path).is_file()


def test_reconciler_repairs_missing_projection_and_reports_orphan_blob(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    store = storage(tmp_path)
    source = tmp_path / "chapter.cbz"
    make_cbz(source)
    imported = LegacyCbzImporter(session_factory=sessions, storage=store).import_file(
        source, dry_run=False
    )
    assert imported.status == "activated"
    with sessions() as session:
        projection = session.scalar(select(LibraryProjection))
        assert projection is not None
        projected_path = store.library_root / projection.relative_path
    projected_path.unlink()
    orphan = store.blob_root / "ff" / "orphan.cbz"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(source.read_bytes())

    report = StorageReconciler(session_factory=sessions, storage=store).run()
    assert report.repaired == 1
    assert report.orphan_blobs == 1
    assert projected_path.is_file()
