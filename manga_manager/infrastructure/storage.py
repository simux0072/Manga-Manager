from __future__ import annotations

import hashlib
import io
import os
import re
import shutil
import zipfile
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

from PIL import Image


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"}


@dataclass(frozen=True, slots=True)
class ValidatedCbz:
    checksum: str
    byte_count: int
    image_count: int


@dataclass(frozen=True, slots=True)
class StoredBlob:
    checksum: str
    relative_path: str
    byte_count: int
    image_count: int


class StorageCapacityError(OSError):
    def __init__(self, *, free_bytes: int, incoming_bytes: int, min_free_bytes: int) -> None:
        self.free_bytes = free_bytes
        self.incoming_bytes = incoming_bytes
        self.min_free_bytes = min_free_bytes
        super().__init__(
            "storage capacity unavailable: "
            f"free={free_bytes} incoming={incoming_bytes} reserve={min_free_bytes}"
        )


class ContentAddressedStorage:
    def __init__(
        self,
        root: Path,
        *,
        max_page_bytes: int,
        max_chapter_bytes: int,
        max_pages: int,
        min_download_pages: int = 1,
        min_free_bytes: int = 0,
    ) -> None:
        self.root = root
        self.blob_root = root / "blobs"
        self.staging_root = root / "staging"
        self.library_root = root / "library"
        self.kavita_root = root / "kavita-library"
        self.max_page_bytes = max_page_bytes
        self.max_chapter_bytes = max_chapter_bytes
        self.max_pages = max_pages
        self.min_download_pages = min_download_pages
        self.min_free_bytes = min_free_bytes

    def ensure_directories(self) -> None:
        self.blob_root.mkdir(parents=True, exist_ok=True)
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.library_root.mkdir(parents=True, exist_ok=True)
        self.kavita_root.mkdir(parents=True, exist_ok=True)

    def validate_cbz(self, path: Path) -> ValidatedCbz:
        byte_count = path.stat().st_size
        if byte_count > self.max_chapter_bytes:
            raise ValueError(f"archive exceeds max chapter bytes: {byte_count}")
        checksum = file_checksum(path)
        image_count = 0
        total_uncompressed = 0
        has_comic_info = False
        try:
            with zipfile.ZipFile(path) as archive:
                for info in archive.infolist():
                    member = PurePosixPath(info.filename)
                    if member.is_absolute() or ".." in member.parts:
                        raise ValueError(f"unsafe zip member: {info.filename}")
                    if info.is_dir():
                        continue
                    total_uncompressed += info.file_size
                    if total_uncompressed > self.max_chapter_bytes:
                        raise ValueError("uncompressed archive exceeds max chapter bytes")
                    is_image = member.suffix.lower() in IMAGE_SUFFIXES
                    if is_image and info.file_size > self.max_page_bytes:
                        raise ValueError(f"page exceeds max page bytes: {info.filename}")
                    # Reading each member once verifies its ZIP CRC. Reuse image bytes for
                    # Pillow validation instead of archive.testzip() followed by a second read.
                    data = archive.read(info)
                    if member.as_posix().lower() == "comicinfo.xml":
                        has_comic_info = True
                    if not is_image:
                        continue
                    image_count += 1
                    if image_count > self.max_pages:
                        raise ValueError(f"archive contains too many pages: {image_count}")
                    try:
                        with Image.open(io.BytesIO(data)) as image:
                            image.verify()
                    except Exception as exc:
                        raise ValueError(f"invalid image member: {info.filename}") from exc
        except (zipfile.BadZipFile, RuntimeError) as exc:
            raise ValueError(f"invalid ZIP archive: {exc}") from exc
        if not has_comic_info:
            raise ValueError("archive is missing ComicInfo.xml")
        if not image_count:
            raise ValueError("archive contains no chapter images")
        return ValidatedCbz(checksum, byte_count, image_count)

    def store_existing(self, source: Path) -> StoredBlob:
        self.ensure_directories()
        validated = self.validate_cbz(source)
        relative = Path("blobs") / validated.checksum[:2] / f"{validated.checksum}.cbz"
        destination = self.root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
            try:
                try:
                    os.link(source, temporary)
                except OSError:
                    # Imports prefer same-filesystem hardlinks, which allocate no archive data.
                    # Enforce the download watermark only when a real copy is required.
                    self.require_free_space(validated.byte_count)
                    shutil.copy2(source, temporary)
                if file_checksum(temporary) != validated.checksum:
                    raise ValueError("stored blob checksum does not match source")
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
        elif file_checksum(destination) != validated.checksum:
            raise ValueError(f"existing blob checksum mismatch: {destination}")
        return StoredBlob(
            checksum=validated.checksum,
            relative_path=relative.as_posix(),
            byte_count=validated.byte_count,
            image_count=validated.image_count,
        )

    async def store_pages(
        self,
        pages: AsyncIterator[bytes],
        *,
        comic_info_xml: str,
        progress: Callable[[int], None] | None = None,
    ) -> StoredBlob:
        self.ensure_directories()
        self.require_free_space(self.max_chapter_bytes)
        staging = self.staging_root / f"{uuid4().hex}.cbz.tmp"
        image_count = 0
        total_bytes = 0
        try:
            with zipfile.ZipFile(staging, "w", compression=zipfile.ZIP_STORED) as archive:
                archive.writestr("ComicInfo.xml", comic_info_xml)
                async for page in pages:
                    image_count += 1
                    if image_count > self.max_pages:
                        raise ValueError(f"chapter exceeds page limit: {self.max_pages}")
                    if len(page) > self.max_page_bytes:
                        raise ValueError(f"page exceeds max page bytes: {len(page)}")
                    total_bytes += len(page)
                    if total_bytes > self.max_chapter_bytes:
                        raise ValueError("chapter exceeds max chapter bytes")
                    extension = validated_image_extension(page)
                    archive.writestr(f"{image_count:04d}.{extension}", page)
                    if progress is not None:
                        progress(image_count)
            if image_count < self.min_download_pages:
                raise ValueError(
                    f"chapter contains {image_count} images; minimum is {self.min_download_pages}"
                )
            return self.store_existing(staging)
        finally:
            staging.unlink(missing_ok=True)

    def projection_path(self, storage_key: str, chapter_id: int, display_number: str) -> Path:
        filename = f"ch-{chapter_id}-{safe_component(display_number)}.cbz"
        return Path("Manga") / storage_key / filename

    def materialize(self, blob_relative_path: str, projection_relative_path: str) -> Path:
        return self._materialize(self.library_root, blob_relative_path, projection_relative_path)

    def materialize_kavita(
        self, blob_relative_path: str, projection_relative_path: str
    ) -> Path:
        return self._materialize(self.kavita_root, blob_relative_path, projection_relative_path)

    def _materialize(
        self, root: Path, blob_relative_path: str, projection_relative_path: str
    ) -> Path:
        source = self.root / blob_relative_path
        if not source.is_file():
            raise FileNotFoundError(source)
        destination = root / projection_relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
        try:
            try:
                os.link(source, temporary)
            except OSError:
                shutil.copy2(source, temporary)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return destination

    def require_free_space(self, incoming_bytes: int) -> None:
        existing = self.root if self.root.exists() else self.root.parent
        usage = shutil.disk_usage(existing)
        if usage.free - incoming_bytes < self.min_free_bytes:
            raise StorageCapacityError(
                free_bytes=usage.free,
                incoming_bytes=incoming_bytes,
                min_free_bytes=self.min_free_bytes,
            )


def file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_component(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-.")
    return cleaned[:100] or "unknown"


def validated_image_extension(data: bytes) -> str:
    try:
        with Image.open(io.BytesIO(data)) as image:
            image.verify()
            image_format = image.format
    except Exception as exc:
        raise ValueError("invalid chapter image") from exc
    if not image_format:
        raise ValueError("chapter image format is missing")
    return image_format.lower().replace("jpeg", "jpg")
