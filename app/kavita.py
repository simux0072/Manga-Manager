from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import httpx

from app.settings import settings


@dataclass(frozen=True)
class KavitaSeries:
    id: int
    name: str
    library_id: int | None = None
    folder_path: str = ""
    mal_id: str = ""
    anilist_id: str = ""


@dataclass(frozen=True)
class KavitaChapter:
    id: int
    number: str
    volume_id: int | None = None
    pages_total: int = 0


@dataclass(frozen=True)
class KavitaReadProgress:
    chapter_id: int
    pages_read: int = 0
    pages_total: int = 0


class KavitaClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        local_library_root: Path | None = None,
        kavita_library_root: Path | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.local_library_root = local_library_root or settings.library_root
        self.kavita_library_root = (
            kavita_library_root if kavita_library_root is not None else settings.kavita_library_root
        )

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.api_key)

    def headers(self) -> dict[str, str]:
        return {"x-api-key": self.api_key}

    async def authkey_expires(self) -> str | None:
        if not self.configured:
            return None
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{self.base_url}/api/Plugin/authkey-expires",
                headers=self.headers(),
            )
            response.raise_for_status()
            return response.json()

    async def scan_all(self) -> None:
        if not self.configured:
            return
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{self.base_url}/api/Library/scan-all",
                headers=self.headers(),
            )
            response.raise_for_status()

    async def scan_folder(self, folder_path: Path) -> None:
        if not self.configured:
            return
        kavita_folder_path = self.kavita_path_for_local(folder_path)
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{self.base_url}/api/Library/scan-folder",
                headers=self.headers(),
                json={
                    "apiKey": self.api_key,
                    "folderPath": str(kavita_folder_path),
                    "abortOnNoSeriesMatch": False,
                },
            )
            response.raise_for_status()

    async def scan_folder_or_all(self, folder_path: Path) -> None:
        kavita_folder_path = self.kavita_path_for_local(folder_path)
        kavita_library_root = self.kavita_path_for_local(self.local_library_root)
        if _same_folder_path(kavita_folder_path, kavita_library_root):
            await self.scan_all()
            return

        # Kavita's targeted scan endpoint uses SingleOrDefault when resolving a
        # folder. A stale/duplicate Kavita series mapping therefore raises a
        # fatal server-side exception instead of returning a useful response.
        # Detect that state from the catalog first and use the safe full scan.
        try:
            series = await self.list_series()
        except Exception:
            series = []
        folder_matches = sum(
            1
            for candidate in series
            if candidate.folder_path
            and _same_folder_path(Path(candidate.folder_path), kavita_folder_path)
        )
        if folder_matches > 1:
            await self.scan_all()
            return

        try:
            await self.scan_folder(folder_path)
        except Exception:
            await self.scan_all()

    async def list_series(self) -> list[KavitaSeries]:
        if not self.configured:
            return []
        page = 1
        page_size = 100
        result: list[KavitaSeries] = []
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                response = await client.post(
                    f"{self.base_url}/api/Series/all-v2",
                    params={"PageNumber": page, "PageSize": page_size},
                    headers=self.headers(),
                    json={},
                )
                response.raise_for_status()
                payload = response.json()
                items = payload if isinstance(payload, list) else payload.get("items", [])
                if not items:
                    break
                result.extend(parse_series(item) for item in items)
                if len(items) < page_size:
                    break
                page += 1
        return result

    async def series_detail(self, series_id: int) -> list[KavitaChapter]:
        if not self.configured:
            return []
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{self.base_url}/api/Series/series-detail",
                params={"seriesId": series_id},
                headers=self.headers(),
            )
            response.raise_for_status()
        payload = response.json()
        chapters = list(payload.get("chapters") or [])
        for volume in payload.get("volumes") or []:
            chapters.extend(volume.get("chapters") or [])
        return [chapter for item in chapters if (chapter := parse_chapter(item)) is not None]

    async def chapter_progress(
        self, chapter_id: int, pages_total: int = 0
    ) -> KavitaReadProgress | None:
        if not self.configured:
            return None
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{self.base_url}/api/Reader/get-progress",
                params={"chapterId": chapter_id},
                headers=self.headers(),
            )
            response.raise_for_status()
        payload = response.json()
        return parse_read_progress(payload, chapter_id, pages_total)

    async def mark_series_read(self, series_id: int) -> None:
        await self._mark_series("mark-read", series_id)

    async def mark_series_unread(self, series_id: int) -> None:
        await self._mark_series("mark-unread", series_id)

    async def _mark_series(self, action: str, series_id: int) -> None:
        if not self.configured:
            return
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{self.base_url}/api/Reader/{action}",
                headers=self.headers(),
                json={"seriesId": series_id},
            )
            response.raise_for_status()

    async def want_to_read(self) -> list[KavitaSeries]:
        if not self.configured:
            return []
        page = 1
        page_size = 100
        result: list[KavitaSeries] = []
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                response = await client.post(
                    f"{self.base_url}/api/want-to-read/v2",
                    params={"PageNumber": page, "PageSize": page_size},
                    headers=self.headers(),
                    json={},
                )
                response.raise_for_status()
                payload = response.json()
                items = payload if isinstance(payload, list) else payload.get("items", [])
                if not items:
                    break
                result.extend(parse_series(item) for item in items)
                if len(items) < page_size:
                    break
                page += 1
        return result

    async def add_want_to_read(self, series_ids: list[int]) -> None:
        await self.update_want_to_read("add-series", series_ids)

    async def remove_want_to_read(self, series_ids: list[int]) -> None:
        await self.update_want_to_read("remove-series", series_ids)

    async def upload_series_cover(self, series_id: int, data_url: str) -> None:
        await self._upload_cover("series", series_id, data_url)

    async def upload_chapter_cover(self, chapter_id: int, data_url: str) -> None:
        await self._upload_cover("chapter", chapter_id, data_url)

    async def _upload_cover(self, entity: str, entity_id: int, data_url: str) -> None:
        if not self.configured or not data_url:
            return
        encoded = data_url.split(",", 1)[1] if data_url.startswith("data:") else data_url
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.base_url}/api/Upload/{entity}",
                headers=self.headers(),
                json={"id": entity_id, "url": encoded},
            )
            response.raise_for_status()

    async def update_want_to_read(self, action: str, series_ids: list[int]) -> None:
        if not self.configured or not series_ids:
            return
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{self.base_url}/api/want-to-read/{action}",
                headers=self.headers(),
                json={"seriesIds": series_ids},
            )
            response.raise_for_status()

    def kavita_path_for_local(self, local_path: Path) -> Path:
        if self.kavita_library_root is None:
            return local_path
        try:
            relative = local_path.resolve().relative_to(self.local_library_root.resolve())
        except ValueError:
            return local_path
        return self.kavita_library_root / relative

    def series_url(self, library_id: int, series_id: int) -> str:
        return settings.kavita_series_url_template.format(
            base_url=self.base_url,
            library_id=library_id,
            series_id=series_id,
        )

    def chapter_url(self, library_id: int, series_id: int, chapter_id: int) -> str:
        return settings.kavita_chapter_url_template.format(
            base_url=self.base_url,
            library_id=library_id,
            series_id=series_id,
            chapter_id=chapter_id,
        )


def parse_series(item: dict) -> KavitaSeries:
    return KavitaSeries(
        id=int(item.get("id") or 0),
        name=str(item.get("name") or item.get("localizedName") or item.get("originalName") or ""),
        library_id=item.get("libraryId"),
        # folderPath may be the shared library parent for every series. Kavita's
        # lowestFolderPath identifies the actual series directory and is safe matching evidence.
        folder_path=str(item.get("lowestFolderPath") or item.get("folderPath") or ""),
        mal_id=str(item.get("malId") or ""),
        anilist_id=str(item.get("aniListId") or ""),
    )


def _same_folder_path(left: Path, right: Path) -> bool:
    """Compare Kavita paths without requiring either container path to exist."""
    return Path(left) == Path(right)


def parse_chapter(item: dict) -> KavitaChapter | None:
    chapter_id = item.get("id")
    if not chapter_id:
        return None
    number = item.get("number") or item.get("range")
    if not number:
        min_number = item.get("minNumber")
        max_number = item.get("maxNumber")
        number = str(min_number if min_number == max_number else min_number or "")
    number = str(number).strip()
    if not number:
        return None
    return KavitaChapter(
        id=int(chapter_id),
        number=number,
        volume_id=item.get("volumeId"),
        pages_total=int(item.get("pagesTotal") or item.get("pages") or item.get("totalPages") or 0),
    )


def parse_read_progress(
    item: dict | None, chapter_id: int, pages_total: int = 0
) -> KavitaReadProgress | None:
    if not isinstance(item, dict):
        return None
    progress_chapter_id = int(item.get("chapterId") or item.get("chapter_id") or chapter_id)
    return KavitaReadProgress(
        chapter_id=progress_chapter_id,
        pages_read=int(item.get("pagesRead") or item.get("pageNum") or item.get("page") or 0),
        pages_total=int(
            item.get("pagesTotal")
            or item.get("pages")
            or item.get("totalPages")
            or pages_total
            or 0
        ),
    )


def configured_kavita_client(
    local_library_root: Path | None = None,
    kavita_library_root: Path | None = None,
) -> KavitaClient:
    return KavitaClient(
        settings.kavita_url,
        settings.kavita_api_key,
        local_library_root=local_library_root,
        kavita_library_root=kavita_library_root,
    )
