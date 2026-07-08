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


class KavitaClient:
    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

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
        kavita_folder_path = kavita_path_for_local(folder_path)
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
        folder_path=str(item.get("folderPath") or item.get("lowestFolderPath") or ""),
        mal_id=str(item.get("malId") or ""),
        anilist_id=str(item.get("aniListId") or ""),
    )


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
    )


def kavita_path_for_local(local_path: Path) -> Path:
    kavita_root = settings.kavita_library_root
    if kavita_root is None:
        return local_path
    try:
        relative = local_path.resolve().relative_to(settings.library_root.resolve())
    except ValueError:
        return local_path
    return kavita_root / relative


def local_path_for_kavita(kavita_path: str) -> Path:
    path = Path(kavita_path)
    kavita_root = settings.kavita_library_root
    if kavita_root is None:
        return path
    try:
        relative = path.resolve().relative_to(kavita_root.resolve())
    except ValueError:
        return path
    return settings.library_root / relative


def configured_kavita_client() -> KavitaClient:
    return KavitaClient(settings.kavita_url, settings.kavita_api_key)
