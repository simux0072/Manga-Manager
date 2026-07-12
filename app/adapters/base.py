from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass

from app.domain import ChapterItem, SeriesItem


@dataclass(frozen=True)
class FrontierSentinel:
    source_id: str
    latest_chapter: str


class ChapterTemporarilyUnavailable(RuntimeError):
    def __init__(self, message: str, retry_after=None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class SourceRateLimited(RuntimeError):
    def __init__(self, message: str, retry_after=None, source: str = "") -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.source = source


class SourceAdapter(ABC):
    source: str
    base_url: str

    @abstractmethod
    async def list_recent(self) -> list[SeriesItem]:
        raise NotImplementedError

    async def list_recent_frontier(self, sentinels: list[FrontierSentinel]) -> list[SeriesItem]:
        return await self.list_recent()

    @abstractmethod
    async def get_chapters(self, source_series: SeriesItem) -> list[ChapterItem]:
        raise NotImplementedError

    @abstractmethod
    async def download_chapter_pages(self, chapter: ChapterItem) -> list[bytes]:
        raise NotImplementedError

    async def iter_chapter_pages(
        self,
        chapter: ChapterItem,
        progress: Callable[[int, int, int], None] | None = None,
    ) -> AsyncIterator[bytes]:
        pages = await self.download_chapter_pages(chapter)
        total_bytes = 0
        for index, page in enumerate(pages, start=1):
            total_bytes += len(page)
            if progress:
                progress(index, len(pages), total_bytes)
            yield page

    async def aclose(self) -> None:
        return None
