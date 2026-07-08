from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from app.domain import ChapterItem, SeriesItem


class ChapterTemporarilyUnavailable(RuntimeError):
    def __init__(self, message: str, retry_after=None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class SourceAdapter(ABC):
    source: str
    base_url: str

    @abstractmethod
    async def list_recent(self) -> list[SeriesItem]:
        raise NotImplementedError

    @abstractmethod
    async def get_chapters(self, source_series: SeriesItem) -> list[ChapterItem]:
        raise NotImplementedError

    @abstractmethod
    async def download_chapter_pages(self, chapter: ChapterItem) -> list[bytes]:
        raise NotImplementedError

    async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
        for page in await self.download_chapter_pages(chapter):
            yield page

    async def aclose(self) -> None:
        return None
