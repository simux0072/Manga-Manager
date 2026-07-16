from app.adapters.asura import AsuraAdapter
from app.adapters.base import SourceAdapter
from app.adapters.kingofshojo import KingOfShojoAdapter
from app.adapters.mangafire import MangaFireAdapter


def adapter_for_source(source: str) -> SourceAdapter | None:
    if source == "asura":
        return AsuraAdapter()
    if source == "mangafire":
        return MangaFireAdapter()
    if source == "kingofshojo":
        return KingOfShojoAdapter()
    return None


class SourceAdapterPool:
    """Process-local adapters that retain HTTP connection pools between jobs."""

    def __init__(self) -> None:
        self._adapters: dict[str, SourceAdapter] = {}

    def get(self, source: str) -> SourceAdapter | None:
        if source not in self._adapters:
            adapter = adapter_for_source(source)
            if adapter is None:
                return None
            self._adapters[source] = adapter
        return self._adapters[source]

    async def aclose(self) -> None:
        adapters, self._adapters = list(self._adapters.values()), {}
        for adapter in adapters:
            await adapter.aclose()
