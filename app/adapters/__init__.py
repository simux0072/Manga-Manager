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
