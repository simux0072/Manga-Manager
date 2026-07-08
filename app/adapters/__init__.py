from app.adapters.asura import AsuraAdapter
from app.adapters.base import SourceAdapter
from app.adapters.kingofshojo import KingOfShojoAdapter
from app.adapters.mangafire import MangaFireAdapter
from app.settings import settings


def enabled_source_names() -> list[str]:
    names: list[str] = []
    if settings.enable_asura:
        names.append("asura")
    if settings.enable_mangafire:
        names.append("mangafire")
    if settings.enable_kingofshojo:
        names.append("kingofshojo")
    return names


def adapter_for_source(source: str) -> SourceAdapter | None:
    if source not in enabled_source_names():
        return None
    if source == "asura":
        return AsuraAdapter()
    if source == "mangafire":
        return MangaFireAdapter()
    if source == "kingofshojo":
        return KingOfShojoAdapter()
    return None
