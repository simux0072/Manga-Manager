from manga_manager.application.provider_duplicates import duplicate_identity_evidence
from manga_manager.infrastructure.db_models import CatalogSourceSeries


def identity(title: str, source_id: str) -> CatalogSourceSeries:
    return CatalogSourceSeries(
        series_id=1,
        source="mangafire",
        source_id=source_id,
        title=title,
        normalized_title=title.casefold(),
        url=f"https://mangafire.test/{source_id}",
    )


def test_close_cover_and_chapters_do_not_collapse_unrelated_provider_titles() -> None:
    evidence = duplicate_identity_evidence(
        identity("Alpha Journey", "alpha"),
        identity("Crimson Orchard", "crimson"),
        left_chapters={str(value) for value in range(1, 20)},
        right_chapters={str(value) for value in range(1, 20)},
        cover_hash_distance=1,
    )

    assert evidence["strong_cover_similarity"] is True
    assert evidence["strong_chapter_overlap"] is True
    assert evidence["title_tokens_agree"] is False
    assert evidence["equivalent"] is False


def test_close_cover_chapters_and_distinctive_title_tokens_collapse_duplicates() -> None:
    evidence = duplicate_identity_evidence(
        identity("The Regressed Mercenary's Machinations", "machinations"),
        identity("The Regressed Mercenary Has a Plan", "plan"),
        left_chapters={str(value) for value in range(1, 99)},
        right_chapters={str(value) for value in range(1, 99)},
        cover_hash_distance=4,
    )

    assert evidence["shared_title_tokens"] == ["mercenary", "regressed"]
    assert evidence["equivalent"] is True
