from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from manga_manager.domain.catalog import normalize_title
from manga_manager.domain.matching import (
    canonical_source_url,
    normalized_source_id,
    provider_identities_equivalent,
)
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogCoverSignature,
    CatalogExternalIdentifier,
    CatalogSourceSeries,
)


MINIMUM_STRONG_CHAPTERS = 5
MINIMUM_CHAPTER_OVERLAP_RATIO = 0.9
MAXIMUM_EQUIVALENT_COVER_HASH_DISTANCE = 8
MINIMUM_SHARED_TITLE_TOKENS = 2
TITLE_TOKEN_STOP_WORDS = frozenset({"a", "an", "and", "of", "the", "to"})


def _hamming_distance(left: str, right: str) -> int:
    try:
        return (int(left, 16) ^ int(right, 16)).bit_count()
    except ValueError:
        return 64


def _signature_distance(left: CatalogCoverSignature | None, right: CatalogCoverSignature | None) -> int:
    left_hashes = [str(value) for value in (left.feature_json if left else {}).get("hashes", [])]
    right_hashes = [str(value) for value in (right.feature_json if right else {}).get("hashes", [])]
    return min(
        (_hamming_distance(a, b) for a in left_hashes for b in right_hashes),
        default=64,
    )


def _title_tokens(value: str) -> set[str]:
    return {
        token
        for token in (normalize_title(value) or "").split()
        if len(token) > 1 and token not in TITLE_TOKEN_STOP_WORDS
    }


@dataclass(frozen=True, slots=True)
class ProviderDuplicateGroup:
    source: str
    normalized_title: str
    identity_ids: tuple[int, ...]
    series_ids: tuple[int, ...]
    evidence: dict[str, Any]


def duplicate_identity_evidence(
    left: CatalogSourceSeries,
    right: CatalogSourceSeries,
    *,
    left_chapters: set[str],
    right_chapters: set[str],
    left_external_ids: dict[str, str] | None = None,
    right_external_ids: dict[str, str] | None = None,
    cover_hash_distance: int = 64,
) -> dict[str, Any]:
    """Classify two same-provider identities without trusting their title alone."""
    overlap = len(left_chapters & right_chapters)
    minimum = min(len(left_chapters), len(right_chapters))
    ratio = overlap / minimum if minimum else 0.0
    left_external_ids = left_external_ids or {}
    right_external_ids = right_external_ids or {}
    conflicting_external_ids = sorted(
        provider
        for provider in left_external_ids.keys() & right_external_ids.keys()
        if left_external_ids[provider] != right_external_ids[provider]
    )
    identity_equivalent = provider_identities_equivalent(left, right)
    left_title = normalize_title(left.title) or left.normalized_title
    right_title = normalize_title(right.title) or right.normalized_title
    title_agrees = bool(left_title) and left_title == right_title
    shared_title_tokens = sorted(_title_tokens(left.title) & _title_tokens(right.title))
    title_tokens_agree = len(shared_title_tokens) >= MINIMUM_SHARED_TITLE_TOKENS
    strong_chapter_overlap = (
        minimum >= MINIMUM_STRONG_CHAPTERS
        and ratio >= MINIMUM_CHAPTER_OVERLAP_RATIO
    )
    strong_cover_similarity = cover_hash_distance <= MAXIMUM_EQUIVALENT_COVER_HASH_DISTANCE
    equivalent = (
        identity_equivalent
        or (title_agrees and strong_chapter_overlap and not conflicting_external_ids)
        or (
            strong_cover_similarity
            and title_tokens_agree
            and strong_chapter_overlap
            and not conflicting_external_ids
        )
    )
    return {
        "equivalent": equivalent,
        "identity_equivalent": identity_equivalent,
        "title_agrees": title_agrees,
        "shared_title_tokens": shared_title_tokens,
        "title_tokens_agree": title_tokens_agree,
        "chapter_overlap": overlap,
        "compared_chapters": minimum,
        "chapter_overlap_ratio": round(ratio, 4),
        "strong_chapter_overlap": strong_chapter_overlap,
        "cover_hash_distance": cover_hash_distance,
        "strong_cover_similarity": strong_cover_similarity,
        "conflicting_external_ids": conflicting_external_ids,
    }


def provider_duplicate_groups(
    session: Session,
    *,
    series_ids: set[int] | None = None,
    excluded_sources: set[str] | None = None,
    lock: bool = False,
) -> list[ProviderDuplicateGroup]:
    """Find disjoint, mutually equivalent same-provider identity groups."""
    query = select(CatalogSourceSeries).order_by(CatalogSourceSeries.id)
    if series_ids is not None:
        query = query.where(CatalogSourceSeries.series_id.in_(series_ids or {-1}))
    if excluded_sources:
        query = query.where(CatalogSourceSeries.source.not_in(excluded_sources))
    if lock:
        query = query.with_for_update()
    identities = session.scalars(query).all()
    identity_ids = {row.id for row in identities}
    if not identity_ids:
        return []
    chapters: dict[int, set[str]] = {identity_id: set() for identity_id in identity_ids}
    for identity_id, canonical_number in session.execute(
        select(
            CatalogChapterRelease.source_series_id,
            CatalogChapter.canonical_number,
        )
        .join(CatalogChapter, CatalogChapter.id == CatalogChapterRelease.chapter_id)
        .where(
            CatalogChapterRelease.source_series_id.in_(identity_ids or {-1}),
            CatalogChapter.sort_number.is_not(None),
        )
    ):
        chapters[identity_id].add(canonical_number)
    external_ids: dict[int, dict[str, str]] = {
        identity_id: {} for identity_id in identity_ids
    }
    for identity_id, provider, value in session.execute(
        select(
            CatalogExternalIdentifier.source_series_id,
            CatalogExternalIdentifier.provider,
            CatalogExternalIdentifier.value,
        ).where(CatalogExternalIdentifier.source_series_id.in_(identity_ids or {-1}))
    ):
        external_ids[identity_id][provider] = value

    signatures = {
        row.source_series_id: row
        for row in session.scalars(
            select(CatalogCoverSignature).where(
                CatalogCoverSignature.source_series_id.in_(identity_ids)
            )
        )
    }

    # Exact titles remain a candidate source. A second source catches provider duplicates whose
    # translated titles differ: they must share a close cover hash and a strong numeric chapter
    # fingerprint before they can enter an equivalent cluster. This is intentionally only a
    # review-queue collapse signal; it never accepts a match automatically.
    candidate_pairs: set[tuple[int, int]] = set()
    title_buckets: dict[tuple[str, str], list[CatalogSourceSeries]] = {}
    for identity in identities:
        title = normalize_title(identity.title) or identity.normalized_title
        if title:
            title_buckets.setdefault((identity.source, title), []).append(identity)
    for rows in title_buckets.values():
        candidate_pairs.update(
            tuple(sorted((left.id, right.id))) for left, right in combinations(rows, 2)
        )

    identity_buckets: dict[tuple[str, str, str], list[CatalogSourceSeries]] = {}
    for identity in identities:
        normalized_id = normalized_source_id(identity.source_id)
        normalized_url = canonical_source_url(identity.url)
        if normalized_id:
            identity_buckets.setdefault(
                (identity.source, "source_id", normalized_id), []
            ).append(identity)
        if normalized_url:
            identity_buckets.setdefault(
                (identity.source, "url", normalized_url), []
            ).append(identity)
    for rows in identity_buckets.values():
        candidate_pairs.update(
            tuple(sorted((left.id, right.id))) for left, right in combinations(rows, 2)
        )

    cover_buckets: dict[tuple[str, int, str], list[int]] = {}
    identities_by_id = {row.id: row for row in identities}
    for identity in identities:
        signature = signatures.get(identity.id)
        hashes = [
            str(value)
            for value in (signature.feature_json if signature else {}).get("hashes", [])
        ]
        for hash_value in hashes:
            if len(hash_value) != 16:
                continue
            # Nine disjoint bands make this an exhaustive candidate filter for the eight-bit
            # Hamming threshold: at least one band must be identical. The final full-hash
            # comparison still decides equivalence, so a shared short band is never evidence.
            try:
                bits = f"{int(hash_value, 16):064b}"
            except ValueError:
                continue
            for band_index in range(9):
                start = band_index * 7
                end = 64 if band_index == 8 else start + 7
                band = bits[start:end]
                cover_buckets.setdefault((identity.source, band_index, band), []).append(
                    identity.id
                )
    for ids in cover_buckets.values():
        candidate_pairs.update(tuple(sorted(pair)) for pair in combinations(sorted(set(ids)), 2))

    pair_evidence: dict[tuple[int, int], dict[str, Any]] = {}
    for pair in sorted(candidate_pairs):
        left, right = (identities_by_id[value] for value in pair)
        if left.source != right.source:
            continue
        distance = _signature_distance(signatures.get(left.id), signatures.get(right.id))
        evidence = duplicate_identity_evidence(
            left,
            right,
            left_chapters=chapters[left.id],
            right_chapters=chapters[right.id],
            left_external_ids=external_ids[left.id],
            right_external_ids=external_ids[right.id],
            cover_hash_distance=distance,
        )
        if evidence["equivalent"]:
            pair_evidence[pair] = evidence

    result: list[ProviderDuplicateGroup] = []
    for source in sorted({row.source for row in identities}):
        rows = [row for row in identities if row.source == source]
        # Build deterministic cliques. Transitive title/chapter similarity alone is not enough:
        # every member must be independently equivalent to every other member in the group.
        groups: list[list[CatalogSourceSeries]] = []
        for row in rows:
            target = next(
                (
                    group
                    for group in groups
                    if all(
                        pair_evidence.get(tuple(sorted((row.id, member.id))), {}).get(
                            "equivalent", False
                        )
                        for member in group
                    )
                ),
                None,
            )
            if target is None:
                groups.append([row])
            else:
                target.append(row)
        for group in groups:
            if len(group) < 2:
                continue
            evidence_rows = [
                {
                    "identity_ids": list(pair),
                    **pair_evidence[pair],
                }
                for pair in combinations(sorted(row.id for row in group), 2)
            ]
            result.append(
                ProviderDuplicateGroup(
                    source=source,
                    normalized_title=normalize_title(group[0].title) or group[0].normalized_title,
                    identity_ids=tuple(row.id for row in group),
                    series_ids=tuple(sorted({row.series_id for row in group})),
                    evidence={
                        "policy": "provider_identity_or_cover_chapter_equivalence",
                        "pairs": evidence_rows,
                    },
                )
            )
    return result


def equivalent_series_clusters(
    session: Session,
    series_ids: set[int],
) -> dict[int, tuple[int, ...]]:
    """Map each equivalent canonical record to its complete provider-duplicate cluster."""
    parents = {series_id: series_id for series_id in series_ids}

    def find(value: int) -> int:
        while parents[value] != value:
            parents[value] = parents[parents[value]]
            value = parents[value]
        return value

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parents[max(left_root, right_root)] = min(left_root, right_root)

    for group in provider_duplicate_groups(session, series_ids=series_ids):
        for left, right in combinations(group.series_ids, 2):
            union(left, right)
    clusters: dict[int, list[int]] = {}
    for series_id in series_ids:
        clusters.setdefault(find(series_id), []).append(series_id)
    by_series: dict[int, tuple[int, ...]] = {}
    for values in clusters.values():
        cluster = tuple(sorted(values))
        for series_id in values:
            by_series[series_id] = cluster
    return by_series
