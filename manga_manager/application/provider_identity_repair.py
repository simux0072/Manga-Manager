from __future__ import annotations

from dataclasses import asdict, dataclass

from sqlalchemy import delete, func, select

from app.adapters.asura import asura_series_url, split_asura_source_id
from app.domain import title_similarity
from manga_manager.infrastructure.catalog_repository import CatalogRepository
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogObservation,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    CatalogSourceState,
)


@dataclass(slots=True)
class ProviderRepairRecord:
    stable_id: str
    identity_ids: list[int]
    action: str
    evidence: dict


class ProviderIdentityRepair:
    def audit(self, session, *, lock: bool = False) -> list[ProviderRepairRecord]:
        groups: dict[str, list[CatalogSourceSeries]] = {}
        identity_query = select(CatalogSourceSeries).where(CatalogSourceSeries.source == "asura")
        if lock:
            identity_query = identity_query.with_for_update()
        for row in session.scalars(identity_query):
            stable, _revision = split_asura_source_id(row.source_id or row.url)
            groups.setdefault(stable, []).append(row)
        records = []
        for stable, rows in sorted(groups.items()):
            if len(rows) == 1 and rows[0].source_id == stable:
                continue
            titles = min(
                (title_similarity(a.title, b.title) for a in rows for b in rows if a.id != b.id),
                default=1.0,
            )
            chapters = [self._chapters(session, row.id) for row in rows]
            overlap = min(
                (len(a & b) for index, a in enumerate(chapters) for b in chapters[index + 1:]),
                default=0,
            )
            covers = {row.cover_url for row in rows if row.cover_url}
            cover_agrees = len(covers) == 1 and bool(covers)
            safe = titles >= 0.85 and (
                overlap >= 1 or cover_agrees or len({row.series_id for row in rows}) == 1
            )
            records.append(ProviderRepairRecord(
                stable_id=stable,
                identity_ids=[row.id for row in rows],
                action="consolidate" if safe else "quarantine",
                evidence={
                    "minimum_title_similarity": round(titles, 4),
                    "chapter_overlap": overlap,
                    "cover_agrees": cover_agrees,
                },
            ))
        alias_query = (
            select(CatalogSeriesAlias)
            .join(
                CatalogSourceSeries,
                CatalogSourceSeries.id == CatalogSeriesAlias.source_series_id,
            )
            .where(
                CatalogSourceSeries.source == "asura",
                func.lower(CatalogSeriesAlias.display_value).in_(
                    ("asura scans home", "asura scans", "home", "manga list")
                ),
            )
        )
        if lock:
            alias_query = alias_query.with_for_update()
        stale_aliases = session.scalars(alias_query).all()
        if stale_aliases:
            records.append(
                ProviderRepairRecord(
                    stable_id="provider-aliases",
                    identity_ids=[row.id for row in stale_aliases],
                    action="cleanup_aliases",
                    evidence={"stale_alias_count": len(stale_aliases)},
                )
            )
        return records

    def apply(self, session, records: list[ProviderRepairRecord]) -> None:
        state = session.get(CatalogSourceState, "asura")
        revision = str((state.cursor_json if state else {}).get("global_revision") or "")
        repository = CatalogRepository()
        for record in records:
            if record.action == "cleanup_aliases":
                session.execute(
                    delete(CatalogSeriesAlias).where(
                        CatalogSeriesAlias.id.in_(record.identity_ids)
                    )
                )
                continue
            rows = [session.get(CatalogSourceSeries, value) for value in record.identity_ids]
            rows = [row for row in rows if row is not None]
            if record.action != "consolidate" or not rows:
                existing = session.scalar(
                    select(CatalogObservation).where(
                        CatalogObservation.source == "asura",
                        CatalogObservation.observation_type == "ambiguous_provider_identity",
                        CatalogObservation.source_key == record.stable_id,
                        CatalogObservation.state == "quarantined",
                    )
                )
                if existing is None:
                    session.add(CatalogObservation(
                        source="asura", observation_type="ambiguous_provider_identity",
                        source_key=record.stable_id, state="quarantined",
                        reason="provider identity repair requires manual review",
                        payload_json=asdict(record),
                    ))
                continue
            keeper = max(rows, key=lambda row: (len(self._chapters(session, row.id)), -row.id))
            series_ids = sorted({row.series_id for row in rows})
            if len(series_ids) > 1:
                from manga_manager.web.app import merge_canonical_series

                target_id = merge_canonical_series(session, series_ids)
                session.flush()
                rows = session.scalars(select(CatalogSourceSeries).where(
                    CatalogSourceSeries.series_id == target_id,
                    CatalogSourceSeries.source == "asura",
                )).all()
                keeper = rows[0]
            for duplicate in rows:
                if duplicate.id == keeper.id:
                    continue
                for release in session.scalars(select(CatalogChapterRelease).where(
                    CatalogChapterRelease.source_series_id == duplicate.id
                )):
                    existing = session.scalar(select(CatalogChapterRelease).where(
                        CatalogChapterRelease.source_series_id == keeper.id,
                        CatalogChapterRelease.source_release_id == release.source_release_id,
                    ))
                    if existing is None:
                        release.source_series_id = keeper.id
                    else:
                        from manga_manager.web.app import _replace_release_references

                        _replace_release_references(session, release, existing)
                session.execute(delete(CatalogSeriesAlias).where(
                    CatalogSeriesAlias.source_series_id == duplicate.id
                ))
                session.delete(duplicate)
            keeper.source_id = record.stable_id
            keeper.normalized_source_id = record.stable_id
            keeper.revision_override = ""
            keeper.url = asura_series_url(record.stable_id, revision)
            for release in session.scalars(select(CatalogChapterRelease).where(
                CatalogChapterRelease.source_series_id == keeper.id
            )):
                suffix = release.url.split("/chapter/", 1)[-1]
                release.url = f"{keeper.url.rstrip('/')}/chapter/{suffix.lstrip('/')}"
            repository._recompute_latest(session, keeper.series_id)

    @staticmethod
    def _chapters(session, source_series_id: int) -> set[str]:
        return set(session.scalars(
            select(CatalogChapter.canonical_number).join(CatalogChapterRelease).where(
                CatalogChapterRelease.source_series_id == source_series_id
            )
        ))
