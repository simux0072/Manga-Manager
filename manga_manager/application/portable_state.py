from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from manga_manager.application.download_plans import DownloadPlanCoordinator, TRACKED_STATES
from manga_manager.domain.catalog import canonical_chapter_number, chapter_sort_number, normalize_title
from manga_manager.domain.jobs import JobKind, SourceRefreshPayload
from manga_manager.domain.matching import provider_identities_equivalent
from manga_manager.domain.providers import provider_names
from manga_manager.infrastructure.db_models import (
    CatalogAlternateSourceListing,
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogExternalIdentifier,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    CatalogSourceState,
    ChapterArtifact,
)
from manga_manager.infrastructure.job_queue import JobQueue


FORMAT_NAME = "manga-manager-portable-state"
FORMAT_VERSION = 1
STATUS_RANK = {"untracked": 0, "paused": 1, "caught_up": 2, "interested": 3, "reading": 4}
READING_RANK = {"unread": 0, "reading": 1, "read": 2}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PortableModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PortableIdentityRef(PortableModel):
    source: str = Field(min_length=1, max_length=50, pattern=r"^[a-z0-9_-]+$")
    source_id: str = Field(min_length=1, max_length=500)

    @property
    def key(self) -> tuple[str, str]:
        return self.source, normalized_identity(self.source, self.source_id)

    @model_validator(mode="after")
    def provider_is_supported(self) -> "PortableIdentityRef":
        if self.source not in provider_names():
            raise ValueError(f"unsupported portable provider: {self.source}")
        return self


class PortableAlternate(PortableModel):
    source_id: str = Field(min_length=1, max_length=500)
    title: str = Field(default="", max_length=500)
    url: str = Field(default="", max_length=4096)

    @model_validator(mode="after")
    def public_url_is_valid(self) -> "PortableAlternate":
        if self.url and urlsplit(self.url).scheme not in {"http", "https"}:
            raise ValueError("portable URLs must use HTTP or HTTPS")
        return self


class PortableSource(PortableIdentityRef):
    title: str = Field(min_length=1, max_length=500)
    url: str = Field(min_length=1, max_length=4096)
    description: str = ""
    revision_override: str = Field(default="", max_length=20)
    aliases: list[str] = Field(default_factory=list)
    external_ids: dict[str, str] = Field(default_factory=dict)
    alternates: list[PortableAlternate] = Field(default_factory=list)

    @model_validator(mode="after")
    def details_are_bounded(self) -> "PortableSource":
        if urlsplit(self.url).scheme not in {"http", "https"}:
            raise ValueError("portable URLs must use HTTP or HTTPS")
        if any(not value or len(value) > 500 for value in self.aliases):
            raise ValueError("portable aliases must contain 1 to 500 characters")
        if any(
            not provider
            or len(provider) > 50
            or not value
            or len(value) > 200
            for provider, value in self.external_ids.items()
        ):
            raise ValueError("portable external identifiers are invalid")
        return self


class PortableReadingState(PortableModel):
    chapter: str = Field(min_length=1, max_length=100)
    status: Literal["unread", "reading", "read"]
    read_at: datetime | None = None


class PortableSeries(PortableModel):
    title: str = Field(min_length=1, max_length=500)
    status: Literal["untracked", "interested", "reading", "caught_up", "paused"]
    downloaded_on_export: bool = False
    description: str = ""
    aliases: list[str] = Field(default_factory=list)
    sources: list[PortableSource] = Field(min_length=1)
    reading: list[PortableReadingState] = Field(default_factory=list)

    @model_validator(mode="after")
    def providers_are_unique(self) -> "PortableSeries":
        providers = [source.source for source in self.sources]
        if len(providers) != len(set(providers)):
            raise ValueError("a portable merged series cannot contain a provider twice")
        return self


class PortableSeparation(PortableModel):
    left: PortableIdentityRef
    right: PortableIdentityRef

    @model_validator(mode="after")
    def identities_are_distinct(self) -> "PortableSeparation":
        if self.left.key == self.right.key:
            raise ValueError("a portable separation must contain two identities")
        return self


class PortableProviderPreference(PortableModel):
    source: str = Field(min_length=1, max_length=50, pattern=r"^[a-z0-9_-]+$")
    enabled: bool


class PortableState(PortableModel):
    format: Literal[FORMAT_NAME] = FORMAT_NAME
    version: Literal[FORMAT_VERSION] = FORMAT_VERSION
    exported_at: datetime
    series: list[PortableSeries]
    separations: list[PortableSeparation] = Field(default_factory=list)
    providers: list[PortableProviderPreference] = Field(default_factory=list)

    @model_validator(mode="after")
    def identities_are_globally_unique(self) -> "PortableState":
        owners: dict[tuple[str, str], str] = {}
        for series in self.series:
            for identity in series.sources:
                if identity.key in owners:
                    raise ValueError(
                        f"portable identity {identity.source}:{identity.source_id} occurs in "
                        "more than one canonical series"
                    )
                owners[identity.key] = series.title
        missing = {
            identity.key
            for separation in self.separations
            for identity in (separation.left, separation.right)
            if identity.key not in owners
        }
        if missing:
            rendered = ", ".join(f"{source}:{source_id}" for source, source_id in sorted(missing))
            raise ValueError(f"separations reference identities missing from series: {rendered}")
        return self


class PortableImportReport(PortableModel):
    applied: bool
    series: int
    sources: int
    separations: int
    providers: int
    create_series: int = 0
    create_sources: int = 0
    merge_series: int = 0
    reading_states: int = 0
    refresh_jobs: int = 0
    download_plans: int = 0
    conflicts: list[str] = Field(default_factory=list)


class PortableStateConflict(RuntimeError):
    pass


def normalized_identity(source: str, source_id: str) -> str:
    if source == "asura":
        from app.adapters.asura import split_asura_source_id

        return split_asura_source_id(source_id)[0]
    return source_id.strip()


def identity_key(identity: CatalogSourceSeries) -> tuple[str, str]:
    return identity.source, normalized_identity(identity.source, identity.source_id)


def _selected_series_ids(session: Session) -> set[int]:
    selected = set(
        session.scalars(
            select(CatalogSeries.id).where(CatalogSeries.status.in_(TRACKED_STATES))
        ).all()
    )
    selected.update(
        session.scalars(
            select(CatalogChapter.series_id)
            .join(ChapterArtifact, ChapterArtifact.chapter_id == CatalogChapter.id)
            .where(ChapterArtifact.state == "active")
            .distinct()
        ).all()
    )
    selected.update(
        series_id
        for series_id, count in session.execute(
            select(CatalogSourceSeries.series_id, func.count())
            .group_by(CatalogSourceSeries.series_id)
            .having(func.count() > 1)
        )
    )
    rejected = session.scalars(
        select(CatalogMatchDecision).where(CatalogMatchDecision.decision == "rejected")
    ).all()
    rejected_identity_ids = {
        identity_id
        for decision in rejected
        for identity_id in (
            decision.left_source_series_id,
            decision.right_source_series_id,
        )
    }
    if rejected_identity_ids:
        selected.update(
            session.scalars(
                select(CatalogSourceSeries.series_id).where(
                    CatalogSourceSeries.id.in_(rejected_identity_ids)
                )
            ).all()
        )
    return selected


def export_portable_state(session: Session) -> PortableState:
    selected_ids = _selected_series_ids(session)
    rows = session.scalars(
        select(CatalogSeries)
        .where(CatalogSeries.id.in_(selected_ids or {-1}))
        .order_by(CatalogSeries.normalized_title, CatalogSeries.id)
    ).all()
    series_items: list[PortableSeries] = []
    exported_identity_ids: set[int] = set()
    for series in rows:
        source_rows = session.scalars(
            select(CatalogSourceSeries)
            .where(CatalogSourceSeries.series_id == series.id)
            .order_by(CatalogSourceSeries.source, CatalogSourceSeries.source_id)
        ).all()
        if not source_rows:
            continue
        exported_identity_ids.update(row.id for row in source_rows)
        downloaded = bool(
            session.scalar(
                select(ChapterArtifact.id)
                .join(CatalogChapter, CatalogChapter.id == ChapterArtifact.chapter_id)
                .where(
                    CatalogChapter.series_id == series.id,
                    ChapterArtifact.state == "active",
                )
                .limit(1)
            )
        )
        restore_status = (
            "interested" if downloaded and series.status == "untracked" else series.status
        )
        aliases = list(
            session.scalars(
                select(CatalogSeriesAlias.display_value)
                .where(CatalogSeriesAlias.series_id == series.id)
                .order_by(CatalogSeriesAlias.normalized_value)
            ).all()
        )
        reading = [
            PortableReadingState(
                chapter=chapter.canonical_number,
                status=state.status,
                read_at=state.read_at,
            )
            for chapter, state in session.execute(
                select(CatalogChapter, CatalogChapterReadingState)
                .join(
                    CatalogChapterReadingState,
                    CatalogChapterReadingState.chapter_id == CatalogChapter.id,
                )
                .where(CatalogChapter.series_id == series.id)
                .order_by(CatalogChapter.sort_number, CatalogChapter.canonical_number)
            )
            if state.status != "unread" or state.read_at is not None
        ]
        sources = []
        for source in source_rows:
            source_aliases = list(
                session.scalars(
                    select(CatalogSeriesAlias.display_value)
                    .where(CatalogSeriesAlias.source_series_id == source.id)
                    .order_by(CatalogSeriesAlias.normalized_value)
                ).all()
            )
            external_ids = {
                provider: value
                for provider, value in session.execute(
                    select(
                        CatalogExternalIdentifier.provider,
                        CatalogExternalIdentifier.value,
                    )
                    .where(CatalogExternalIdentifier.source_series_id == source.id)
                    .order_by(CatalogExternalIdentifier.provider)
                )
            }
            alternates = [
                PortableAlternate(source_id=row.source_id, title=row.title, url=row.url)
                for row in session.scalars(
                    select(CatalogAlternateSourceListing)
                    .where(CatalogAlternateSourceListing.primary_source_series_id == source.id)
                    .order_by(CatalogAlternateSourceListing.source_id)
                )
            ]
            sources.append(
                PortableSource(
                    source=source.source,
                    source_id=source.source_id,
                    title=source.title or series.title,
                    url=source.url,
                    description=source.description or "",
                    revision_override=source.revision_override or "",
                    aliases=source_aliases,
                    external_ids=external_ids,
                    alternates=alternates,
                )
            )
        series_items.append(
            PortableSeries(
                title=series.title,
                status=restore_status,
                downloaded_on_export=downloaded,
                description=series.description or "",
                aliases=aliases,
                sources=sources,
                reading=reading,
            )
        )

    separations = []
    for decision in session.scalars(
        select(CatalogMatchDecision)
        .where(
            CatalogMatchDecision.decision == "rejected",
            CatalogMatchDecision.left_source_series_id.in_(exported_identity_ids or {-1}),
            CatalogMatchDecision.right_source_series_id.in_(exported_identity_ids or {-1}),
        )
        .order_by(CatalogMatchDecision.id)
    ):
        left = session.get(CatalogSourceSeries, decision.left_source_series_id)
        right = session.get(CatalogSourceSeries, decision.right_source_series_id)
        if left is None or right is None:
            continue
        separations.append(
            PortableSeparation(
                left=PortableIdentityRef(source=left.source, source_id=left.source_id),
                right=PortableIdentityRef(source=right.source, source_id=right.source_id),
            )
        )
    providers = [
        PortableProviderPreference(source=row.source, enabled=row.manual_enabled)
        for row in session.scalars(select(CatalogSourceState).order_by(CatalogSourceState.source))
    ]
    return PortableState(
        exported_at=utcnow(),
        series=series_items,
        separations=separations,
        providers=providers,
    )


def write_portable_state(state: PortableState, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(
        json.dumps(state.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.chmod(0o600)
    temporary.replace(output)


def load_portable_state(source: Path) -> PortableState:
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read portable state: {exc}") from exc
    return PortableState.model_validate(raw)


def _existing_identity_map(session: Session) -> dict[tuple[str, str], CatalogSourceSeries]:
    result: dict[tuple[str, str], CatalogSourceSeries] = {}
    for identity in session.scalars(select(CatalogSourceSeries).order_by(CatalogSourceSeries.id)):
        result.setdefault(identity_key(identity), identity)
    return result


def _resolve_existing_identity(
    portable: PortableSource,
    identities: dict[tuple[str, str], CatalogSourceSeries],
) -> CatalogSourceSeries | None:
    exact = identities.get(portable.key)
    if exact is not None:
        return exact
    matches = [
        identity
        for identity in identities.values()
        if identity.source == portable.source
        and provider_identities_equivalent(identity, portable)
    ]
    if len(matches) > 1:
        raise PortableStateConflict(
            f"multiple local identities match {portable.source}:{portable.source_id}"
        )
    return matches[0] if matches else None


def plan_portable_import(session: Session, state: PortableState) -> PortableImportReport:
    identities = _existing_identity_map(session)
    claimed_series: dict[int, str] = {}
    conflicts: list[str] = []
    create_series = 0
    create_sources = 0
    merge_series = 0
    for portable_series in state.series:
        resolved = {
            source.key: _resolve_existing_identity(source, identities)
            for source in portable_series.sources
        }
        existing_series_ids = {
            resolved[source.key].series_id
            for source in portable_series.sources
            if resolved[source.key] is not None
        }
        create_sources += sum(resolved[source.key] is None for source in portable_series.sources)
        if not existing_series_ids:
            create_series += 1
        else:
            merge_series += max(0, len(existing_series_ids) - 1)
        for series_id in existing_series_ids:
            previous = claimed_series.get(series_id)
            if previous is not None and previous != portable_series.title:
                conflicts.append(
                    f'local manga #{series_id} is claimed by both "{previous}" and '
                    f'"{portable_series.title}"'
                )
            claimed_series[series_id] = portable_series.title
    portable_owners = {
        source.key: portable_series.title
        for portable_series in state.series
        for source in portable_series.sources
    }
    for separation in state.separations:
        if portable_owners[separation.left.key] == portable_owners[separation.right.key]:
            conflicts.append(
                "snapshot marks two identities as both merged and separated: "
                f"{separation.left.source}:{separation.left.source_id} / "
                f"{separation.right.source}:{separation.right.source_id}"
            )
    return PortableImportReport(
        applied=False,
        series=len(state.series),
        sources=sum(len(series.sources) for series in state.series),
        separations=len(state.separations),
        providers=len(state.providers),
        create_series=create_series,
        create_sources=create_sources,
        merge_series=merge_series,
        reading_states=sum(len(series.reading) for series in state.series),
        conflicts=conflicts,
    )


def _merge_existing_series(session: Session, series_ids: set[int]) -> int:
    if len(series_ids) == 1:
        return next(iter(series_ids))
    from manga_manager.web.app import merge_canonical_series

    try:
        return merge_canonical_series(session, sorted(series_ids))
    except HTTPException as exc:
        raise PortableStateConflict(str(exc.detail)) from exc


def _upsert_alias(
    session: Session,
    *,
    series_id: int,
    value: str,
    source_series_id: int | None = None,
) -> None:
    normalized = normalize_title(value)
    if not normalized:
        return
    existing = session.scalar(
        select(CatalogSeriesAlias).where(
            CatalogSeriesAlias.series_id == series_id,
            CatalogSeriesAlias.normalized_value == normalized,
        )
    )
    if existing is None:
        session.add(
            CatalogSeriesAlias(
                series_id=series_id,
                source_series_id=source_series_id,
                display_value=value,
                normalized_value=normalized,
            )
        )


def _upsert_source_details(
    session: Session,
    canonical: CatalogSeries,
    source_row: CatalogSourceSeries,
    portable: PortableSource,
) -> None:
    source_row.series_id = canonical.id
    source_row.source_id = portable.source_id
    source_row.normalized_source_id = normalized_identity(portable.source, portable.source_id)
    source_row.title = portable.title
    source_row.normalized_title = normalize_title(portable.title)
    source_row.url = portable.url
    source_row.description = portable.description
    source_row.revision_override = portable.revision_override
    for value in portable.aliases:
        _upsert_alias(
            session,
            series_id=canonical.id,
            source_series_id=source_row.id,
            value=value,
        )
    for provider, value in portable.external_ids.items():
        existing = session.scalar(
            select(CatalogExternalIdentifier).where(
                CatalogExternalIdentifier.source_series_id == source_row.id,
                CatalogExternalIdentifier.provider == provider,
            )
        )
        conflict = session.scalar(
            select(CatalogExternalIdentifier).where(
                CatalogExternalIdentifier.provider == provider,
                CatalogExternalIdentifier.value == value,
            )
        )
        if existing is not None:
            if conflict is None or conflict.id == existing.id:
                existing.value = value
        elif conflict is None:
            session.add(
                CatalogExternalIdentifier(
                    series_id=canonical.id,
                    source_series_id=source_row.id,
                    provider=provider,
                    value=value,
                )
            )
    for alternate in portable.alternates:
        existing = session.scalar(
            select(CatalogAlternateSourceListing).where(
                CatalogAlternateSourceListing.source == portable.source,
                CatalogAlternateSourceListing.source_id == alternate.source_id,
            )
        )
        if existing is None:
            session.add(
                CatalogAlternateSourceListing(
                    primary_source_series_id=source_row.id,
                    source=portable.source,
                    source_id=alternate.source_id,
                    title=alternate.title or portable.title,
                    url=alternate.url or portable.url,
                    evidence_json={"origin": "portable_import"},
                )
            )


def _upsert_reading_state(
    session: Session, canonical: CatalogSeries, portable: PortableReadingState
) -> None:
    number = canonical_chapter_number(portable.chapter)
    chapter = session.scalar(
        select(CatalogChapter).where(
            CatalogChapter.series_id == canonical.id,
            CatalogChapter.canonical_number == number,
        )
    )
    if chapter is None:
        chapter = CatalogChapter(
            series_id=canonical.id,
            canonical_number=number,
            display_number=portable.chapter,
            sort_number=chapter_sort_number(number),
        )
        session.add(chapter)
        session.flush([chapter])
    state = session.get(CatalogChapterReadingState, chapter.id)
    if state is None:
        session.add(
            CatalogChapterReadingState(
                chapter_id=chapter.id,
                status=portable.status,
                read_at=portable.read_at,
            )
        )
    elif READING_RANK[portable.status] >= READING_RANK[state.status]:
        state.status = portable.status
        state.read_at = portable.read_at or state.read_at


def apply_portable_import(
    session: Session, state: PortableState, *, queue: JobQueue | None = None
) -> PortableImportReport:
    report = plan_portable_import(session, state)
    if report.conflicts:
        raise PortableStateConflict("; ".join(report.conflicts))
    queue = queue or JobQueue()
    identities = _existing_identity_map(session)
    restored: dict[tuple[str, str], CatalogSourceSeries] = {}
    refresh_jobs = 0
    download_plans = 0
    digest = hashlib.sha256(
        json.dumps(state.model_dump(mode="json"), sort_keys=True).encode()
    ).hexdigest()[:12]
    workflow_key = f"portable-import:{digest}"
    for portable_series in state.series:
        resolved = {
            source.key: _resolve_existing_identity(source, identities)
            for source in portable_series.sources
        }
        existing_series_ids = {
            resolved[source.key].series_id
            for source in portable_series.sources
            if resolved[source.key] is not None
        }
        if existing_series_ids:
            canonical_id = _merge_existing_series(session, existing_series_ids)
            canonical = session.get(CatalogSeries, canonical_id)
            if canonical is None:
                raise PortableStateConflict(f'could not restore "{portable_series.title}"')
        else:
            canonical = CatalogSeries(
                title=portable_series.title,
                normalized_title=normalize_title(portable_series.title),
                description=portable_series.description,
                status=portable_series.status,
            )
            session.add(canonical)
            session.flush([canonical])
        canonical.title = canonical.title or portable_series.title
        canonical.normalized_title = normalize_title(canonical.title)
        if not canonical.description and portable_series.description:
            canonical.description = portable_series.description
        if STATUS_RANK[portable_series.status] > STATUS_RANK[canonical.status]:
            canonical.status = portable_series.status
        for alias in portable_series.aliases:
            _upsert_alias(session, series_id=canonical.id, value=alias)
        for portable_source in portable_series.sources:
            source_row = resolved[portable_source.key]
            if source_row is None:
                source_row = CatalogSourceSeries(
                    series_id=canonical.id,
                    source=portable_source.source,
                    source_id=portable_source.source_id,
                    normalized_source_id=normalized_identity(
                        portable_source.source, portable_source.source_id
                    ),
                    title=portable_source.title,
                    normalized_title=normalize_title(portable_source.title),
                    url=portable_source.url,
                )
                session.add(source_row)
                session.flush([source_row])
                identities[portable_source.key] = source_row
            _upsert_source_details(session, canonical, source_row, portable_source)
            restored[portable_source.key] = source_row
        for reading in portable_series.reading:
            _upsert_reading_state(session, canonical, reading)
        session.flush()
        for portable_source in portable_series.sources:
            source_row = restored[portable_source.key]
            metadata = (
                {"asura_revision_override": portable_source.revision_override}
                if portable_source.revision_override
                else {}
            )
            _job, created = queue.enqueue(
                session,
                kind=JobKind.SOURCE_REFRESH,
                dedupe_key=f"refresh:{portable_source.source}:{portable_source.source_id}",
                payload=SourceRefreshPayload(
                    source=portable_source.source,
                    source_id=portable_source.source_id,
                    title=portable_source.title,
                    url=portable_source.url,
                    aliases=tuple(portable_source.aliases),
                    description=portable_source.description,
                    external_ids=portable_source.external_ids,
                    metadata=metadata,
                    workflow_key=workflow_key,
                    acquisition_critical=canonical.status in TRACKED_STATES,
                ),
                priority=10 if canonical.status in TRACKED_STATES else 55,
                max_attempts=4,
                source=portable_source.source,
                series_key=str(canonical.id),
                workflow_key=workflow_key,
                group_key=workflow_key,
                coalesce=True,
            )
            refresh_jobs += int(created)
        if canonical.status in TRACKED_STATES:
            DownloadPlanCoordinator(queue).track(session, canonical.id)
            download_plans += 1

    for separation in state.separations:
        left = restored[separation.left.key]
        right = restored[separation.right.key]
        if left.series_id == right.series_id:
            raise PortableStateConflict(
                "cannot restore a separation after its identities were already merged: "
                f"{left.source}:{left.source_id} / {right.source}:{right.source_id}"
            )
        left_id, right_id = sorted((left.id, right.id))
        decision = session.scalar(
            select(CatalogMatchDecision).where(
                CatalogMatchDecision.left_source_series_id == left_id,
                CatalogMatchDecision.right_source_series_id == right_id,
            )
        )
        if decision is None:
            decision = CatalogMatchDecision(
                left_source_series_id=left_id,
                right_source_series_id=right_id,
            )
            session.add(decision)
        decision.decision = "rejected"
        decision.decided_by = "portable_import"
        decision.decided_at = utcnow()
        decision.scorer_version = "portable-v1"
        decision.evidence_json = {"origin": "portable_import"}

    for preference in state.providers:
        provider = session.get(CatalogSourceState, preference.source)
        if provider is None:
            provider = CatalogSourceState(source=preference.source)
            session.add(provider)
        provider.manual_enabled = preference.enabled
        provider.updated_at = utcnow()
    session.flush()
    return report.model_copy(
        update={
            "applied": True,
            "refresh_jobs": refresh_jobs,
            "download_plans": download_plans,
        }
    )
