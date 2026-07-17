from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    CheckConstraint,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from manga_manager.domain.jobs import JobKind, JobState


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class JobBase(DeclarativeBase):
    pass


ACTIVE_STATES_SQL = "'queued', 'leased', 'retry_wait'"
JOB_STATES_SQL = ", ".join(f"'{state.value}'" for state in JobState)
JOB_KINDS_SQL = ", ".join(f"'{kind.value}'" for kind in JobKind)


class WorkJob(JobBase):
    __tablename__ = "job"
    __table_args__ = (
        CheckConstraint(f"kind IN ({JOB_KINDS_SQL})", name="ck_job_kind"),
        CheckConstraint(f"status IN ({JOB_STATES_SQL})", name="ck_job_status"),
        CheckConstraint("dedupe_key <> ''", name="ck_job_dedupe_key_not_empty"),
        CheckConstraint("attempts >= 0", name="ck_job_attempts_nonnegative"),
        CheckConstraint("max_attempts >= 1", name="ck_job_max_attempts_positive"),
        CheckConstraint(
            "status <> 'leased' OR (lease_owner <> '' AND lease_expires_at IS NOT NULL)",
            name="ck_job_lease_fields",
        ),
        Index(
            "uq_job_active_dedupe",
            "kind",
            "dedupe_key",
            unique=True,
            sqlite_where=text(f"status IN ({ACTIVE_STATES_SQL})"),
            postgresql_where=text(f"status IN ({ACTIVE_STATES_SQL})"),
        ),
        Index(
            "ix_job_claim",
            "status",
            "available_at",
            "priority",
            "created_at",
        ),
        Index("ix_job_lease_expiry", "status", "lease_expires_at"),
        Index("ix_job_pool_claim", "pool", "status", "available_at", "priority"),
        Index("ix_job_source_status", "source", "status"),
        Index("ix_job_cycle_group_status", "cycle_id", "group_key", "status"),
        Index("ix_job_workflow_status", "workflow_key", "status"),
        Index(
            "uq_job_leased_chapter_series",
            "series_key",
            unique=True,
            sqlite_where=text(
                "kind IN ('chapter_download', 'library_repair') "
                "AND status = 'leased' AND series_key <> ''"
            ),
            postgresql_where=text(
                "kind IN ('chapter_download', 'library_repair') "
                "AND status = 'leased' AND series_key <> ''"
            ),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[str] = mapped_column(String(30), index=True)
    dedupe_key: Mapped[str] = mapped_column(String(500))
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"),
        default=dict,
    )
    source: Mapped[str] = mapped_column(String(50), default="", index=True)
    series_key: Mapped[str] = mapped_column(String(100), default="", index=True)
    pool: Mapped[str] = mapped_column(String(50), default="maintenance", index=True)
    cycle_id: Mapped[int | None] = mapped_column(
        ForeignKey("workload_cycle.id", ondelete="SET NULL"), nullable=True, index=True
    )
    workflow_key: Mapped[str] = mapped_column(String(100), default="", index=True)
    group_key: Mapped[str] = mapped_column(String(500), default="", index=True)
    logical_units: Mapped[int] = mapped_column(Integer, default=1)
    pending_payload: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    status: Mapped[str] = mapped_column(String(20), default=JobState.QUEUED.value, index=True)
    priority: Mapped[int] = mapped_column(Integer, default=100)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    lease_owner: Mapped[str] = mapped_column(String(200), default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str] = mapped_column(String(100), default="")
    error_message: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    progress_phase: Mapped[str] = mapped_column(String(50), default="")
    progress_current: Mapped[int] = mapped_column(Integer, default=0)
    progress_total: Mapped[int] = mapped_column(Integer, default=0)
    progress_unit: Mapped[str] = mapped_column(String(30), default="")
    progress_bytes: Mapped[int] = mapped_column(Integer, default=0)
    progress_message: Mapped[str] = mapped_column(Text, default="")
    progress_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class WorkloadCycle(JobBase):
    __tablename__ = "workload_cycle"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'settled')", name="ck_workload_cycle_status"),
        Index(
            "uq_workload_cycle_active",
            "status",
            unique=True,
            sqlite_where=text("status = 'active'"),
            postgresql_where=text("status = 'active'"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(String(20), default="active", index=True)
    total_units: Mapped[int] = mapped_column(Integer, default=0)
    successful_units: Mapped[int] = mapped_column(Integer, default=0)
    failed_units: Mapped[int] = mapped_column(Integer, default=0)
    cancelled_units: Mapped[int] = mapped_column(Integer, default=0)
    added_units: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class JobDailyAggregate(JobBase):
    __tablename__ = "job_daily_aggregate"
    __table_args__ = (
        UniqueConstraint(
            "day",
            "kind",
            "source",
            "status",
            "error_code",
            name="uq_job_daily_aggregate_bucket",
        ),
        Index("ix_job_daily_aggregate_day", "day"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    day: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    kind: Mapped[str] = mapped_column(String(30))
    source: Mapped[str] = mapped_column(String(50), default="")
    status: Mapped[str] = mapped_column(String(20))
    error_code: Mapped[str] = mapped_column(String(100), default="")
    job_count: Mapped[int] = mapped_column(Integer, default=0)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    duration_seconds: Mapped[int] = mapped_column(BigInteger, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class JobPermit(JobBase):
    __tablename__ = "job_permit"
    __table_args__ = (
        UniqueConstraint("job_id", "pool", name="uq_job_permit_job_pool"),
        Index("ix_job_permit_pool_expiry", "pool", "lease_expires_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("job.id", ondelete="CASCADE"), nullable=False, index=True
    )
    pool: Mapped[str] = mapped_column(String(50), nullable=False)
    owner: Mapped[str] = mapped_column(String(200), nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class StorageState(JobBase):
    __tablename__ = "storage_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    paused: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    free_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    min_free_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    reserved_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    reason: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class StorageReservation(JobBase):
    __tablename__ = "storage_reservation"
    __table_args__ = (Index("ix_storage_reservation_expiry", "lease_expires_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("job.id", ondelete="CASCADE"), unique=True, nullable=False, index=True
    )
    owner: Mapped[str] = mapped_column(String(200), nullable=False)
    reserved_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class JobEvent(JobBase):
    __tablename__ = "job_event"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('enqueued', 'leased', 'progress', 'retry_scheduled', 'succeeded', "
            "'failed', 'cancelled', 'released', 'lease_expired', 'rerouted')",
            name="ck_job_event_type",
        ),
        Index("ix_job_event_job_created", "job_id", "created_at"),
        Index("ix_job_event_created", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("job.id", ondelete="CASCADE"), nullable=False, index=True
    )
    event_type: Mapped[str] = mapped_column(String(30))
    status: Mapped[str] = mapped_column(String(20))
    owner: Mapped[str] = mapped_column(String(200), default="")
    message: Mapped[str] = mapped_column(Text, default="")
    details: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"),
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class WorkerHeartbeat(JobBase):
    __tablename__ = "worker_heartbeat"
    __table_args__ = (
        CheckConstraint(
            "status IN ('starting', 'running', 'draining', 'stopped')",
            name="ck_worker_heartbeat_status",
        ),
        Index("ix_worker_heartbeat_seen", "heartbeat_at"),
    )

    worker_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    status: Mapped[str] = mapped_column(String(20), default="starting")
    active_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("job.id", ondelete="SET NULL"), nullable=True, index=True
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"),
        default=dict,
    )


def new_storage_key() -> str:
    return uuid4().hex


class CatalogSeries(JobBase):
    __tablename__ = "series_v2"
    __table_args__ = (
        CheckConstraint(
            "status IN ('untracked', 'interested', 'reading', 'caught_up', 'paused')",
            name="ck_series_v2_status",
        ),
        CheckConstraint(
            "integrity_state IN ('unknown', 'healthy', 'attention', 'quarantined')",
            name="ck_series_v2_integrity_state",
        ),
        Index("ix_series_v2_latest_cursor", "latest_release_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    storage_key: Mapped[str] = mapped_column(
        String(64), unique=True, index=True, default=new_storage_key
    )
    title: Mapped[str] = mapped_column(String(500), index=True)
    normalized_title: Mapped[str] = mapped_column(String(500), index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    cover_url: Mapped[str] = mapped_column(Text, default="")
    cover_checksum: Mapped[str] = mapped_column(String(64), default="")
    cover_relative_path: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(30), default="untracked", index=True)
    integrity_state: Mapped[str] = mapped_column(String(20), default="unknown", index=True)
    latest_release_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    latest_release_number: Mapped[str] = mapped_column(String(100), default="")
    latest_release_source: Mapped[str] = mapped_column(String(50), default="")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    kavita_series_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    kavita_library_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kavita_synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    kavita_cover_checksum: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogSourceSeries(JobBase):
    __tablename__ = "source_series_v2"
    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_source_series_v2_identity"),
        UniqueConstraint("series_id", "source", name="uq_source_series_v2_series_source"),
        Index("ix_source_series_v2_source_checked", "source", "last_checked_at"),
        Index("ix_source_series_v2_normalized_identity", "source", "normalized_source_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(
        ForeignKey("series_v2.id", ondelete="CASCADE"), index=True
    )
    source: Mapped[str] = mapped_column(String(50), index=True)
    source_id: Mapped[str] = mapped_column(String(500))
    normalized_source_id: Mapped[str] = mapped_column(String(500), default="")
    revision_override: Mapped[str] = mapped_column(String(20), default="")
    title: Mapped[str] = mapped_column(String(500))
    normalized_title: Mapped[str] = mapped_column(String(500), index=True)
    url: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text, default="")
    cover_url: Mapped[str] = mapped_column(Text, default="")
    popularity: Mapped[float] = mapped_column(default=0.0)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    last_checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    detail_fetched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    observation_version: Mapped[str] = mapped_column(String(100), default="")
    observation_seen_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class CatalogAlternateSourceListing(JobBase):
    __tablename__ = "alternate_source_listing_v2"
    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_alternate_source_listing_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    primary_source_series_id: Mapped[int] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), index=True
    )
    source: Mapped[str] = mapped_column(String(50), index=True)
    source_id: Mapped[str] = mapped_column(String(500))
    title: Mapped[str] = mapped_column(String(500))
    url: Mapped[str] = mapped_column(Text)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogCoverFingerprint(JobBase):
    __tablename__ = "cover_fingerprint_v2"
    __table_args__ = (
        UniqueConstraint(
            "source_series_id", "algorithm", name="uq_cover_fingerprint_v2_source_algorithm"
        ),
        Index("ix_cover_fingerprint_v2_hash", "algorithm", "hash_hex"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_series_id: Mapped[int] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), index=True
    )
    algorithm: Mapped[str] = mapped_column(String(40), default="dhash-crop-v2")
    hash_hex: Mapped[str] = mapped_column(String(128), index=True)
    content_sha256: Mapped[str] = mapped_column(String(64), default="")
    width: Mapped[int] = mapped_column(Integer, default=0)
    height: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogCoverAsset(JobBase):
    __tablename__ = "cover_asset_v2"

    source_series_id: Mapped[int] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), primary_key=True
    )
    content_checksum: Mapped[str] = mapped_column(String(64), index=True)
    relative_path: Mapped[str] = mapped_column(Text)
    content_type: Mapped[str] = mapped_column(String(100), default="")
    source_url: Mapped[str] = mapped_column(Text, default="")
    width: Mapped[int] = mapped_column(Integer, default=0)
    height: Mapped[int] = mapped_column(Integer, default=0)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogCoverSignature(JobBase):
    __tablename__ = "cover_signature_v2"
    __table_args__ = (Index("ix_cover_signature_algorithm", "algorithm_version"),)

    source_series_id: Mapped[int] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), primary_key=True
    )
    algorithm_version: Mapped[str] = mapped_column(String(50))
    hash_band_0: Mapped[str] = mapped_column(String(16), default="", index=True)
    hash_band_1: Mapped[str] = mapped_column(String(16), default="", index=True)
    hash_band_2: Mapped[str] = mapped_column(String(16), default="", index=True)
    hash_band_3: Mapped[str] = mapped_column(String(16), default="", index=True)
    feature_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    keypoints_blob: Mapped[bytes] = mapped_column(LargeBinary, default=b"")
    descriptors_blob: Mapped[bytes] = mapped_column(LargeBinary, default=b"")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogSeriesAlias(JobBase):
    __tablename__ = "series_alias_v2"
    __table_args__ = (
        UniqueConstraint("series_id", "normalized_value", name="uq_series_alias_v2_value"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(
        ForeignKey("series_v2.id", ondelete="CASCADE"), index=True
    )
    source_series_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), nullable=True, index=True
    )
    display_value: Mapped[str] = mapped_column(String(500))
    normalized_value: Mapped[str] = mapped_column(String(500), index=True)


class CatalogExternalIdentifier(JobBase):
    __tablename__ = "external_identifier_v2"
    __table_args__ = (
        UniqueConstraint("provider", "value", name="uq_external_identifier_v2_provider_value"),
        UniqueConstraint(
            "source_series_id", "provider", name="uq_external_identifier_v2_source_provider"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(
        ForeignKey("series_v2.id", ondelete="CASCADE"), index=True
    )
    source_series_id: Mapped[int] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), index=True
    )
    provider: Mapped[str] = mapped_column(String(50), index=True)
    value: Mapped[str] = mapped_column(String(200), index=True)


class CatalogChapter(JobBase):
    __tablename__ = "chapter_v2"
    __table_args__ = (
        UniqueConstraint("series_id", "canonical_number", name="uq_chapter_v2_series_number"),
        Index("ix_chapter_v2_series_sort", "series_id", "sort_number"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(
        ForeignKey("series_v2.id", ondelete="CASCADE"), index=True
    )
    canonical_number: Mapped[str] = mapped_column(String(100))
    display_number: Mapped[str] = mapped_column(String(100))
    sort_number: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    title: Mapped[str] = mapped_column(String(500), default="")
    kavita_chapter_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    kavita_volume_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kavita_mapped_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    kavita_cover_checksum: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogChapterReadingState(JobBase):
    __tablename__ = "chapter_reading_state_v2"
    __table_args__ = (
        CheckConstraint(
            "status IN ('unread', 'reading', 'read')",
            name="ck_chapter_reading_state_v2_status",
        ),
        Index("ix_chapter_reading_state_v2_status_updated", "status", "updated_at"),
    )

    chapter_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_v2.id", ondelete="CASCADE"), primary_key=True
    )
    status: Mapped[str] = mapped_column(String(20), default="unread", index=True)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SeriesDownloadPlan(JobBase):
    __tablename__ = "series_download_plan"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'complete', 'cancelled')",
            name="ck_series_download_plan_status",
        ),
        CheckConstraint(
            "phase IN ('priority', 'backfill', 'complete', 'cancelled')",
            name="ck_series_download_plan_phase",
        ),
        Index("ix_series_download_plan_status_phase", "status", "phase"),
    )

    series_id: Mapped[int] = mapped_column(
        ForeignKey("series_v2.id", ondelete="CASCADE"), primary_key=True
    )
    status: Mapped[str] = mapped_column(String(20), default="active", index=True)
    phase: Mapped[str] = mapped_column(String(20), default="priority", index=True)
    total_chapters: Mapped[int] = mapped_column(Integer, default=0)
    satisfied_chapters: Mapped[int] = mapped_column(Integer, default=0)
    attention_chapters: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ChapterDownloadIntent(JobBase):
    __tablename__ = "chapter_download_intent"
    __table_args__ = (
        CheckConstraint(
            "tier IN ('current', 'priority', 'backfill')",
            name="ck_chapter_download_intent_tier",
        ),
        CheckConstraint(
            "state IN ('blocked', 'pending', 'queued', 'satisfied', 'attention', 'cancelled')",
            name="ck_chapter_download_intent_state",
        ),
        UniqueConstraint("series_id", "chapter_id", name="uq_chapter_download_intent_chapter"),
        Index("ix_chapter_download_intent_plan_state", "series_id", "tier", "state"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(
        ForeignKey("series_v2.id", ondelete="CASCADE"), index=True
    )
    chapter_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_v2.id", ondelete="CASCADE"), index=True
    )
    tier: Mapped[str] = mapped_column(String(20), default="backfill", index=True)
    state: Mapped[str] = mapped_column(String(20), default="blocked", index=True)
    job_id: Mapped[int | None] = mapped_column(
        ForeignKey("job.id", ondelete="SET NULL"), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogChapterRelease(JobBase):
    __tablename__ = "chapter_release_v2"
    __table_args__ = (
        UniqueConstraint(
            "source_series_id", "source_release_id", name="uq_chapter_release_v2_identity"
        ),
        Index("ix_chapter_release_v2_source_published", "source", "published_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_v2.id", ondelete="CASCADE"), index=True
    )
    source_series_id: Mapped[int] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), index=True
    )
    source: Mapped[str] = mapped_column(String(50), index=True)
    source_release_id: Mapped[str] = mapped_column(String(500))
    title: Mapped[str] = mapped_column(String(500), default="")
    url: Mapped[str] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    downloadable_after: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class ChapterReleaseAttempt(JobBase):
    __tablename__ = "chapter_release_attempt"
    __table_args__ = (
        CheckConstraint(
            "outcome IN ('failed', 'fallback_queued', 'fallback_succeeded', 'succeeded', 'upgraded')",
            name="ck_chapter_release_attempt_outcome",
        ),
        Index("ix_chapter_release_attempt_chapter_created", "chapter_id", "created_at"),
        Index("ix_chapter_release_attempt_release_outcome", "chapter_release_id", "outcome"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_v2.id", ondelete="CASCADE"), index=True
    )
    chapter_release_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_release_v2.id", ondelete="CASCADE"), index=True
    )
    job_id: Mapped[int | None] = mapped_column(
        ForeignKey("job.id", ondelete="SET NULL"), nullable=True, index=True
    )
    source: Mapped[str] = mapped_column(String(50), index=True)
    outcome: Mapped[str] = mapped_column(String(30), index=True)
    error_code: Mapped[str] = mapped_column(String(100), default="")
    error_message: Mapped[str] = mapped_column(Text, default="")
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    details_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogSourceState(JobBase):
    __tablename__ = "source_state_v2"
    __table_args__ = (
        CheckConstraint(
            "health_status IN ('healthy', 'degraded', 'cooldown')",
            name="ck_source_state_v2_health",
        ),
    )

    source: Mapped[str] = mapped_column(String(50), primary_key=True)
    manual_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    health_status: Mapped[str] = mapped_column(String(20), default="healthy", index=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str] = mapped_column(Text, default="")
    cursor_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    frontier_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=list
    )
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_request_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ProviderPolicy(JobBase):
    __tablename__ = "provider_policy"

    source: Mapped[str] = mapped_column(String(50), primary_key=True)
    learned_job_limit: Mapped[int] = mapped_column(Integer, default=1)
    learned_page_limit: Mapped[int] = mapped_column(Integer, default=1)
    request_interval_seconds: Mapped[float] = mapped_column(default=0.0)
    cooldown_seconds: Mapped[int] = mapped_column(Integer, default=300)
    clean_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_limited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_exploration_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    successful_tier_runs: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ProviderEndpointState(JobBase):
    __tablename__ = "provider_endpoint_state"
    __table_args__ = (
        UniqueConstraint("source", "traffic_class", name="uq_provider_endpoint_source_class"),
        Index("ix_provider_endpoint_cooldown", "cooldown_until"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    traffic_class: Mapped[str] = mapped_column(String(20), index=True)
    request_interval_seconds: Mapped[float] = mapped_column(default=0.0)
    next_request_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ProviderBenchmarkRun(JobBase):
    __tablename__ = "provider_benchmark_run"
    __table_args__ = (Index("ix_provider_benchmark_source_started", "source", "started_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    state: Mapped[str] = mapped_column(String(20), default="running", index=True)
    requested_tier: Mapped[int] = mapped_column(Integer, default=1)
    stable_tier: Mapped[int] = mapped_column(Integer, default=1)
    request_count: Mapped[int] = mapped_column(Integer, default=0)
    success_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    limiting_signal: Mapped[str] = mapped_column(Text, default="")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    report_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )


class ProviderRequestSample(JobBase):
    __tablename__ = "provider_request_sample"
    __table_args__ = (Index("ix_provider_sample_run_created", "run_id", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("provider_benchmark_run.id", ondelete="CASCADE"), nullable=True, index=True
    )
    source: Mapped[str] = mapped_column(String(50), index=True)
    host: Mapped[str] = mapped_column(String(255), default="")
    status_code: Mapped[int] = mapped_column(Integer, default=0)
    latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    byte_count: Mapped[int] = mapped_column(Integer, default=0)
    error_code: Mapped[str] = mapped_column(String(100), default="")
    retry_after_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    headers_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogMatchDecision(JobBase):
    __tablename__ = "match_decision_v2"
    __table_args__ = (
        CheckConstraint(
            "decision IN ('pending', 'accepted', 'rejected')",
            name="ck_match_decision_v2_decision",
        ),
        UniqueConstraint(
            "left_source_series_id", "right_source_series_id", name="uq_match_decision_v2_pair"
        ),
        Index("ix_match_decision_v2_status_confidence", "decision", "confidence"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    left_source_series_id: Mapped[int] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), index=True
    )
    right_source_series_id: Mapped[int] = mapped_column(
        ForeignKey("source_series_v2.id", ondelete="CASCADE"), index=True
    )
    decision: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    confidence: Mapped[float] = mapped_column(default=0.0)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    scorer_version: Mapped[str] = mapped_column(String(50), default="legacy")
    feature_vector_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    decided_by: Mapped[str] = mapped_column(String(100), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class MatchTrainingLabel(JobBase):
    __tablename__ = "match_training_label"
    __table_args__ = (
        CheckConstraint("label IN (0, 1)", name="ck_match_training_label_value"),
        Index("ix_match_training_label_created", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    original_decision_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    label: Mapped[int] = mapped_column(Integer)
    origin: Mapped[str] = mapped_column(String(50), default="review")
    scorer_version: Mapped[str] = mapped_column(String(50), default="legacy")
    feature_vector_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    evidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    left_identity_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    right_identity_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CatalogObservation(JobBase):
    __tablename__ = "catalog_observation_v2"
    __table_args__ = (
        CheckConstraint(
            "state IN ('observed', 'accepted', 'quarantined', 'rejected')",
            name="ck_catalog_observation_v2_state",
        ),
        Index("ix_catalog_observation_v2_source_state", "source", "state", "observed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    observation_type: Mapped[str] = mapped_column(String(50), index=True)
    source_key: Mapped[str] = mapped_column(String(500), default="")
    state: Mapped[str] = mapped_column(String(20), default="observed", index=True)
    reason: Mapped[str] = mapped_column(Text, default="")
    payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(none_as_null=True), "postgresql"), default=dict
    )
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ArtifactBlob(JobBase):
    __tablename__ = "artifact_blob"

    checksum: Mapped[str] = mapped_column(String(64), primary_key=True)
    relative_path: Mapped[str] = mapped_column(Text, unique=True)
    byte_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ChapterArtifact(JobBase):
    __tablename__ = "chapter_artifact"
    __table_args__ = (
        CheckConstraint(
            "state IN ('active', 'inactive', 'quarantined')",
            name="ck_chapter_artifact_state",
        ),
        Index(
            "uq_chapter_artifact_active",
            "chapter_id",
            unique=True,
            sqlite_where=text("state = 'active'"),
            postgresql_where=text("state = 'active'"),
        ),
        Index("ix_chapter_artifact_state_created", "state", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_v2.id", ondelete="CASCADE"), index=True
    )
    chapter_release_id: Mapped[int | None] = mapped_column(
        ForeignKey("chapter_release_v2.id", ondelete="SET NULL"), nullable=True, index=True
    )
    blob_checksum: Mapped[str] = mapped_column(
        ForeignKey("artifact_blob.checksum", ondelete="RESTRICT"), index=True
    )
    state: Mapped[str] = mapped_column(String(20), default="active", index=True)
    provenance: Mapped[str] = mapped_column(String(50), default="download")
    source: Mapped[str] = mapped_column(String(50), default="")
    image_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class LibraryProjection(JobBase):
    __tablename__ = "library_projection"
    __table_args__ = (
        UniqueConstraint("artifact_id", name="uq_library_projection_artifact"),
        UniqueConstraint("relative_path", name="uq_library_projection_path"),
    )

    chapter_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_v2.id", ondelete="CASCADE"), primary_key=True
    )
    artifact_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_artifact.id", ondelete="CASCADE"), index=True
    )
    relative_path: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class KavitaProjection(JobBase):
    __tablename__ = "kavita_projection"
    __table_args__ = (UniqueConstraint("relative_path", name="uq_kavita_projection_path"),)

    chapter_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_v2.id", ondelete="CASCADE"), primary_key=True
    )
    artifact_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_artifact.id", ondelete="CASCADE"), index=True
    )
    relative_path: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ArtifactMetadataRewrite(JobBase):
    __tablename__ = "artifact_metadata_rewrite"
    __table_args__ = (
        UniqueConstraint("new_blob_checksum", name="uq_metadata_rewrite_new_blob"),
        Index("ix_metadata_rewrite_chapter_created", "chapter_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_id: Mapped[int] = mapped_column(
        ForeignKey("chapter_v2.id", ondelete="CASCADE"), index=True
    )
    old_blob_checksum: Mapped[str] = mapped_column(String(64), index=True)
    new_blob_checksum: Mapped[str] = mapped_column(String(64), index=True)
    old_comic_info: Mapped[bytes] = mapped_column(LargeBinary)
    new_comic_info: Mapped[bytes] = mapped_column(LargeBinary)
    reason: Mapped[str] = mapped_column(String(100), default="metadata")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
