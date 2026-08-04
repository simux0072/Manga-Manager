from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

import manga_manager.infrastructure.database as database_module
from manga_manager.infrastructure.db_models import WorkJob
from manga_manager.infrastructure.database import (
    create_database_engine,
    create_session_factory,
    DEFAULT_ALEMBIC_CONFIG,
    run_migrations,
)


def test_runtime_rejects_non_postgresql_database() -> None:
    with pytest.raises(ValueError, match="requires a PostgreSQL"):
        create_database_engine("sqlite:///:memory:")


def test_web_database_pool_has_bounded_browser_burst_capacity(monkeypatch) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_create_engine(database_url: str, **options):
        captured["database_url"] = database_url
        captured.update(options)
        return sentinel

    monkeypatch.setattr(database_module, "create_engine", fake_create_engine)

    engine = create_database_engine(
        "postgresql+psycopg://manga:manga@postgres:5432/manga_manager",
        role="web",
    )

    assert engine is sentinel
    assert captured["pool_size"] == 4
    assert captured["max_overflow"] == 2
    assert captured["pool_timeout"] == 10


def test_v2_migration_builds_job_constraints_and_indexes(tmp_path: Path) -> None:
    database_path = tmp_path / "v2.db"
    database_url = f"sqlite:///{database_path}"
    run_migrations(database_url)
    engine = create_database_engine(database_url, allow_sqlite_for_tests=True)
    inspector = inspect(engine)

    assert {
        "alembic_version",
        "job",
        "job_event",
        "worker_heartbeat",
        "series_v2",
        "source_series_v2",
        "series_alias_v2",
        "external_identifier_v2",
        "chapter_v2",
        "chapter_release_v2",
        "source_state_v2",
        "artifact_blob",
        "chapter_artifact",
        "library_projection",
        "job_permit",
        "match_decision_v2",
        "catalog_observation_v2",
        "chapter_reading_state_v2",
        "series_download_plan",
        "chapter_download_intent",
        "provider_policy",
        "provider_benchmark_run",
        "provider_request_sample",
        "alternate_source_listing_v2",
        "cover_fingerprint_v2",
        "chapter_release_attempt",
        "provider_endpoint_state",
        "storage_state",
        "storage_reservation",
        "cover_asset_v2",
        "cover_signature_v2",
        "kavita_projection",
        "artifact_metadata_rewrite",
            "match_training_label",
            "workload_cycle",
            "job_daily_aggregate",
    } == set(inspector.get_table_names())
    indexes = {index["name"] for index in inspector.get_indexes("job")}
    assert {
        "ix_job_claim",
        "ix_job_kind",
        "ix_job_lease_expiry",
        "ix_job_status",
        "uq_job_active_dedupe",
        "ix_job_pool_claim",
        "ix_job_source_status",
        "uq_job_leased_library_repair_series",
    } <= indexes
    checks = {constraint["name"] for constraint in inspector.get_check_constraints("job")}
    assert {
        "ck_job_kind",
        "ck_job_status",
        "ck_job_lease_fields",
        "ck_job_max_attempts_positive",
    } <= checks
    assert "quality_rank" in {
        column["name"] for column in inspector.get_columns("chapter_release_v2")
    }
    assert "quality_rank" in {
        column["name"] for column in inspector.get_columns("chapter_artifact")
    }

    sessions = create_session_factory(engine)
    with sessions() as session:
        version = session.scalar(text("SELECT version_num FROM alembic_version"))
    assert version == "0025_shared_worker_fairness"


def test_catalog_recovery_migration_downgrades_and_reapplies_on_sqlite(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'round-trip.db'}"
    run_migrations(database_url)
    config = Config(str(DEFAULT_ALEMBIC_CONFIG))
    config.set_main_option("sqlalchemy.url", database_url)

    command.downgrade(config, "0016_refresh_storage_hardening")
    engine = create_database_engine(database_url, allow_sqlite_for_tests=True)
    assert "kavita_projection" not in inspect(engine).get_table_names()
    command.upgrade(config, "head")

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0025_shared_worker_fairness"
        )


def test_elastic_worker_migration_reroutes_ready_refresh_jobs(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'elastic-workers.db'}"
    run_migrations(database_url)
    config = Config(str(DEFAULT_ALEMBIC_CONFIG))
    config.set_main_option("sqlalchemy.url", database_url)
    command.downgrade(config, "0023_mangadex_provider")
    engine = create_database_engine(database_url, allow_sqlite_for_tests=True)
    with Session(engine) as session, session.begin():
        session.add_all(
            [
                WorkJob(
                    kind="source_refresh",
                    dedupe_key="refresh:mangafire:example",
                    payload={
                        "version": 3,
                        "source": "mangafire",
                        "source_id": "example",
                        "title": "Example",
                        "url": "https://mangafire.to/title/example",
                    },
                    source="mangafire",
                    pool="pull:mangafire",
                ),
                WorkJob(
                    kind="source_refresh",
                    dedupe_key="refresh:mangadex:retry",
                    payload={
                        "version": 3,
                        "source": "mangadex",
                        "source_id": "retry",
                        "title": "Retry",
                        "url": "https://mangadex.org/title/retry",
                    },
                    source="mangadex",
                    pool="pull:mangadex",
                    status="retry_wait",
                ),
                WorkJob(
                    kind="source_refresh",
                    dedupe_key="refresh:asura:leased",
                    payload={
                        "version": 3,
                        "source": "asura",
                        "source_id": "leased",
                        "title": "Leased",
                        "url": "https://asurascans.com/comics/leased",
                    },
                    source="asura",
                    pool="pull:asura",
                    status="leased",
                    lease_owner="old-worker",
                    lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
                ),
            ]
        )

    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(
            text("SELECT pool FROM job WHERE dedupe_key='refresh:mangafire:example'")
        ) == "refresh:mangafire"
        assert connection.scalar(
            text("SELECT pool FROM job WHERE dedupe_key='refresh:mangadex:retry'")
        ) == "refresh:mangadex"
        assert connection.scalar(
            text("SELECT pool FROM job WHERE dedupe_key='refresh:asura:leased'")
        ) == "pull:asura"

    command.downgrade(config, "0023_mangadex_provider")
    with engine.connect() as connection:
        assert connection.scalar(
            text("SELECT pool FROM job WHERE dedupe_key='refresh:mangafire:example'")
        ) == "pull:mangafire"
    command.upgrade(config, "head")
