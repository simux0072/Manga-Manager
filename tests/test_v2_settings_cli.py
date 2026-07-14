from __future__ import annotations

import pytest

from manga_manager.cli import build_parser
from manga_manager.settings import V2Settings


def test_v2_settings_require_postgresql() -> None:
    with pytest.raises(ValueError, match="V2_DATABASE_URL is required"):
        V2Settings(database_url="").require_database_url()
    with pytest.raises(ValueError, match=r"postgresql\+"):
        V2Settings(database_url="sqlite:///test.db").require_database_url()
    assert (
        V2Settings(database_url="postgresql+psycopg://localhost/test").require_database_url()
        == "postgresql+psycopg://localhost/test"
    )


def test_normal_worker_cannot_enable_asura_concurrency_two() -> None:
    with pytest.raises(ValueError, match="less than or equal to 1"):
        V2Settings(asura_download_concurrency=2)


def test_cli_exposes_transition_commands() -> None:
    parser = build_parser()
    assert parser.parse_args(["migrate"]).command == "migrate"
    assert parser.parse_args(["worker"]).command == "worker"
    diagnostics = parser.parse_args(
        ["diagnostic-bundle", "--output", "/tmp/diagnostics.json", "--recent-failures", "25"]
    )
    assert diagnostics.command == "diagnostic-bundle"
    assert diagnostics.recent_failures == 25
    pull = parser.parse_args(["enqueue-pull", "asura"])
    assert pull.command == "enqueue-pull"
    assert pull.source == "asura"
    importer = parser.parse_args(["import-cbz", "/tmp/library", "--dry-run"])
    assert importer.command == "import-cbz"
    assert importer.dry_run is True
    download = parser.parse_args(["enqueue-download", "42"])
    assert download.command == "enqueue-download"
    assert download.chapter_release_id == 42
    assert parser.parse_args(["reconcile-storage"]).command == "reconcile-storage"
    kavita = parser.parse_args(["enqueue-kavita", "7"])
    assert kavita.command == "enqueue-kavita"
    assert kavita.series_id == 7
    assert parser.parse_args(["enqueue-probe"]).command == "enqueue-probe"
    benchmark = parser.parse_args(
        [
            "benchmark-workers",
            "--source",
            "asura",
            "--concurrency",
            "2",
            "--duration",
            "30",
            "--max-jobs",
            "2",
            "--dry-run",
        ]
    )
    assert benchmark.concurrency == 2
    validate = parser.parse_args(
        [
            "validate-legacy",
            "legacy.db",
            "--storage-root",
            "storage",
            "--report",
            "report.json",
        ]
    )
    assert validate.command == "validate-legacy"
    recovery = parser.parse_args(
        ["repair-catalog-recovery", "legacy.db", "--report", "recovery.json", "--apply"]
    )
    assert recovery.command == "repair-catalog-recovery"
    assert recovery.apply is True
    training = parser.parse_args(["export-match-training", "training-data"])
    assert training.command == "export-match-training"
    library_repair = parser.parse_args(["enqueue-library-repair", "--all-tracked"])
    assert library_repair.command == "enqueue-library-repair"
    assert library_repair.all_tracked is True
    provider_repair = parser.parse_args(
        ["repair-provider-identities", "--report", "providers.json", "--apply"]
    )
    assert provider_repair.command == "repair-provider-identities"
    assert provider_repair.apply is True
    refresh_repair = parser.parse_args(
        ["reconcile-refresh-queue", "--report", "refresh.json"]
    )
    assert refresh_repair.command == "reconcile-refresh-queue"
    assert refresh_repair.apply is False
