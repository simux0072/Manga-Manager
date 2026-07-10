from __future__ import annotations

import pytest

from manga_manager.cli import build_parser
from manga_manager.settings import V2Settings


def test_v2_settings_require_postgresql() -> None:
    with pytest.raises(ValueError, match="V2_DATABASE_URL is required"):
        V2Settings(database_url="").require_database_url()
    with pytest.raises(ValueError, match=r"postgresql\+"):
        V2Settings(database_url="sqlite:///test.db").require_database_url()
    assert V2Settings(
        database_url="postgresql+psycopg://localhost/test"
    ).require_database_url() == "postgresql+psycopg://localhost/test"


def test_cli_exposes_transition_commands() -> None:
    parser = build_parser()
    assert parser.parse_args(["migrate"]).command == "migrate"
    assert parser.parse_args(["worker"]).command == "worker"
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
