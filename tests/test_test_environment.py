from __future__ import annotations

import importlib.util
import os
import subprocess
from pathlib import Path

from manga_manager.application.cbz_import import read_comic_info
from manga_manager.infrastructure.storage import ContentAddressedStorage


ROOT = Path(__file__).resolve().parents[1]


def load_seed_module():
    path = ROOT / "scripts" / "seed-test-data.py"
    spec = importlib.util.spec_from_file_location("manga_manager_seed_test_data", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generated_cbz_is_small_valid_and_has_canonical_metadata(tmp_path: Path) -> None:
    seed = load_seed_module()
    archive = tmp_path / "fixture.cbz"
    seed.write_cbz(
        archive,
        title="Synthetic Test Series",
        chapter="2",
        color=(25, 80, 160),
    )
    storage = ContentAddressedStorage(
        tmp_path / "storage",
        max_page_bytes=1_000_000,
        max_chapter_bytes=5_000_000,
        max_pages=10,
        min_download_pages=3,
        min_free_bytes=0,
    )

    validated = storage.validate_cbz(archive)

    assert validated.image_count == 3
    assert validated.byte_count < 100_000
    assert read_comic_info(archive) == ("Synthetic Test Series", "2")


def test_scale_profile_enforces_acceptance_floor() -> None:
    seed = load_seed_module()

    try:
        seed.seed_scale(None, None, series_count=1_999, job_count=25_000)
    except ValueError as exc:
        assert "at least 2000 series" in str(exc)
    else:
        raise AssertionError("undersized scale fixture was accepted")


def test_runtime_entrypoints_do_not_reinvoke_uv() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    compose = (ROOT / "docker-compose.yml").read_text()
    stage = (ROOT / "scripts" / "stage-local.sh").read_text()

    assert 'CMD ["uv", "run"' not in dockerfile
    assert '["uv", "run", "--frozen"' not in compose
    assert "uv run --frozen" not in stage
    assert "max-size=10m" in stage
    assert "max-size: 10m" in compose
    kavita = (ROOT / "scripts" / "kavita-local.sh").read_text()
    assert "jvmilazz0/kavita:0.9.0.2" in kavita
    assert "jvmilazz0/kavita:latest" not in kavita
    docker_ignore = (ROOT / ".dockerignore").read_text().splitlines()
    assert "local-archives" in docker_ignore
    assert ".uv-cache" in docker_ignore
    assert "frontend/test-results" in docker_ignore
    environment = (ROOT / "scripts" / "test-environment.sh").read_text()
    assert "HostConfig.Memory" in environment
    assert "1073741824" in environment


def test_reset_preview_names_legacy_kavita_resources() -> None:
    result = subprocess.run(
        [str(ROOT / "scripts" / "reset-local-data.sh"), "preview"],
        cwd=ROOT,
        env={**os.environ, "STAGE_STORAGE_ROOT": str(ROOT / ".local" / "missing-storage")},
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Legacy Kavita container: manga-manager-kavita" in result.stdout
    assert "Legacy Kavita volume: manga-manager-kavita-config" in result.stdout


def test_reset_archive_checksum_is_relocatable() -> None:
    reset_script = (ROOT / "scripts" / "reset-local-data.sh").read_text()

    assert 'sha256sum "$dump_name"' in reset_script
    assert 'sha256sum "$archive_dir/$dump_name"' not in reset_script


def test_test_environment_refuses_reset_root_outside_repository() -> None:
    result = subprocess.run(
        [str(ROOT / "scripts" / "test-environment.sh"), "status"],
        cwd=ROOT,
        env={**os.environ, "TEST_ENV_ROOT": "/tmp/outside-manga-manager-test"},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "refusing test path outside repository" in result.stderr
