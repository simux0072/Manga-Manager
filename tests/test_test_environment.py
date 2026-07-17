from __future__ import annotations

import importlib.util
import os
import subprocess
import urllib.error
from pathlib import Path

import pytest

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


def load_kavita_setup_module():
    path = ROOT / "scripts" / "kavita-e2e-setup.py"
    spec = importlib.util.spec_from_file_location("manga_manager_kavita_setup", path)
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
    assert 'docker build --platform "$STAGE_PLATFORM"' in stage
    assert 'STAGE_PLATFORM:-linux/amd64' not in stage
    assert "max-size: 10m" in compose
    assert 'docker stop --time "${STAGE_POSTGRES_STOP_SECONDS:-300}"' in stage
    assert 'docker rm -f "$postgres"' not in stage
    kavita = (ROOT / "scripts" / "kavita-local.sh").read_text()
    assert "jvmilazz0/kavita:0.8.9" in kavita
    assert "jvmilazz0/kavita:0.9.0.2" not in kavita
    assert "jvmilazz0/kavita:latest" not in kavita
    assert 'current_image=$(docker inspect' in kavita
    assert '[ "$current_image" != "$image" ]' in kavita
    assert kavita.index('docker build -t "$app_image" .') < kavita.index("expected_mount=")
    docker_ignore = (ROOT / ".dockerignore").read_text().splitlines()
    assert "local-archives" in docker_ignore
    assert ".uv-cache" in docker_ignore
    assert "frontend/test-results" in docker_ignore
    environment = (ROOT / "scripts" / "test-environment.sh").read_text()
    assert "HostConfig.Memory" in environment
    assert "1073741824" in environment
    assert "wait_for_stage_check" in environment
    assert '\"busy\": true' in environment
    assert "small_validation=passed" in environment
    assert '"$self" scale-check' in environment
    assert "scripts/kavita-cover-check.py" in environment
    assert 'kavita_covers=ok pairs=$checked_covers' in environment
    assert "TEST_SCALE_CHAPTER_COUNT:-100000" in environment
    assert "TEST_SCALE_JOB_COUNT:-100000" in environment
    assert '"$0" scale-check' in environment
    scale_verifier = (ROOT / "scripts" / "verify-scale-api.py").read_text()
    assert "X-SQL-Query-Count" in scale_verifier
    assert "max-route-queries" in scale_verifier
    assert '\"operations\": \"/api/v2/operations\"' in scale_verifier


def test_scale_parser_accepts_database_only_chapter_fixture_size() -> None:
    seed = load_seed_module()

    parsed = seed.parser().parse_args(
        ["--profile", "scale", "--chapter-count", "100000", "--job-count", "100000"]
    )

    assert parsed.chapter_count == 100_000
    assert parsed.job_count == 100_000


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
    assert "SELECT to_regclass('public.alembic_version')" in reset_script
    assert "database is not migrated" in reset_script


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


def test_kavita_setup_refetches_library_after_empty_create_response() -> None:
    setup = load_kavita_setup_module()
    libraries: list[dict] = []

    def fake_request(path: str, *, method: str = "GET", payload=None, token: str = ""):
        assert token == "token"
        if path == "/api/Library/libraries":
            return list(libraries)
        if path == "/api/Library/create":
            assert method == "POST"
            libraries.append({"id": 7, "name": payload["name"]})
            return None
        raise AssertionError(path)

    setup.request = fake_request

    assert setup.ensure_library("token") == {"id": 7, "name": "Manga Manager E2E"}


def test_kavita_launcher_preserves_pending_password() -> None:
    launcher = (ROOT / "scripts" / "kavita-local.sh").read_text()

    assert "$project-kavita-pending.env" in launcher
    assert "$state_dir/$project-kavita.env" in launcher
    assert 'elif [ -f "$legacy_env_file" ]' in launcher
    assert "umask 077" in launcher
    assert 'rm -f "$pending_env"' in launcher
    assert "KAVITA_WAIT_SECONDS:-900" in launcher
    assert '[ "$provision_status" -eq 42 ] && [ "$had_credentials" = false ]' in launcher
    assert 'docker volume rm "$volume"' in launcher
    assert 'KAVITA_BUILD:-false' in launcher


def test_isolated_environment_rebuilds_the_test_image_by_default() -> None:
    launcher = (ROOT / "scripts" / "test-environment.sh").read_text()

    assert 'KAVITA_BUILD="${TEST_ENV_BUILD:-true}" scripts/kavita-local.sh up' in launcher


def test_kavita_setup_classifies_persistent_admin_mismatch(monkeypatch) -> None:
    setup = load_kavita_setup_module()

    monkeypatch.setattr(setup, "WAIT_SECONDS", 1)
    monkeypatch.setattr(
        setup,
        "request",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            urllib.error.HTTPError(
                url="http://kavita/api/Account/login",
                code=401,
                msg="Unauthorized",
                hdrs=None,
                fp=None,
            )
        ),
    )

    with pytest.raises(setup.CredentialMismatchError, match="different administrator"):
        setup.main()
