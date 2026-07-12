from pathlib import Path

import pytest

from app.kavita import KavitaClient


class FakeResponse:
    def __init__(self, payload=None):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeAsyncClient:
    request = None

    def __init__(self, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def post(self, url, headers):
        FakeAsyncClient.request = {"url": url, "headers": headers}
        return FakeResponse()


@pytest.mark.asyncio
async def test_kavita_client_uses_x_api_key(monkeypatch):
    import app.kavita as kavita

    monkeypatch.setattr(kavita.httpx, "AsyncClient", FakeAsyncClient)

    await KavitaClient("http://kavita", "secret").scan_all()

    assert FakeAsyncClient.request == {
        "url": "http://kavita/api/Library/scan-all",
        "headers": {"x-api-key": "secret"},
    }


class FakeFullAsyncClient:
    requests = []

    def __init__(self, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def post(self, url, headers, json=None, params=None):
        FakeFullAsyncClient.requests.append(
            {"method": "POST", "url": url, "headers": headers, "json": json, "params": params}
        )
        if url.endswith("/api/Series/all-v2"):
            return FakeResponse(
                [
                    {
                        "id": 7,
                        "name": "Example",
                        "libraryId": 2,
                        "folderPath": "/library/Manga/Example",
                        "lowestFolderPath": "/library/Manga/Example/Specific",
                        "malId": "11",
                    }
                ]
            )
        if url.endswith("/api/want-to-read/v2"):
            return FakeResponse([{"id": 8, "name": "Wanted", "libraryId": 2}])
        return FakeResponse()

    async def get(self, url, headers, params=None):
        FakeFullAsyncClient.requests.append(
            {"method": "GET", "url": url, "headers": headers, "params": params}
        )
        if url.endswith("/api/Series/series-detail"):
            return FakeResponse({"chapters": [{"id": 42, "number": "12", "volumeId": 3, "pages": 11}]})
        if url.endswith("/api/Reader/get-progress"):
            return FakeResponse({"chapterId": params["chapterId"], "pageNum": 11})
        return FakeResponse(None)


@pytest.mark.asyncio
async def test_kavita_client_folder_scan_series_and_want_to_read(monkeypatch, tmp_path):
    import app.kavita as kavita

    FakeFullAsyncClient.requests = []
    monkeypatch.setattr(kavita.httpx, "AsyncClient", FakeFullAsyncClient)

    client = KavitaClient("http://kavita", "secret")
    await client.scan_folder(tmp_path / "Example")
    series = await client.list_series()
    chapters = await client.series_detail(7)
    progress = await client.chapter_progress(42, pages_total=11)
    wanted = await client.want_to_read()
    await client.add_want_to_read([7])

    assert FakeFullAsyncClient.requests[0]["json"] == {
        "apiKey": "secret",
        "folderPath": str(tmp_path / "Example"),
        "abortOnNoSeriesMatch": False,
    }
    assert series[0].id == 7
    assert series[0].folder_path == "/library/Manga/Example/Specific"
    assert series[0].mal_id == "11"
    assert chapters[0].id == 42
    assert chapters[0].number == "12"
    assert chapters[0].pages_total == 11
    assert progress.chapter_id == 42
    assert progress.pages_read == 11
    assert progress.pages_total == 11
    assert wanted[0].name == "Wanted"
    assert FakeFullAsyncClient.requests[-1]["json"] == {"seriesIds": [7]}
    assert {
        "method": "GET",
        "url": "http://kavita/api/Reader/get-progress",
        "headers": {"x-api-key": "secret"},
        "params": {"chapterId": 42},
    } in FakeFullAsyncClient.requests


def test_kavita_client_renders_urls(monkeypatch):
    import app.kavita as kavita

    monkeypatch.setattr(
        kavita.settings,
        "kavita_series_url_template",
        "{base_url}/series/{series_id}?library={library_id}",
    )
    monkeypatch.setattr(
        kavita.settings,
        "kavita_chapter_url_template",
        "{base_url}/series/{series_id}/chapter/{chapter_id}",
    )

    client = KavitaClient("http://kavita/", "secret")

    assert client.series_url(2, 7) == "http://kavita/series/7?library=2"
    assert client.chapter_url(2, 7, 42) == "http://kavita/series/7/chapter/42"


def test_kavita_path_translation_same_path(monkeypatch, tmp_path):
    import app.kavita as kavita

    monkeypatch.setattr(kavita.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(kavita.settings, "kavita_library_root", None)

    local_path = tmp_path / "library" / "Manga" / "Example"

    assert kavita.kavita_path_for_local(local_path) == local_path
    assert kavita.local_path_for_kavita(str(local_path)) == local_path


def test_kavita_path_translation_different_container_path(monkeypatch, tmp_path):
    import app.kavita as kavita

    monkeypatch.setattr(kavita.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(kavita.settings, "kavita_library_root", Path("/kavita-manga"))

    local_path = tmp_path / "library" / "Manga" / "Example"

    assert kavita.kavita_path_for_local(local_path) == Path("/kavita-manga/Manga/Example")
    assert kavita.local_path_for_kavita("/kavita-manga/Manga/Example") == local_path
