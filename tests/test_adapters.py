import pytest
import httpx
from bs4 import BeautifulSoup

from app.adapters.asura import AsuraAdapter
from app.adapters.http import HttpSourceClient
from app.adapters.base import SourceRateLimited
from app.adapters.kingofshojo import KingOfShojoAdapter
from app.adapters.mangafire import MangaFireAdapter
from app.adapters.parsing import extract_image_urls
from app.domain import SeriesItem


def test_extract_image_urls_supports_lazy_attrs_and_scripts():
    soup = BeautifulSoup(
        """
        <html>
          <img src="/static/logo.png">
          <img data-src="/chapters/001/001.webp">
          <img srcset="/chapters/001/002.jpg 800w, /chapters/001/002-small.jpg 400w">
          <script>
            window.pages = ["https://cdn.example.com/chapter/003.png"];
          </script>
        </html>
        """,
        "html.parser",
    )

    assert extract_image_urls(soup, "https://example.com") == [
        "https://example.com/chapters/001/001.webp",
        "https://example.com/chapters/001/002.jpg",
        "https://cdn.example.com/chapter/003.png",
    ]


def test_asura_parses_comics_and_filters_chapter_images():
    soup = BeautifulSoup(
        """
        <a href="/comics/star-embracing-swordmaster-a80d257e">
          <img src="https://cdn.asurascans.com/asura-images/covers/star.webp">
          Star-Embracing Swordmaster Chapter 128 Just now
        </a>
        <div><a href="/comics/star-embracing-swordmaster-a80d257e/chapter/128">Chapter 128 Sep 29, 2025</a></div>
        """,
        "html.parser",
    )

    adapter = AsuraAdapter()
    recent = adapter.parse_recent_series(soup)

    assert recent[0].source_id == "comics/star-embracing-swordmaster-a80d257e"
    assert recent[0].title == "Star-Embracing Swordmaster"

    chapters = adapter.parse_chapters(soup, recent[0])
    assert [(chapter.number, chapter.title, chapter.url) for chapter in chapters] == [
        ("128", "Chapter 128", "https://asurascans.com/comics/star-embracing-swordmaster-a80d257e/chapter/128")
    ]
    assert chapters[0].published_at is not None

    chapter_soup = BeautifulSoup(
        """
        <img src="https://cdn.asurascans.com/asura-images/covers/star.webp">
        <img src="https://cdn.asurascans.com/asura-images/chapters/star/128/001.webp?v=1">
        """,
        "html.parser",
    )
    assert adapter.parse_chapter_image_urls(chapter_soup) == [
        "https://cdn.asurascans.com/asura-images/chapters/star/128/001.webp?v=1"
    ]


def test_asura_uses_parent_card_cover():
    soup = BeautifulSoup(
        """
        <div class="card">
          <img data-src="/covers/parent.webp">
          <a href="/comics/parent-cover">Parent Cover Chapter 4</a>
        </div>
        """,
        "html.parser",
    )

    recent = AsuraAdapter().parse_recent_series(soup)

    assert recent[0].cover_url == "https://asurascans.com/covers/parent.webp"


def test_asura_uses_chapter_url_when_label_has_no_number():
    soup = BeautifulSoup(
        """
        <a href="/comics/example/chapter/1">first chapter</a>
        <a href="/comics/example/chapter/128">latest chapter</a>
        """,
        "html.parser",
    )
    source = SeriesItem(
        source="asura",
        source_id="comics/example",
        title="Example",
        url="https://asurascans.com/comics/example",
    )

    chapters = AsuraAdapter().parse_chapters(soup, source)

    assert [chapter.number for chapter in chapters] == ["1", "128"]


def test_kingofshojo_filters_template_chapters_and_non_reader_images():
    soup = BeautifulSoup(
        """
        <a href="https://kingofshojo.com/selena-chapter-128/">Latest: Chapter 128</a>
        <a href="#/chapter-{{number}}">Chapter {{number}} {{date}}</a>
        <a href="https://kingofshojo.com#/chapter-%7B%7Bnumber%7D%7D">Chapter {{number}} {{date}}</a>
        <a href="https://kingofshojo.com/selena-chapter-127-5/">Chapter 127.5</a>
        """,
        "html.parser",
    )
    adapter = KingOfShojoAdapter()
    source = SeriesItem(
        source="kingofshojo",
        source_id="manga/selena",
        title="Selena",
        url="https://kingofshojo.com/manga/selena/",
    )

    chapters = adapter.parse_chapters(soup, source)

    assert [chapter.number for chapter in chapters] == ["128", "127.5"]

    chapter_soup = BeautifulSoup(
        """
        <img src="https://kingofshojo.com/wp-content/uploads/2024/03/wewtwt.png">
        <img src="https://cdn.kingofshojo.com/king-bucket/298784/59/1_result.webp">
        <img src="https://i0.wp.com/kingofshojo.com/wp-content/uploads/cover.jpg?resize=600,1000">
        """,
        "html.parser",
    )
    assert adapter.parse_chapter_image_urls(chapter_soup) == [
        "https://cdn.kingofshojo.com/king-bucket/298784/59/1_result.webp"
    ]


def test_kingofshojo_filters_navigation_series_and_extracts_dates():
    soup = BeautifulSoup(
        """
        <a href="/manga/?status=&type=manhwa&order=">Manhwa</a>
        <a href="/manga/list-mode">Text Mode</a>
        <a href="/manga/tears-on-a-withered-flower/">
          <img data-lazy-src="/covers/tears.webp">Tears on a Withered Flower
        </a>
        <div><a href="https://kingofshojo.com/tears-on-a-withered-flower-chapter-109/">Latest: Chapter 109 Jul 07, 2026</a></div>
        """,
        "html.parser",
    )
    adapter = KingOfShojoAdapter()
    recent = adapter.parse_recent_series(soup)
    assert [item.title for item in recent] == ["Tears on a Withered Flower"]
    assert recent[0].cover_url == "https://kingofshojo.com/covers/tears.webp"

    chapters = adapter.parse_chapters(
        soup,
        SeriesItem("kingofshojo", "manga/tears-on-a-withered-flower", "Tears", "https://kingofshojo.com/manga/tears-on-a-withered-flower/"),
    )
    assert chapters[0].number == "109"
    assert chapters[0].title == "Chapter 109"
    assert chapters[0].published_at is not None


def test_kingofshojo_uses_parent_card_cover():
    soup = BeautifulSoup(
        """
        <article style="background-image: url('/covers/card.jpg')">
          <a href="/manga/card-cover/">Card Cover</a>
        </article>
        """,
        "html.parser",
    )

    recent = KingOfShojoAdapter().parse_recent_series(soup)

    assert recent[0].cover_url == "https://kingofshojo.com/covers/card.jpg"


def test_mangafire_uses_api_payloads_for_titles_chapters_and_pages():
    adapter = MangaFireAdapter()
    recent = adapter.parse_recent_series(
        {
            "data": {
                "items": [
                    {
                        "hid": "gl3",
                        "slug": "gun-x-clover",
                        "title": "Gun X Clover",
                        "url": "/title/gl3-gun-x-clover",
                        "poster": {"medium": "https://img.example/poster.jpg"},
                        "rank": 123,
                    }
                ]
            }
        }
    )

    assert recent[0].source_id == "gl3"
    assert recent[0].url == "https://mangafire.to/title/gl3-gun-x-clover"
    assert recent[0].popularity == 123

    chapters = adapter.parse_chapters(
        {
            "data": {
                "items": [
                    {"id": 156, "number": 60, "name": "Love & Clover", "language": "en"},
                    {"id": 155, "number": 60, "name": "Spanish", "language": "es"},
                ]
            }
        },
        recent[0],
    )

    assert len(chapters) == 1
    assert chapters[0].url == "https://mangafire.to/title/gl3-gun-x-clover/chapter/156"
    assert adapter.parse_chapter_image_urls(
        {"data": {"pages": [{"url": "https://m3z.mfcdn3.xyz/mf/page.jpg"}]}}
    ) == ["https://m3z.mfcdn3.xyz/mf/page.jpg"]


def test_mangafire_parses_current_chapter_payload_variants():
    adapter = MangaFireAdapter()
    source = SeriesItem(
        source="mangafire",
        source_id="gl3",
        title="Gun X Clover",
        url="https://mangafire.to/title/gl3-gun-x-clover",
    )

    chapters = adapter.parse_chapters(
        {
            "data": {
                "chapters": [
                    {
                        "hid": "abc123",
                        "chapter": "Chapter 61",
                        "title": "The Return",
                        "language": {"code": "en-us"},
                        "created_at": 1_720_000_000,
                    },
                    {
                        "hid": "skip",
                        "chapter": "Chapter 60",
                        "title": "Spanish",
                        "language": {"name": "Spanish"},
                    },
                ]
            }
        },
        source,
    )

    assert len(chapters) == 1
    assert chapters[0].number == "61"
    assert chapters[0].title == "The Return"
    assert chapters[0].url == "https://mangafire.to/title/gl3-gun-x-clover/chapter/abc123"


def test_mangafire_keeps_english_oneshot_chapter_zero_and_filters_non_english():
    adapter = MangaFireAdapter()
    source = SeriesItem(
        source="mangafire",
        source_id="one",
        title="One Shot",
        url="https://mangafire.to/title/one-one-shot",
    )

    chapters = adapter.parse_chapters(
        {
            "data": {
                "items": [
                    {"hid": "en0", "number": 0, "name": "Oneshot", "language": "English"},
                    {"hid": "es0", "number": 0, "name": "Especial", "language": "es"},
                    {"hid": "fr1", "number": 1, "name": "French", "language": {"name": "French"}},
                ]
            }
        },
        source,
    )

    assert [chapter.number for chapter in chapters] == ["0"]
    assert chapters[0].title == "Oneshot"
    assert chapters[0].url == "https://mangafire.to/title/one-one-shot/chapter/en0"


def test_mangafire_parses_top_level_chapters_with_missing_language():
    adapter = MangaFireAdapter()
    source = SeriesItem(
        source="mangafire",
        source_id="gl3",
        title="Gun X Clover",
        url="https://mangafire.to/title/gl3-gun-x-clover",
    )

    chapters = adapter.parse_chapters(
        {"chapters": [{"chapterId": 777, "chapterNumber": 62, "name": "Chapter 62"}]},
        source,
    )

    assert [chapter.number for chapter in chapters] == ["62"]
    assert chapters[0].url == "https://mangafire.to/title/gl3-gun-x-clover/chapter/777"


def test_mangafire_parses_detail_metadata():
    adapter = MangaFireAdapter()

    item = adapter.parse_series_detail(
        {
            "data": {
                "hid": "gl3",
                "title": "Gun X Clover",
                "url": "/title/gl3-gun-x-clover",
                "poster": {"large": "https://img.example/large.webp"},
                "synopsisHtml": "<p>A school action series.</p>",
                "altTitles": ["Gun Clover"],
                "genres": [{"name": "Action"}, {"name": "School Life"}],
                "follows": 999,
                "rating": 8.7,
                "malId": "39281",
                "anilistId": "69281",
            }
        }
    )

    assert item.aliases == ("Gun Clover",)
    assert item.description == "A school action series."
    assert item.genres == ("Action", "School Life")
    assert item.popularity == 999
    assert item.external_ids == {"mal": "39281", "anilist": "69281"}
    assert item.metadata["follows"] == 999
    assert item.metadata["rating"] == 8.7


def test_mangafire_defaults_to_new_latest_updates_mode(monkeypatch):
    adapter = MangaFireAdapter()

    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_discovery_mode", "new")

    query = adapter.recent_titles_query()

    assert "sort=chapter_updated_at:desc" in query
    assert "hot=1" not in query


def test_mangafire_hot_mode_is_explicit(monkeypatch):
    adapter = MangaFireAdapter()

    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_discovery_mode", "hot")

    assert "hot=1" in adapter.recent_titles_query()


async def test_http_client_rejects_oversized_page(monkeypatch):
    async def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            content=b"12345",
            request=request,
        )

    monkeypatch.setattr("app.adapters.http.settings.max_page_bytes", 3)
    client = HttpSourceClient(
        "https://example.com",
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(RuntimeError, match="max_page_bytes"):
            await client.get_bytes("https://example.com/page.jpg")
    finally:
        await client.client.aclose()


async def test_http_client_raises_rate_limit_with_retry_after():
    async def handler(request):
        return httpx.Response(
            429,
            headers={"retry-after": "120"},
            request=request,
        )

    client = HttpSourceClient(
        "https://example.com",
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(SourceRateLimited) as exc_info:
            await client.get_soup("/")
    finally:
        await client.client.aclose()

    assert exc_info.value.retry_after is not None
