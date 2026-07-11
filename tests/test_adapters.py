import asyncio

import pytest
import httpx
from bs4 import BeautifulSoup

from app.adapters.asura import AsuraAdapter
from app.adapters.http import HttpSourceClient
from app.adapters.base import FrontierSentinel, SourceRateLimited
from app.adapters.kingofshojo import KingOfShojoAdapter
from app.adapters.mangafire import MangaFireAdapter
from app.adapters.http import iter_ordered_bytes
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
        (
            "128",
            "Chapter 128",
            "https://asurascans.com/comics/star-embracing-swordmaster-a80d257e/chapter/128",
        )
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


def test_asura_detail_prefers_visible_summary_and_filters_polluted_alias():
    soup = BeautifulSoup(
        """
        <h1>Example Hero</h1>
        <div class="summary__content">Visible series summary.</div>
        <p>Example Hero Asura Scans Home First Chapter</p>
        """,
        "html.parser",
    )

    item = AsuraAdapter().parse_series_detail(
        soup,
        SeriesItem(
            "asura", "comics/example", "Example Hero", "https://asurascans.com/comics/example"
        ),
    )

    assert item.description == "Visible series summary."
    assert "Asura Scans Home" not in item.aliases


def test_asura_uses_chapter_url_when_label_has_no_number():
    soup = BeautifulSoup(
        """
        <a href="/comics/example/chapter/1">first chapter</a>
        <a href="/comics/example/chapter/128">latest chapter</a>
        <div><a href="/comics/example/chapter/7">Chapter 7</a></div>
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

    assert [chapter.number for chapter in chapters] == ["7"]


def test_asura_accepts_astro_stitched_chapter_images():
    soup = BeautifulSoup(
        """
        <astro-island props="{&quot;pages&quot;:[1,[[0,{&quot;url&quot;:[0,&quot;https://cdn.asurascans.com/asura-images/chapters-stitched/example/1/001.webp?v=1&quot;]}],[0,{&quot;url&quot;:[0,&quot;https://cdn.asurascans.com/asura-images/chapters-stitched/example/1/002.webp?v=1&quot;]}]]]}"></astro-island>
        <img src="https://cdn.asurascans.com/asura-images/covers/example.webp">
        """,
        "html.parser",
    )

    assert AsuraAdapter().parse_chapter_image_urls(soup) == [
        "https://cdn.asurascans.com/asura-images/chapters-stitched/example/1/001.webp?v=1",
        "https://cdn.asurascans.com/asura-images/chapters-stitched/example/1/002.webp?v=1",
    ]


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
        SeriesItem(
            "kingofshojo",
            "manga/tears-on-a-withered-flower",
            "Tears",
            "https://kingofshojo.com/manga/tears-on-a-withered-flower/",
        ),
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


def test_kingofshojo_extracts_aliases_from_description_prefix_and_cleans_cover():
    soup = BeautifulSoup(
        """
        <h1>Sashimi Master</h1>
        <div class="entry-content">
          Read manhwa Sashimi Master / Conquering the Academy with Just a Sashimi Knife / Sashimi Blade
          Synopsis A student survives with a sashimi knife.
        </div>
        <div class="summary_image"><img data-src="/wp-content/uploads/cover.webp"></div>
        """,
        "html.parser",
    )

    item = KingOfShojoAdapter().parse_series_detail(
        soup,
        SeriesItem(
            "kingofshojo",
            "manga/sashimi",
            "Sashimi Master",
            "https://kingofshojo.com/manga/sashimi/",
        ),
    )

    assert "Conquering the Academy with Just a Sashimi Knife" in item.aliases
    assert not item.description.startswith("Read manhwa")
    assert item.cover_url == "https://kingofshojo.com/wp-content/uploads/cover.webp"


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
                        "latestChapter": 61,
                        "chapterUpdatedAt": "5m ago",
                    }
                ]
            }
        }
    )

    assert recent[0].source_id == "gl3"
    assert recent[0].url == "https://mangafire.to/title/gl3-gun-x-clover"
    assert recent[0].popularity == 123
    assert recent[0].metadata["latest_chapter"] == 61
    assert recent[0].metadata["chapter_updated_at"] == "5m ago"
    assert recent[0].metadata["recent_chapters"][0]["number"] == "61"

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


def test_mangafire_parses_updated_html_and_filters_non_english_chapters():
    soup = BeautifulSoup(
        """
        <div class="unit">
          <a href="/title/gl3-gun-x-clover" title="Gun X Clover">
            <img data-src="/covers/gun.webp">
          </a>
          <a href="/title/gl3-gun-x-clover/chapter/en61">Chapter 61 English Jul 09, 2026</a>
          <a href="/title/gl3-gun-x-clover/chapter/es60">Chapter 60 Spanish Jul 09, 2026</a>
        </div>
        <div class="unit">
          <a href="/title/ab1-other">Other Chapter 5</a>
        </div>
        """,
        "html.parser",
    )

    adapter = MangaFireAdapter()
    recent = adapter.parse_updated_page(soup)
    chapters = adapter.parse_updated_chapters_for_link(
        soup.select_one("a[href='/title/gl3-gun-x-clover']"),
        "gl3",
        "/title/gl3-gun-x-clover",
    )

    assert recent[0].source_id == "gl3"
    assert recent[0].title == "Gun X Clover"
    assert recent[0].cover_url == "https://mangafire.to/covers/gun.webp"
    assert [chapter.number for chapter in chapters] == ["61"]
    assert chapters[0].url == "https://mangafire.to/title/gl3-gun-x-clover/chapter/en61"


def test_mangafire_parses_current_manga_links_with_trailing_id():
    soup = BeautifulSoup(
        """
        <div class="unit">
          <a href="/manga/example-series.abc123" title="Example Series">
            <img data-src="/covers/example.webp">
          </a>
          <a href="/manga/example-series.abc123/chapter/en61">Chapter 61 English Jul 09, 2026</a>
        </div>
        """,
        "html.parser",
    )

    recent = MangaFireAdapter().parse_updated_page(soup)

    assert recent[0].source_id == "abc123"
    assert recent[0].url == "https://mangafire.to/manga/example-series.abc123"
    assert recent[0].metadata["recent_chapters"][0]["number"] == "61"


def test_kingofshojo_rejects_logo_cover_and_prefers_real_summary_image():
    soup = BeautifulSoup(
        """
        <h1>Example</h1>
        <meta property="og:image" content="https://kingofshojo.com/wp-content/uploads/wewtwt.png">
        <div class="summary_image">
          <img data-src="https://cdn.kingofshojo.com/king-bucket/images/example-cover.webp">
        </div>
        """,
        "html.parser",
    )

    item = KingOfShojoAdapter().parse_series_detail(
        soup,
        SeriesItem(
            "kingofshojo", "manga/example", "Example", "https://kingofshojo.com/manga/example/"
        ),
    )

    assert item.cover_url == "https://cdn.kingofshojo.com/king-bucket/images/example-cover.webp"


def test_mangafire_html_detail_fallback_parses_metadata():
    soup = BeautifulSoup(
        """
        <h1>Gun X Clover</h1>
        <meta property="og:image" content="https://img.example/cover.webp">
        <div class="synopsis">A school action series.</div>
        <p>Alternative Titles: Gun Clover / Clover Gun Status Ongoing</p>
        <a href="/genre/action">Action</a>
        """,
        "html.parser",
    )

    item = MangaFireAdapter().parse_series_detail_html(
        soup,
        SeriesItem(
            "mangafire", "gl3", "Gun X Clover", "https://mangafire.to/title/gl3-gun-x-clover"
        ),
    )

    assert item.description == "A school action series."
    assert item.aliases == ("Gun Clover", "Clover Gun")
    assert item.cover_url == "https://img.example/cover.webp"
    assert item.genres == ("Action",)


def test_mangafire_html_detail_preserves_comma_aliases():
    soup = BeautifulSoup(
        """
        <h1>Escape Me If You Can</h1>
        <p>Alternative Titles: Darana Bwa, Naegeseo / I Can't Escape, He Won't Let Me Go / Run Away, From Me / Nigerarenai, Nigasanai Status Ongoing</p>
        """,
        "html.parser",
    )

    item = MangaFireAdapter().parse_series_detail_html(
        soup,
        SeriesItem(
            "mangafire", "oxr4y", "Escape Me If You Can", "https://mangafire.to/title/oxr4y"
        ),
    )

    assert item.aliases == (
        "Darana Bwa, Naegeseo",
        "I Can't Escape, He Won't Let Me Go",
        "Run Away, From Me",
        "Nigerarenai, Nigasanai",
    )


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
                "languages": ["en"],
                "hasVolumes": False,
                "themes": [{"title": "Time Travel"}],
                "demographics": [{"title": "Shoujo"}],
                "authors": [{"title": "MONCHER"}],
                "artists": [{"title": "Jjingttang"}],
                "latestChapter": 100,
                "chapterUpdatedAt": "47m ago",
                "follows": 999,
                "rating": 8.7,
                "ratingCount": 42,
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
    assert item.metadata["rating_count"] == 42
    assert item.metadata["languages"] == ["en"]
    assert item.metadata["has_volumes"] is False
    assert item.metadata["themes"] == ["Time Travel"]
    assert item.metadata["demographics"] == ["Shoujo"]
    assert item.metadata["authors"] == ["MONCHER"]
    assert item.metadata["artists"] == ["Jjingttang"]


def test_mangafire_api_detail_keeps_escape_me_alt_titles():
    adapter = MangaFireAdapter()

    item = adapter.parse_series_detail(
        {
            "data": {
                "hid": "oxr4y",
                "title": "Escape Me If You Can",
                "url": "/title/oxr4y",
                "altTitles": [
                    "Darana Bwa, Naegeseo",
                    "I Can't Escape, He Won't Let Me Go",
                    "Run Away, From Me",
                    "Nigerarenai, Nigasanai",
                    "Escape Me If You Can",
                    "  Run Away, From Me  ",
                    "",
                ],
            }
        }
    )

    assert item.aliases == (
        "Darana Bwa, Naegeseo",
        "I Can't Escape, He Won't Let Me Go",
        "Run Away, From Me",
        "Nigerarenai, Nigasanai",
    )


async def test_mangafire_recent_frontier_uses_titles_api_with_browser_headers(monkeypatch):
    requests = []

    async def handler(request):
        requests.append(request)
        assert request.url.path == "/api/titles"
        assert request.url.params["order[chapter_updated_at]"] == "desc"
        assert request.url.params["limit"] == "2"
        assert request.headers["accept"] == "application/json"
        assert request.headers["x-requested-with"] == "XMLHttpRequest"
        assert request.headers["referer"] == "https://mangafire.to/"
        assert "Mozilla/5.0" in request.headers["user-agent"]
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "hid": "gl3",
                        "title": "Gun X Clover",
                        "url": "/title/gl3-gun-x-clover",
                        "poster": {"large": "https://img.example/large.jpg"},
                        "latestChapter": 61,
                        "chapterUpdatedAt": "just now",
                    }
                ],
                "meta": {"hasNext": False},
            },
            request=request,
        )

    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_recent_limit", 2)
    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_recent_pages", 1)
    adapter = MangaFireAdapter()
    adapter.client = HttpSourceClient(
        "https://mangafire.to",
        transport=httpx.MockTransport(handler),
    )
    try:
        items = await adapter.list_recent_frontier([])
    finally:
        await adapter.aclose()

    assert len(requests) == 1
    assert items[0].source_id == "gl3"
    assert items[0].cover_url == "https://img.example/large.jpg"
    assert items[0].metadata["recent_chapters"][0]["number"] == "61"


async def test_mangafire_recent_frontier_imports_api_rows_when_homepage_is_js_shell(monkeypatch):
    requested_paths = []

    async def handler(request):
        requested_paths.append(request.url.path)
        if request.url.path == "/":
            return httpx.Response(
                200,
                text='<html><body><div id="app"></div><script src="/app.js"></script></body></html>',
                request=request,
            )
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "hid": "api1",
                        "title": "API Title",
                        "url": "/title/api1-api-title",
                        "latestChapter": 12,
                    }
                ]
            },
            request=request,
        )

    adapter = MangaFireAdapter()
    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_recent_pages", 1)
    adapter.client = HttpSourceClient(
        "https://mangafire.to",
        transport=httpx.MockTransport(handler),
    )
    try:
        items = await adapter.list_recent_frontier([])
    finally:
        await adapter.aclose()

    assert requested_paths == ["/api/titles"]
    assert [item.title for item in items] == ["API Title"]


async def test_mangafire_recent_frontier_stops_after_api_sentinel_hits(monkeypatch):
    pages = []

    async def handler(request):
        pages.append(request.url.params["page"])
        page = request.url.params["page"]
        payload = {
            "1": {
                "items": [
                    {
                        "hid": "fresh",
                        "title": "Fresh",
                        "url": "/title/fresh-fresh",
                        "latestChapter": 2,
                    }
                ]
            },
            "2": {
                "items": [
                    {
                        "hid": "known",
                        "title": "Known",
                        "url": "/title/known-known",
                        "latestChapter": 10,
                    }
                ]
            },
        }[page]
        return httpx.Response(200, json=payload, request=request)

    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_recent_pages", 5)
    monkeypatch.setattr("app.adapters.mangafire.settings.source_frontier_required_hits", 1)
    adapter = MangaFireAdapter()
    adapter.client = HttpSourceClient(
        "https://mangafire.to",
        transport=httpx.MockTransport(handler),
    )
    try:
        items = await adapter.list_recent_frontier([FrontierSentinel("known", "10")])
    finally:
        await adapter.aclose()

    assert pages == ["1", "2"]
    assert [item.source_id for item in items] == ["fresh", "known"]


async def test_mangafire_recent_frontier_falls_back_to_html_when_api_fails(monkeypatch):
    requested_paths = []

    async def handler(request):
        requested_paths.append(request.url.path)
        if request.url.path == "/api/titles":
            return httpx.Response(500, request=request)
        return httpx.Response(
            200,
            text="""
            <div class="unit">
              <a href="/title/gl3-gun-x-clover" title="Gun X Clover">
                <img data-src="/covers/gun.webp">
              </a>
              <a href="/title/gl3-gun-x-clover/chapter/en61">Chapter 61 English Jul 09, 2026</a>
            </div>
            """,
            request=request,
        )

    adapter = MangaFireAdapter()
    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_recent_pages", 1)
    adapter.client = HttpSourceClient(
        "https://mangafire.to",
        transport=httpx.MockTransport(handler),
    )
    try:
        items = await adapter.list_recent_frontier([])
    finally:
        await adapter.aclose()

    assert requested_paths == ["/api/titles", "/"]
    assert items[0].source_id == "gl3"
    assert items[0].metadata["recent_chapters"][0]["number"] == "61"


def test_mangafire_defaults_to_new_latest_updates_mode(monkeypatch):
    adapter = MangaFireAdapter()

    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_discovery_mode", "new")

    query = adapter.recent_titles_query()

    assert "order%5Bchapter_updated_at%5D=desc" in query
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


async def test_ordered_page_fetch_uses_bounded_sliding_window():
    class WindowClient:
        base_url = "https://mangafire.window.test"

        def __init__(self):
            self.started: list[str] = []
            self.release_first = asyncio.Event()

        async def get_bytes(self, url: str, referer: str = "") -> bytes:
            self.started.append(url)
            if url.endswith("/0"):
                await self.release_first.wait()
            return url.encode()

    client = WindowClient()
    iterator = iter_ordered_bytes(
        client,
        [f"https://mangafire.window.test/{index}" for index in range(100)],
        concurrency=3,
    )
    first_page = asyncio.create_task(anext(iterator))
    await asyncio.sleep(0.05)
    assert len(client.started) == 3
    client.release_first.set()
    assert await first_page == b"https://mangafire.window.test/0"
    await iterator.aclose()
