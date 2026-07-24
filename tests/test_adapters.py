import asyncio
import html
import json

import pytest
import httpx
from bs4 import BeautifulSoup

from app.adapters.asura import AsuraAdapter
from app.adapters.http import (
    HttpSourceClient,
    configure_provider_waiter,
    configure_request_observer,
)
from app.adapters.base import FrontierSentinel, SourceRateLimited
from app.adapters.kingofshojo import KingOfShojoAdapter
from app.adapters.mangadex import (
    MangaDexAdapter,
    select_chapter_releases,
    series_from_chapter,
)
from app.adapters.mangafire import MangaFireAdapter
from app.adapters.http import iter_ordered_bytes
from app.adapters.parsing import extract_image_urls
from app.domain import SeriesItem


class StaticVrf:
    def __init__(self) -> None:
        self.refreshes = 0

    async def token(self, _path: str, _params: dict[str, object]) -> str:
        return "test-token"

    async def refresh(self) -> None:
        self.refreshes += 1


def mangadex_chapter(
    release_id: str,
    *,
    number: str = "12",
    language: str = "en",
    manga_id: str = "manga-id",
    group: str = "Group",
    official: bool = False,
    verified: bool = False,
    pages: int = 20,
) -> dict:
    return {
        "id": release_id,
        "type": "chapter",
        "attributes": {
            "chapter": number,
            "title": "",
            "translatedLanguage": language,
            "pages": pages,
            "readableAt": "2026-07-24T08:00:00+00:00",
        },
        "relationships": [
            {
                "id": manga_id,
                "type": "manga",
                "attributes": {
                    "title": {"en": "Example Manga"},
                    "altTitles": [{"ja-ro": "Example"}],
                    "description": {"en": "Summary"},
                    "tags": [],
                },
            },
            {
                "id": f"group-{release_id}",
                "type": "scanlation_group",
                "attributes": {
                    "name": group,
                    "official": official,
                    "verified": verified,
                },
            },
        ],
    }


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

    assert recent[0].source_id == "comics/star-embracing-swordmaster"
    assert recent[0].metadata["asura_revision"] == "a80d257e"
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
                    {
                        "id": 154,
                        "number": 60,
                        "name": "Love & Clover",
                        "language": "en",
                        "type": "unofficial",
                    },
                    {
                        "id": 156,
                        "number": 60,
                        "name": "Love & Clover",
                        "language": "en",
                        "type": "official",
                    },
                    {"id": 155, "number": 60, "name": "Spanish", "language": "es"},
                ]
            }
        },
        recent[0],
    )

    assert len(chapters) == 1
    assert chapters[0].url == "https://mangafire.to/title/gl3-gun-x-clover/chapter/156"
    assert chapters[0].metadata == {
        "release_type": "official",
        "verified": True,
        "quality_rank": 100,
    }
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
    adapter.vrf = StaticVrf()
    try:
        items = await adapter.list_recent_frontier([])
    finally:
        await adapter.aclose()

    assert len(requests) == 1
    assert items[0].source_id == "gl3"
    assert items[0].cover_url == "https://img.example/large.jpg"
    assert items[0].metadata["recent_chapters"][0]["number"] == "61"


async def test_mangafire_api_refreshes_rejected_rotating_token():
    requests = 0

    async def handler(request):
        nonlocal requests
        requests += 1
        assert request.url.params["vrf"] == "test-token"
        if requests == 1:
            return httpx.Response(403, json={"message": "Missing token."}, request=request)
        return httpx.Response(200, json={"data": {"pages": []}}, request=request)

    adapter = MangaFireAdapter()
    adapter.client = HttpSourceClient(
        "https://mangafire.to",
        transport=httpx.MockTransport(handler),
        source="mangafire",
    )
    adapter.vrf = StaticVrf()
    try:
        assert await adapter.api_json("/chapters/release-id") == {"data": {"pages": []}}
    finally:
        await adapter.aclose()

    assert requests == 2
    assert adapter.vrf.refreshes == 1


def test_mangadex_latest_feed_keeps_only_configured_language():
    english = series_from_chapter(mangadex_chapter("english"))
    japanese = series_from_chapter(mangadex_chapter("japanese", language="ja"))

    assert english is not None
    assert english.source == "mangadex"
    assert english.source_id == "manga-id"
    assert english.metadata["latest_chapter"] == "12"
    assert japanese is None


def test_mangadex_duplicate_chapters_prefer_official_then_verified_release():
    source = SeriesItem(
        source="mangadex",
        source_id="manga-id",
        title="Example Manga",
        url="https://mangadex.org/title/manga-id",
    )
    rows = [
        mangadex_chapter("unverified", group="Unofficial"),
        mangadex_chapter("verified", group="Verified", verified=True),
        mangadex_chapter("official", group="Official", official=True),
        mangadex_chapter("other-language", language="es", official=True),
        mangadex_chapter("next-chapter", number="13", verified=True),
    ]

    chapters = select_chapter_releases(rows, source)

    assert [chapter.number for chapter in chapters] == ["12", "13"]
    assert chapters[0].metadata["release_id"] == "official"
    assert chapters[0].metadata["quality_rank"] > chapters[1].metadata["quality_rank"]
    assert {
        row["release_id"] for row in chapters[0].metadata["alternate_releases"]
    } == {"unverified", "verified"}


def test_mangadex_duplicate_chapters_prefer_more_complete_release_within_tier():
    source = SeriesItem(
        source="mangadex",
        source_id="manga-id",
        title="Example Manga",
        url="https://mangadex.org/title/manga-id",
    )
    shorter = mangadex_chapter(
        "shorter",
        verified=True,
        pages=9,
    )
    shorter["attributes"]["readableAt"] = "2026-07-24T09:00:00+00:00"
    complete = mangadex_chapter(
        "complete",
        verified=True,
        pages=28,
    )
    complete["attributes"]["readableAt"] = "2026-07-23T09:00:00+00:00"

    chapters = select_chapter_releases(
        [shorter, complete],
        source,
    )

    assert [chapter.metadata["release_id"] for chapter in chapters] == ["complete"]
    assert chapters[0].metadata["alternate_releases"][0]["release_id"] == "shorter"


async def test_mangadex_at_home_download_uses_original_not_data_saver_pages():
    requested: list[str] = []

    async def api_handler(request):
        requested.append(str(request.url))
        return httpx.Response(
            200,
            json={
                "baseUrl": "https://uploads.mangadex.test",
                "chapter": {
                    "hash": "hash",
                    "data": ["001.png", "002.png"],
                    "dataSaver": ["001-low.jpg", "002-low.jpg"],
                },
            },
            request=request,
        )

    async def image_handler(request):
        requested.append(str(request.url))
        return httpx.Response(
            200,
            headers={"content-type": "image/png"},
            content=request.url.path.encode(),
            request=request,
        )

    adapter = MangaDexAdapter()
    adapter.at_home_client = HttpSourceClient(
        "https://api.mangadex.org",
        transport=httpx.MockTransport(api_handler),
        source="mangadex",
    )
    adapter.client = HttpSourceClient(
        "https://api.mangadex.org",
        transport=httpx.MockTransport(image_handler),
        source="mangadex",
    )
    chapter = select_chapter_releases(
        [mangadex_chapter("release-id")],
        SeriesItem("mangadex", "manga-id", "Example", "https://mangadex.org/title/manga-id"),
    )[0]
    try:
        pages = await adapter.download_chapter_pages(chapter)
    finally:
        await adapter.aclose()

    assert len(pages) == 2
    assert any("/data/hash/001.png" in url for url in requested)
    assert all("data-saver" not in url and "-low.jpg" not in url for url in requested)


async def test_asura_second_page_404_is_normal_pagination_end(monkeypatch):
    requested = []

    async def handler(request):
        page = request.url.params.get("page", "1")
        requested.append(page)
        if page == "1":
            return httpx.Response(
                200,
                text=(
                    '<section><h2>Latest Updates</h2>'
                    '<a href="/comics/one"><img src="/cover.jpg">One</a>'
                    '</section><a href="/?page=2">2</a>'
                ),
                request=request,
            )
        return httpx.Response(404, request=request)

    monkeypatch.setattr("app.adapters.asura.settings.asura_recent_pages", 3)
    adapter = AsuraAdapter()
    adapter.client = HttpSourceClient(
        "https://asurascans.com", transport=httpx.MockTransport(handler)
    )
    try:
        items = await adapter.list_recent_frontier([FrontierSentinel("comics/older", "9")])
    finally:
        await adapter.aclose()

    assert requested == ["1", "2"]
    assert [item.title for item in items] == ["One"]
    assert adapter.listing_diagnostics["tracked_fallback_required"] is True


async def test_asura_follows_query_pagination_and_scopes_every_page(monkeypatch):
    requested_pages = []

    async def handler(request):
        page = int(request.url.params.get("page", "1"))
        requested_pages.append(page)
        source_id = "known" if page == 4 else f"fresh-{page}"
        chapter = "10" if page == 4 else str(20 - page)
        return httpx.Response(
            200,
            text=f"""
            <section><h2>Trending Comics</h2>
              <a href="/comics/noise-{page}">Trending noise {page}</a>
            </section>
            <section><h2>Latest Updates</h2><div>
              <a href="/comics/{source_id}">Series {page}</a>
              <a href="/comics/{source_id}/chapter/{chapter}">Chapter {chapter}</a>
            </div></section>
            {f'<button aria-label="Page {page + 1}">{page + 1}</button>' if page < 4 else ''}
            """,
            request=request,
        )

    monkeypatch.setattr("app.adapters.asura.settings.asura_recent_pages", 5)
    monkeypatch.setattr("app.adapters.asura.settings.source_frontier_required_hits", 1)
    adapter = AsuraAdapter()
    adapter.client = HttpSourceClient(
        "https://asurascans.com", transport=httpx.MockTransport(handler)
    )
    try:
        items = await adapter.list_recent_frontier([FrontierSentinel("comics/known", "10")])
    finally:
        await adapter.aclose()

    assert requested_pages == [1, 2, 3, 4]
    assert [item.source_id for item in items] == [
        "comics/fresh-1",
        "comics/fresh-2",
        "comics/fresh-3",
        "comics/known",
    ]
    assert all("noise" not in item.source_id for item in items)
    assert adapter.listing_diagnostics["frontier_reached"] is True


async def test_asura_homepage_prefers_chronological_card_over_trending_duplicate(monkeypatch):
    async def handler(request):
        return httpx.Response(
            200,
            text="""
            <section>
              <h2>Trending Comics</h2>
              <div><a href="/comics/goblin-inc-1d35e5bd"><img src="/old.jpg">Goblin Inc</a></div>
            </section>
            <section>
              <h2>Latest Updates</h2>
              <div>
                <a href="/comics/goblin-inc-1d35e5bd"><img src="/new.jpg">Goblin Inc</a>
                <a href="/comics/goblin-inc-1d35e5bd/chapter/5">Chapter 5 just now</a>
              </div>
            </section>
            """,
            request=request,
        )

    monkeypatch.setattr("app.adapters.asura.settings.asura_recent_pages", 1)
    adapter = AsuraAdapter()
    adapter.client = HttpSourceClient(
        "https://asurascans.com", transport=httpx.MockTransport(handler)
    )
    try:
        items = await adapter.list_recent_frontier([])
    finally:
        await adapter.aclose()

    assert [item.title for item in items] == ["Goblin Inc"]
    assert items[0].cover_url == "https://asurascans.com/new.jpg"
    assert items[0].metadata["recent_chapters"][0]["number"] == "5"
    assert adapter.listing_diagnostics["listing_exhausted"] is True


async def test_asura_reads_every_embedded_latest_update_not_only_rendered_first_page(
    monkeypatch,
):
    chapters = [
        [
            0,
            {
                "name": [0, str(index)],
                "number": [0, index],
                "title": [0],
                "comic_name": [0, f"Series {index}"],
                "comic_public_url": [0, f"/comics/series-{index}-1d35e5bd"],
                "comic_cover": [0, f"https://cdn.test/series-{index}.webp"],
            },
        ]
        for index in range(1, 26)
    ]
    props = html.escape(json.dumps({"chapters": [1, chapters]}), quote=True)

    async def handler(request):
        return httpx.Response(
            200,
            text=f"""
            <section><h2>Latest Updates</h2>
              <a href="/comics/series-1-1d35e5bd">Series 1</a>
            </section>
            <astro-island component-url="/_astro/LatestUpdates.hash.js" props="{props}">
            </astro-island>
            """,
            request=request,
        )

    monkeypatch.setattr("app.adapters.asura.settings.asura_recent_pages", 20)
    adapter = AsuraAdapter()
    adapter.client = HttpSourceClient(
        "https://asurascans.com", transport=httpx.MockTransport(handler)
    )
    try:
        items = await adapter.list_recent_frontier([])
    finally:
        await adapter.aclose()

    assert len(items) == 25
    assert items[-1].source_id == "comics/series-25"
    assert items[-1].metadata["recent_chapters"][0]["number"] == "25"
    assert adapter.listing_diagnostics["pages_fetched"] == 1
    assert adapter.listing_diagnostics["listing_exhausted"] is True


async def test_kingofshojo_follows_ordered_catalog_query_beyond_three_pages(monkeypatch):
    requested_pages = []

    async def handler(request):
        page = int(request.url.params["page"])
        requested_pages.append(page)
        source_id = "known" if page == 4 else f"fresh-{page}"
        chapter = "10" if page == 4 else str(20 - page)
        return httpx.Response(
            200,
            text=f"""
            <aside><a href="/manga/sidebar-noise/" title="Sidebar noise">Noise</a></aside>
            <div class="postbody"><div class="listupd"><div class="bsx">
              <a href="/manga/{source_id}/" title="Series {page}">
                <img src="/cover-{page}.jpg">
                <div class="epxs">Chapter {chapter}</div>
              </a>
            </div></div></div>
            {('<div class="pagination"><a href="?page=' + str(page + 1) + '&order=update">Next</a></div>') if page < 4 else ''}
            """,
            request=request,
        )

    monkeypatch.setattr("app.adapters.kingofshojo.settings.kingofshojo_recent_pages", 5)
    monkeypatch.setattr("app.adapters.kingofshojo.settings.source_frontier_required_hits", 1)
    adapter = KingOfShojoAdapter()
    adapter.client = HttpSourceClient(
        "https://kingofshojo.com", transport=httpx.MockTransport(handler)
    )
    try:
        items = await adapter.list_recent_frontier([FrontierSentinel("manga/known", "10")])
    finally:
        await adapter.aclose()

    assert requested_pages == [1, 2, 3, 4]
    assert adapter.listing_diagnostics == {
        "pages_fetched": 4,
        "frontier_reached": True,
        "listing_exhausted": False,
        "safety_limit_reached": False,
    }
    assert [item.source_id for item in items] == [
        "manga/fresh-1",
        "manga/fresh-2",
        "manga/fresh-3",
        "manga/known",
    ]
    assert all(item.title != "Sidebar noise" for item in items)
    assert [item.metadata["recent_chapters"][0]["number"] for item in items] == [
        "19",
        "18",
        "17",
        "10",
    ]


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
    adapter.vrf = StaticVrf()
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
    adapter.vrf = StaticVrf()
    try:
        items = await adapter.list_recent_frontier([FrontierSentinel("known", "10")])
    finally:
        await adapter.aclose()

    assert pages == ["1", "2"]
    assert [item.source_id for item in items] == ["fresh", "known"]


async def test_mangafire_frontier_honors_configured_window_beyond_three_pages(monkeypatch):
    pages = []

    async def handler(request):
        page = int(request.url.params["page"])
        pages.append(page)
        source_id = "known" if page == 4 else f"fresh-{page}"
        chapter = 10 if page == 4 else 20 - page
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "hid": source_id,
                        "title": f"Series {page}",
                        "url": f"/title/{source_id}-series-{page}",
                        "latestChapter": chapter,
                    }
                ],
                "meta": {"hasNext": True},
            },
            request=request,
        )

    monkeypatch.setattr("app.adapters.mangafire.settings.mangafire_recent_pages", 5)
    monkeypatch.setattr("app.adapters.mangafire.settings.source_frontier_required_hits", 1)
    adapter = MangaFireAdapter()
    adapter.client = HttpSourceClient(
        "https://mangafire.to", transport=httpx.MockTransport(handler)
    )
    adapter.vrf = StaticVrf()
    try:
        items = await adapter.list_recent_frontier([FrontierSentinel("known", "10")])
    finally:
        await adapter.aclose()

    assert pages == [1, 2, 3, 4]
    assert adapter.listing_diagnostics["frontier_reached"] is True
    assert adapter.listing_diagnostics["safety_limit_reached"] is False
    assert [item.source_id for item in items] == [
        "fresh-1",
        "fresh-2",
        "fresh-3",
        "known",
    ]


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
    adapter.vrf = StaticVrf()
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


async def test_shared_provider_scheduler_runs_even_when_static_interval_is_zero():
    calls: list[tuple[str, str, float]] = []

    async def waiter(source: str, traffic_class: str, interval: float) -> None:
        calls.append((source, traffic_class, interval))

    configure_provider_waiter(waiter)
    client = HttpSourceClient("https://mangafire.to", throttle_seconds=0)
    try:
        await client.wait_for_throttle("https://cdn.mangafire.to/page.webp")
    finally:
        configure_provider_waiter(None)
        await client.client.aclose()

    assert calls == [("mangafire", "cdn", 0)]


async def test_cover_cdn_request_is_attributed_to_explicit_provider():
    observations: list[dict] = []

    async def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            content=b"cover",
            request=request,
        )

    configure_request_observer(observations.append)
    client = HttpSourceClient(
        "https://i0.wp.com",
        source="kingofshojo",
        provider_origin_url="https://kingofshojo.com",
        transport=httpx.MockTransport(handler),
    )
    try:
        assert await client.get_bytes("https://i0.wp.com/cover.jpg") == b"cover"
    finally:
        configure_request_observer(None)
        await client.aclose()

    assert observations[0]["source"] == "kingofshojo"
    assert observations[0]["host"] == "i0.wp.com"
    assert observations[0]["traffic_class"] == "cdn"
