from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx


BASE_URL = "https://mangafire.to"
API_URL = f"{BASE_URL}/api/titles"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def main() -> int:
    args = parse_args()
    rows: list[dict[str, Any]] = []
    headers = {
        "Accept": "application/json",
        "Referer": f"{BASE_URL}/",
        "User-Agent": USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
    }

    with httpx.Client(headers=headers, follow_redirects=True, timeout=30.0) as client:
        for page in range(args.page, args.page + args.pages):
            payload = fetch_page(client, page=page, limit=args.limit)
            rows.extend(parse_rows(payload, page=page))

    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
    else:
        print_table(rows)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List MangaFire's newest title updates from /api/titles."
    )
    parser.add_argument("--limit", type=positive_int, default=30, help="rows to fetch per page")
    parser.add_argument("--page", type=positive_int, default=1, help="first page to fetch")
    parser.add_argument(
        "--pages",
        type=positive_int,
        default=1,
        help="number of consecutive pages to fetch",
    )
    parser.add_argument("--json", action="store_true", help="dump parsed rows as JSON")
    return parser.parse_args()


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def fetch_page(client: httpx.Client, *, page: int, limit: int) -> Any:
    response = client.get(
        API_URL,
        params={
            "order[chapter_updated_at]": "desc",
            "limit": limit,
            "page": page,
        },
    )
    response.raise_for_status()
    return response.json()


def parse_rows(payload: Any, *, page: int) -> list[dict[str, Any]]:
    rows = []
    for entry in api_items(payload):
        hid = title_hid(entry)
        raw_updated_at = first_present(
            entry,
            "chapterUpdatedAt",
            "chapter_updated_at",
            "latestChapterUpdatedAt",
            "updatedAt",
            "updated_at",
        )
        latest = latest_chapter(entry)
        if raw_updated_at in (None, "") and isinstance(latest, dict):
            raw_updated_at = first_present(
                latest,
                "updatedAt",
                "updated_at",
                "createdAt",
                "created_at",
                "uploadedAt",
                "uploaded_at",
                "publishedAt",
                "published_at",
            )
        rows.append(
            {
                "page": page,
                "title": str(entry.get("title") or entry.get("name") or ""),
                "type": title_type(entry.get("type")),
                "latest_chapter": latest_chapter_label(latest, entry),
                "updated_at": raw_updated_at,
                "update_age": age_text(raw_updated_at),
                "hid": hid,
                "url": title_url(entry, hid),
                "raw": entry,
            }
        )
    return rows


def api_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("items", "titles", "data"):
            if isinstance(data.get(key), list):
                return [item for item in data[key] if isinstance(item, dict)]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    for key in ("items", "titles"):
        if isinstance(payload.get(key), list):
            return [item for item in payload[key] if isinstance(item, dict)]
    return []


def title_hid(entry: dict[str, Any]) -> str:
    hid = entry.get("hid") or entry.get("id")
    if hid:
        return str(hid)
    return hid_from_url(str(entry.get("url") or ""))


def hid_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if path.startswith("manga/"):
        slug = path.split("/", 1)[1].strip("/")
        return slug.rsplit(".", 1)[-1] if "." in slug else slug
    if path.startswith("title/"):
        return path.split("/", 1)[1].split("-", 1)[0]
    return ""


def title_type(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("name") or value.get("title") or value.get("slug") or "")
    return str(value or "")


def latest_chapter(entry: dict[str, Any]) -> Any:
    latest = first_present(
        entry,
        "latestChapter",
        "latest_chapter",
        "lastChapter",
        "last_chapter",
    )
    if isinstance(latest, list):
        return latest[0] if latest else None
    return latest


def latest_chapter_label(latest: Any, entry: dict[str, Any]) -> str:
    if isinstance(latest, dict):
        label = first_present(
            latest,
            "name",
            "title",
            "number",
            "chapter",
            "chapterNumber",
            "chapter_number",
        )
        return chapter_label(label)
    if latest not in (None, ""):
        return chapter_label(latest)
    label = first_present(
        entry,
        "latestChapterName",
        "latest_chapter_name",
        "latestChapterNumber",
        "latest_chapter_number",
        "chapter",
    )
    return chapter_label(label)


def chapter_label(value: Any) -> str:
    if value in (None, ""):
        return ""
    text = str(value)
    return text if text.lower().startswith("chapter") else f"Chapter {text}"


def title_url(entry: dict[str, Any], hid: str) -> str:
    url = str(entry.get("url") or "")
    if url:
        return urljoin(BASE_URL, url)
    return urljoin(BASE_URL, f"/title/{hid}") if hid else BASE_URL


def first_present(values: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in values and values[key] not in (None, ""):
            return values[key]
    return None


def age_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    parsed = parse_datetime(value)
    if parsed is None:
        return str(value)
    now = datetime.now(timezone.utc)
    delta = now - parsed
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return plural(minutes, "minute")
    hours = minutes // 60
    if hours < 48:
        return plural(hours, "hour")
    days = hours // 24
    if days < 60:
        return plural(days, "day")
    months = days // 30
    if months < 24:
        return plural(months, "month")
    years = days // 365
    return plural(years, "year")


def parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, int | float):
        parsed = datetime.fromtimestamp(normalize_epoch(value), tz=timezone.utc)
    else:
        text = str(value).strip()
        if text.isdigit():
            parsed = datetime.fromtimestamp(normalize_epoch(int(text)), tz=timezone.utc)
        else:
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_epoch(value: int | float) -> float:
    return value / 1000 if value > 10_000_000_000 else value


def plural(value: int, unit: str) -> str:
    suffix = "" if value == 1 else "s"
    return f"{value} {unit}{suffix} ago"


def print_table(rows: list[dict[str, Any]]) -> None:
    display_rows = [
        [
            str(index),
            str(row["title"]),
            str(row["type"]),
            str(row["latest_chapter"]),
            str(row["update_age"]),
            str(row["hid"]),
            str(row["url"]),
        ]
        for index, row in enumerate(rows, start=1)
    ]
    headers = ["#", "Title", "Type", "Latest", "Updated", "HID", "URL"]
    widths = column_widths([headers, *display_rows])
    print(format_row(headers, widths))
    print(format_row(["-" * width for width in widths], widths))
    for row in display_rows:
        print(format_row(row, widths))


def column_widths(rows: list[list[str]]) -> list[int]:
    return [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]


def format_row(row: list[str], widths: list[int]) -> str:
    padded = [value.ljust(widths[index]) for index, value in enumerate(row)]
    return "  ".join(padded)


if __name__ == "__main__":
    raise SystemExit(main())
