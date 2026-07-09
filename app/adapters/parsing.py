from __future__ import annotations

import json
import re
from html import unescape
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup


IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif")


def first_attr(tag, *names: str) -> str:
    for name in names:
        value = tag.get(name)
        if value:
            if name == "srcset":
                return str(value).split(",")[0].strip().split(" ")[0]
            return str(value)
    return ""


def image_attr(tag) -> str:
    return first_attr(
        tag,
        "data-src",
        "data-lazy-src",
        "data-original",
        "data-url",
        "data-cfsrc",
        "srcset",
        "src",
    )


def nearby_cover_attr(link) -> str:
    image = link.select_one("img")
    if image:
        return image_attr(image)
    for parent in link.parents:
        if getattr(parent, "name", None) in {"body", "html", "[document]"}:
            break
        image = parent.select_one("img")
        if image:
            return image_attr(image)
        for attr in ("data-bg", "data-background", "data-src", "style"):
            value = str(parent.get(attr) or "")
            if not value:
                continue
            if attr == "style":
                match = re.search(r"url\(['\"]?([^'\")]+)", value)
                if match:
                    return match.group(1)
            else:
                return value
    og_image = link.find_next("meta", attrs={"property": "og:image"})
    return str(og_image.get("content") or "") if og_image else ""


def parse_source_date(value: str) -> datetime | None:
    value = " ".join((value or "").replace(",", " ").split())
    if not value:
        return None
    now = datetime.now(timezone.utc)
    lowered = value.lower()
    if "just now" in lowered or "today" in lowered:
        return now
    if "yesterday" in lowered:
        return now - timedelta(days=1)
    for fmt in ("%b %d %Y", "%B %d %Y", "%Y-%m-%d", "%m/%d/%Y"):
        match = re.search(r"([A-Za-z]{3,9}\s+\d{1,2}\s+\d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})", value)
        if not match:
            continue
        try:
            return datetime.strptime(match.group(1), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def clean_chapter_title(number: str, title: str, published_at: datetime | None = None) -> str:
    title = " ".join((title or "").replace("Latest:", "").split())
    if published_at is not None:
        for fmt in ("%b %-d, %Y", "%B %-d, %Y", "%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
            try:
                title = title.replace(published_at.strftime(fmt), "")
            except ValueError:
                continue
    title = re.sub(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b", "", title)
    title = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "", title)
    title = re.sub(r"\b(?:just now|today|yesterday|last week|\d+\s+\w+\s+ago)\b", "", title, flags=re.I)
    title = " ".join(title.split("·")).strip()
    title = " ".join(title.split())
    return title or f"Chapter {number}"


def extract_image_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    urls: list[str] = []
    for image in soup.select("img"):
        candidate = first_attr(
            image,
            "data-src",
            "data-lazy-src",
            "data-original",
            "data-url",
            "srcset",
            "src",
        )
        if is_probable_page_image(candidate):
            urls.append(urljoin(base_url, candidate))

    for script in soup.select("script"):
        text = script.string or script.get_text(" ", strip=True)
        if not text:
            continue
        urls.extend(extract_urls_from_script(text, base_url))

    for island in soup.select("astro-island[props]"):
        props = island.get("props")
        if props:
            urls.extend(extract_urls_from_script(str(props), base_url))

    urls.extend(extract_urls_from_script(soup.decode(), base_url))
    return dedupe_preserving_order(urls)


def extract_urls_from_script(text: str, base_url: str) -> list[str]:
    urls: list[str] = []
    text = unescape(text).replace("\\/", "/")
    for raw in re.findall(r"https?://[^'\"<>\s]+", text):
        cleaned = raw.rstrip("),];")
        if is_probable_page_image(cleaned):
            urls.append(cleaned)

    for match in re.findall(r"(\[[^\]]{20,}\]|\{[^{}]{20,}\})", text):
        try:
            payload = json.loads(match)
        except Exception:
            continue
        urls.extend(extract_urls_from_json(payload, base_url))
    return urls


def extract_urls_from_json(payload, base_url: str) -> list[str]:
    urls: list[str] = []
    if isinstance(payload, str):
        if is_probable_page_image(payload):
            urls.append(urljoin(base_url, payload))
    elif isinstance(payload, list):
        for item in payload:
            urls.extend(extract_urls_from_json(item, base_url))
    elif isinstance(payload, dict):
        for value in payload.values():
            urls.extend(extract_urls_from_json(value, base_url))
    return urls


def is_probable_page_image(url: str) -> bool:
    if not url:
        return False
    url = unescape(str(url)).replace("\\/", "/").strip()
    lowered = url.lower().split("?")[0]
    if not lowered.endswith(IMAGE_EXTENSIONS):
        return False
    blocked = ("logo", "avatar", "banner", "cover", "thumbnail", "thumb", "icon", "placeholder")
    return not any(token in lowered for token in blocked)


def dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result
