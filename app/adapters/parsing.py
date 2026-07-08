from __future__ import annotations

import json
import re
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

    return dedupe_preserving_order(urls)


def extract_urls_from_script(text: str, base_url: str) -> list[str]:
    urls: list[str] = []
    for raw in re.findall(r"https?://[^'\"\\\s]+", text):
        cleaned = raw.replace("\\/", "/")
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
