from __future__ import annotations

import asyncio
from email.utils import parsedate_to_datetime
import time
from datetime import datetime, timedelta, timezone

import httpx
from bs4 import BeautifulSoup

from app.adapters.base import SourceRateLimited
from app.settings import settings

_page_semaphores: dict[str, asyncio.Semaphore] = {}


class HttpSourceClient:
    def __init__(
        self,
        base_url: str,
        timeout: float | None = None,
        throttle_seconds: float = 0.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout or settings.request_timeout_seconds
        self.throttle_seconds = throttle_seconds
        self.transport = transport
        self._client: httpx.AsyncClient | None = None
        self._last_request_at = 0.0
        self._lock = asyncio.Lock()

    async def get_soup(self, path_or_url: str) -> BeautifulSoup:
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        response = await self.request("GET", url)
        return BeautifulSoup(response.text, "html.parser")

    async def get_json(self, path_or_url: str):
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        response = await self.request(
            "GET",
            url,
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        return response.json()

    async def get_bytes(self, url: str, referer: str = "") -> bytes:
        for attempt in range(1, 3):
            try:
                return await self._get_bytes_once(url, referer)
            except (httpx.RemoteProtocolError, httpx.ReadError) as exc:
                if attempt == 2 or not is_partial_body_error(exc):
                    raise
                await asyncio.sleep(0.25 * attempt)
        raise RuntimeError("unreachable")

    async def _get_bytes_once(self, url: str, referer: str = "") -> bytes:
        headers = {}
        if referer:
            headers["Referer"] = referer
        await self.wait_for_throttle()
        async with self.client.stream("GET", url, headers=headers) as response:
            self.raise_for_status(response)
            content_type = response.headers.get("content-type", "")
            if content_type and not content_type.startswith("image/"):
                raise RuntimeError(f"unexpected content type {content_type} for {url}")
            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    too_large = int(content_length) > settings.max_page_bytes
                except ValueError:
                    too_large = False
                if too_large:
                    raise RuntimeError(
                        f"image exceeds max_page_bytes ({settings.max_page_bytes}) for {url}"
                    )
            content = bytearray()
            async for chunk in response.aiter_bytes():
                content.extend(chunk)
                if len(content) > settings.max_page_bytes:
                    raise RuntimeError(
                        f"image exceeds max_page_bytes ({settings.max_page_bytes}) for {url}"
                    )
        return bytes(content)

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={"User-Agent": settings.user_agent},
                follow_redirects=True,
                transport=self.transport,
            )
        return self._client

    async def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        await self.wait_for_throttle()
        response = await self.client.request(method, url, headers=headers)
        self.raise_for_status(response)
        return response

    def raise_for_status(self, response: httpx.Response) -> None:
        if response.status_code == 429:
            raise SourceRateLimited(
                f"rate limited by {self.base_url}",
                retry_after=retry_after_from_headers(response.headers),
            )
        response.raise_for_status()

    async def wait_for_throttle(self) -> None:
        if self.throttle_seconds <= 0:
            return
        async with self._lock:
            elapsed = time.monotonic() - self._last_request_at
            remaining = self.throttle_seconds - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
            self._last_request_at = time.monotonic()

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


def page_concurrency_for_source(source: str) -> int:
    if source == "asura":
        return settings.asura_page_concurrency
    if source == "mangafire":
        return settings.mangafire_page_concurrency
    if source == "kingofshojo":
        return settings.kingofshojo_page_concurrency
    return 1


async def iter_ordered_bytes(
    client: HttpSourceClient,
    urls: list[str],
    *,
    referer: str = "",
    concurrency: int = 1,
):
    if concurrency <= 1:
        for url in urls:
            yield await client.get_bytes(url, referer=referer)
        return

    semaphore = asyncio.Semaphore(concurrency)
    source_semaphore = page_semaphore_for_client(client)

    async def fetch(url: str) -> bytes:
        async with semaphore:
            async with source_semaphore:
                return await client.get_bytes(url, referer=referer)

    tasks = [asyncio.create_task(fetch(url)) for url in urls]
    try:
        for task in tasks:
            yield await task
    except Exception:
        for task in tasks:
            task.cancel()
        raise


def page_semaphore_for_client(client: HttpSourceClient) -> asyncio.Semaphore:
    semaphore = _page_semaphores.get(client.base_url)
    if semaphore is None:
        semaphore = asyncio.Semaphore(page_concurrency_for_base_url(client.base_url))
        _page_semaphores[client.base_url] = semaphore
    return semaphore


def page_concurrency_for_base_url(base_url: str) -> int:
    if "asura" in base_url:
        return settings.asura_page_concurrency
    if "mangafire" in base_url:
        return settings.mangafire_page_concurrency
    if "kingofshojo" in base_url:
        return settings.kingofshojo_page_concurrency
    return 1


def is_partial_body_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return (
        "peer closed connection without sending complete message body" in text
        or "incomplete message body" in text
    )


def retry_after_from_headers(headers: httpx.Headers) -> datetime | None:
    value = headers.get("retry-after")
    if not value:
        return None
    now = datetime.now(timezone.utc)
    try:
        seconds = int(value)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    return now + timedelta(seconds=max(seconds, 0))
