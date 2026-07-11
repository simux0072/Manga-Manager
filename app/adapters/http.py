from __future__ import annotations

import asyncio
from email.utils import parsedate_to_datetime
import time
import weakref
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx
from bs4 import BeautifulSoup

from app.adapters.base import SourceRateLimited
from app.settings import settings

_page_semaphores: dict[str, asyncio.Semaphore] = {}
_request_schedulers: dict[str, tuple[asyncio.Lock, float]] = {}
_worker_page_semaphores: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
WORKER_INFLIGHT_BYTE_BUDGET = 256 * 1024 * 1024
_provider_waiter: Callable[[str, float], Awaitable[None]] | None = None


@dataclass(slots=True)
class ReservedPage:
    content: bytes
    semaphore: asyncio.Semaphore
    released: bool = False

    def release(self) -> None:
        if not self.released:
            self.semaphore.release()
            self.released = True


def configure_provider_waiter(
    waiter: Callable[[str, float], Awaitable[None]] | None,
) -> None:
    global _provider_waiter
    _provider_waiter = waiter


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
        for attempt in range(1, 4):
            try:
                return await self._get_bytes_once(url, referer)
            except (httpx.RemoteProtocolError, httpx.ReadError) as exc:
                if attempt == 3 or not is_partial_body_error(exc):
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
        if _provider_waiter is not None:
            await _provider_waiter(source_for_base_url(self.base_url), self.throttle_seconds)
            return
        lock, last_request_at = _request_schedulers.setdefault(self.base_url, (asyncio.Lock(), 0.0))
        async with lock:
            # Fetch again after waiting for the lock; another client may have updated it.
            _, last_request_at = _request_schedulers[self.base_url]
            elapsed = time.monotonic() - last_request_at
            remaining = self.throttle_seconds - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
            _request_schedulers[self.base_url] = (lock, time.monotonic())

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

    window = max(1, concurrency)
    semaphore = asyncio.Semaphore(window)
    source_semaphore = page_semaphore_for_client(client)
    worker_semaphore = worker_page_semaphore()

    async def fetch(url: str) -> ReservedPage:
        async with semaphore:
            async with source_semaphore:
                await worker_semaphore.acquire()
                try:
                    return ReservedPage(
                        await client.get_bytes(url, referer=referer), worker_semaphore
                    )
                except BaseException:
                    worker_semaphore.release()
                    raise

    tasks: dict[int, asyncio.Task[ReservedPage]] = {}
    next_index = 0

    def fill_window() -> None:
        nonlocal next_index
        while next_index < len(urls) and len(tasks) < window:
            tasks[next_index] = asyncio.create_task(fetch(urls[next_index]))
            next_index += 1

    fill_window()
    try:
        for index in range(len(urls)):
            task = tasks.pop(index)
            page = await task
            fill_window()
            try:
                yield page.content
            finally:
                page.release()
    except Exception:
        raise
    finally:
        for task in tasks.values():
            task.cancel()
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for result in results:
            if isinstance(result, ReservedPage):
                result.release()


def worker_page_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    semaphore = _worker_page_semaphores.get(loop)
    if semaphore is None:
        max_page_bytes = max(1, settings.max_page_bytes)
        capacity = max(1, WORKER_INFLIGHT_BYTE_BUDGET // max_page_bytes)
        semaphore = asyncio.Semaphore(capacity)
        _worker_page_semaphores[loop] = semaphore
    return semaphore


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


def source_for_base_url(base_url: str) -> str:
    if "asura" in base_url:
        return "asura"
    if "mangafire" in base_url:
        return "mangafire"
    if "kingofshojo" in base_url:
        return "kingofshojo"
    return base_url


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
