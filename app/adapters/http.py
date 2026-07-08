from __future__ import annotations

import asyncio
import time

import httpx
from bs4 import BeautifulSoup

from app.settings import settings


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
        headers = {}
        if referer:
            headers["Referer"] = referer
        await self.wait_for_throttle()
        async with self.client.stream("GET", url, headers=headers) as response:
            response.raise_for_status()
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
        response.raise_for_status()
        return response

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
