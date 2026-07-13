from __future__ import annotations

import hashlib
import io
from urllib.parse import urlsplit

from PIL import Image, ImageOps
from sqlalchemy import select

from app.adapters.http import HttpSourceClient
from manga_manager.infrastructure.db_models import (
    CatalogCoverFingerprint,
    CatalogMatchDecision,
    CatalogSourceSeries,
)
from manga_manager.domain.providers import PROVIDER_ORIGINS
from manga_manager.worker.runtime import SessionFactory


ALGORITHM = "dhash-crop-v2"


def fingerprint_cover(content: bytes) -> tuple[str, str, int, int]:
    with Image.open(io.BytesIO(content)) as opened:
        image = ImageOps.exif_transpose(opened).convert("RGB")
        width, height = image.size
        # Publisher/site badges tend to live at the edges; compare the central artwork.
        x_margin, y_margin = int(width * 0.08), int(height * 0.06)
        image = image.crop((x_margin, y_margin, width - x_margin, height - y_margin))
        gray = image.resize((9, 8), Image.Resampling.LANCZOS).convert("L")
        values = list(gray.get_flattened_data())
        bits = 0
        for y in range(8):
            for x in range(8):
                bits = (bits << 1) | int(values[y * 9 + x] > values[y * 9 + x + 1])
    return f"{bits:016x}", hashlib.sha256(content).hexdigest(), width, height


def hamming_distance(left: str, right: str) -> int:
    return (int(left, 16) ^ int(right, 16)).bit_count()


class CoverEvidenceService:
    def __init__(self, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory

    async def refresh_for_source_series(self, source_series_id: int) -> None:
        with self.session_factory() as session:
            decisions = session.scalars(
                select(CatalogMatchDecision).where(
                    CatalogMatchDecision.decision == "pending",
                    (CatalogMatchDecision.left_source_series_id == source_series_id)
                    | (CatalogMatchDecision.right_source_series_id == source_series_id),
                )
            ).all()
            ids = {
                value
                for row in decisions
                for value in (row.left_source_series_id, row.right_source_series_id)
            }
        for identity_id in ids:
            await self._ensure_fingerprint(identity_id)
        with self.session_factory() as session, session.begin():
            for decision in session.scalars(
                select(CatalogMatchDecision).where(
                    CatalogMatchDecision.id.in_([d.id for d in decisions])
                )
            ):
                left = self._fingerprint(session, decision.left_source_series_id)
                right = self._fingerprint(session, decision.right_source_series_id)
                evidence = dict(decision.evidence_json or {})
                if left is not None and right is not None:
                    distance = hamming_distance(left.hash_hex, right.hash_hex)
                    evidence["cover_compared"] = True
                    evidence["cover_distance"] = distance
                    evidence["cover_match"] = distance <= 10
                    decision.confidence = max(decision.confidence, 0.96 if distance <= 6 else 0.88)
                else:
                    evidence["cover_compared"] = False
                    evidence.pop("cover_match", None)
                decision.evidence_json = evidence

    async def _ensure_fingerprint(self, source_series_id: int) -> None:
        with self.session_factory() as session:
            if self._fingerprint(session, source_series_id) is not None:
                return
            identity = session.get(CatalogSourceSeries, source_series_id)
            cover_url = identity.cover_url if identity else ""
            source = identity.source if identity else ""
        if not cover_url:
            return
        parts = urlsplit(cover_url)
        client = HttpSourceClient(
            f"{parts.scheme}://{parts.netloc}",
            source=source,
            provider_origin_url=PROVIDER_ORIGINS.get(source),
        )
        try:
            content = await client.get_bytes(cover_url)
            hash_hex, sha256, width, height = fingerprint_cover(content)
        except Exception:
            return
        finally:
            await client.aclose()
        with self.session_factory() as session, session.begin():
            if self._fingerprint(session, source_series_id) is None:
                session.add(
                    CatalogCoverFingerprint(
                        source_series_id=source_series_id,
                        algorithm=ALGORITHM,
                        hash_hex=hash_hex,
                        content_sha256=sha256,
                        width=width,
                        height=height,
                    )
                )

    @staticmethod
    def _fingerprint(session, source_series_id: int) -> CatalogCoverFingerprint | None:
        return session.scalar(
            select(CatalogCoverFingerprint).where(
                CatalogCoverFingerprint.source_series_id == source_series_id,
                CatalogCoverFingerprint.algorithm == ALGORITHM,
            )
        )
