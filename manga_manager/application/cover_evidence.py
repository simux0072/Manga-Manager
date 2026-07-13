from __future__ import annotations

import hashlib
import io
from pathlib import Path
from urllib.parse import urlsplit

import cv2
import numpy as np
from PIL import Image, ImageOps
from sqlalchemy import select

from app.adapters.http import HttpSourceClient
from app.domain import title_similarity
from manga_manager.domain.providers import PROVIDER_ORIGINS
from manga_manager.domain.providers import KNOWN_SOURCES
from manga_manager.infrastructure.db_models import (
    CatalogCoverAsset,
    CatalogCoverFingerprint,
    CatalogCoverSignature,
    CatalogMatchDecision,
    CatalogSourceSeries,
)
from manga_manager.settings import V2Settings
from manga_manager.worker.runtime import SessionFactory


LEGACY_ALGORITHM = "dhash-crop-v2"
ALGORITHM = "orb-multihash-v1"


def fingerprint_cover(content: bytes) -> tuple[str, str, int, int]:
    """Retain the v2 fingerprint interface for reports and existing migrations."""
    with Image.open(io.BytesIO(content)) as opened:
        image = ImageOps.exif_transpose(opened).convert("RGB")
        width, height = image.size
        image = _fractional_crop(image, 0.08, 0.06)
        hash_hex = _dhash(image)
    return hash_hex, hashlib.sha256(content).hexdigest(), width, height


def hamming_distance(left: str, right: str) -> int:
    return (int(left, 16) ^ int(right, 16)).bit_count()


def _fractional_crop(image: Image.Image, x_fraction: float, y_fraction: float) -> Image.Image:
    width, height = image.size
    x, y = int(width * x_fraction), int(height * y_fraction)
    return image.crop((x, y, max(x + 1, width - x), max(y + 1, height - y)))


def _dhash(image: Image.Image) -> str:
    gray = image.resize((9, 8), Image.Resampling.LANCZOS).convert("L")
    values = list(gray.get_flattened_data())
    bits = 0
    for y in range(8):
        for x in range(8):
            bits = (bits << 1) | int(values[y * 9 + x] > values[y * 9 + x + 1])
    return f"{bits:016x}"


def cover_signature(content: bytes) -> tuple[dict, bytes, bytes]:
    with Image.open(io.BytesIO(content)) as opened:
        image = ImageOps.exif_transpose(opened).convert("RGB")
        width, height = image.size
        hashes = [_dhash(_fractional_crop(image, margin, margin * 0.75)) for margin in (0, .05, .1, .15)]
    encoded = np.frombuffer(content, dtype=np.uint8)
    gray = cv2.imdecode(encoded, cv2.IMREAD_GRAYSCALE)
    if gray is None:
        raise ValueError("cover cannot be decoded by OpenCV")
    orb = cv2.ORB_create(nfeatures=1000, fastThreshold=7)
    keypoints, descriptors = orb.detectAndCompute(gray, None)
    points = np.asarray([point.pt for point in keypoints], dtype=np.float32)
    descriptors = (
        np.asarray(descriptors, dtype=np.uint8) if descriptors is not None else np.empty((0, 32), np.uint8)
    )
    features = {
        "width": width,
        "height": height,
        "hashes": hashes,
        "keypoint_count": int(len(points)),
        "descriptor_rows": int(len(descriptors)),
    }
    return features, points.tobytes(), descriptors.tobytes()


def compare_signatures(left: CatalogCoverSignature, right: CatalogCoverSignature) -> dict:
    left_hashes = list((left.feature_json or {}).get("hashes") or [])
    right_hashes = list((right.feature_json or {}).get("hashes") or [])
    hash_distance = min(
        (hamming_distance(a, b) for a in left_hashes for b in right_hashes), default=64
    )
    try:
        left_points = np.frombuffer(left.keypoints_blob, dtype=np.float32).reshape((-1, 2))
        right_points = np.frombuffer(right.keypoints_blob, dtype=np.float32).reshape((-1, 2))
        left_descriptors = np.frombuffer(left.descriptors_blob, dtype=np.uint8).reshape((-1, 32))
        right_descriptors = np.frombuffer(right.descriptors_blob, dtype=np.uint8).reshape((-1, 32))
    except ValueError:
        return {
            "cover_compared": False,
            "cover_signature_invalid": True,
            "cover_hash_distance": hash_distance,
            "cover_match": False,
        }
    if len(left_points) != len(left_descriptors) or len(right_points) != len(right_descriptors):
        return {
            "cover_compared": False,
            "cover_signature_invalid": True,
            "cover_hash_distance": hash_distance,
            "cover_match": False,
        }
    good = []
    if len(left_descriptors) >= 2 and len(right_descriptors) >= 2:
        try:
            pairs = cv2.BFMatcher(cv2.NORM_HAMMING).knnMatch(
                left_descriptors, right_descriptors, k=2
            )
            good = [
                first
                for pair in pairs
                if len(pair) == 2
                for first, second in [pair]
                if first.distance < 0.78 * second.distance
            ]
        except cv2.error:
            return {
                "cover_compared": False,
                "cover_signature_invalid": True,
                "cover_hash_distance": hash_distance,
                "cover_match": False,
            }
    inliers = 0
    if len(good) >= 4:
        source = np.float32([left_points[match.queryIdx] for match in good]).reshape(-1, 1, 2)
        target = np.float32([right_points[match.trainIdx] for match in good]).reshape(-1, 1, 2)
        try:
            _, mask = cv2.findHomography(source, target, cv2.RANSAC, 4.0)
            inliers = int(mask.sum()) if mask is not None else 0
        except cv2.error:
            inliers = 0
    ratio = inliers / max(len(good), 1)
    strong = inliers >= 12 and ratio >= 0.65 and hash_distance <= 14
    return {
        "cover_compared": True,
        "cover_hash_distance": hash_distance,
        "cover_ratio_matches": len(good),
        "cover_inliers": inliers,
        "cover_inlier_ratio": round(ratio, 6),
        "cover_match": strong,
    }


def signature_hash_distance(
    left: CatalogCoverSignature, right: CatalogCoverSignature
) -> int:
    left_hashes = list((left.feature_json or {}).get("hashes") or [])
    right_hashes = list((right.feature_json or {}).get("hashes") or [])
    return min(
        (hamming_distance(a, b) for a in left_hashes for b in right_hashes),
        default=64,
    )


class CoverEvidenceService:
    def __init__(self, session_factory: SessionFactory, storage_root: Path | None = None) -> None:
        self.session_factory = session_factory
        self.storage_root = storage_root or V2Settings().storage_root

    async def refresh_for_source_series(self, source_series_id: int) -> None:
        await self._ensure_signature(source_series_id)
        with self.session_factory() as session:
            source = session.get(CatalogSourceSeries, source_series_id)
            signature = session.get(CatalogCoverSignature, source_series_id)
            if source is None:
                return
            candidates = session.scalars(
                select(CatalogSourceSeries).where(
                    CatalogSourceSeries.id != source.id,
                    CatalogSourceSeries.source != source.source,
                    CatalogSourceSeries.series_id != source.series_id,
                )
            ).all()
            candidate_ids = [candidate.id for candidate in candidates]
        # Fetches happen through each provider scheduler as its series is refreshed. Do not fan out
        # arbitrary network traffic here; compare current series with already cached signatures.
        with self.session_factory() as session, session.begin():
            source = session.get(CatalogSourceSeries, source_series_id)
            signature = session.get(CatalogCoverSignature, source_series_id)
            if source is None:
                return
            for candidate in session.scalars(
                select(CatalogSourceSeries).where(CatalogSourceSeries.id.in_(candidate_ids))
            ):
                other = session.get(CatalogCoverSignature, candidate.id)
                title_score = title_similarity(source.title, candidate.title)
                quick_distance = (
                    signature_hash_distance(signature, other) if signature and other else 64
                )
                if title_score < 0.65 and quick_distance > 18:
                    continue
                visual = (
                    compare_signatures(signature, other)
                    if signature and other
                    else {"cover_compared": False}
                )
                if title_score < 0.65 and visual.get("cover_hash_distance", 64) > 14:
                    continue
                left_id, right_id = sorted((source.id, candidate.id))
                decision = session.scalar(
                    select(CatalogMatchDecision).where(
                        CatalogMatchDecision.left_source_series_id == left_id,
                        CatalogMatchDecision.right_source_series_id == right_id,
                    )
                )
                from manga_manager.application.matching_score import score_series_pair

                features = score_series_pair(session, source.series_id, candidate.series_id)
                confidence = float(features["score"])
                evidence = {
                    **features,
                    "policy": "manual_review_required",
                    "title_or_alias": [source.title, candidate.title],
                }
                if decision is None:
                    session.add(
                        CatalogMatchDecision(
                            left_source_series_id=left_id,
                            right_source_series_id=right_id,
                            confidence=confidence,
                            evidence_json=evidence,
                            scorer_version=str(features["scorer_version"]),
                            feature_vector_json=features,
                        )
                    )
                elif decision.decision == "pending":
                    decision.confidence = confidence
                    decision.evidence_json = evidence
                    decision.scorer_version = str(features["scorer_version"])
                    decision.feature_vector_json = features

    async def _ensure_signature(self, source_series_id: int) -> None:
        with self.session_factory() as session:
            identity = session.get(CatalogSourceSeries, source_series_id)
            existing = session.get(CatalogCoverSignature, source_series_id)
            existing_asset = session.get(CatalogCoverAsset, source_series_id)
            if identity is None or not identity.cover_url:
                return
            if identity.source not in KNOWN_SOURCES:
                return
            if (
                existing is not None
                and existing.algorithm_version == ALGORITHM
                and existing_asset is not None
                and existing_asset.source_url == identity.cover_url
            ):
                return
            cover_url, source = identity.cover_url, identity.source
        parts = urlsplit(cover_url)
        client = HttpSourceClient(
            f"{parts.scheme}://{parts.netloc}",
            source=source,
            provider_origin_url=PROVIDER_ORIGINS.get(source),
        )
        try:
            content = await client.get_bytes(cover_url)
            if not content or len(content) > 5 * 1024 * 1024:
                raise ValueError("cover is empty or exceeds 5 MiB")
            legacy_hash, checksum, width, height = fingerprint_cover(content)
            features, keypoints, descriptors = cover_signature(content)
            with Image.open(io.BytesIO(content)) as image:
                image_format = (image.format or "jpg").lower().replace("jpeg", "jpg")
            relative = Path("covers") / checksum[:2] / f"{checksum}.{image_format}"
            destination = self.storage_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.exists():
                temporary = destination.with_suffix(destination.suffix + ".tmp")
                temporary.write_bytes(content)
                temporary.replace(destination)
        except Exception:
            return
        finally:
            await client.aclose()
        with self.session_factory() as session, session.begin():
            asset = session.get(CatalogCoverAsset, source_series_id) or CatalogCoverAsset(
                source_series_id=source_series_id,
                content_checksum=checksum,
                relative_path=relative.as_posix(),
            )
            asset.content_checksum = checksum
            asset.relative_path = relative.as_posix()
            asset.content_type = f"image/{image_format}"
            asset.source_url = cover_url
            asset.width = width
            asset.height = height
            session.add(asset)
            signature = session.get(CatalogCoverSignature, source_series_id) or CatalogCoverSignature(
                source_series_id=source_series_id,
                algorithm_version=ALGORITHM,
            )
            signature.algorithm_version = ALGORITHM
            signature.feature_json = features
            signature.keypoints_blob = keypoints
            signature.descriptors_blob = descriptors
            session.add(signature)
            legacy = session.scalar(
                select(CatalogCoverFingerprint).where(
                    CatalogCoverFingerprint.source_series_id == source_series_id,
                    CatalogCoverFingerprint.algorithm == LEGACY_ALGORITHM,
                )
            )
            if legacy is None:
                session.add(
                    CatalogCoverFingerprint(
                        source_series_id=source_series_id,
                        algorithm=LEGACY_ALGORITHM,
                        hash_hex=legacy_hash,
                        content_sha256=checksum,
                        width=width,
                        height=height,
                    )
                )
