from __future__ import annotations

import hashlib
import io
import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

from PIL import Image, ImageOps
from sqlalchemy import or_, select, tuple_

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
from manga_manager.infrastructure.bounded_executor import AsyncBoundedExecutor
from manga_manager.settings import V2Settings
from manga_manager.worker.runtime import SessionFactory


LEGACY_ALGORITHM = "dhash-crop-v2"
ALGORITHM = "orb-normalized-multihash-v2"
THUMBNAIL_MAX_SIZE = (480, 720)
FEATURE_LONG_EDGE = 720
_COVER_EXECUTOR = AsyncBoundedExecutor(workers=1, thread_name_prefix="manga-cover")


@lru_cache(maxsize=1)
def _image_modules():
    """Load the heavy native matcher only in processes that actually compare covers.

    The web service imports thumbnail helpers from this module, but Suggested Matches only reads
    cover evidence that workers have already stored.  Importing OpenCV and NumPy eagerly made the
    256 MiB web container retain their native heaps and worker threads while merely serving cards.
    Keep the dependency lazy and use one OpenCV lane; provider workers already bound cover CPU
    work with ``_COVER_EXECUTOR``.
    """
    # The cover executor is deliberately single-lane. Native math libraries should not create a
    # second, hidden pool sized for the development host when this runs on a four-core Pi.
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")

    import cv2
    import numpy as np

    cv2.setNumThreads(1)
    return cv2, np


async def run_cover_cpu(function, /, *args, **kwargs):
    """Run image work in one bounded lane without blocking provider downloads."""
    return await _COVER_EXECUTOR.run(function, *args, **kwargs)


def signature_bands(features: dict) -> tuple[str, str, str, str]:
    hashes = [str(value) for value in features.get("hashes") or []]
    primary = hashes[0][:16] if hashes else ""
    return tuple(primary[index * 4 : (index + 1) * 4] for index in range(4))


def process_cover_content(content: bytes, storage_root: Path) -> tuple:
    legacy_hash, checksum, width, height = fingerprint_cover(content)
    features, keypoints, descriptors = cover_signature(content)
    with Image.open(io.BytesIO(content)) as image:
        image_format = (image.format or "jpg").lower().replace("jpeg", "jpg")
    relative = Path("covers") / checksum[:2] / f"{checksum}.{image_format}"
    destination = storage_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_bytes(content)
        temporary.replace(destination)
    ensure_cover_thumbnail(destination, checksum, storage_root)
    return (
        legacy_hash,
        checksum,
        width,
        height,
        features,
        keypoints,
        descriptors,
        image_format,
        relative,
    )


def thumbnail_relative_path(checksum: str) -> Path:
    return Path("covers") / "thumbnails" / checksum[:2] / f"{checksum}.webp"


def ensure_cover_thumbnail(source: Path, checksum: str, storage_root: Path) -> Path:
    """Create a small, immutable grid derivative without replacing matching input."""
    relative = thumbnail_relative_path(checksum)
    destination = storage_root / relative
    if destination.exists():
        return relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".webp.tmp")
    with Image.open(source) as opened:
        image = ImageOps.exif_transpose(opened).convert("RGB")
        image.thumbnail(THUMBNAIL_MAX_SIZE, Image.Resampling.LANCZOS)
        image.save(temporary, format="WEBP", quality=82, method=4)
    temporary.replace(destination)
    return relative


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
    cv2, np = _image_modules()
    with Image.open(io.BytesIO(content)) as opened:
        image = ImageOps.exif_transpose(opened).convert("RGB")
        width, height = image.size
        hashes = [
            _dhash(_fractional_crop(image, margin, margin * 0.75))
            for margin in (0, 0.05, 0.1, 0.15)
        ]
        # Provider covers range from small thumbnails to multi-megapixel originals. ORB's
        # scale pyramid cannot bridge a tenfold size difference reliably, so calculate every
        # descriptor in the same coordinate space while retaining the original for storage.
        scale = FEATURE_LONG_EDGE / max(width, height)
        normalized_size = (
            max(1, round(width * scale)),
            max(1, round(height * scale)),
        )
        normalized = image.resize(normalized_size, Image.Resampling.LANCZOS)
        gray = cv2.cvtColor(np.asarray(normalized), cv2.COLOR_RGB2GRAY)
    orb = cv2.ORB_create(nfeatures=1000, fastThreshold=7)
    keypoints, descriptors = orb.detectAndCompute(gray, None)
    points = np.asarray([point.pt for point in keypoints], dtype=np.float32)
    descriptors = (
        np.asarray(descriptors, dtype=np.uint8)
        if descriptors is not None
        else np.empty((0, 32), np.uint8)
    )
    features = {
        "width": width,
        "height": height,
        "normalized_width": normalized_size[0],
        "normalized_height": normalized_size[1],
        "hashes": hashes,
        "keypoint_count": int(len(points)),
        "descriptor_rows": int(len(descriptors)),
    }
    return features, points.tobytes(), descriptors.tobytes()


def compare_signatures(left: CatalogCoverSignature, right: CatalogCoverSignature) -> dict:
    cv2, np = _image_modules()
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
            "cover_evidence_state": "invalid",
        }
    if len(left_points) != len(left_descriptors) or len(right_points) != len(right_descriptors):
        return {
            "cover_compared": False,
            "cover_signature_invalid": True,
            "cover_hash_distance": hash_distance,
            "cover_match": False,
            "cover_evidence_state": "invalid",
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
                "cover_evidence_state": "invalid",
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
    strong_geometry = inliers >= 12 and ratio >= 0.65 and hash_distance <= 18
    strong_hash_with_geometry = inliers >= 4 and ratio >= 0.6 and hash_distance <= 4
    cover_match = strong_geometry or strong_hash_with_geometry
    likely = not cover_match and (
        (inliers >= 8 and ratio >= 0.5 and hash_distance <= 22)
        or (inliers >= 4 and ratio >= 0.5 and hash_distance <= 10)
    )
    different = hash_distance >= 28 and inliers < 4
    if cover_match:
        state = "match"
    elif likely:
        state = "likely"
    elif different:
        state = "different"
    else:
        state = "inconclusive"
    return {
        "cover_compared": True,
        "cover_hash_distance": hash_distance,
        "cover_ratio_matches": len(good),
        "cover_inliers": inliers,
        "cover_inlier_ratio": round(ratio, 6),
        "cover_match": cover_match,
        "cover_evidence_state": state,
    }


def signature_hash_distance(left: CatalogCoverSignature, right: CatalogCoverSignature) -> int:
    left_hashes = list((left.feature_json or {}).get("hashes") or [])
    right_hashes = list((right.feature_json or {}).get("hashes") or [])
    return min(
        (hamming_distance(a, b) for a in left_hashes for b in right_hashes),
        default=64,
    )


def _visual_candidate_shortlist(
    source_title: str,
    signature: CatalogCoverSignature,
    rows: list[tuple[CatalogSourceSeries, CatalogCoverSignature]],
) -> list[tuple[int, int, str]]:
    selected: list[tuple[int, int, str]] = []
    for candidate, other in rows:
        title_score = title_similarity(source_title, candidate.title)
        quick_distance = signature_hash_distance(signature, other)
        if title_score < 0.65 and quick_distance > 18:
            continue
        visual = compare_signatures(signature, other)
        if title_score < 0.65 and visual.get("cover_hash_distance", 64) > 14:
            continue
        selected.append((candidate.id, candidate.series_id, candidate.title))
    return selected


def _score_cover_candidates(
    session_factory: SessionFactory,
    source_series_id: int,
    candidate_series_ids: list[int],
) -> dict[int, dict[str, object]]:
    from manga_manager.application.matching_score import score_candidate_set

    with session_factory() as session:
        return score_candidate_set(session, [source_series_id], candidate_series_ids)


def _rescore_pending_decisions(
    session_factory: SessionFactory,
    canonical_series_id: int,
) -> int:
    from manga_manager.application.matching_score import (
        rescore_pending_decisions_for_series,
    )

    with session_factory() as session, session.begin():
        return rescore_pending_decisions_for_series(session, canonical_series_id)


class CoverEvidenceService:
    def __init__(self, session_factory: SessionFactory, storage_root: Path | None = None) -> None:
        self.session_factory = session_factory
        self.storage_root = storage_root or V2Settings().storage_root

    async def refresh_for_source_series(self, source_series_id: int) -> None:
        await self._ensure_signature(source_series_id)
        # Fetches happen through each provider scheduler as its series is refreshed. Do not fan out
        # arbitrary network traffic here; compare current series with already cached signatures.
        with self.session_factory() as session:
            source = session.get(CatalogSourceSeries, source_series_id)
            signature = session.get(CatalogCoverSignature, source_series_id)
            if source is None:
                return
            if signature is None:
                return
            bands = signature_bands(signature.feature_json or {})
            title_prefix = source.normalized_title[:12]
            shortlist_ids = session.scalars(
                select(CatalogSourceSeries.id)
                .outerjoin(
                    CatalogCoverSignature,
                    CatalogCoverSignature.source_series_id == CatalogSourceSeries.id,
                )
                .where(
                    CatalogSourceSeries.id != source.id,
                    CatalogSourceSeries.source != source.source,
                    CatalogSourceSeries.series_id != source.series_id,
                    or_(
                        CatalogSourceSeries.normalized_title.like(f"{title_prefix}%"),
                        CatalogCoverSignature.hash_band_0 == bands[0],
                        CatalogCoverSignature.hash_band_1 == bands[1],
                        CatalogCoverSignature.hash_band_2 == bands[2],
                        CatalogCoverSignature.hash_band_3 == bands[3],
                    ),
                )
                .order_by(CatalogSourceSeries.id)
                .limit(200)
            ).all()
            candidate_rows = list(
                session.execute(
                    select(CatalogSourceSeries, CatalogCoverSignature)
                    .join(
                        CatalogCoverSignature,
                        CatalogCoverSignature.source_series_id == CatalogSourceSeries.id,
                    )
                    .where(CatalogSourceSeries.id.in_(shortlist_ids or [-1]))
                )
            )
            source_id = source.id
            canonical_id = source.series_id
            source_title = source.title
        # Catalog ingestion necessarily runs before a newly fetched signature exists. Revisit all
        # existing pending proposals for this canonical series once the signature is ready. This
        # updates evidence only; decisions remain pending until a user confirms them.
        await run_cover_cpu(
            _rescore_pending_decisions,
            self.session_factory,
            canonical_id,
        )
        candidates = await run_cover_cpu(
            _visual_candidate_shortlist,
            source_title,
            signature,
            candidate_rows,
        )
        if not candidates:
            return
        candidate_series_ids = sorted({candidate[1] for candidate in candidates})
        features_by_series = await run_cover_cpu(
            _score_cover_candidates,
            self.session_factory,
            canonical_id,
            candidate_series_ids,
        )
        pairs = {tuple(sorted((source_id, candidate[0]))) for candidate in candidates}
        with self.session_factory() as session, session.begin():
            existing = {
                (decision.left_source_series_id, decision.right_source_series_id): decision
                for decision in session.scalars(
                    select(CatalogMatchDecision).where(
                        tuple_(
                            CatalogMatchDecision.left_source_series_id,
                            CatalogMatchDecision.right_source_series_id,
                        ).in_(pairs or {(-1, -1)})
                    )
                )
            }
            for candidate_id, candidate_series_id, candidate_title in candidates:
                left_id, right_id = sorted((source_id, candidate_id))
                decision = existing.get((left_id, right_id))
                features = features_by_series[candidate_series_id]
                confidence = float(features["score"])
                evidence = {
                    **features,
                    "policy": "manual_review_required",
                    "title_or_alias": [source_title, candidate_title],
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
        cached_original: Path | None = None
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
                original = self.storage_root / existing_asset.relative_path
                if original.is_file():
                    await run_cover_cpu(
                        ensure_cover_thumbnail,
                        original,
                        existing_asset.content_checksum,
                        self.storage_root,
                    )
                bands = signature_bands(existing.feature_json or {})
                if any(bands) and not existing.hash_band_0:
                    (
                        existing.hash_band_0,
                        existing.hash_band_1,
                        existing.hash_band_2,
                        existing.hash_band_3,
                    ) = bands
                    session.commit()
                return
            cover_url, source = identity.cover_url, identity.source
            if existing_asset is not None and existing_asset.source_url == cover_url:
                candidate = self.storage_root / existing_asset.relative_path
                if candidate.is_file():
                    cached_original = candidate
        try:
            if cached_original is not None:
                content = await run_cover_cpu(cached_original.read_bytes)
            else:
                parts = urlsplit(cover_url)
                client = HttpSourceClient(
                    f"{parts.scheme}://{parts.netloc}",
                    source=source,
                    provider_origin_url=PROVIDER_ORIGINS.get(source),
                )
                try:
                    content = await client.get_bytes(cover_url)
                finally:
                    await client.aclose()
            if not content or len(content) > 5 * 1024 * 1024:
                raise ValueError("cover is empty or exceeds 5 MiB")
            (
                legacy_hash,
                checksum,
                width,
                height,
                features,
                keypoints,
                descriptors,
                image_format,
                relative,
            ) = await run_cover_cpu(process_cover_content, content, self.storage_root)
        except Exception:
            return
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
            signature = session.get(
                CatalogCoverSignature, source_series_id
            ) or CatalogCoverSignature(
                source_series_id=source_series_id,
                algorithm_version=ALGORITHM,
            )
            signature.algorithm_version = ALGORITHM
            signature.feature_json = features
            (
                signature.hash_band_0,
                signature.hash_band_1,
                signature.hash_band_2,
                signature.hash_band_3,
            ) = signature_bands(features)
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
