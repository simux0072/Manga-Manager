from __future__ import annotations

import io

import numpy as np

from PIL import Image, ImageDraw

from manga_manager.application.cover_evidence import (
    ALGORITHM,
    compare_signatures,
    cover_signature,
    fingerprint_cover,
    hamming_distance,
)
from manga_manager.infrastructure.db_models import CatalogCoverSignature


def cover(*, badge: bool = False, inverted: bool = False) -> bytes:
    image = Image.new("RGB", (240, 360), "white" if not inverted else "black")
    draw = ImageDraw.Draw(image)
    draw.rectangle((55, 35, 185, 325), fill="navy" if not inverted else "yellow")
    draw.ellipse((80, 90, 160, 170), fill="orange" if not inverted else "purple")
    if badge:
        draw.rectangle((0, 0, 44, 32), fill="red")
    output = io.BytesIO()
    image.save(output, format="JPEG", quality=90)
    return output.getvalue()


def test_fingerprint_ignores_small_edge_badges_but_rejects_different_art() -> None:
    original = fingerprint_cover(cover())[0]
    branded = fingerprint_cover(cover(badge=True))[0]
    different = fingerprint_cover(cover(inverted=True))[0]
    assert hamming_distance(original, branded) <= 10
    assert hamming_distance(original, different) > 10


def signature(content: bytes, identity: int) -> CatalogCoverSignature:
    features, points, descriptors = cover_signature(content)
    return CatalogCoverSignature(
        source_series_id=identity,
        algorithm_version=ALGORITHM,
        feature_json=features,
        keypoints_blob=points,
        descriptors_blob=descriptors,
    )


def detailed_cover(*, zoom: bool = False, translated_title: bool = False) -> bytes:
    rng = np.random.default_rng(42)
    pixels = rng.integers(0, 256, (540, 360, 3), dtype=np.uint8)
    image = Image.fromarray(pixels, "RGB")
    draw = ImageDraw.Draw(image)
    draw.ellipse((80, 100, 280, 360), outline="white", width=12)
    draw.line((30, 430, 330, 50), fill="black", width=10)
    if translated_title:
        draw.rectangle((10, 430, 350, 535), fill="navy")
        draw.text((30, 465), "ENGLISH TITLE", fill="white")
    if zoom:
        image = image.crop((25, 35, 335, 500)).resize((360, 540))
    output = io.BytesIO()
    image.save(output, format="JPEG", quality=92)
    return output.getvalue()


def test_orb_signature_matches_translated_overlay_and_zoom_but_rejects_other_art() -> None:
    original = signature(detailed_cover(), 1)
    translated = signature(detailed_cover(translated_title=True), 2)
    zoomed = signature(detailed_cover(zoom=True), 3)
    unrelated = signature(cover(inverted=True), 4)

    assert compare_signatures(original, translated)["cover_match"] is True
    assert compare_signatures(original, zoomed)["cover_match"] is True
    assert compare_signatures(original, unrelated)["cover_match"] is False


def test_corrupt_signature_is_treated_as_inconclusive() -> None:
    original = signature(detailed_cover(), 1)
    corrupt = signature(detailed_cover(), 2)
    corrupt.keypoints_blob = corrupt.keypoints_blob[:-8]

    evidence = compare_signatures(original, corrupt)

    assert evidence["cover_compared"] is False
    assert evidence["cover_signature_invalid"] is True
    assert evidence["cover_match"] is False
