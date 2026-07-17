from __future__ import annotations

import hashlib
import io

import numpy as np
import pytest
from PIL import Image, ImageDraw
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from manga_manager.application.cover_evidence import (
    ALGORITHM,
    CoverEvidenceService,
    compare_signatures,
    cover_signature,
    fingerprint_cover,
    hamming_distance,
    process_cover_content,
    signature_bands,
    thumbnail_relative_path,
)
from manga_manager.application.cover_backfill import CoverBackfillPlanner
from manga_manager.infrastructure.db_models import (
    CatalogCoverAsset,
    CatalogCoverSignature,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSourceSeries,
    JobBase,
    WorkJob,
)


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


def test_cover_processing_keeps_original_and_creates_bounded_webp(tmp_path) -> None:
    content = detailed_cover()

    result = process_cover_content(content, tmp_path)
    checksum, original = result[1], result[-1]
    thumbnail = tmp_path / thumbnail_relative_path(checksum)

    assert (tmp_path / original).read_bytes() == content
    assert thumbnail.is_file()
    with Image.open(thumbnail) as image:
        assert image.format == "WEBP"
        assert image.width <= 480
        assert image.height <= 720


def test_cover_backfill_does_not_requeue_an_exhausted_url_forever() -> None:
    engine = create_engine("sqlite://")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        series = CatalogSeries(title="Example", normalized_title="example")
        session.add(series)
        session.flush()
        exhausted = CatalogSourceSeries(
            series_id=series.id,
            source="asura",
            source_id="exhausted",
            title="Example",
            normalized_title="example",
            url="https://asurascans.com/comics/example",
            cover_url="https://cdn.asurascans.com/missing.webp",
        )
        pending = CatalogSourceSeries(
            series_id=series.id,
            source="mangafire",
            source_id="pending",
            title="Example",
            normalized_title="example",
            url="https://mangafire.to/manga/example",
            cover_url="https://static.mfcdn.nl/cover.jpg",
        )
        session.add_all([exhausted, pending])
        session.flush()
        revision = hashlib.sha1(exhausted.cover_url.encode("utf-8")).hexdigest()[:12]
        session.add(
            WorkJob(
                kind="cover_backfill",
                dedupe_key=f"cover:{exhausted.id}:{revision}",
                payload={"version": 1, "source_series_id": exhausted.id},
                status="failed",
            )
        )

    with sessions() as session, session.begin():
        created = CoverBackfillPlanner().enqueue_pending(session, limit=10)
        queued = session.query(WorkJob).filter_by(status="queued").all()

    assert created == 1
    assert [row.payload["source_series_id"] for row in queued] == [pending.id]


@pytest.mark.asyncio
async def test_cover_refresh_runs_cpu_scoring_outside_the_async_loop(tmp_path) -> None:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, autoflush=False, expire_on_commit=False)
    with sessions() as session, session.begin():
        for source, source_id, content in (
            ("asura", "example-a", detailed_cover()),
            ("mangafire", "example-b", detailed_cover(translated_title=True)),
        ):
            series = CatalogSeries(title="Example", normalized_title="example")
            session.add(series)
            session.flush()
            identity = CatalogSourceSeries(
                series_id=series.id,
                source=source,
                source_id=source_id,
                title="Example",
                normalized_title="example",
                url=f"https://example.test/{source_id}",
                cover_url=f"https://covers.test/{source_id}.jpg",
            )
            session.add(identity)
            session.flush()
            result = process_cover_content(content, tmp_path)
            cover_signature_row = signature(content, identity.id)
            (
                cover_signature_row.hash_band_0,
                cover_signature_row.hash_band_1,
                cover_signature_row.hash_band_2,
                cover_signature_row.hash_band_3,
            ) = signature_bands(cover_signature_row.feature_json)
            session.add_all(
                [
                    cover_signature_row,
                    CatalogCoverAsset(
                        source_series_id=identity.id,
                        content_checksum=result[1],
                        relative_path=result[-1].as_posix(),
                        source_url=identity.cover_url,
                        width=result[2],
                        height=result[3],
                    ),
                ]
            )
        first_identity_id = session.scalar(
            select(CatalogSourceSeries.id).order_by(CatalogSourceSeries.id)
        )

    await CoverEvidenceService(sessions, tmp_path).refresh_for_source_series(first_identity_id)

    with sessions() as session:
        decision = session.scalar(select(CatalogMatchDecision))
        assert decision is not None
        assert decision.evidence_json["cover_compared"] is True
