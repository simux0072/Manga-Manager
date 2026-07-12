from __future__ import annotations

import io

from PIL import Image, ImageDraw

from manga_manager.application.cover_evidence import fingerprint_cover, hamming_distance


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
