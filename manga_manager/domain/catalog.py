from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation


def normalize_title(title: str) -> str:
    value = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
    value = value.lower()
    value = re.sub(r"\([^)]*\)|\[[^]]*]", " ", value)
    value = re.sub(r"\b(the|a|an|manga|manhwa|manhua|webtoon|official)\b", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def canonical_chapter_number(value: str) -> str:
    match = re.search(r"(\d+(?:\.\d+)?)", value)
    if not match:
        return " ".join(value.strip().lower().split())
    try:
        number = Decimal(match.group(1)).normalize()
    except InvalidOperation:
        return match.group(1)
    return format(number, "f")


def chapter_sort_number(value: str) -> Decimal | None:
    canonical = canonical_chapter_number(value)
    try:
        return Decimal(canonical)
    except InvalidOperation:
        return None
