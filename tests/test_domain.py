from app.domain import normalize_chapter_number, normalize_title, should_replace, title_similarity


def test_normalize_title_removes_noise():
    assert normalize_title("The Villainess (Official) Manhwa!") == "villainess"


def test_chapter_number_decimal_is_preserved():
    assert normalize_chapter_number("Chapter 127.5: Side Story") == "127.5"


def test_priority_replacement_order():
    assert should_replace("kingofshojo", "mangafire")
    assert should_replace("mangafire", "asura")
    assert not should_replace("asura", "kingofshojo")


def test_title_similarity_token_overlap():
    assert (
        title_similarity("Father, I Don't Want This Marriage", "Father I Dont Want to Get Married")
        > 0.4
    )
