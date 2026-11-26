import pytest

from rag_ingest.chunking import chunk_text, estimate_tokens


def test_short_paragraphs_are_packed_together() -> None:
    chunks = chunk_text("one\n\ntwo\n\nthree", max_tokens=100)
    assert [c.text for c in chunks] == ["one\n\ntwo\n\nthree"]


def test_paragraphs_split_across_chunks_when_over_budget() -> None:
    paragraph = "word " * 40  # ~50 tokens
    chunks = chunk_text(f"{paragraph}\n\n{paragraph}\n\n{paragraph}", max_tokens=60)
    assert len(chunks) == 3
    assert all(c.token_estimate <= 60 for c in chunks)


def test_long_paragraph_split_on_word_boundaries() -> None:
    text = " ".join(f"w{i}" for i in range(500))
    chunks = chunk_text(text, max_tokens=50)
    assert len(chunks) > 1
    assert all(c.token_estimate <= 50 for c in chunks)
    assert " ".join(c.text for c in chunks).split() == text.split()


def test_indices_are_sequential() -> None:
    chunks = chunk_text("a " * 400, max_tokens=20)
    assert [c.index for c in chunks] == list(range(len(chunks)))


@pytest.mark.parametrize("text", ["", "   ", "\n\n\n"])
def test_blank_text_has_no_chunks(text: str) -> None:
    assert chunk_text(text) == []


def test_whitespace_inside_paragraph_is_normalised() -> None:
    assert chunk_text("a\n b\t c")[0].text == "a b c"


def test_estimate_is_at_least_one() -> None:
    assert estimate_tokens("") == 1
    assert estimate_tokens("x" * 40) == 10
