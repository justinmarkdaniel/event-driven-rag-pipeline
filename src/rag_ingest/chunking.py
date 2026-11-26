"""Paragraph-aware chunking with a token budget."""

from __future__ import annotations

import math
import re

from rag_ingest.domain import Chunk

_PARAGRAPH_BREAK = re.compile(r"\n\s*\n")
_CHARS_PER_TOKEN = 4


def estimate_tokens(text: str) -> int:
    """Rough token count (~4 characters per token for English).

    Only used for budgeting, so overestimating slightly is fine. Swap in the embedding model's
    tokenizer if chunks need to sit right at the model's context limit.
    """
    return max(1, math.ceil(len(text) / _CHARS_PER_TOKEN))


def chunk_text(text: str, max_tokens: int = 512) -> list[Chunk]:
    """Pack whole paragraphs into chunks of at most ``max_tokens``.

    A paragraph that is too long on its own is split on word boundaries.
    """
    if max_tokens < 1:
        raise ValueError("max_tokens must be >= 1")

    pieces: list[str] = []
    for paragraph in _PARAGRAPH_BREAK.split(text):
        paragraph = " ".join(paragraph.split())
        if paragraph:
            pieces.extend(_split_long(paragraph, max_tokens))

    chunks: list[str] = []
    current: list[str] = []
    for piece in pieces:
        candidate = "\n\n".join([*current, piece])
        if current and estimate_tokens(candidate) > max_tokens:
            chunks.append("\n\n".join(current))
            current = [piece]
        else:
            current.append(piece)
    if current:
        chunks.append("\n\n".join(current))

    return [Chunk(i, body, estimate_tokens(body)) for i, body in enumerate(chunks)]


def _split_long(paragraph: str, max_tokens: int) -> list[str]:
    if estimate_tokens(paragraph) <= max_tokens:
        return [paragraph]
    parts: list[str] = []
    current: list[str] = []
    for word in paragraph.split(" "):
        if current and estimate_tokens(" ".join([*current, word])) > max_tokens:
            parts.append(" ".join(current))
            current = []
        current.append(word)
    if current:
        parts.append(" ".join(current))
    return parts
