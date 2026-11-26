"""Interfaces the ingestion service depends on. Adapters live in ``rag_ingest.adapters``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from rag_ingest.domain import ClaimResult, DocumentRef, EmbeddedChunk


class DocumentStore(Protocol):
    def load(self, ref: DocumentRef) -> str: ...


class Registry(Protocol):
    """Records which content has been indexed, so identical documents are embedded once.

    ``claim`` must be atomic: when two workers race on the same hash, exactly one gets
    ``CLAIMED``. A claim is a lease, so a worker that dies mid-document does not block that
    content forever.
    """

    def claim(self, content_hash: str, source_key: str, lease_seconds: int) -> ClaimResult: ...

    def complete(self, content_hash: str) -> None: ...

    def release(self, content_hash: str) -> None: ...


class Embedder(Protocol):
    def embed(self, texts: Sequence[str]) -> list[list[float]]: ...


class VectorSink(Protocol):
    def write(self, chunks: Sequence[EmbeddedChunk]) -> None: ...
