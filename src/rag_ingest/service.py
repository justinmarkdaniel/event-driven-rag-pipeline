"""The ingestion use case: one document in, embedded chunks out, at most once per content."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from rag_ingest.chunking import chunk_text
from rag_ingest.domain import (
    Chunk,
    ClaimResult,
    DocumentBusyError,
    DocumentRef,
    EmbeddedChunk,
    Outcome,
)
from rag_ingest.ports import DocumentStore, Embedder, Registry, VectorSink
from rag_ingest.rate_limiter import TokenBucket

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionSettings:
    max_chunk_tokens: int = 512
    max_batch_size: int = 16
    claim_lease_seconds: int = 900
    max_rate_limit_wait_seconds: float = 20.0


def content_hash(text: str) -> str:
    """Exact-duplicate key. Renamed or re-uploaded copies of the same text share it."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class IngestionService:
    """Loads, deduplicates, chunks, embeds and stores a single document.

    Failures are raised, not swallowed: the caller (the queue consumer) decides whether to
    redeliver. Anything that may succeed on a later attempt is a ``RetryableError`` or an
    exception from the embedding client.
    """

    def __init__(
        self,
        documents: DocumentStore,
        registry: Registry,
        embedder: Embedder,
        sink: VectorSink,
        limiter: TokenBucket,
        settings: IngestionSettings | None = None,
    ) -> None:
        self._documents = documents
        self._registry = registry
        self._embedder = embedder
        self._sink = sink
        self._limiter = limiter
        self._settings = settings or IngestionSettings()
        if self._settings.max_chunk_tokens > limiter.tokens_per_minute:
            raise ValueError("max_chunk_tokens exceeds the per-minute token limit")

    def process(self, ref: DocumentRef) -> Outcome:
        text = self._documents.load(ref)
        chunks = chunk_text(text, self._settings.max_chunk_tokens)
        if not chunks:
            logger.info("skipping empty document", extra={"key": ref.key})
            return Outcome.EMPTY

        digest = content_hash(text)
        claim = self._registry.claim(digest, ref.key, self._settings.claim_lease_seconds)
        if claim is ClaimResult.ALREADY_INDEXED:
            logger.info("skipping duplicate", extra={"key": ref.key, "content_hash": digest})
            return Outcome.DUPLICATE
        if claim is ClaimResult.IN_PROGRESS:
            raise DocumentBusyError(f"{digest} is being indexed by another worker")

        try:
            embedded = self._embed(digest, ref.key, chunks)
            self._sink.write(embedded)
            self._registry.complete(digest)
        except Exception:
            self._registry.release(digest)
            raise

        logger.info("indexed", extra={"key": ref.key, "chunks": len(embedded)})
        return Outcome.INDEXED

    def _embed(self, digest: str, key: str, chunks: Sequence[Chunk]) -> list[EmbeddedChunk]:
        embedded: list[EmbeddedChunk] = []
        for batch in self._batches(chunks):
            tokens = sum(chunk.token_estimate for chunk in batch)
            self._limiter.acquire(tokens, self._settings.max_rate_limit_wait_seconds)
            vectors = self._embedder.embed([chunk.text for chunk in batch])
            if len(vectors) != len(batch):
                raise ValueError(f"embedder returned {len(vectors)} vectors for {len(batch)} texts")
            embedded.extend(
                EmbeddedChunk(digest, key, chunk.index, chunk.text, vector)
                for chunk, vector in zip(batch, vectors, strict=True)
            )
        return embedded

    def _batches(self, chunks: Sequence[Chunk]) -> Iterator[list[Chunk]]:
        """Group chunks so each batch fits both the batch size and the token bucket."""
        batch: list[Chunk] = []
        tokens = 0
        for chunk in chunks:
            too_many = len(batch) == self._settings.max_batch_size
            too_big = tokens + chunk.token_estimate > self._limiter.tokens_per_minute
            if batch and (too_many or too_big):
                yield batch
                batch, tokens = [], 0
            batch.append(chunk)
            tokens += chunk.token_estimate
        if batch:
            yield batch
