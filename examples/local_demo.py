"""Run the pipeline locally with in-memory adapters and a flaky embedder.

    python examples/local_demo.py

Shows deduplication of identical content, transient failures recovering on redelivery,
and a persistently failing document ending up in the dead-letter queue.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from rag_ingest.adapters.memory import (
    HashEmbedder,
    InMemoryDocumentStore,
    InMemoryRegistry,
    InMemoryVectorSink,
    LocalQueue,
)
from rag_ingest.domain import DocumentRef
from rag_ingest.rate_limiter import TokenBucket
from rag_ingest.service import IngestionService

DOCUMENTS = {
    "handbook.txt": "Expenses are reimbursed monthly.\n\nTravel must be pre-approved.",
    "handbook-copy.txt": "Expenses are reimbursed monthly.\n\nTravel must be pre-approved.",
    "release-notes.txt": "Version 2 adds SSO.\n\nVersion 2 removes the legacy API.",
    "poison.txt": "This document always fails to embed.",
}


class FlakyEmbedder(HashEmbedder):
    """Fails the first call for each text, and every call for text containing 'always fails'."""

    def __init__(self) -> None:
        super().__init__()
        self._seen: set[str] = set()

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        key = "|".join(texts)
        if "always fails" in key or key not in self._seen:
            self._seen.add(key)
            raise ConnectionError("embedding API timed out")
        return super().embed(texts)


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    documents, sink = InMemoryDocumentStore(), InMemoryVectorSink()
    service = IngestionService(
        documents,
        InMemoryRegistry(),
        FlakyEmbedder(),
        sink,
        TokenBucket(requests_per_minute=600, tokens_per_minute=100_000),
    )
    queue = LocalQueue(max_receive_count=3)
    for key, text in DOCUMENTS.items():
        ref = DocumentRef("local", key)
        documents.put(ref, text)
        queue.send(ref)

    while (message := queue.receive()) is not None:
        attempt = f"{message.ref.key} (receive {message.receive_count})"
        try:
            print(f"{attempt}: {service.process(message.ref).value}")
        except Exception as error:
            print(f"{attempt}: failed - {error}")
            queue.nack(message)

    print(f"\nchunks stored: {len(sink.chunks)}")
    print(f"dead letters: {[m.ref.key for m in queue.dead_letters]}")


if __name__ == "__main__":
    main()
