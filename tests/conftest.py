from __future__ import annotations

from dataclasses import dataclass

import pytest

from rag_ingest.adapters.memory import (
    HashEmbedder,
    InMemoryDocumentStore,
    InMemoryRegistry,
    InMemoryVectorSink,
)
from rag_ingest.domain import DocumentRef
from rag_ingest.rate_limiter import TokenBucket
from rag_ingest.service import IngestionService, IngestionSettings


@dataclass
class Pipeline:
    service: IngestionService
    documents: InMemoryDocumentStore
    registry: InMemoryRegistry
    embedder: HashEmbedder
    sink: InMemoryVectorSink

    def add(self, key: str, text: str) -> DocumentRef:
        ref = DocumentRef("bucket", key)
        self.documents.put(ref, text)
        return ref


def make_pipeline(embedder: HashEmbedder | None = None, **settings: object) -> Pipeline:
    documents, registry, sink = InMemoryDocumentStore(), InMemoryRegistry(), InMemoryVectorSink()
    embedder = embedder or HashEmbedder()
    service = IngestionService(
        documents,
        registry,
        embedder,
        sink,
        TokenBucket(requests_per_minute=10_000, tokens_per_minute=1_000_000),
        IngestionSettings(**settings),  # type: ignore[arg-type]
    )
    return Pipeline(service, documents, registry, embedder, sink)


@pytest.fixture
def pipeline() -> Pipeline:
    return make_pipeline()
