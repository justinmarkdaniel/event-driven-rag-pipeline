from collections.abc import Sequence

import pytest
from conftest import Pipeline, make_pipeline

from rag_ingest.adapters.memory import HashEmbedder
from rag_ingest.domain import DocumentBusyError, Outcome, RateLimitedError
from rag_ingest.rate_limiter import TokenBucket
from rag_ingest.service import IngestionService, content_hash


class FailingEmbedder(HashEmbedder):
    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        raise ConnectionError("embedding API unavailable")


class ShortEmbedder(HashEmbedder):
    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return super().embed(texts)[:-1]


def test_indexes_every_chunk(pipeline: Pipeline) -> None:
    ref = pipeline.add("a.txt", "first paragraph\n\nsecond paragraph")

    assert pipeline.service.process(ref) is Outcome.INDEXED
    assert [c.text for c in pipeline.sink.chunks] == ["first paragraph\n\nsecond paragraph"]
    assert pipeline.sink.chunks[0].source_key == "a.txt"
    assert pipeline.registry.status(content_hash("first paragraph\n\nsecond paragraph")) == (
        "COMPLETE"
    )


def test_identical_content_under_another_key_is_not_embedded_twice(pipeline: Pipeline) -> None:
    pipeline.service.process(pipeline.add("a.txt", "same text"))
    calls = pipeline.embedder.calls

    outcome = pipeline.service.process(pipeline.add("copy-of-a.txt", "same text"))

    assert outcome is Outcome.DUPLICATE
    assert pipeline.embedder.calls == calls
    assert len(pipeline.sink.chunks) == 1


def test_redelivered_message_is_a_duplicate(pipeline: Pipeline) -> None:
    ref = pipeline.add("a.txt", "text")
    pipeline.service.process(ref)
    assert pipeline.service.process(ref) is Outcome.DUPLICATE


def test_concurrent_identical_document_is_retried_not_double_indexed(pipeline: Pipeline) -> None:
    pipeline.registry.claim(content_hash("same"), "other-worker.txt", lease_seconds=60)

    with pytest.raises(DocumentBusyError):
        pipeline.service.process(pipeline.add("b.txt", "same"))
    assert pipeline.sink.chunks == []


def test_embedding_failure_releases_claim_so_retry_can_succeed() -> None:
    failing = make_pipeline(embedder=FailingEmbedder())
    ref = failing.add("a.txt", "text")
    with pytest.raises(ConnectionError):
        failing.service.process(ref)

    assert failing.registry.status(content_hash("text")) is None
    assert failing.sink.chunks == []


def test_mismatched_embedding_count_fails_loudly() -> None:
    broken = make_pipeline(embedder=ShortEmbedder())
    with pytest.raises(ValueError, match="vectors"):
        broken.service.process(broken.add("a.txt", "one\n\ntwo"))


def test_empty_document_is_skipped_without_claiming(pipeline: Pipeline) -> None:
    assert pipeline.service.process(pipeline.add("empty.txt", "  \n\n ")) is Outcome.EMPTY
    assert pipeline.embedder.calls == 0


def test_chunks_are_embedded_in_batches() -> None:
    batched = make_pipeline(max_chunk_tokens=5, max_batch_size=4)
    text = "\n\n".join(f"paragraph number {i}" for i in range(10))

    batched.service.process(batched.add("a.txt", text))

    assert batched.embedder.calls == 3  # 10 chunks in batches of 4
    assert [c.chunk_index for c in batched.sink.chunks] == list(range(10))


def test_rate_limit_exhaustion_is_retryable_and_releases_claim(pipeline: Pipeline) -> None:
    limiter = TokenBucket(requests_per_minute=1, tokens_per_minute=1000)
    service = IngestionService(
        pipeline.documents, pipeline.registry, pipeline.embedder, pipeline.sink, limiter
    )
    service.process(pipeline.add("a.txt", "first"))

    with pytest.raises(RateLimitedError):
        service.process(pipeline.add("b.txt", "second"))
    assert pipeline.registry.status(content_hash("second")) is None


def test_chunk_size_larger_than_token_budget_is_a_configuration_error(pipeline) -> None:
    with pytest.raises(ValueError):
        IngestionService(
            pipeline.documents,
            pipeline.registry,
            pipeline.embedder,
            pipeline.sink,
            TokenBucket(requests_per_minute=10, tokens_per_minute=100),
        )
