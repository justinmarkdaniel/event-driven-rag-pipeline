"""In-process adapters for tests and the local demo."""

from __future__ import annotations

import hashlib
import threading
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from rag_ingest.domain import ClaimResult, DocumentRef, EmbeddedChunk


class InMemoryDocumentStore:
    def __init__(self) -> None:
        self._objects: dict[tuple[str, str], str] = {}

    def put(self, ref: DocumentRef, text: str) -> None:
        self._objects[(ref.bucket, ref.key)] = text

    def load(self, ref: DocumentRef) -> str:
        return self._objects[(ref.bucket, ref.key)]


@dataclass
class _Entry:
    status: str
    source_key: str
    lease_expires_at: float


class InMemoryRegistry:
    """Same claim semantics as the DynamoDB registry, guarded by a lock instead of a condition."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._entries: dict[str, _Entry] = {}
        self._lock = threading.Lock()

    def claim(self, content_hash: str, source_key: str, lease_seconds: int) -> ClaimResult:
        now = self._clock()
        with self._lock:
            entry = self._entries.get(content_hash)
            if entry is not None and entry.status == "COMPLETE":
                return ClaimResult.ALREADY_INDEXED
            if entry is not None and entry.lease_expires_at > now:
                return ClaimResult.IN_PROGRESS
            self._entries[content_hash] = _Entry("PROCESSING", source_key, now + lease_seconds)
            return ClaimResult.CLAIMED

    def complete(self, content_hash: str) -> None:
        with self._lock:
            self._entries[content_hash].status = "COMPLETE"

    def release(self, content_hash: str) -> None:
        with self._lock:
            entry = self._entries.get(content_hash)
            if entry is not None and entry.status == "PROCESSING":
                del self._entries[content_hash]

    def status(self, content_hash: str) -> str | None:
        entry = self._entries.get(content_hash)
        return entry.status if entry else None


class HashEmbedder:
    """Deterministic stand-in for an embedding model: same text, same vector."""

    def __init__(self, dimension: int = 8) -> None:
        self.dimension = dimension
        self.calls = 0

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += 1
        return [self._vector(text) for text in texts]

    def _vector(self, text: str) -> list[float]:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        return [byte / 255 for byte in digest[: self.dimension]]


class InMemoryVectorSink:
    def __init__(self) -> None:
        self.chunks: list[EmbeddedChunk] = []

    def write(self, chunks: Sequence[EmbeddedChunk]) -> None:
        self.chunks.extend(chunks)


@dataclass
class Message:
    ref: DocumentRef
    receive_count: int = 0


@dataclass
class LocalQueue:
    """Mimics SQS redrive: a message that fails ``max_receive_count`` times moves to the DLQ."""

    max_receive_count: int = 3
    pending: deque[Message] = field(default_factory=deque)
    dead_letters: list[Message] = field(default_factory=list)

    def send(self, ref: DocumentRef) -> None:
        self.pending.append(Message(ref))

    def receive(self) -> Message | None:
        if not self.pending:
            return None
        message = self.pending.popleft()
        message.receive_count += 1
        return message

    def nack(self, message: Message) -> None:
        if message.receive_count >= self.max_receive_count:
            self.dead_letters.append(message)
        else:
            self.pending.append(message)
