"""Value types shared by the service and its adapters."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True, slots=True)
class DocumentRef:
    """Where a document lives, e.g. an S3 bucket and key."""

    bucket: str
    key: str


@dataclass(frozen=True, slots=True)
class Chunk:
    index: int
    text: str
    token_estimate: int


@dataclass(frozen=True, slots=True)
class EmbeddedChunk:
    content_hash: str
    source_key: str
    chunk_index: int
    text: str
    embedding: list[float]


class Outcome(Enum):
    INDEXED = "indexed"
    DUPLICATE = "duplicate"
    EMPTY = "empty"


class ClaimResult(Enum):
    """Result of trying to take ownership of a content hash before indexing it."""

    CLAIMED = "claimed"
    ALREADY_INDEXED = "already_indexed"
    IN_PROGRESS = "in_progress"


class RetryableError(Exception):
    """Processing failed for a transient reason; the message should be redelivered."""


class DocumentBusyError(RetryableError):
    """Another worker currently holds the claim on identical content."""


class RateLimitedError(RetryableError):
    """The embedding budget would not free up within the allowed wait."""
