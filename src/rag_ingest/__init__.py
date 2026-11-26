"""Event-driven document ingestion for retrieval-augmented generation."""

from rag_ingest.domain import Chunk, DocumentRef, EmbeddedChunk, Outcome
from rag_ingest.service import IngestionService

__all__ = ["Chunk", "DocumentRef", "EmbeddedChunk", "IngestionService", "Outcome"]
