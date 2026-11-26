"""AWS Lambda entry point: SQS messages carrying S3 ``ObjectCreated`` notifications.

Uses partial batch responses (``ReportBatchItemFailures``): only the messages that failed
are made visible again, and SQS moves a message to the DLQ once it has been received
``maxReceiveCount`` times. Retry policy therefore lives in one place, the queue.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from functools import cache
from typing import Any
from urllib.parse import unquote_plus

from rag_ingest.domain import DocumentRef
from rag_ingest.service import IngestionService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: dict[str, Any], context: object) -> dict[str, list[dict[str, str]]]:
    return handle(event, _service())


def handle(event: dict[str, Any], service: IngestionService) -> dict[str, list[dict[str, str]]]:
    failures = []
    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            for ref in document_refs(record["body"]):
                outcome = service.process(ref)
                logger.info("processed %s: %s", ref.key, outcome.value)
        except Exception:
            logger.exception("failed message %s", message_id)
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}


def document_refs(body: str) -> Iterator[DocumentRef]:
    """Extract objects from an S3 event notification. S3's ``TestEvent`` yields nothing."""
    notification = json.loads(body)
    for record in notification.get("Records", []):
        s3 = record["s3"]
        # Object keys arrive URL-encoded, with spaces as '+'.
        yield DocumentRef(bucket=s3["bucket"]["name"], key=unquote_plus(s3["object"]["key"]))


@cache
def _service() -> IngestionService:
    """Built once per Lambda container and reused across invocations."""
    import boto3

    from rag_ingest.adapters.aws import (
        BedrockEmbedder,
        DynamoDBRegistry,
        S3DocumentStore,
        S3JsonlVectorSink,
    )
    from rag_ingest.rate_limiter import TokenBucket

    s3 = boto3.client("s3")
    return IngestionService(
        documents=S3DocumentStore(s3),
        registry=DynamoDBRegistry(boto3.client("dynamodb"), os.environ["REGISTRY_TABLE"]),
        embedder=BedrockEmbedder(boto3.client("bedrock-runtime"), os.environ["EMBEDDING_MODEL_ID"]),
        sink=S3JsonlVectorSink(s3, os.environ["OUTPUT_BUCKET"], os.environ["OUTPUT_PREFIX"]),
        limiter=TokenBucket(
            requests_per_minute=float(os.environ["EMBEDDING_RPM_PER_WORKER"]),
            tokens_per_minute=float(os.environ["EMBEDDING_TPM_PER_WORKER"]),
        ),
    )
