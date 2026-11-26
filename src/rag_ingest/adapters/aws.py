"""AWS adapters: S3 for documents and output, DynamoDB for the registry, Bedrock for embeddings."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence
from typing import Any

from botocore.exceptions import ClientError

from rag_ingest.domain import ClaimResult, DocumentRef, EmbeddedChunk

_PROCESSING = "PROCESSING"
_COMPLETE = "COMPLETE"


class S3DocumentStore:
    def __init__(self, s3: Any) -> None:
        self._s3 = s3

    def load(self, ref: DocumentRef) -> str:
        body: bytes = self._s3.get_object(Bucket=ref.bucket, Key=ref.key)["Body"].read()
        return body.decode("utf-8")


class DynamoDBRegistry:
    """Content-hash registry using conditional writes, so claims are atomic across workers.

    Table schema: partition key ``content_hash`` (S).
    """

    def __init__(self, dynamodb: Any, table_name: str, clock: Callable[[], float] = time.time):
        self._db = dynamodb
        self._table = table_name
        self._clock = clock

    def claim(self, content_hash: str, source_key: str, lease_seconds: int) -> ClaimResult:
        now = int(self._clock())
        try:
            self._db.put_item(
                TableName=self._table,
                Item={
                    "content_hash": {"S": content_hash},
                    "status": {"S": _PROCESSING},
                    "source_key": {"S": source_key},
                    "lease_expires_at": {"N": str(now + lease_seconds)},
                },
                # New content, or a claim whose holder died without releasing it.
                ConditionExpression=(
                    "attribute_not_exists(content_hash) OR "
                    "(#status = :processing AND lease_expires_at < :now)"
                ),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":processing": {"S": _PROCESSING},
                    ":now": {"N": str(now)},
                },
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            return self._existing_claim(content_hash)
        return ClaimResult.CLAIMED

    def complete(self, content_hash: str) -> None:
        self._db.update_item(
            TableName=self._table,
            Key={"content_hash": {"S": content_hash}},
            UpdateExpression="SET #status = :complete, indexed_at = :now REMOVE lease_expires_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":complete": {"S": _COMPLETE},
                ":now": {"N": str(int(self._clock()))},
            },
        )

    def release(self, content_hash: str) -> None:
        try:
            self._db.delete_item(
                TableName=self._table,
                Key={"content_hash": {"S": content_hash}},
                ConditionExpression="#status = :processing",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":processing": {"S": _PROCESSING}},
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

    def _existing_claim(self, content_hash: str) -> ClaimResult:
        item = self._db.get_item(
            TableName=self._table,
            Key={"content_hash": {"S": content_hash}},
            ConsistentRead=True,
        ).get("Item")
        if item and item["status"]["S"] == _COMPLETE:
            return ClaimResult.ALREADY_INDEXED
        return ClaimResult.IN_PROGRESS


class BedrockEmbedder:
    """Amazon Titan Text Embeddings V2. The model takes one input per request."""

    def __init__(self, bedrock_runtime: Any, model_id: str, dimensions: int = 1024) -> None:
        self._client = bedrock_runtime
        self._model_id = model_id
        self._dimensions = dimensions

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> list[float]:
        response = self._client.invoke_model(
            modelId=self._model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps({"inputText": text, "dimensions": self._dimensions, "normalize": True}),
        )
        embedding: list[float] = json.loads(response["body"].read())["embedding"]
        return embedding


class S3JsonlVectorSink:
    """Writes one JSON Lines object per document under ``prefix``, keyed by content hash.

    Keying by hash makes a redelivered message overwrite rather than duplicate. Load the
    output into the vector store of your choice, or replace this adapter with one for it.
    """

    def __init__(self, s3: Any, bucket: str, prefix: str) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")

    def write(self, chunks: Sequence[EmbeddedChunk]) -> None:
        if not chunks:
            return
        lines = (
            json.dumps(
                {
                    "content_hash": c.content_hash,
                    "source_key": c.source_key,
                    "chunk_index": c.chunk_index,
                    "text": c.text,
                    "embedding": c.embedding,
                }
            )
            for c in chunks
        )
        self._s3.put_object(
            Bucket=self._bucket,
            Key=f"{self._prefix}/{chunks[0].content_hash}.jsonl",
            Body="\n".join(lines).encode("utf-8"),
            ContentType="application/x-ndjson",
        )
