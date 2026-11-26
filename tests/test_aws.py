import io
import json
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_aws

from rag_ingest.adapters.aws import (
    BedrockEmbedder,
    DynamoDBRegistry,
    S3DocumentStore,
    S3JsonlVectorSink,
)
from rag_ingest.adapters.memory import HashEmbedder
from rag_ingest.domain import ClaimResult, DocumentRef
from rag_ingest.handler import document_refs, handle
from rag_ingest.rate_limiter import TokenBucket
from rag_ingest.service import IngestionService

REGION = "us-east-1"
TABLE = "registry"


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        dynamodb = boto3.client("dynamodb", region_name=REGION)
        s3.create_bucket(Bucket="docs")
        s3.create_bucket(Bucket="out")
        dynamodb.create_table(
            TableName=TABLE,
            KeySchema=[{"AttributeName": "content_hash", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "content_hash", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield {"s3": s3, "dynamodb": dynamodb}


def sqs_event(*keys: str) -> dict[str, Any]:
    return {
        "Records": [
            {
                "messageId": f"m{i}",
                "body": json.dumps(
                    {"Records": [{"s3": {"bucket": {"name": "docs"}, "object": {"key": key}}}]}
                ),
            }
            for i, key in enumerate(keys)
        ]
    }


class TestDynamoDBRegistry:
    def test_claim_is_exclusive_until_lease_expires(self, aws: dict[str, Any]) -> None:
        now = [1000.0]
        registry = DynamoDBRegistry(aws["dynamodb"], TABLE, clock=lambda: now[0])

        assert registry.claim("h", "a", 60) is ClaimResult.CLAIMED
        assert registry.claim("h", "b", 60) is ClaimResult.IN_PROGRESS
        now[0] += 61
        assert registry.claim("h", "b", 60) is ClaimResult.CLAIMED

    def test_completed_hash_is_already_indexed_forever(self, aws: dict[str, Any]) -> None:
        now = [1000.0]
        registry = DynamoDBRegistry(aws["dynamodb"], TABLE, clock=lambda: now[0])
        registry.claim("h", "a", 60)
        registry.complete("h")
        now[0] += 10_000

        assert registry.claim("h", "b", 60) is ClaimResult.ALREADY_INDEXED

    def test_release_frees_claim_but_not_completion(self, aws: dict[str, Any]) -> None:
        registry = DynamoDBRegistry(aws["dynamodb"], TABLE)
        registry.claim("h", "a", 60)
        registry.release("h")
        assert registry.claim("h", "a", 60) is ClaimResult.CLAIMED

        registry.complete("h")
        registry.release("h")
        assert registry.claim("h", "a", 60) is ClaimResult.ALREADY_INDEXED


def test_bedrock_embedder_sends_titan_request() -> None:
    client = boto3.client("bedrock-runtime", region_name=REGION)
    stubber = Stubber(client)
    stubber.add_response(
        "invoke_model",
        {"body": io.BytesIO(b'{"embedding": [0.1, 0.2]}'), "contentType": "application/json"},
        {
            "modelId": "amazon.titan-embed-text-v2:0",
            "contentType": "application/json",
            "accept": "application/json",
            "body": json.dumps({"inputText": "hello", "dimensions": 2, "normalize": True}),
        },
    )
    with stubber:
        vectors = BedrockEmbedder(client, "amazon.titan-embed-text-v2:0", dimensions=2).embed(
            ["hello"]
        )
    assert vectors == [[0.1, 0.2]]


def test_object_keys_are_url_decoded() -> None:
    body = sqs_event("reports/q3+summary%281%29.txt")["Records"][0]["body"]
    assert list(document_refs(body)) == [DocumentRef("docs", "reports/q3 summary(1).txt")]


def test_s3_test_event_yields_nothing() -> None:
    assert list(document_refs(json.dumps({"Event": "s3:TestEvent"}))) == []


def test_handler_end_to_end_reports_only_failed_messages(aws: dict[str, Any]) -> None:
    s3 = aws["s3"]
    s3.put_object(Bucket="docs", Key="incoming/a.txt", Body=b"alpha\n\nbeta")
    s3.put_object(Bucket="docs", Key="incoming/copy.txt", Body=b"alpha\n\nbeta")
    service = IngestionService(
        S3DocumentStore(s3),
        DynamoDBRegistry(aws["dynamodb"], TABLE),
        HashEmbedder(),
        S3JsonlVectorSink(s3, "out", "embeddings/"),
        TokenBucket(requests_per_minute=100, tokens_per_minute=10_000),
    )

    result = handle(
        sqs_event("incoming/a.txt", "incoming/missing.txt", "incoming/copy.txt"), service
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    written = s3.list_objects_v2(Bucket="out")["Contents"]
    assert len(written) == 1  # the copy was deduplicated
    record = json.loads(s3.get_object(Bucket="out", Key=written[0]["Key"])["Body"].read())
    assert record["source_key"] == "incoming/a.txt"
    assert record["text"] == "alpha\n\nbeta"
