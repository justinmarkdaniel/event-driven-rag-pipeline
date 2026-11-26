# Event-driven RAG ingestion

Document ingestion for retrieval-augmented generation on AWS: files land in S3, an
`ObjectCreated` notification goes through SQS, and a Lambda chunks, embeds (Bedrock Titan) and
stores them. Identical content is embedded once, embedding calls stay inside the account's
rate limits, and documents that keep failing end up in a dead-letter queue with an alarm on it.

```
S3 incoming/ ──ObjectCreated──▶ SQS ──▶ Lambda ──▶ Bedrock embeddings ──▶ S3 embeddings/*.jsonl
                                 │        │
                                 ▼        └──▶ DynamoDB content-hash registry
                                DLQ ──▶ CloudWatch alarm
```

## How it works

**One retry mechanism: the queue.** The handler uses SQS partial batch responses
(`ReportBatchItemFailures`), so only failed messages become visible again. After
`max_receive_count` deliveries SQS moves the message to the DLQ. The application never
re-enqueues anything itself, and the visibility timeout is 6× the function timeout so a slow
invocation is not raced by a redelivery of its own message.

**Exactly-once embedding per content.** Documents are keyed by the SHA-256 of their text,
so a re-upload or a renamed copy is recognised. (This catches exact duplicates; near-duplicate
detection would need MinHash or embedding similarity on top.) Before embedding, a worker
*claims* the hash in DynamoDB with a conditional write:

- `CLAIMED`: this worker owns it. On success the entry becomes `COMPLETE`. On failure the
  claim is released so the retry can take it.
- `ALREADY_INDEXED`: skipped.
- `IN_PROGRESS`: another worker is embedding identical content right now, so the message is
  retried later instead of embedding it twice.

Claims are leases (15 min by default). A worker that dies mid-document blocks that content
only until the lease expires. Output objects are keyed by content hash, so even a
lease-expiry race overwrites rather than duplicates.

**Rate limiting.** A token bucket covers both requests/min and tokens/min, waits for exactly
as long as the deficit needs, and raises a retryable error rather than sleeping past its
budget. Chunks are batched to fit the bucket. Because the bucket is per process, Terraform
caps Lambda concurrency and gives each worker `quota / max_concurrency`.

**Chunking.** Whole paragraphs are packed up to a token budget (512 by default), and an
over-long paragraph is split on word boundaries. Token counts use a 4-characters-per-token
estimate, which is fine for budgeting. Swap in the model's tokenizer if you need to sit right
at its context limit.

## Layout

```
src/rag_ingest/
  service.py        the use case: load → dedupe → chunk → embed → store
  ports.py          interfaces the service depends on (store, registry, embedder, sink)
  chunking.py       paragraph-aware chunking
  rate_limiter.py   request + token bucket
  handler.py        Lambda entry point (SQS → S3 event parsing → service)
  adapters/aws.py   S3, DynamoDB (conditional writes), Bedrock Titan
  adapters/memory.py  in-process versions for tests and the demo
infra/              Terraform: buckets, queues, DLQ, registry, Lambda, IAM, alarm
examples/local_demo.py
```

The service only knows the interfaces in `ports.py`. Swapping S3 JSONL output for
pgvector or OpenSearch means writing one `VectorSink`, with no change to the service.

## Run it locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

pytest                       # unit tests + AWS adapters against moto
python examples/local_demo.py
```

The demo uses an embedder that fails the first call for every document and always fails on
one "poison" document:

```
handbook.txt (receive 1): failed - embedding API timed out
handbook-copy.txt (receive 1): indexed
release-notes.txt (receive 1): failed - embedding API timed out
poison.txt (receive 1): failed - embedding API timed out
handbook.txt (receive 2): duplicate
release-notes.txt (receive 2): indexed
poison.txt (receive 2): failed - embedding API timed out
poison.txt (receive 3): failed - embedding API timed out

chunks stored: 2
dead letters: ['poison.txt']
```

## Deploy

Requires Bedrock model access for `amazon.titan-embed-text-v2:0` in the target region.
Lambda's Python runtime includes boto3, so the package is just `src/` and Terraform zips it.

```bash
cd infra
terraform init
terraform apply -var environment=dev

aws s3 cp report.txt "s3://$(terraform output -raw documents_bucket)/incoming/report.txt"
```

Set `embedding_rpm`, `embedding_tpm` and `max_concurrency` to match your Bedrock quotas.

## Limits

- Text documents only (UTF-8). PDFs and HTML need an extraction step before chunking.
- Output is JSON Lines in S3, ready to bulk-load into a vector store. There is no query side
  here.
- Updating a document under the same key with new content indexes the new content but does
  not remove the old chunks.
