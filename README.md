# Event-Driven RAG Ingestion Pipeline

Production-grade async document ingestion pipeline for RAG systems. Handles deduplication, rate limiting, retries, and failure routing with complete Terraform infrastructure-as-code.

## Why This Matters

Most RAG projects skip the hard part: reliable ingestion at scale. This pipeline includes the following features:
- **Semantic Deduplication**: Hash-based registry prevents duplicate embeddings
- **Rate Limiting**: Token bucket ensures you don't exhaust embedding API quotas
- **Resilience**: SQS + DLQ routes failures to dead-letter queue after 3 retries
- **Observability**: Detailed metrics (processed, deduplicated, retried, routed_to_dlq)

## Architecture

**Ingestion Flow:**
1. Document arrives → Compute content hash → Check DynamoDB registry
2. If duplicate: skip. If new: queue message to SQS
3. Worker dequeues → Chunks document → Estimates tokens
4. Apply rate limiting → If over quota: retry queue message
5. Call embedding API → If fails: retry (max 3) → If exhausted: route to DLQ
6. Success: store vectors + register hash in DynamoDB

**Infrastructure (Terraform):**
- S3 bucket for raw documents (with lifecycle/archival rules)
- SQS queue with DLQ (max receive count = 3)
- DynamoDB table for deduplication registry (TTL enabled)
- Lambda processor role with least-privilege IAM policy
- Lambda event source mapping for SQS → Lambda triggering

## Quick Start

```bash
pip install -r requirements.txt

# Run local simulation (no AWS credentials needed)
python test_pipeline.py
```

Output shows:
- Documents ingested and deduplicated
- Rate limiting triggering (intentional small limits for demo)
- Failures routed to DLQ after max retries
- Final metrics: processed, deduplicated, retried, routed_to_dlq

## Production Deployment

```bash
# Deploy infrastructure
terraform init
terraform apply

# Package Lambda handler and deploy
cd handler && zip -r ../processor_payload.zip . && cd ..
terraform apply
```

The Terraform config includes:
- S3 lifecycle policies (archive after 30 days)
- DynamoDB TTL for automatic cleanup
- Lambda timeout and memory sizing
- Least-privilege IAM policies

## Additional Features

- **Async Architecture Thinking**: Event-driven, decoupled ingestion and processing
- **Resilience Patterns**: DLQ routing, exponential backoff (implicit in retries), rate limiting
- **Production Infrastructure**: Terraform IaC, IAM least-privilege, S3 lifecycle, DynamoDB TTL
- **Metrics-Driven Design**: Observable failures, retry counts, deduplication efficiency
- **Edge Case Handling**: Duplicate detection, rate limit exhaustion, API failures all accounted for
