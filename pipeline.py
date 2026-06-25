import time
import hashlib
import queue
import random
from typing import Dict, Any, List, Set, Optional

class TokenBucketRateLimiter:
    """Token Bucket rate limiter for RPM (Requests Per Minute) and TPM (Tokens Per Minute)."""
    def __init__(self, rpm: int, tpm: int):
        self.rpm = rpm
        self.tpm = tpm
        self.tokens_rpm = float(rpm)
        self.tokens_tpm = float(tpm)
        self.last_update = time.time()

    def _replenish(self):
        now = time.time()
        delta = now - self.last_update
        self.last_update = now
        
        # Add tokens proportionally to time elapsed
        self.tokens_rpm = min(self.rpm, self.tokens_rpm + delta * (self.rpm / 60.0))
        self.tokens_tpm = min(self.tpm, self.tokens_tpm + delta * (self.tpm / 60.0))

    def consume(self, num_tokens: int) -> bool:
        self._replenish()
        if self.tokens_rpm >= 1 and self.tokens_tpm >= num_tokens:
            self.tokens_rpm -= 1
            self.tokens_tpm -= num_tokens
            return True
        return False

    def wait_and_consume(self, num_tokens: int, max_retries: int = 5) -> bool:
        """Blocks until tokens are available or max_retries is reached."""
        for _ in range(max_retries):
            if self.consume(num_tokens):
                return True
            time.sleep(0.5) # Wait before retrying
        return False

class RAGPipeline:
    def __init__(self, rpm_limit: int = 10, tpm_limit: int = 1000):
        # Queues representing SQS & DLQ
        self.sqs_queue = queue.Queue()
        self.dlq_queue = queue.Queue()
        
        # Mock Databases
        self.dynamodb_registry: Dict[str, str] = {} # doc_hash -> filename
        self.vector_store: List[Dict[str, Any]] = [] # Stored document embeddings
        
        # Rate Limiter
        self.rate_limiter = TokenBucketRateLimiter(rpm=rpm_limit, tpm=tpm_limit)
        
        # Ingestion metrics
        self.metrics = {
            "processed": 0,
            "deduplicated": 0,
            "retried": 0,
            "routed_to_dlq": 0
        }

    def compute_hash(self, content: str) -> str:
        """MD5 Hash to simulate semantic deduplication checking."""
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def ingest_document(self, filename: str, content: str):
        """Pre-ingest checking hashes. If unique, push message into the SQS Queue."""
        doc_hash = self.compute_hash(content)
        
        if doc_hash in self.dynamodb_registry:
            print(f"[DEDUPLICATION] Skipping '{filename}'. Match found in registry (hash: {doc_hash}).")
            self.metrics["deduplicated"] += 1
            return
            
        print(f"[INGESTION] Registering document '{filename}' & queuing for processing.")
        # Simulating S3 put event, sending object location to SQS queue
        message = {
            "filename": filename,
            "content": content,
            "doc_hash": doc_hash,
            "retry_count": 0
        }
        self.sqs_queue.put(message)

    def process_queue(self):
        """Worker loop pulling tasks from the SQS queue and simulating embedding generation."""
        print("\n=== Processing Ingestion Queue ===")
        while not self.sqs_queue.empty():
            msg = self.sqs_queue.get()
            filename = msg["filename"]
            content = msg["content"]
            doc_hash = msg["doc_hash"]
            retries = msg["retry_count"]
            
            # Simple semantic chunking (split by double newline / paragraph)
            chunks = [c.strip() for c in content.split("\n\n") if c.strip()]
            num_tokens = sum(len(c.split()) for c in chunks) # Simple token estimation (word count)
            
            print(f"\n[WORKER] Processing '{filename}' ({len(chunks)} chunks, ~{num_tokens} tokens)")
            
            # Apply Rate Limiting
            if not self.rate_limiter.wait_and_consume(num_tokens):
                print(f"[RATE-LIMIT] Token bucket exhausted for '{filename}'!")
                if retries < 2:
                    print(f"[RETRY] Re-queuing '{filename}' (Attempt {retries + 1}/3 due to rate limit)")
                    msg["retry_count"] += 1
                    self.sqs_queue.put(msg)
                    self.metrics["retried"] += 1
                else:
                    print(f"[DLQ] Max retries exhausted. Routing '{filename}' to Dead Letter Queue.")
                    self.dlq_queue.put(msg)
                    self.metrics["routed_to_dlq"] += 1
                continue

            # Simulate network or embedding API failure (15% failure rate to demonstrate DLQ routing)
            # Or force a failure if the document has a specific word to test DLQ reliably
            is_corrupted = "corrupt" in content.lower()
            if is_corrupted or random.random() < 0.15:
                print(f"[ERROR] Embedding API failed for '{filename}'.")
                if retries < 2:
                    print(f"[RETRY] Re-queuing '{filename}' (Attempt {retries + 1}/3)")
                    msg["retry_count"] += 1
                    self.sqs_queue.put(msg)
                    self.metrics["retried"] += 1
                else:
                    print(f"[DLQ] Max retries exhausted. Routing '{filename}' to Dead Letter Queue.")
                    self.dlq_queue.put(msg)
                    self.metrics["routed_to_dlq"] += 1
                continue

            # Success Path: Generate mock embedding (vector of size 128)
            for idx, chunk in enumerate(chunks):
                mock_embedding = [random.uniform(-1, 1) for _ in range(128)]
                self.vector_store.append({
                    "doc_hash": doc_hash,
                    "filename": filename,
                    "chunk_id": idx,
                    "text": chunk,
                    "embedding": mock_embedding
                })
            
            # Update DynamoDB registry
            self.dynamodb_registry[doc_hash] = filename
            self.metrics["processed"] += 1
            print(f"[SUCCESS] Fully indexed '{filename}'.")

        print("=== Queue Empty ===")
