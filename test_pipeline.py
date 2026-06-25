from pipeline import RAGPipeline
import time

def main():
    print("====================================================")
    print("     EVENT-DRIVEN RAG INGESTION PIPELINE TEST       ")
    print("====================================================")
    
    # Initialize pipeline with small limits to trigger limits easily
    pipeline = RAGPipeline(rpm_limit=3, tpm_limit=50)

    # 1. Define sample documents
    doc_a = "This is a standard document containing project configurations and system architecture details. It is clean and correct."
    doc_b = "This is a standard document containing project configurations and system architecture details. It is clean and correct." # Exact duplicate
    doc_c = "This is a document designed to consume many tokens. " * 10 # Large content
    doc_d = "This document is corrupt. It will consistently trigger API failures to demonstrate routing to the DLQ."

    # 2. Ingest documents (ingest checks deduplication hashes)
    print("\n--- Ingesting Initial Documents ---")
    pipeline.ingest_document("doc_a.txt", doc_a)
    pipeline.ingest_document("doc_c.txt", doc_c)
    pipeline.ingest_document("doc_d.txt", doc_d)

    # 3. Process the queue
    print("\n--- Processing SQS Queue (Batch 1) ---")
    pipeline.process_queue()

    # 4. Ingest duplicate document to test deduplication
    print("\n--- Ingesting Duplicate Document ---")
    pipeline.ingest_document("doc_b.txt", doc_b) # Deduplication check should skip this now that doc_a is processed

    # 4. Print results
    print("\n--- PIPELINE METRICS & STATES ---")
    print(f"Processed Documents  : {pipeline.metrics['processed']}")
    print(f"Deduplicated Docs   : {pipeline.metrics['deduplicated']}")
    print(f"Total Retry Counts   : {pipeline.metrics['retried']}")
    print(f"Routed to DLQ        : {pipeline.metrics['routed_to_dlq']}")
    print("----------------------------------------------------")
    print(f"Items in DynamoDB    : {list(pipeline.dynamodb_registry.values())}")
    print(f"Items in DLQ         : {[msg['filename'] for msg in list(pipeline.dlq_queue.queue)]}")
    print(f"Total Vector Chunks  : {len(pipeline.vector_store)}")
    print("====================================================")

if __name__ == "__main__":
    main()
