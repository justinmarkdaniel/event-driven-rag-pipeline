output "documents_bucket" {
  description = "Upload documents under incoming/ to ingest them."
  value       = aws_s3_bucket.documents.id
}

output "queue_url" {
  value = aws_sqs_queue.ingest.id
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.id
}

output "registry_table" {
  value = aws_dynamodb_table.registry.name
}
