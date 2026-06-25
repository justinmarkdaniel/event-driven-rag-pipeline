# Terraform Configuration for Event-Driven RAG Ingestion Pipeline
# Configures: S3 Bucket, SQS Queue, SQS DLQ, DynamoDB Deduplication Registry, and Lambda Processor

terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# 1. S3 Bucket for Raw Document Ingestion
resource "aws_s3_bucket" "rag_raw_documents" {
  bucket        = "rag-raw-documents-ingestion-bucket-prod"
  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket_lifecycle" {
  bucket = aws_s3_bucket.rag_raw_documents.id

  rule {
    id     = "archive-old-docs"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "GLACIER"
    }
  }
}

# 2. Dead Letter Queue (DLQ) for Failed Embeddings/Processing
resource "aws_sqs_queue" "rag_ingestion_dlq" {
  name                      = "rag-ingestion-dlq"
  message_retention_seconds = 1209600 # 14 days
}

# 3. Primary Ingestion Queue
resource "aws_sqs_queue" "rag_ingestion_queue" {
  name                      = "rag-ingestion-queue"
  delay_seconds             = 0
  max_message_size          = 262144 # 256 KB
  message_retention_seconds = 86400  # 1 day
  receive_wait_time_seconds = 10     # Long polling enabled

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.rag_ingestion_dlq.arn
    maxReceiveCount     = 3 # Route to DLQ after 3 failures
  })
}

# 4. DynamoDB Table for Semantic Deduplication & Metadata Registry
resource "aws_dynamodb_table" "document_registry" {
  name         = "rag-document-registry"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "doc_hash"

  attribute {
    name = "doc_hash"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Environment = "production"
    Component   = "RAG-Pipeline"
  }
}

# 5. Lambda Role and Policy
resource "aws_iam_role" "lambda_processor_role" {
  name = "rag-lambda-processor-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_processor_policy" {
  name = "rag-lambda-processor-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Effect   = "Allow"
        Resource = aws_sqs_queue.rag_ingestion_queue.arn
      },
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.rag_raw_documents.arn,
          "${aws_s3_bucket.rag_raw_documents.arn}/*"
        ]
      },
      {
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Effect   = "Allow"
        Resource = aws_dynamodb_table.document_registry.arn
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_processor_role.name
  policy_arn = aws_iam_policy.lambda_processor_policy.arn
}

# 6. Lambda Function Definition
resource "aws_lambda_function" "rag_processor" {
  filename      = "processor_payload.zip"
  function_name = "rag-document-processor"
  role          = aws_iam_role.lambda_processor_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.10"
  timeout       = 60
  memory_size   = 512

  environment {
    variables = {
      DYNAMODB_TABLE = aws_dynamodb_table.document_registry.name
      SQS_QUEUE_URL  = aws_sqs_queue.rag_ingestion_queue.id
    }
  }
}

# 7. SQS -> Lambda Event Mapping
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.rag_ingestion_queue.arn
  function_name    = aws_lambda_function.rag_processor.arn
  batch_size       = 5
}
