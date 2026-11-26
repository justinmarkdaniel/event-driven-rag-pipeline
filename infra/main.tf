# S3 (incoming/) --ObjectCreated--> SQS --> Lambda --> Bedrock embeddings --> S3 (embeddings/)
#                                    |                    |
#                                    +--> DLQ             +--> DynamoDB content-hash registry

data "aws_caller_identity" "current" {}

locals {
  prefix          = "${var.name}-${var.environment}"
  incoming_prefix = "incoming/"
  output_prefix   = "embeddings/"
  # AWS guidance: visibility timeout of at least 6x the function timeout for SQS triggers,
  # so a message is not redelivered while a slow invocation is still working on it.
  visibility_timeout = 6 * var.lambda_timeout_seconds
}

# --- Storage ------------------------------------------------------------------------

resource "aws_s3_bucket" "documents" {
  # Bucket names are global; the account id keeps this one unique.
  bucket = "${local.prefix}-documents-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_public_access_block" "documents" {
  bucket                  = aws_s3_bucket.documents.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "documents" {
  bucket = aws_s3_bucket.documents.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "documents" {
  bucket = aws_s3_bucket.documents.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_dynamodb_table" "registry" {
  name         = "${local.prefix}-registry"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "content_hash"

  attribute {
    name = "content_hash"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}

# --- Queues -------------------------------------------------------------------------

resource "aws_sqs_queue" "dlq" {
  name                      = "${local.prefix}-dlq"
  message_retention_seconds = 1209600 # 14 days, the SQS maximum
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "ingest" {
  name                       = "${local.prefix}-queue"
  visibility_timeout_seconds = local.visibility_timeout
  message_retention_seconds  = 345600 # 4 days
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.ingest.arn]
  })
}

data "aws_iam_policy_document" "s3_to_sqs" {
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.ingest.arn]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.documents.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "ingest" {
  queue_url = aws_sqs_queue.ingest.id
  policy    = data.aws_iam_policy_document.s3_to_sqs.json
}

resource "aws_s3_bucket_notification" "documents" {
  bucket = aws_s3_bucket.documents.id

  queue {
    queue_arn     = aws_sqs_queue.ingest.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = local.incoming_prefix # output goes elsewhere, so no feedback loop
  }

  depends_on = [aws_sqs_queue_policy.ingest]
}

# --- Processor ----------------------------------------------------------------------

data "archive_file" "processor" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/build/processor.zip"
  excludes    = ["**/__pycache__/**"]
}

resource "aws_cloudwatch_log_group" "processor" {
  name              = "/aws/lambda/${local.prefix}-processor"
  retention_in_days = var.log_retention_days
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "processor" {
  statement {
    sid       = "ConsumeQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.ingest.arn]
  }

  statement {
    sid       = "ReadIncoming"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.documents.arn}/${local.incoming_prefix}*"]
  }

  statement {
    sid       = "WriteEmbeddings"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.documents.arn}/${local.output_prefix}*"]
  }

  statement {
    sid       = "Registry"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"]
    resources = [aws_dynamodb_table.registry.arn]
  }

  statement {
    sid       = "Embed"
    actions   = ["bedrock:InvokeModel"]
    resources = ["arn:aws:bedrock:${var.region}::foundation-model/${var.embedding_model_id}"]
  }

  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.processor.arn}:*"]
  }
}

resource "aws_iam_role" "processor" {
  name               = "${local.prefix}-processor"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "processor" {
  name   = "processor"
  role   = aws_iam_role.processor.id
  policy = data.aws_iam_policy_document.processor.json
}

resource "aws_lambda_function" "processor" {
  function_name    = "${local.prefix}-processor"
  role             = aws_iam_role.processor.arn
  runtime          = "python3.12"
  handler          = "rag_ingest.handler.lambda_handler"
  filename         = data.archive_file.processor.output_path
  source_code_hash = data.archive_file.processor.output_base64sha256
  timeout          = var.lambda_timeout_seconds
  memory_size      = 512

  environment {
    variables = {
      REGISTRY_TABLE     = aws_dynamodb_table.registry.name
      OUTPUT_BUCKET      = aws_s3_bucket.documents.id
      OUTPUT_PREFIX      = local.output_prefix
      EMBEDDING_MODEL_ID = var.embedding_model_id
      # Each concurrent invocation gets an equal share of the account quota.
      EMBEDDING_RPM_PER_WORKER = floor(var.embedding_rpm / var.max_concurrency)
      EMBEDDING_TPM_PER_WORKER = floor(var.embedding_tpm / var.max_concurrency)
    }
  }

  depends_on = [aws_cloudwatch_log_group.processor]
}

resource "aws_lambda_event_source_mapping" "ingest" {
  event_source_arn        = aws_sqs_queue.ingest.arn
  function_name           = aws_lambda_function.processor.arn
  batch_size              = 5
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = var.max_concurrency
  }
}

# --- Alerting -----------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${local.prefix}-dlq-not-empty"
  alarm_description   = "Documents failed ingestion ${var.max_receive_count} times and need attention."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}
