variable "name" {
  description = "Prefix for every resource name."
  type        = string
  default     = "rag-ingest"
}

variable "environment" {
  description = "Deployment environment, e.g. dev or prod."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region. Bedrock model access must be enabled for the model in this region."
  type        = string
  default     = "us-east-1"
}

variable "embedding_model_id" {
  description = "Bedrock embedding model."
  type        = string
  default     = "amazon.titan-embed-text-v2:0"
}

variable "embedding_rpm" {
  description = "Account-wide embedding requests per minute to stay under."
  type        = number
  default     = 1500
}

variable "embedding_tpm" {
  description = "Account-wide embedding tokens per minute to stay under."
  type        = number
  default     = 300000
}

variable "max_concurrency" {
  description = "Maximum concurrent Lambda invocations; the embedding quota is divided between them."
  type        = number
  default     = 5

  validation {
    condition     = var.max_concurrency >= 2
    error_message = "SQS event source mappings require maximum_concurrency >= 2."
  }
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout. The queue visibility timeout is derived from it."
  type        = number
  default     = 120
}

variable "max_receive_count" {
  description = "Deliveries before a message is moved to the dead-letter queue."
  type        = number
  default     = 3
}

variable "log_retention_days" {
  type    = number
  default = 30
}
