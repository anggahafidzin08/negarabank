variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}

variable "oracle_username" {
  description = "Oracle database username"
  type        = string
  sensitive   = true
}

variable "oracle_password" {
  description = "Oracle database password"
  type        = string
  sensitive   = true
}

variable "oracle_host" {
  description = "Oracle database host (on-prem)"
  type        = string
}

variable "oracle_port" {
  description = "Oracle database port"
  type        = number
  default     = 1521
}

variable "databricks_token" {
  description = "Databricks API token"
  type        = string
  sensitive   = true
}

variable "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers (comma-separated)"
  type        = string
}
