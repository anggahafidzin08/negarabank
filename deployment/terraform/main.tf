terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Secrets Manager: Oracle Credentials
resource "aws_secretsmanager_secret" "oracle_creds" {
  name                    = "negarabank/oracle/jdbc"
  description             = "Oracle JDBC credentials"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "oracle_creds" {
  secret_id = aws_secretsmanager_secret.oracle_creds.id
  secret_string = jsonencode({
    username = var.oracle_username
    password = var.oracle_password
    host     = var.oracle_host
    port     = var.oracle_port
  })
}

# IAM Role for EC2 JDBC Gateway
resource "aws_iam_role" "ec2_jdbc_role" {
  name = "negarabank-ec2-jdbc-gateway"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "ec2_secrets_access" {
  name = "negarabank-ec2-secrets-access"
  role = aws_iam_role.ec2_jdbc_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.oracle_creds.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = "arn:aws:s3:::negarabank-*/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2_jdbc_profile" {
  name = "negarabank-ec2-jdbc-profile"
  role = aws_iam_role.ec2_jdbc_role.name
}

# Security Group for EC2 JDBC Gateway
resource "aws_security_group" "ec2_jdbc_sg" {
  name        = "negarabank-ec2-jdbc-gateway"
  description = "Security group for EC2 JDBC gateway"

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]  # Databricks VPC CIDR
  }

  egress {
    from_port   = 1521
    to_port     = 1521
    protocol    = "tcp"
    cidr_blocks = ["203.0.113.0/24"]  # On-prem Oracle network (example)
  }

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # For AWS Secrets Manager
  }
}

# S3 Buckets
resource "aws_s3_bucket" "bronze" {
  bucket = "negarabank-bronze-${var.aws_account_id}"
}

resource "aws_s3_bucket" "silver" {
  bucket = "negarabank-silver-${var.aws_account_id}"
}

resource "aws_s3_bucket" "gold" {
  bucket = "negarabank-gold-${var.aws_account_id}"
}

resource "aws_s3_bucket" "checkpoints" {
  bucket = "negarabank-checkpoints-${var.aws_account_id}"
}

# S3 Intelligent-Tiering Configuration
resource "aws_s3_bucket_intelligent_tiering_configuration" "gold_tiering" {
  bucket = aws_s3_bucket.gold.id
  name   = "ArchivePolicy"

  tiering {
    access_tier = "ARCHIVE_ACCESS"
    days        = 90  # Move to Glacier after 90 days
  }
}
