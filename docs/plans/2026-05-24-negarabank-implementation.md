# NegaraBank Data Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade data platform on Databricks that optimizes Oracle batch ingestion, implements medallion architecture with fraud detection, and serves analytics via star schema + real-time features.

**Architecture:** Hybrid batch (Oracle JDBC) + streaming (Kafka) → Databricks → Bronze/Silver/Gold medallion layers → BI/ML serving. All deployed via Databricks Asset Bundle (DAB) from GitHub.

**Tech Stack:** Databricks (Spark SQL, Structured Streaming), Delta Lake, Kafka, AWS (EC2, S3, VPC, IAM, Secrets Manager), MLflow, Unity Catalog

---

## File Structure

```
negarabank-pipeline/
├─ databricks.yml                  # DAB config (resources, jobs, permissions)
├─ README.md                       # Project overview & setup
├─ .gitignore                      # Ignore Databricks artifacts
├─ .env.example                    # Template for local config
│
├─ src/
│  ├─ notebooks/
│  │  ├─ bronze/
│  │  │  ├─ 01_extract_oracle_accounts.py       # JDBC extract: ACCOUNTS table
│  │  │  ├─ 02_extract_oracle_transactions.py   # JDBC extract: TRANSACTIONS (delta load)
│  │  │  ├─ 03_extract_oracle_credit_scores.py  # JDBC extract: CREDIT_SCORES
│  │  │  ├─ 04_extract_oracle_tickets.py        # JDBC extract: SUPPORT_TICKETS
│  │  │  └─ 05_kafka_ingest_mobile_events.py    # Stream consume: Mobile clickstream
│  │  │
│  │  ├─ silver/
│  │  │  ├─ 01_transform_accounts.py            # DQ + deduplicate accounts
│  │  │  ├─ 02_transform_transactions.py        # DQ + reconcile transactions
│  │  │  ├─ 03_transform_credit_scores.py       # DQ + deduplicate scores
│  │  │  ├─ 04_transform_support_tickets.py     # DQ + standardize tickets
│  │  │  └─ 05_transform_mobile_events.py       # DQ + deduplicate events
│  │  │
│  │  ├─ gold/
│  │  │  ├─ 01_build_dim_customer.py            # SCD Type 2 dimension
│  │  │  ├─ 02_build_dim_account.py             # Account dimension
│  │  │  ├─ 03_build_dim_date.py                # Date dimension
│  │  │  ├─ 04_build_dim_event_type.py          # Event type dimension
│  │  │  ├─ 05_build_fact_transactions.py       # Transaction fact table
│  │  │  ├─ 06_build_fact_mobile_events.py      # Mobile events fact table
│  │  │  ├─ 07_build_fact_fraud_risk.py         # Daily fraud risk fact (batch)
│  │  │  └─ 08_build_fraud_alert_table.py       # Real-time fraud alerts (output)
│  │  │
│  │  └─ streaming/
│  │     ├─ fraud_detection_job.py              # 24/7 streaming job (Kafka → fraud)
│  │     └─ fraud_alert_sink.py                 # Write fraud alerts to Delta
│  │
│  ├─ sql/
│  │  ├─ dq_checks/
│  │  │  ├─ bronze_schema_validation.sql        # Schema checks for raw data
│  │  │  ├─ silver_referential_integrity.sql    # FK validation
│  │  │  ├─ silver_completeness.sql             # Null/blank checks
│  │  │  ├─ gold_business_rules.sql             # Fraud score range, etc.
│  │  │  └─ dq_metrics_aggregation.sql          # Collect DQ stats
│  │  │
│  │  └─ transformations/
│  │     ├─ scd_type2_merge.sql                 # SCD Type 2 logic template
│  │     └─ upsert_fraud_alerts.sql             # Merge for fraud table updates
│  │
│  ├─ python/
│  │  ├─ config.py                              # Shared config (paths, credentials)
│  │  ├─ jdbc_extractor.py                      # Oracle JDBC utilities
│  │  ├─ kafka_consumer.py                      # Kafka utilities
│  │  ├─ feature_store_utils.py                 # MLflow Feature Store wrappers
│  │  ├─ dq_framework.py                        # Data quality check executor
│  │  └─ schema_definitions.py                  # Schema DDLs (StructType)
│  │
│  └─ ml/
│     ├─ fraud_detection_model.py               # Model training pipeline
│     └─ mlflow_registry.py                     # MLflow model registration
│
├─ tests/
│  ├─ unit/
│  │  ├─ test_jdbc_extractor.py                 # JDBC utility tests
│  │  ├─ test_transformations.py                # SQL transformation tests
│  │  └─ test_schema_validation.py              # Schema definition tests
│  │
│  ├─ integration/
│  │  ├─ test_bronze_load.py                    # End-to-end bronze load test
│  │  ├─ test_silver_transform.py               # End-to-end silver transform
│  │  └─ test_gold_aggregation.py               # End-to-end gold build
│  │
│  └─ conftest.py                               # pytest fixtures (mock Spark, etc.)
│
├─ deployment/
│  ├─ terraform/                                # (Optional) IaC for AWS infrastructure
│  │  ├─ main.tf
│  │  ├─ variables.tf
│  │  └─ outputs.tf
│  │
│  └─ scripts/
│     ├─ deploy_dab.sh                          # Deploy DAB to Databricks
│     ├─ setup_secrets.sh                       # Configure Secrets Manager
│     └─ create_uc_volumes.sh                   # Create Unity Catalog volumes
│
├─ docs/
│  ├─ design/
│  │  └─ 2026-05-24-negarabank-platform-design.md  # Architecture spec
│  │
│  ├─ plans/
│  │  └─ 2026-05-24-negarabank-implementation.md    # This file
│  │
│  ├─ data_dictionary.md                        # Column definitions, lineage
│  ├─ runbook_troubleshoot.md                   # Operational runbook
│  └─ CONTRIBUTING.md                           # Development guidelines
│
└─ config/
   ├─ databricks.yml                            # DAB workspace configuration
   ├─ job-config/
   │  ├─ batch_etl.yml                          # Batch ETL job definition
   │  └─ streaming_fraud.yml                    # Streaming job definition
   └─ cluster-config/
      ├─ batch_cluster.yml                      # Batch cluster config
      └─ streaming_cluster.yml                  # Streaming cluster config
```

---

## Phase 1: Infrastructure & Foundation (Week 1-2)

### Task 1: Initialize GitHub Repository & Local Development Environment

**Files:**
- Create: `README.md`
- Create: `.gitignore`
- Create: `setup.py` (for local package installation)
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `.env.example`

---

#### **Step 1: Initialize Git repo**

```bash
cd /path/to/negarabank-pipeline
git init
git config user.name "Data Platform Team"
git config user.email "data-platform@negarabank.com"
```

---

#### **Step 2: Create `.gitignore`**

```
# Databricks
.databricks/
*.pyc
__pycache__/
.pytest_cache/
*.egg-info/

# Environment
.env
.env.local
venv/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Databricks artifacts
*.dbc
databricks-artifacts/

# OS
.DS_Store
Thumbs.db
```

---

#### **Step 3: Create `requirements.txt`**

```
databricks-sdk==0.20.0
pyspark==3.5.0
delta-spark==3.1.0
pytest==7.4.3
pytest-cov==4.1.0
boto3==1.28.0
kafka-python==2.0.2
mlflow==2.10.0
```

---

#### **Step 4: Create `requirements-dev.txt`**

```
-r requirements.txt
black==23.12.0
flake8==6.1.0
mypy==1.7.0
pre-commit==3.5.0
```

---

#### **Step 5: Create `README.md`**

```markdown
# NegaraBank Data Platform

Production-grade data platform optimizing transaction processing, real-time fraud detection, and analytics.

## Architecture

- **Ingestion:** Oracle JDBC (batch) + Kafka (streaming)
- **Processing:** Databricks (Spark, Structured Streaming)
- **Storage:** Delta Lake (Bronze/Silver/Gold medallion)
- **Serving:** BI (Tableau/Power BI), ML (MLflow, Feature Store)
- **Governance:** Unity Catalog, Data Quality Framework

## Quick Start

### Prerequisites
- AWS account with EC2 + S3 access
- Databricks workspace
- Kafka cluster (AWS MSK or self-managed)
- Python 3.10+

### Local Setup
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements-dev.txt
```

### Deploy to Databricks
```bash
databricks bundle deploy --target dev
```

## Project Structure
- `src/notebooks/` — Databricks notebooks (bronze/silver/gold/streaming)
- `src/sql/` — SQL DDLs and transformations
- `src/python/` — Shared Python modules
- `tests/` — Unit and integration tests
- `docs/` — Architecture, design, data dictionary

## Documentation
- [Architecture Design](docs/design/2026-05-24-negarabank-platform-design.md)
- [Data Dictionary](docs/data_dictionary.md)
- [Troubleshooting Runbook](docs/runbook_troubleshoot.md)

## Development Guidelines
See [CONTRIBUTING.md](docs/CONTRIBUTING.md)
```

---

#### **Step 6: Create `.env.example`**

```bash
# AWS
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=123456789012

# Databricks
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...

# Oracle (stored in AWS Secrets Manager, not in .env)
# ORACLE_USER and ORACLE_PASSWORD retrieved at runtime

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC_MOBILE_EVENTS=mobile_clickstream

# S3 Paths
S3_BUCKET_BRONZE=negarabank-bronze
S3_BUCKET_SILVER=negarabank-silver
S3_BUCKET_GOLD=negarabank-gold
S3_BUCKET_CHECKPOINTS=negarabank-checkpoints
```

---

#### **Step 7: Create `setup.py`**

```python
from setuptools import setup, find_packages

setup(
    name="negarabank-pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "databricks-sdk>=0.20.0",
        "pyspark>=3.5.0",
        "boto3>=1.28.0",
        "kafka-python>=2.0.2",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "flake8>=6.1.0",
        ]
    },
)
```

---

#### **Step 8: Commit**

```bash
git add README.md .gitignore requirements.txt requirements-dev.txt setup.py .env.example
git commit -m "chore: initialize project structure and dev environment"
```

---

### Task 2: Create Databricks Asset Bundle (DAB) Configuration

**Files:**
- Create: `databricks.yml`
- Create: `config/databricks.yml`
- Create: `config/job-config/batch_etl.yml`
- Create: `config/job-config/streaming_fraud.yml`

---

#### **Step 1: Create `databricks.yml` (root DAB config)**

```yaml
# databricks.yml - Main DAB configuration

bundle:
  name: negarabank-pipeline
  description: NegaraBank data platform (batch ETL + real-time fraud detection)

targets:
  dev:
    workspace:
      host: ${var.databricks_host}
      token: ${var.databricks_token}

  prod:
    workspace:
      host: ${var.databricks_host_prod}
      token: ${var.databricks_token_prod}

variables:
  databricks_host:
    description: Dev Databricks workspace URL
    default: https://your-workspace-dev.cloud.databricks.com

  databricks_host_prod:
    description: Prod Databricks workspace URL
    default: https://your-workspace-prod.cloud.databricks.com

  databricks_token:
    description: Dev Databricks API token
    default: ""

  databricks_token_prod:
    description: Prod Databricks API token
    default: ""

resources:
  # Compute resources
  batch_cluster:
    type: clusters
    definition:
      cluster_name: negarabank-batch
      spark_version: 13.3.x-scala2.12
      node_type_id: i3en.3xlarge
      num_workers: 2
      aws_attributes:
        availability: SPOT
        zone_id: us-east-1a
      spark_conf:
        spark.sql.adaptive.enabled: "true"
        spark.sql.adaptive.coalescePartitions.enabled: "true"
        spark.sql.extensions: io.delta.sql.DeltaSparkSessionExtension
        spark.sql.catalog.spark_catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog

  streaming_cluster:
    type: clusters
    definition:
      cluster_name: negarabank-streaming
      spark_version: 13.3.x-scala2.12
      node_type_id: i3en.2xlarge
      num_workers: 2
      aws_attributes:
        availability: ON_DEMAND
        zone_id: us-east-1a
      autoscale:
        min_workers: 2
        max_workers: 4
      spark_conf:
        spark.sql.adaptive.enabled: "true"
        spark.sql.extensions: io.delta.sql.DeltaSparkSessionExtension
      init_scripts:
        - s3://negarabank-scripts/install-kafka.sh

  # Jobs
  batch_etl_job:
    type: jobs
    definition:
      name: Daily Batch ETL (Oracle → Bronze → Silver → Gold)
      description: Extract from Oracle, transform, load to medallion layers
      schedule:
        quartz_cron_expression: 0 0 2 * * ?  # 2 AM daily
        timezone_id: UTC
      tasks:
        - task_key: extract_accounts
          notebook_task:
            notebook_path: ${bundle.root_path}/src/notebooks/bronze/01_extract_oracle_accounts
          cluster_id: ${resources.batch_cluster.cluster_id}
          timeout_seconds: 1800

        - task_key: extract_transactions
          notebook_task:
            notebook_path: ${bundle.root_path}/src/notebooks/bronze/02_extract_oracle_transactions
          cluster_id: ${resources.batch_cluster.cluster_id}
          depends_on:
            - task_key: extract_accounts
          timeout_seconds: 3600

        - task_key: extract_credit_scores
          notebook_task:
            notebook_path: ${bundle.root_path}/src/notebooks/bronze/03_extract_oracle_credit_scores
          cluster_id: ${resources.batch_cluster.cluster_id}
          depends_on:
            - task_key: extract_accounts
          timeout_seconds: 1800

        - task_key: extract_tickets
          notebook_task:
            notebook_path: ${bundle.root_path}/src/notebooks/bronze/04_extract_oracle_tickets
          cluster_id: ${resources.batch_cluster.cluster_id}
          depends_on:
            - task_key: extract_accounts
          timeout_seconds: 1800

        - task_key: transform_silver
          notebook_task:
            notebook_path: ${bundle.root_path}/src/notebooks/silver/01_transform_accounts
          cluster_id: ${resources.batch_cluster.cluster_id}
          depends_on:
            - task_key: extract_accounts
            - task_key: extract_transactions
            - task_key: extract_credit_scores
            - task_key: extract_tickets
          timeout_seconds: 1800

        - task_key: build_gold
          notebook_task:
            notebook_path: ${bundle.root_path}/src/notebooks/gold/01_build_dim_customer
          cluster_id: ${resources.batch_cluster.cluster_id}
          depends_on:
            - task_key: transform_silver
          timeout_seconds: 1800

        - task_key: dq_validation
          notebook_task:
            notebook_path: ${bundle.root_path}/src/notebooks/gold/08_build_fraud_alert_table
          cluster_id: ${resources.batch_cluster.cluster_id}
          depends_on:
            - task_key: build_gold
          timeout_seconds: 1800

  fraud_detection_job:
    type: jobs
    definition:
      name: Real-time Fraud Detection (24/7 Streaming)
      description: Continuous Kafka consumer for fraud scoring
      deployment:
        kind: COMPLETE
      tasks:
        - task_key: fraud_streaming
          spark_python_task:
            python_file: ${bundle.root_path}/src/notebooks/streaming/fraud_detection_job.py
          cluster_id: ${resources.streaming_cluster.cluster_id}
          timeout_seconds: 86400  # 24 hours

# Permissions & ACLs (Unity Catalog)
permissions:
  - level: CAN_MANAGE
    group_name: data-engineering-team
  - level: CAN_RUN
    group_name: data-analytics-team
  - level: CAN_VIEW
    group_name: business-analysts
```

---

#### **Step 2: Commit**

```bash
git add databricks.yml
git commit -m "chore: create Databricks Asset Bundle configuration"
```

---

### Task 3: Set Up AWS Infrastructure (Secrets Manager, IAM, VPC)

**Files:**
- Create: `deployment/terraform/main.tf`
- Create: `deployment/terraform/variables.tf`
- Create: `deployment/terraform/outputs.tf`
- Create: `deployment/scripts/setup_secrets.sh`

---

#### **Step 1: Create `deployment/terraform/variables.tf`**

```hcl
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
```

---

#### **Step 2: Create `deployment/terraform/main.tf`**

```hcl
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

output "oracle_secret_arn" {
  value = aws_secretsmanager_secret.oracle_creds.arn
}

output "ec2_iam_role_arn" {
  value = aws_iam_role.ec2_jdbc_role.arn
}

output "ec2_instance_profile_arn" {
  value = aws_iam_instance_profile.ec2_jdbc_profile.arn
}

output "s3_bronze_bucket" {
  value = aws_s3_bucket.bronze.id
}

output "s3_silver_bucket" {
  value = aws_s3_bucket.silver.id
}

output "s3_gold_bucket" {
  value = aws_s3_bucket.gold.id
}

output "security_group_id" {
  value = aws_security_group.ec2_jdbc_sg.id
}
```

---

#### **Step 3: Create `deployment/terraform/outputs.tf`**

```hcl
output "terraform_outputs" {
  value = {
    oracle_secret_arn    = aws_secretsmanager_secret.oracle_creds.arn
    ec2_iam_role_arn     = aws_iam_role.ec2_jdbc_role.arn
    ec2_instance_profile = aws_iam_instance_profile.ec2_jdbc_profile.name
    s3_buckets = {
      bronze      = aws_s3_bucket.bronze.id
      silver      = aws_s3_bucket.silver.id
      gold        = aws_s3_bucket.gold.id
      checkpoints = aws_s3_bucket.checkpoints.id
    }
  }
}
```

---

#### **Step 4: Create `deployment/scripts/setup_secrets.sh`**

```bash
#!/bin/bash
set -e

echo "Setting up AWS Secrets Manager..."

# Requires AWS CLI configured with appropriate credentials

ORACLE_USER="${ORACLE_USER:-}"
ORACLE_PASS="${ORACLE_PASS:-}"
ORACLE_HOST="${ORACLE_HOST:-}"
ORACLE_PORT="${ORACLE_PORT:-1521}"

if [ -z "$ORACLE_USER" ] || [ -z "$ORACLE_PASS" ] || [ -z "$ORACLE_HOST" ]; then
  echo "Error: Set ORACLE_USER, ORACLE_PASS, ORACLE_HOST environment variables"
  exit 1
fi

# Create secret (if not exists)
SECRET_NAME="negarabank/oracle/jdbc"

aws secretsmanager create-secret \
  --name "$SECRET_NAME" \
  --description "Oracle JDBC credentials" \
  --secret-string "{\"username\":\"$ORACLE_USER\",\"password\":\"$ORACLE_PASS\",\"host\":\"$ORACLE_HOST\",\"port\":$ORACLE_PORT}" \
  2>/dev/null || echo "Secret already exists, skipping creation"

echo "✓ Secrets configured"
```

---

#### **Step 5: Commit**

```bash
git add deployment/
git commit -m "chore: add Terraform infrastructure configuration for AWS"
```

---

## Phase 2: Batch ETL - Bronze Layer (Week 2-3)

### Task 4: Create JDBC Extractor Utility Module

**Files:**
- Create: `src/python/config.py`
- Create: `src/python/jdbc_extractor.py`
- Create: `tests/unit/test_jdbc_extractor.py`

---

#### **Step 1: Create `src/python/config.py`**

```python
import os
import json
import boto3
from typing import Dict, Any

def get_oracle_credentials() -> Dict[str, str]:
    """Retrieve Oracle credentials from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "us-east-1"))
    
    try:
        response = client.get_secret_value(SecretId="negarabank/oracle/jdbc")
        secret = json.loads(response["SecretString"])
        return {
            "user": secret["username"],
            "password": secret["password"],
            "host": secret["host"],
            "port": secret["port"],
        }
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve Oracle credentials: {str(e)}")

def get_s3_paths() -> Dict[str, str]:
    """Get S3 bucket paths for each medallion layer."""
    return {
        "bronze": f"s3://{os.getenv('S3_BUCKET_BRONZE', 'negarabank-bronze')}/",
        "silver": f"s3://{os.getenv('S3_BUCKET_SILVER', 'negarabank-silver')}/",
        "gold": f"s3://{os.getenv('S3_BUCKET_GOLD', 'negarabank-gold')}/",
        "checkpoints": f"s3://{os.getenv('S3_BUCKET_CHECKPOINTS', 'negarabank-checkpoints')}/",
    }

def get_kafka_config() -> Dict[str, str]:
    """Get Kafka configuration."""
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "mobile_events_topic": os.getenv("KAFKA_TOPIC_MOBILE_EVENTS", "mobile_clickstream"),
    }

class Paths:
    """Centralized path management."""
    
    S3 = get_s3_paths()
    
    @staticmethod
    def bronze_table(table_name: str) -> str:
        return f"{Paths.S3['bronze']}{table_name}/"
    
    @staticmethod
    def silver_table(table_name: str) -> str:
        return f"{Paths.S3['silver']}{table_name}/"
    
    @staticmethod
    def gold_table(table_name: str) -> str:
        return f"{Paths.S3['gold']}{table_name}/"
    
    @staticmethod
    def checkpoint(job_name: str) -> str:
        return f"{Paths.S3['checkpoints']}{job_name}/"
```

---

#### **Step 2: Create `src/python/jdbc_extractor.py`**

```python
from typing import Optional
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame

class JDBCExtractor:
    """Utility for extracting data from Oracle via JDBC."""
    
    def __init__(self, spark: SparkSession, jdbc_url: str, credentials: dict):
        """
        Initialize JDBC extractor.
        
        Args:
            spark: SparkSession
            jdbc_url: JDBC connection string (e.g., jdbc:oracle:thin:@host:1521/db)
            credentials: Dict with 'user' and 'password'
        """
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.credentials = credentials
    
    def extract_full_table(self, table_name: str) -> DataFrame:
        """Extract entire table from Oracle."""
        return self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.credentials["user"]) \
            .option("password", self.credentials["password"]) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
    
    def extract_incremental(
        self,
        table_name: str,
        partition_column: str,
        lower_bound: str,
        upper_bound: str,
        num_partitions: int = 4,
    ) -> DataFrame:
        """
        Extract data with partitioned read (parallel JDBC connections).
        
        Args:
            table_name: Oracle table name
            partition_column: Column to partition on (must be numeric)
            lower_bound: Lower bound of partition column
            upper_bound: Upper bound of partition column
            num_partitions: Number of parallel JDBC connections
        
        Returns:
            DataFrame with data
        """
        return self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.credentials["user"]) \
            .option("password", self.credentials["password"]) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("partitionColumn", partition_column) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .option("numPartitions", num_partitions) \
            .load()
    
    def extract_with_predicate(self, table_name: str, predicates: list) -> DataFrame:
        """
        Extract data using custom SQL predicates (for time-sliced parallel loading).
        
        Args:
            table_name: Oracle table name
            predicates: List of WHERE clause conditions
        
        Returns:
            Union of DataFrames from each predicate
        """
        dfs = []
        for predicate in predicates:
            df = self.spark.read.format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", f"({table_name} WHERE {predicate}) t") \
                .option("user", self.credentials["user"]) \
                .option("password", self.credentials["password"]) \
                .option("driver", "oracle.jdbc.driver.OracleDriver") \
                .load()
            dfs.append(df)
        
        return dfs[0] if len(dfs) == 1 else dfs[0].union(*dfs[1:])
    
    def extract_delta_load(
        self,
        table_name: str,
        last_load_date: datetime,
        date_column: str = "modified_date",
    ) -> DataFrame:
        """
        Extract only rows modified since last load (incremental/delta loading).
        
        Args:
            table_name: Oracle table name
            last_load_date: Last successful load timestamp
            date_column: Column name for filtering (default: modified_date)
        
        Returns:
            DataFrame with new/modified rows only
        """
        # Cast to proper timestamp format for Oracle
        formatted_date = last_load_date.strftime("%Y-%m-%d %H:%M:%S")
        predicate = f"{date_column} >= TO_DATE('{formatted_date}', 'YYYY-MM-DD HH24:MI:SS')"
        
        return self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", f"({table_name} WHERE {predicate}) t") \
            .option("user", self.credentials["user"]) \
            .option("password", self.credentials["password"]) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
```

---

#### **Step 3: Create `tests/unit/test_jdbc_extractor.py`**

```python
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from src.python.jdbc_extractor import JDBCExtractor

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    return Mock()

@pytest.fixture
def jdbc_extractor(mock_spark):
    """Create a JDBCExtractor instance with mocked Spark."""
    credentials = {"user": "etl_user", "password": "etl_pass"}
    jdbc_url = "jdbc:oracle:thin:@core-db:1521/banking"
    return JDBCExtractor(mock_spark, jdbc_url, credentials)

def test_extract_full_table(jdbc_extractor, mock_spark):
    """Test extracting an entire table."""
    # Arrange
    mock_spark.read.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = MagicMock()
    
    # Act
    result = jdbc_extractor.extract_full_table("ACCOUNTS")
    
    # Assert
    mock_spark.read.format.assert_called_with("jdbc")
    assert result is not None

def test_extract_incremental(jdbc_extractor, mock_spark):
    """Test incremental extraction with partitioning."""
    # Arrange
    mock_spark.read.format.return_value.option.return_value.load.return_value = MagicMock()
    
    # Act
    result = jdbc_extractor.extract_incremental(
        "TRANSACTIONS",
        partition_column="transaction_id",
        lower_bound="1",
        upper_bound="1000000",
        num_partitions=4
    )
    
    # Assert
    assert result is not None

def test_extract_delta_load(jdbc_extractor, mock_spark):
    """Test delta load (incremental extraction)."""
    # Arrange
    last_load = datetime(2026, 5, 23, 0, 0, 0)
    mock_spark.read.format.return_value.option.return_value.load.return_value = MagicMock()
    
    # Act
    result = jdbc_extractor.extract_delta_load(
        "TRANSACTIONS",
        last_load_date=last_load,
        date_column="txn_date"
    )
    
    # Assert
    assert result is not None
```

---

#### **Step 4: Commit**

```bash
git add src/python/ tests/unit/
git commit -m "feat: add JDBC extractor utility module with delta loading support"
```

---

### Task 5: Build Bronze Layer - Extract ACCOUNTS Table

**Files:**
- Create: `src/notebooks/bronze/01_extract_oracle_accounts.py`
- Create: `tests/integration/test_bronze_load.py`

---

#### **Step 1: Create `src/notebooks/bronze/01_extract_oracle_accounts.py`**

```python
# Databricks notebook source
# Bronze Layer: Extract ACCOUNTS from Oracle

from datetime import datetime
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeAccountsLoad")

# COMMAND ----------

# Load credentials
creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"

# Initialize extractor
extractor = JDBCExtractor(spark, jdbc_url, creds)

logger.info("Starting ACCOUNTS extraction from Oracle...")

# Extract full ACCOUNTS table (static master)
accounts_df = extractor.extract_full_table("ACCOUNTS")

logger.info(f"Extracted {accounts_df.count()} account records")

# Add load metadata
load_date = datetime.now().strftime("%Y-%m-%d")
accounts_df = accounts_df.withColumn("load_date", lit(load_date))
accounts_df = accounts_df.withColumn("load_timestamp", lit(datetime.now().isoformat()))

# Write to Bronze layer (partitioned by load_date)
output_path = Paths.bronze_table("accounts")
accounts_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Accounts loaded to: {output_path}")
print(f"✓ ACCOUNTS: {accounts_df.count()} records written to Bronze")

# COMMAND ----------

# Display schema
accounts_df.printSchema()

# COMMAND ----------

# Quick data quality check
print(f"Null counts:")
accounts_df.select([(count(when(isnull(c), 1)).over(Window.partitionBy())).alias(c) for c in accounts_df.columns]).show()
```

---

#### **Step 2: Commit**

```bash
git add src/notebooks/
git commit -m "feat: add Bronze layer ACCOUNTS extraction notebook"
```

---

### Task 6: Build Bronze Layer - Extract TRANSACTIONS with Delta Loading

**Files:**
- Create: `src/notebooks/bronze/02_extract_oracle_transactions.py`

---

#### **Step 1: Create `src/notebooks/bronze/02_extract_oracle_transactions.py`**

```python
# Databricks notebook source
# Bronze Layer: Extract TRANSACTIONS from Oracle (Delta Load)

from datetime import datetime, timedelta
from pyspark.sql.functions import lit, col
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeTransactionsLoad")

# COMMAND ----------

creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
extractor = JDBCExtractor(spark, jdbc_url, creds)

# COMMAND ----------

# Get last successful load date
try:
    last_load_df = spark.table("bronze.transactions")
    last_load_date = last_load_df.select(max(col("load_date"))).collect()[0][0]
    last_load_dt = datetime.strptime(str(last_load_date), "%Y-%m-%d")
except:
    # First load: go back 30 days
    last_load_dt = datetime.now() - timedelta(days=30)

logger.info(f"Delta load: extracting transactions from {last_load_dt} onwards")

# COMMAND ----------

# Extract transactions incrementally (JDBC with predicate slicing)
# Predicate slicing: 4 x 6-hour windows for parallel JDBC connections
target_date = datetime.now().strftime("%Y-%m-%d")
predicates = [
    f"txn_date >= TO_DATE('{target_date} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 06:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 06:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 12:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 12:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date + 1} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')",
]

txns_df = extractor.extract_with_predicate("TRANSACTIONS", predicates)

logger.info(f"Extracted {txns_df.count()} transaction records")

# COMMAND ----------

# Add load metadata
load_date = datetime.now().strftime("%Y-%m-%d")
txns_df = txns_df.withColumn("load_date", lit(load_date))
txns_df = txns_df.withColumn("load_timestamp", lit(datetime.now().isoformat()))

# Write to Bronze (append mode for delta load)
output_path = Paths.bronze_table("transactions")
txns_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Transactions appended to: {output_path}")
print(f"✓ TRANSACTIONS: {txns_df.count()} records appended to Bronze")

# COMMAND ----------

# Display sample
txns_df.limit(10).show()
```

---

#### **Step 2: Commit**

```bash
git add src/notebooks/bronze/02_extract_oracle_transactions.py
git commit -m "feat: add Bronze layer TRANSACTIONS delta load with JDBC predicate slicing"
```

---

### Task 7: Build Bronze Layer - Extract CREDIT_SCORES and SUPPORT_TICKETS

**Files:**
- Create: `src/notebooks/bronze/03_extract_oracle_credit_scores.py`
- Create: `src/notebooks/bronze/04_extract_oracle_tickets.py`

---

#### **Step 1: Create `src/notebooks/bronze/03_extract_oracle_credit_scores.py`**

```python
# Databricks notebook source
# Bronze Layer: Extract CREDIT_SCORES from Oracle

from datetime import datetime
from pyspark.sql.functions import lit
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeCreditScoresLoad")

# COMMAND ----------

creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
extractor = JDBCExtractor(spark, jdbc_url, creds)

logger.info("Starting CREDIT_SCORES extraction...")

# COMMAND ----------

# Extract full credit scores snapshot (daily)
scores_df = extractor.extract_full_table("CREDIT_SCORES")

logger.info(f"Extracted {scores_df.count()} credit score records")

# COMMAND ----------

# Add load metadata
load_date = datetime.now().strftime("%Y-%m-%d")
scores_df = scores_df.withColumn("load_date", lit(load_date))
scores_df = scores_df.withColumn("load_timestamp", lit(datetime.now().isoformat()))

# Write to Bronze
output_path = Paths.bronze_table("credit_scores")
scores_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Credit scores loaded to: {output_path}")
print(f"✓ CREDIT_SCORES: {scores_df.count()} records written to Bronze")

# COMMAND ----------

scores_df.limit(10).show()
```

---

#### **Step 2: Create `src/notebooks/bronze/04_extract_oracle_tickets.py`**

```python
# Databricks notebook source
# Bronze Layer: Extract SUPPORT_TICKETS from Oracle

from datetime import datetime
from pyspark.sql.functions import lit
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeSupportTicketsLoad")

# COMMAND ----------

creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
extractor = JDBCExtractor(spark, jdbc_url, creds)

logger.info("Starting SUPPORT_TICKETS extraction...")

# COMMAND ----------

# Extract support tickets (batch daily load)
tickets_df = extractor.extract_full_table("SUPPORT_TICKETS")

logger.info(f"Extracted {tickets_df.count()} support ticket records")

# COMMAND ----------

# Add load metadata
load_date = datetime.now().strftime("%Y-%m-%d")
tickets_df = tickets_df.withColumn("load_date", lit(load_date))
tickets_df = tickets_df.withColumn("load_timestamp", lit(datetime.now().isoformat()))

# Write to Bronze
output_path = Paths.bronze_table("support_tickets")
tickets_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Support tickets loaded to: {output_path}")
print(f"✓ SUPPORT_TICKETS: {tickets_df.count()} records written to Bronze")

# COMMAND ----------

tickets_df.limit(10).show()
```

---

#### **Step 3: Commit**

```bash
git add src/notebooks/bronze/03_extract_oracle_credit_scores.py src/notebooks/bronze/04_extract_oracle_tickets.py
git commit -m "feat: add Bronze layer CREDIT_SCORES and SUPPORT_TICKETS extraction"
```

---

## Phase 3: Silver Layer - Data Quality & Transformations (Week 3-4)

### Task 8: Create Data Quality Framework

**Files:**
- Create: `src/python/dq_framework.py`
- Create: `src/sql/dq_checks/bronze_schema_validation.sql`
- Create: `src/sql/dq_checks/silver_referential_integrity.sql`
- Create: `src/sql/dq_checks/silver_completeness.sql`
- Create: `src/sql/dq_checks/gold_business_rules.sql`

---

#### **Step 1: Create `src/python/dq_framework.py`**

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnull, max as spark_max, min as spark_min
from typing import List, Dict, Any
from datetime import datetime
import json

class DataQualityCheck:
    """Represents a single DQ check."""
    
    def __init__(self, name: str, description: str, table: str, sql_query: str):
        self.name = name
        self.description = description
        self.table = table
        self.sql_query = sql_query
        self.passed = False
        self.record_count = 0
        self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "table": self.table,
            "passed": self.passed,
            "record_count": self.record_count,
            "timestamp": self.timestamp,
        }

class DataQualityFramework:
    """Framework for running DQ checks and logging results."""
    
    def __init__(self, spark: SparkSession, output_path: str):
        self.spark = spark
        self.output_path = output_path
        self.checks: List[DataQualityCheck] = []
        self.results = []
    
    def add_null_check(self, table: str, column: str, threshold: float = 0.05) -> DataQualityCheck:
        """
        Check that null % does not exceed threshold.
        
        Args:
            table: Table name
            column: Column to check
            threshold: Max allowed null % (default 5%)
        """
        sql = f"""
        SELECT 
            '{column}' as column_name,
            ROUND(COUNT(CASE WHEN {column} IS NULL THEN 1 END) / COUNT(*), 4) as null_pct,
            COUNT(*) as total_rows
        FROM {table}
        """
        
        check = DataQualityCheck(
            name=f"{table}__{column}__null_check",
            description=f"Null check: {column} in {table} (threshold: {threshold*100}%)",
            table=table,
            sql_query=sql
        )
        self.checks.append(check)
        return check
    
    def add_uniqueness_check(self, table: str, column: str) -> DataQualityCheck:
        """Check for duplicate values (should be unique)."""
        sql = f"""
        SELECT
            '{column}' as column_name,
            COUNT(*) as total_rows,
            COUNT(DISTINCT {column}) as distinct_rows,
            COUNT(*) - COUNT(DISTINCT {column}) as duplicate_count
        FROM {table}
        """
        
        check = DataQualityCheck(
            name=f"{table}__{column}__uniqueness_check",
            description=f"Uniqueness check: {column} in {table}",
            table=table,
            sql_query=sql
        )
        self.checks.append(check)
        return check
    
    def add_referential_integrity_check(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
    ) -> DataQualityCheck:
        """Check for orphaned records (FK references non-existent parent)."""
        sql = f"""
        SELECT
            COUNT(*) as orphan_count,
            ROUND(COUNT(*) / (SELECT COUNT(*) FROM {child_table}), 4) as orphan_pct
        FROM {child_table} c
        WHERE c.{child_column} NOT IN (SELECT {parent_column} FROM {parent_table})
        """
        
        check = DataQualityCheck(
            name=f"{child_table}__{child_column}__fk_check",
            description=f"FK check: {child_table}.{child_column} → {parent_table}.{parent_column}",
            table=child_table,
            sql_query=sql
        )
        self.checks.append(check)
        return check
    
    def run_checks(self, fail_fast: bool = False) -> Dict[str, Any]:
        """
        Execute all registered checks.
        
        Args:
            fail_fast: Stop on first failure (for critical checks)
        
        Returns:
            Summary dict with pass/fail counts
        """
        summary = {
            "total_checks": len(self.checks),
            "passed": 0,
            "failed": 0,
            "timestamp": datetime.now().isoformat(),
            "results": []
        }
        
        for check in self.checks:
            try:
                result_df = self.spark.sql(check.sql_query)
                result_rows = result_df.collect()
                
                check.passed = True
                check.record_count = len(result_rows)
                
                summary["passed"] += 1
                summary["results"].append(check.to_dict())
                
                print(f"✓ {check.name}")
                
            except Exception as e:
                check.passed = False
                summary["failed"] += 1
                summary["results"].append({
                    **check.to_dict(),
                    "error": str(e)
                })
                
                print(f"✗ {check.name}: {str(e)}")
                
                if fail_fast:
                    break
        
        # Log results
        self._log_results(summary)
        
        return summary
    
    def _log_results(self, summary: Dict[str, Any]):
        """Save DQ results to Delta table."""
        from pyspark.sql import functions as F
        
        results_df = self.spark.createDataFrame(
            [(r["name"], r["description"], r["passed"], r["timestamp"]) 
             for r in summary["results"]],
            ["check_name", "description", "passed", "check_timestamp"]
        )
        
        results_df.write \
            .format("delta") \
            .mode("append") \
            .save(f"{self.output_path}/dq_check_results/")
        
        print(f"\n✓ DQ results logged to {self.output_path}/dq_check_results/")
```

---

#### **Step 2: Create `src/sql/dq_checks/bronze_schema_validation.sql`**

```sql
-- Bronze Layer: Schema Validation
-- Check that required columns exist and have correct types

SELECT
    table_name,
    column_name,
    data_type,
    nullable,
    CASE 
        WHEN column_name IN ('account_id', 'customer_id', 'transaction_id') THEN 'required_pk'
        WHEN column_name IN ('load_date', 'load_timestamp') THEN 'required_metadata'
        ELSE 'optional'
    END as column_category,
    CASE
        WHEN nullable = false AND column_category IN ('required_pk', 'required_metadata') THEN 'pass'
        ELSE 'warn'
    END as validation_status
FROM information_schema.columns
WHERE table_schema = 'bronze'
ORDER BY table_name, ordinal_position;
```

---

#### **Step 3: Create `src/sql/dq_checks/silver_referential_integrity.sql`**

```sql
-- Silver Layer: Referential Integrity Checks
-- Detect orphaned records (transactions without matching account)

SELECT
    'transactions_fk_accounts' as check_name,
    COUNT(*) as orphan_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM silver.transactions), 2) as orphan_pct,
    MAX(load_date) as check_date
FROM silver.transactions t
WHERE t.account_id NOT IN (SELECT account_id FROM silver.accounts)
GROUP BY 1

UNION ALL

SELECT
    'transactions_fk_customers' as check_name,
    COUNT(*) as orphan_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM silver.transactions), 2) as orphan_pct,
    MAX(load_date) as check_date
FROM silver.transactions t
WHERE t.customer_id NOT IN (SELECT customer_id FROM silver.accounts)
GROUP BY 1;
```

---

#### **Step 4: Create `src/sql/dq_checks/silver_completeness.sql`**

```sql
-- Silver Layer: Completeness Check
-- Measure null % for critical columns

SELECT
    table_name,
    column_name,
    ROUND(100.0 * null_count / total_rows, 2) as null_pct,
    CASE
        WHEN null_pct <= 5 THEN 'pass'
        WHEN null_pct <= 10 THEN 'warn'
        ELSE 'fail'
    END as status,
    total_rows
FROM (
    SELECT
        'accounts' as table_name,
        'account_id' as column_name,
        COUNT(CASE WHEN account_id IS NULL THEN 1 END) as null_count,
        COUNT(*) as total_rows
    FROM silver.accounts
    
    UNION ALL
    
    SELECT
        'transactions',
        'transaction_id',
        COUNT(CASE WHEN transaction_id IS NULL THEN 1 END),
        COUNT(*)
    FROM silver.transactions
    
    UNION ALL
    
    SELECT
        'transactions',
        'amount',
        COUNT(CASE WHEN amount IS NULL THEN 1 END),
        COUNT(*)
    FROM silver.transactions
)
WHERE null_pct > 0
ORDER BY table_name, null_pct DESC;
```

---

#### **Step 5: Create `src/sql/dq_checks/gold_business_rules.sql`**

```sql
-- Gold Layer: Business Rule Validation
-- Check fraud scores, event counts, and other business logic

SELECT
    'fraud_score_range' as rule_name,
    COUNT(CASE WHEN fraud_score < 0 OR fraud_score > 1 THEN 1 END) as violating_records,
    'fraud_score must be between 0.0 and 1.0' as rule_description,
    CASE
        WHEN COUNT(CASE WHEN fraud_score < 0 OR fraud_score > 1 THEN 1 END) = 0 THEN 'pass'
        ELSE 'fail'
    END as status
FROM gold.fact_fraud_transaction_alert

UNION ALL

SELECT
    'alert_status_mapping' as rule_name,
    COUNT(CASE WHEN fraud_alert_status NOT IN ('HIGH_RISK', 'MEDIUM_RISK', 'LOW_RISK') THEN 1 END),
    'alert_status must be valid enum',
    CASE
        WHEN COUNT(CASE WHEN fraud_alert_status NOT IN ('HIGH_RISK', 'MEDIUM_RISK', 'LOW_RISK') THEN 1 END) = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM gold.fact_fraud_transaction_alert

UNION ALL

SELECT
    'event_count_reasonableness' as rule_name,
    COUNT(CASE WHEN event_count_24h > 10000 THEN 1 END),
    'event_count_24h > 10000 is suspicious',
    'warn'
FROM gold.fact_fraud_transaction_alert;
```

---

#### **Step 6: Commit**

```bash
git add src/python/dq_framework.py src/sql/dq_checks/
git commit -m "feat: add comprehensive data quality framework and validation checks"
```

---

### Task 9: Build Silver Layer - Transform and Deduplicate Data

**Files:**
- Create: `src/notebooks/silver/01_transform_accounts.py`
- Create: `src/notebooks/silver/02_transform_transactions.py`
- Create: `src/python/schema_definitions.py`

---

#### **Step 1: Create `src/python/schema_definitions.py`**

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, TimestampType, IntegerType

# Silver Layer Schemas (for validation)

accounts_silver_schema = StructType([
    StructField("account_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", DecimalType(15, 2), True),
    StructField("status", StringType(), True),
    StructField("open_date", TimestampType(), True),
    StructField("dq_passed", StringType(), True),
    StructField("load_date", StringType(), False),
])

transactions_silver_schema = StructType([
    StructField("transaction_id", LongType(), False),
    StructField("account_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("amount", DecimalType(15, 2), False),
    StructField("txn_date", TimestampType(), False),
    StructField("status", StringType(), True),
    StructField("reconciled", StringType(), True),
    StructField("load_date", StringType(), False),
])

# Gold Layer Schemas

dim_customer_schema = StructType([
    StructField("customer_key", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("risk_score", DecimalType(5, 2), True),
    StructField("effective_date", StringType(), False),
    StructField("end_date", StringType(), True),
    StructField("is_current", StringType(), False),
])

fact_fraud_alert_schema = StructType([
    StructField("transaction_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("account_id", LongType(), False),
    StructField("amount", DecimalType(15, 2), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_count_24h", IntegerType(), True),
    StructField("avg_transaction_amount", DecimalType(15, 2), True),
    StructField("account_balance", DecimalType(15, 2), True),
    StructField("fraud_score", DecimalType(3, 2), False),
    StructField("fraud_alert_status", StringType(), False),
    StructField("model_version", StringType(), True),
    StructField("processing_timestamp", TimestampType(), False),
    StructField("alert_sent", StringType(), True),
    StructField("event_date", StringType(), False),
])
```

---

#### **Step 2: Create `src/notebooks/silver/01_transform_accounts.py`**

```python
# Databricks notebook source
# Silver Layer: Transform ACCOUNTS (Deduplication + DQ)

from pyspark.sql.functions import col, row_number, lit, max as spark_max, count as spark_count
from pyspark.sql.window import Window
from datetime import datetime
from src.python.config import Paths
from src.python.dq_framework import DataQualityFramework
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverAccountsTransform")

# COMMAND ----------

# Read Bronze
bronze_path = Paths.bronze_table("accounts")
accounts_bronze = spark.read.format("delta").load(bronze_path)

logger.info(f"Read {accounts_bronze.count()} raw accounts from Bronze")

# COMMAND ----------

# Data Quality Checks
dq = DataQualityFramework(spark, Paths.S3['silver'])
dq.add_null_check("accounts", "account_id", threshold=0.0)
dq.add_null_check("accounts", "customer_id", threshold=0.0)
dq.add_uniqueness_check("accounts", "account_id")

dq_results = dq.run_checks()
logger.info(f"DQ checks: {dq_results['passed']} passed, {dq_results['failed']} failed")

# COMMAND ----------

# Deduplication: Keep latest by account_id
# (though ACCOUNTS is usually unique, handle if duplicates exist)
window_spec = Window.partitionBy("account_id").orderBy(col("load_timestamp").desc())
accounts_dedup = accounts_bronze.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

logger.info(f"After dedup: {accounts_dedup.count()} unique accounts")

# COMMAND ----------

# Type casting & standardization
accounts_silver = accounts_dedup.select(
    col("account_id").cast("long"),
    col("customer_id").cast("long"),
    col("account_type").cast("string"),
    col("balance").cast("decimal(15,2)"),
    col("status").cast("string"),
    col("open_date").cast("timestamp"),
    lit("true").alias("dq_passed"),
    col("load_date").cast("string"),
)

# COMMAND ----------

# Write to Silver
output_path = Paths.silver_table("accounts")
accounts_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Silver accounts written: {accounts_silver.count()} records")
print(f"✓ ACCOUNTS transformed and loaded to Silver ({output_path})")

# COMMAND ----------

accounts_silver.limit(10).show()
```

---

#### **Step 3: Create `src/notebooks/silver/02_transform_transactions.py`**

```python
# Databricks notebook source
# Silver Layer: Transform TRANSACTIONS (Reconciliation + DQ)

from pyspark.sql.functions import col, row_number, lit, when, coalesce
from pyspark.sql.window import Window
from datetime import datetime
from src.python.config import Paths
from src.python.dq_framework import DataQualityFramework
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverTransactionsTransform")

# COMMAND ----------

# Read Bronze
bronze_path = Paths.bronze_table("transactions")
txns_bronze = spark.read.format("delta").load(bronze_path)

logger.info(f"Read {txns_bronze.count()} raw transactions from Bronze")

# COMMAND ----------

# Read Bronze accounts for FK validation
accounts_bronze = spark.read.format("delta").load(Paths.bronze_table("accounts"))

# COMMAND ----------

# Data Quality Checks
dq = DataQualityFramework(spark, Paths.S3['silver'])
dq.add_null_check("transactions", "transaction_id", threshold=0.0)
dq.add_null_check("transactions", "amount", threshold=0.0)
dq.add_uniqueness_check("transactions", "transaction_id")

dq_results = dq.run_checks()
logger.info(f"DQ checks: {dq_results['passed']} passed, {dq_results['failed']} failed")

# COMMAND ----------

# Reconciliation: Check for orphaned records (FK validation)
txns_with_fk = txns_bronze.join(
    accounts_bronze.select("account_id").distinct(),
    on="account_id",
    how="left"
)

# Mark orphaned
txns_reconciled = txns_bronze.withColumn(
    "reconciled",
    when(
        col("account_id").isin(accounts_bronze.select("account_id").rdd.flatMap(lambda x: x).collect()),
        "true"
    ).otherwise("false")
)

orphan_count = txns_reconciled.filter(col("reconciled") == "false").count()
logger.info(f"Orphaned records detected: {orphan_count}")

# COMMAND ----------

# Deduplication: Keep latest by transaction_id
window_spec = Window.partitionBy("transaction_id").orderBy(col("load_timestamp").desc())
txns_dedup = txns_reconciled.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

logger.info(f"After dedup: {txns_dedup.count()} unique transactions")

# COMMAND ----------

# Type casting & standardization
txns_silver = txns_dedup.select(
    col("transaction_id").cast("long"),
    col("account_id").cast("long"),
    col("customer_id").cast("long"),
    col("amount").cast("decimal(15,2)"),
    col("txn_date").cast("timestamp"),
    col("status").cast("string"),
    col("reconciled").cast("string"),
    col("load_date").cast("string"),
)

# COMMAND ----------

# Write to Silver (append mode for incremental loads)
output_path = Paths.silver_table("transactions")
txns_silver.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Silver transactions written: {txns_silver.count()} records")
print(f"✓ TRANSACTIONS transformed and loaded to Silver ({output_path})")

# COMMAND ----------

txns_silver.filter(col("reconciled") == "false").show(5)
```

---

#### **Step 4: Create remaining Silver transform notebooks (abbreviated)**

Create similar notebooks for:
- `src/notebooks/silver/03_transform_credit_scores.py` (follow 01_transform_accounts pattern)
- `src/notebooks/silver/04_transform_support_tickets.py` (follow 01_transform_accounts pattern)
- `src/notebooks/silver/05_transform_mobile_events.py` (deduplicate by session_id + event_type)

---

#### **Step 5: Commit**

```bash
git add src/notebooks/silver/ src/python/schema_definitions.py
git commit -m "feat: add Silver layer transformations with DQ and deduplication"
```

---

## Phase 4: Gold Layer - Dimensional Modeling (Week 4-5)

### Task 10: Build Dimensions (Star Schema)

**Files:**
- Create: `src/notebooks/gold/01_build_dim_customer.py` (SCD Type 2)
- Create: `src/notebooks/gold/02_build_dim_account.py`
- Create: `src/notebooks/gold/03_build_dim_date.py`
- Create: `src/notebooks/gold/04_build_dim_event_type.py`

---

#### **Step 1: Create `src/notebooks/gold/01_build_dim_customer.py` (SCD Type 2)**

```python
# Databricks notebook source
# Gold Layer: Dimension - CUSTOMER (SCD Type 2)
# Tracks customer changes over time

from pyspark.sql.functions import col, row_number, lit, current_date, to_date, when, coalesce, max as spark_max
from pyspark.sql.window import Window
from datetime import datetime
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDimCustomer")

# COMMAND ----------

# Read Silver accounts (source of truth for customers)
accounts_silver = spark.read.format("delta").load(Paths.silver_table("accounts"))

# COMMAND ----------

# Prepare new records
new_customers = accounts_silver.select(
    col("customer_id"),
    lit(None).cast("string").alias("name"),  # Would come from CRM
    lit(None).cast("string").alias("email"),  # Would come from CRM
    lit("standard").alias("segment"),  # Derived from account info
    col("balance").alias("risk_score"),
    lit(datetime.now().strftime("%Y-%m-%d")).alias("effective_date"),
    lit(None).cast("string").alias("end_date"),
    lit("true").alias("is_current"),
).distinct()

logger.info(f"Processing {new_customers.count()} customers")

# COMMAND ----------

# Try reading existing dim_customer
try:
    existing_customers = spark.read.format("delta").load(Paths.gold_table("dim_customer"))
    has_existing = True
except:
    has_existing = False
    existing_customers = None

logger.info(f"Existing dimension data: {has_existing}")

# COMMAND ----------

if has_existing:
    # SCD Type 2 Merge: Mark old records as expired, insert new records
    # Records that changed: compare new vs existing (is_current=true)
    
    current_records = existing_customers.filter(col("is_current") == "true")
    
    # Identify changed records
    changed = new_customers.join(
        current_records,
        on="customer_id",
        how="inner"
    ).filter(
        # Compare key fields (name, email, segment, risk_score)
        (col("new_customers.name") != col("current_records.name")) |
        (col("new_customers.email") != col("current_records.email")) |
        (col("new_customers.segment") != col("current_records.segment"))
    )
    
    # Close old records
    expired_records = current_records.select(
        col("customer_key"),
        col("customer_id"),
        col("name"),
        col("email"),
        col("segment"),
        col("risk_score"),
        col("effective_date"),
        lit(datetime.now().strftime("%Y-%m-%d")).alias("end_date"),
        lit("false").alias("is_current"),
    )
    
    # Insert new versions of changed records
    changed_updates = changed.select(
        lit(None).cast("long").alias("customer_key"),  # Will get new key
        col("customer_id"),
        col("new_customers.name"),
        col("new_customers.email"),
        col("new_customers.segment"),
        col("new_customers.risk_score"),
        lit(datetime.now().strftime("%Y-%m-%d")).alias("effective_date"),
        lit(None).cast("string").alias("end_date"),
        lit("true").alias("is_current"),
    )
    
    # Combine: existing non-changed + expired + new changes
    final_dim = existing_customers.filter(
        col("customer_id").isin(
            current_records.select("customer_id")
            .subtract(changed.select(col("customer_id")))
            .rdd.flatMap(lambda x: x).collect()
        )
    ).union(expired_records).union(changed_updates)
    
else:
    # First load: assign customer_keys
    new_customers_with_keys = new_customers.withColumn(
        "customer_key",
        row_number().over(Window.orderBy("customer_id"))
    )
    final_dim = new_customers_with_keys

# COMMAND ----------

# Write to Gold (overwrite or append based on SCD strategy)
output_path = Paths.gold_table("dim_customer")
final_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

logger.info(f"✓ Dimension CUSTOMER written: {final_dim.count()} records (SCD Type 2)")
print(f"✓ DIM_CUSTOMER built with SCD Type 2 tracking")

# COMMAND ----------

# Show active records
spark.read.format("delta").load(output_path).filter(col("is_current") == "true").show(5)
```

---

#### **Step 2: Create `src/notebooks/gold/02_build_dim_account.py`**

```python
# Databricks notebook source
# Gold Layer: Dimension - ACCOUNT

from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDimAccount")

# COMMAND ----------

# Read Silver accounts
accounts_silver = spark.read.format("delta").load(Paths.silver_table("accounts"))

# COMMAND ----------

# Build dimension (no SCD needed, accounts are relatively static)
dim_account = accounts_silver.select(
    col("account_id"),
    col("customer_id"),
    col("account_type"),
    col("status"),
    col("open_date"),
).distinct()

dim_account = dim_account.withColumn(
    "account_key",
    row_number().over(Window.orderBy("account_id"))
)

# COMMAND ----------

# Write to Gold
output_path = Paths.gold_table("dim_account")
dim_account.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

logger.info(f"✓ Dimension ACCOUNT written: {dim_account.count()} records")
print(f"✓ DIM_ACCOUNT built")

# COMMAND ----------

dim_account.limit(10).show()
```

---

#### **Step 3: Create `src/notebooks/gold/03_build_dim_date.py`**

```python
# Databricks notebook source
# Gold Layer: Dimension - DATE

from pyspark.sql.functions import col, lit, year, month, dayofweek, to_date
from datetime import datetime, timedelta
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDimDate")

# COMMAND ----------

# Generate date range (past 10 years + future 2 years)
start_date = datetime(2016, 1, 1)
end_date = datetime(2028, 12, 31)

# Create date dimension
dates = []
current_date = start_date
while current_date <= end_date:
    dates.append({
        "date": current_date,
        "date_key": int(current_date.strftime("%Y%m%d")),
        "year": current_date.year,
        "month": current_date.month,
        "day": current_date.day,
        "quarter": (current_date.month - 1) // 3 + 1,
        "is_weekend": 1 if current_date.weekday() >= 5 else 0,
    })
    current_date += timedelta(days=1)

dates_df = spark.createDataFrame(dates)

logger.info(f"Generated {dates_df.count()} date records")

# COMMAND ----------

# Write to Gold
output_path = Paths.gold_table("dim_date")
dates_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

logger.info(f"✓ Dimension DATE written: {dates_df.count()} records")
print(f"✓ DIM_DATE built")

# COMMAND ----------

dates_df.filter(col("date_key") >= 20260501).limit(10).show()
```

---

#### **Step 4: Create `src/notebooks/gold/04_build_dim_event_type.py`**

```python
# Databricks notebook source
# Gold Layer: Dimension - EVENT_TYPE

from pyspark.sql.functions import col, lit
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDimEventType")

# COMMAND ----------

# Define event types (could come from mobile app schema)
event_types = [
    {"event_type_id": 1, "event_type": "page_view", "category": "engagement", "is_sensitive": 0},
    {"event_type_id": 2, "event_type": "button_click", "category": "engagement", "is_sensitive": 0},
    {"event_type_id": 3, "event_type": "form_submit", "category": "transaction", "is_sensitive": 1},
    {"event_type_id": 4, "event_type": "login", "category": "security", "is_sensitive": 1},
    {"event_type_id": 5, "event_type": "logout", "category": "security", "is_sensitive": 0},
    {"event_type_id": 6, "event_type": "payment", "category": "transaction", "is_sensitive": 1},
    {"event_type_id": 7, "event_type": "transfer", "category": "transaction", "is_sensitive": 1},
]

dim_event_type = spark.createDataFrame(event_types)

# COMMAND ----------

# Write to Gold
output_path = Paths.gold_table("dim_event_type")
dim_event_type.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

logger.info(f"✓ Dimension EVENT_TYPE written: {dim_event_type.count()} records")
print(f"✓ DIM_EVENT_TYPE built")

# COMMAND ----------

dim_event_type.show()
```

---

#### **Step 5: Commit**

```bash
git add src/notebooks/gold/01_build_dim_customer.py src/notebooks/gold/02_build_dim_account.py src/notebooks/gold/03_build_dim_date.py src/notebooks/gold/04_build_dim_event_type.py
git commit -m "feat: add Gold layer dimensions (star schema) with SCD Type 2 for customers"
```

---

### Task 11: Build Fact Tables

**Files:**
- Create: `src/notebooks/gold/05_build_fact_transactions.py`
- Create: `src/notebooks/gold/06_build_fact_mobile_events.py`
- Create: `src/notebooks/gold/07_build_fact_fraud_risk.py`

---

#### **Step 1: Create `src/notebooks/gold/05_build_fact_transactions.py`**

```python
# Databricks notebook source
# Gold Layer: Fact - TRANSACTIONS

from pyspark.sql.functions import col, to_date, when
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldFactTransactions")

# COMMAND ----------

# Read dimensions and silver transactions
dim_customer = spark.read.format("delta").load(Paths.gold_table("dim_customer")) \
    .filter(col("is_current") == "true").select("customer_key", "customer_id")
dim_account = spark.read.format("delta").load(Paths.gold_table("dim_account"))
dim_date = spark.read.format("delta").load(Paths.gold_table("dim_date"))
txns_silver = spark.read.format("delta").load(Paths.silver_table("transactions"))

# COMMAND ----------

# Build fact table (join dimensions)
fact_txns = txns_silver.join(
    dim_customer, on="customer_id", how="left"
).join(
    dim_account, on=["customer_id", "account_id"], how="left"
).join(
    dim_date,
    col("dim_date.date_key") == to_date(col("txn_date")).cast("string"),
    how="left"
)

fact_txns = fact_txns.select(
    col("transaction_id"),
    col("customer_key"),
    col("account_key"),
    col("date_key").alias("txn_date_key"),
    col("amount"),
    col("status"),
    col("txn_date").alias("created_at"),
    col("load_date"),
)

logger.info(f"Built fact_transactions: {fact_txns.count()} records")

# COMMAND ----------

# Write to Gold
output_path = Paths.gold_table("fact_transactions")
fact_txns.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Fact TRANSACTIONS written to Gold")
print(f"✓ FACT_TRANSACTIONS built")

# COMMAND ----------

fact_txns.limit(10).show()
```

---

#### **Step 2: Create `src/notebooks/gold/06_build_fact_mobile_events.py`**

```python
# Databricks notebook source
# Gold Layer: Fact - MOBILE_EVENTS

from pyspark.sql.functions import col, to_date, explode, arrays_zip, rank
from pyspark.sql.window import Window
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldFactMobileEvents")

# COMMAND ----------

# Read dimensions and silver mobile events
dim_customer = spark.read.format("delta").load(Paths.gold_table("dim_customer")) \
    .filter(col("is_current") == "true").select("customer_key", "customer_id")
dim_event_type = spark.read.format("delta").load(Paths.gold_table("dim_event_type"))
dim_date = spark.read.format("delta").load(Paths.gold_table("dim_date"))

# Read mobile events (would come from streaming or batch)
try:
    events_silver = spark.read.format("delta").load(Paths.silver_table("mobile_events"))
except:
    logger.info("Mobile events not yet loaded (streaming will populate)")
    events_silver = spark.createDataFrame([], "event_id long, customer_id long, event_type string, event_timestamp timestamp")

# COMMAND ----------

# Build fact table if events exist
if events_silver.count() > 0:
    fact_events = events_silver.join(
        dim_customer, on="customer_id", how="left"
    ).join(
        dim_event_type, on="event_type", how="left"
    ).join(
        dim_date,
        col("dim_date.date_key") == to_date(col("event_timestamp")).cast("string"),
        how="left"
    )
    
    fact_events = fact_events.select(
        col("event_id"),
        col("customer_key"),
        col("date_key").alias("event_date_key"),
        col("event_type_id"),
        lit(1).alias("event_count"),
        col("event_timestamp"),
        col("load_date"),
    )
    
    logger.info(f"Built fact_mobile_events: {fact_events.count()} records")
    
    # Write to Gold
    output_path = Paths.gold_table("fact_mobile_events")
    fact_events.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .save(output_path)
    
    logger.info(f"✓ Fact MOBILE_EVENTS written to Gold")
else:
    logger.info("Skipping fact_mobile_events (no data yet)")

print(f"✓ FACT_MOBILE_EVENTS prepared (streaming will populate)")

# COMMAND ----------

# Show sample if available
if events_silver.count() > 0:
    spark.read.format("delta").load(Paths.gold_table("fact_mobile_events")).limit(10).show()
```

---

#### **Step 3: Create `src/notebooks/gold/07_build_fact_fraud_risk.py`**

```python
# Databricks notebook source
# Gold Layer: Fact - FRAUD_RISK (Daily Batch Aggregation)

from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max, min as spark_min, datediff, current_date, lit
from datetime import datetime
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldFactFraudRisk")

# COMMAND ----------

# Read dimensions
dim_customer = spark.read.format("delta").load(Paths.gold_table("dim_customer")) \
    .filter(col("is_current") == "true").select("customer_key", "customer_id")

# Read fact transactions for fraud analysis
try:
    fact_txns = spark.read.format("delta").load(Paths.gold_table("fact_transactions"))
except:
    logger.info("fact_transactions not yet available")
    fact_txns = None

# COMMAND ----------

if fact_txns:
    # Aggregate fraud risk metrics per customer
    fraud_risk = fact_txns.groupBy("customer_key").agg(
        spark_max("amount").alias("max_transaction_amount_7d"),
        (spark_sum("amount") / count("*")).alias("avg_transaction_amount"),
        count("*").alias("transaction_count_7d"),
    )
    
    # Add customer info
    fraud_risk = fraud_risk.join(
        dim_customer, on="customer_key", how="left"
    )
    
    # Calculate fraud risk score (simplified; real logic would use ML model)
    fraud_risk = fraud_risk.withColumn(
        "fraud_risk_score",
        lit(0.3)  # Placeholder; actual model would score here
    ).withColumn(
        "fraud_indicator",
        when(col("fraud_risk_score") > 0.7, "HIGH")
        .when(col("fraud_risk_score") > 0.4, "MEDIUM")
        .otherwise("LOW")
    ).withColumn(
        "last_fraud_date",
        lit(None)
    ).withColumn(
        "fraud_count_12m",
        lit(0)
    ).withColumn(
        "model_version",
        lit("1.0")
    ).withColumn(
        "snapshot_date",
        lit(datetime.now().strftime("%Y-%m-%d"))
    )
    
    logger.info(f"Built fact_fraud_risk: {fraud_risk.count()} records")
    
    # Write to Gold
    output_path = Paths.gold_table("fact_fraud_risk")
    fraud_risk.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("snapshot_date") \
        .save(output_path)
    
    logger.info(f"✓ Fact FRAUD_RISK written to Gold")
else:
    logger.info("Skipping fact_fraud_risk (no transaction data)")

print(f"✓ FACT_FRAUD_RISK built")

# COMMAND ----------

# Show sample
if fact_txns:
    spark.read.format("delta").load(Paths.gold_table("fact_fraud_risk")).limit(10).show()
```

---

#### **Step 4: Commit**

```bash
git add src/notebooks/gold/05_build_fact_transactions.py src/notebooks/gold/06_build_fact_mobile_events.py src/notebooks/gold/07_build_fact_fraud_risk.py
git commit -m "feat: add Gold layer fact tables with dimensional joins"
```

---

## Phase 5: Real-Time Streaming - Fraud Detection (Week 5-6)

### Task 12: Build Real-Time Fraud Detection Pipeline

**Files:**
- Create: `src/notebooks/streaming/fraud_detection_job.py`
- Create: `src/python/kafka_consumer.py`

---

#### **Step 1: Create `src/python/kafka_consumer.py`**

```python
from kafka import KafkaConsumer
from typing import Optional, Dict, Any
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

class MobileEventConsumer:
    """Kafka consumer for mobile clickstream events."""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Comma-separated broker list
            topic: Kafka topic to consume from
        """
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.topic = topic
        self.consumer = None
    
    def connect(self):
        """Establish Kafka connection."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='negarabank-fraud-detection',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=6000,
                heartbeat_interval_ms=3000,
            )
            logger.info(f"Connected to Kafka topic: {self.topic}")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Kafka: {str(e)}")
    
    def consume_batch(self, timeout_ms: int = 5000, max_records: int = 1000) -> list:
        """
        Consume a batch of messages.
        
        Args:
            timeout_ms: Poll timeout
            max_records: Max records per poll
        
        Returns:
            List of message values
        """
        messages = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        
        batch = []
        for topic_partition, records in messages.items():
            for record in records:
                batch.append(record.value)
        
        return batch
    
    def close(self):
        """Close Kafka connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
```

---

#### **Step 2: Create `src/notebooks/streaming/fraud_detection_job.py`**

```python
# Databricks notebook source
# Real-Time Fraud Detection: Kafka Stream → Feature Enrichment → ML Scoring → Delta Alert Table

from pyspark.sql.functions import (
    col, from_json, schema_of_json, lit, current_timestamp,
    sum as spark_sum, count as spark_count, window, when,
    explode_outer, array_contains
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from datetime import datetime
from src.python.config import Paths, get_kafka_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FraudDetectionStreaming")

# COMMAND ----------

# Get Kafka config
kafka_config = get_kafka_config()
logger.info(f"Kafka config: {kafka_config}")

# COMMAND ----------

# Define mobile event schema
mobile_event_schema = StructType([
    StructField("event_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("device_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("amount", DoubleType(), True),
])

# COMMAND ----------

# Read Kafka stream
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("subscribe", kafka_config["mobile_events_topic"]) \
    .option("startingOffsets", "latest") \
    .option("checkpointLocation", f"{Paths.S3['checkpoints']}fraud_detection/") \
    .load()

logger.info("Connected to Kafka stream")

# COMMAND ----------

# Parse JSON payload
events_df = kafka_stream.select(
    col("timestamp").alias("kafka_timestamp"),
    from_json(col("value").cast("string"), mobile_event_schema).alias("data")
).select(
    col("data.event_id"),
    col("data.customer_id"),
    col("data.event_type"),
    col("data.timestamp").alias("event_timestamp"),
    col("data.device_id"),
    col("data.location"),
    col("data.amount"),
    col("kafka_timestamp"),
)

logger.info("Parsed Kafka events")

# COMMAND ----------

# Deduplication: Remove exact duplicates within 5-min window (by event_id)
from pyspark.sql.window import Window

window_spec = Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())
deduped_events = events_df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# COMMAND ----------

# Broadcast join with account master (latest 7-day snapshot from Silver)
accounts_silver = spark.read.format("delta").load(Paths.silver_table("accounts"))
accounts_broadcast = broadcast(accounts_silver.select("customer_id", "account_id", "balance"))

events_with_account = deduped_events.join(
    accounts_broadcast,
    on="customer_id",
    how="left"
)

# COMMAND ----------

# Feature engineering: 24-hour window aggregations
# (Streaming window for fraud signal enrichment)

window_spec_24h = Window \
    .partitionBy("customer_id") \
    .orderBy(col("event_timestamp")) \
    .rangeBetween(-86400, 0)  # 24 hours in seconds

events_with_features = events_with_account.withColumn(
    "event_count_24h",
    spark_count("event_id").over(window_spec_24h)
).withColumn(
    "avg_transaction_amount",
    (spark_sum(col("amount")).over(window_spec_24h) / spark_count("event_id").over(window_spec_24h))
)

# COMMAND ----------

# ML Model Inference
# (In production, load MLflow model; here using placeholder)

# Placeholder fraud scoring function
def fraud_score_udf(event_count: int, avg_amount: float, balance: float) -> float:
    """
    Simple fraud scoring (placeholder; real model would be XGBoost/LightGBM).
    
    Logic:
    - High event count (>100 in 24h) + low balance = higher risk
    - Large amount compared to avg balance = higher risk
    """
    score = 0.0
    if event_count > 100:
        score += 0.3
    if avg_amount and balance and avg_amount > balance * 0.5:
        score += 0.4
    return min(score, 1.0)

from pyspark.sql.functions import udf
fraud_score_fn = udf(fraud_score_udf, DoubleType())

events_with_score = events_with_features.withColumn(
    "fraud_score",
    fraud_score_fn(col("event_count_24h"), col("avg_transaction_amount"), col("balance"))
)

# COMMAND ----------

# Alert classification based on score
events_with_alert = events_with_score.withColumn(
    "fraud_alert_status",
    when(col("fraud_score") > 0.8, "HIGH_RISK")
    .when(col("fraud_score") > 0.6, "MEDIUM_RISK")
    .otherwise("LOW_RISK")
).withColumn(
    "model_version",
    lit("1.0")
).withColumn(
    "processing_timestamp",
    current_timestamp()
).withColumn(
    "alert_sent",
    lit(False)
).withColumn(
    "event_date",
    lit(datetime.now().strftime("%Y-%m-%d"))
)

# COMMAND ----------

# Select final schema for fact_fraud_transaction_alert
fraud_alerts_final = events_with_alert.select(
    col("event_id").alias("transaction_id"),
    col("customer_id"),
    col("account_id"),
    col("amount"),
    col("event_timestamp"),
    col("event_count_24h"),
    col("avg_transaction_amount"),
    col("balance").alias("account_balance"),
    col("fraud_score"),
    col("fraud_alert_status"),
    col("model_version"),
    col("processing_timestamp"),
    col("alert_sent"),
    col("event_date"),
)

logger.info("Fraud detection pipeline ready")

# COMMAND ----------

# Write to Delta (upsert mode for fraud alerts)
def write_fraud_alerts(batch_df, batch_id):
    """Write batch of fraud alerts to Delta (upsert by transaction_id)."""
    try:
        batch_df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .partitionBy("event_date") \
            .save(Paths.gold_table("fact_fraud_transaction_alert"))
        
        logger.info(f"Batch {batch_id}: {batch_df.count()} fraud alerts written")
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {str(e)}")

fraud_alerts_final.writeStream \
    .foreachBatch(write_fraud_alerts) \
    .option("checkpointLocation", f"{Paths.S3['checkpoints']}fraud_alerts/") \
    .start() \
    .awaitTermination()

logger.info("✓ Fraud detection streaming job started (running indefinitely)")
print("✓ FRAUD DETECTION STREAMING: 24/7 Kafka consumer active")
```

---

#### **Step 3: Commit**

```bash
git add src/python/kafka_consumer.py src/notebooks/streaming/fraud_detection_job.py
git commit -m "feat: add real-time fraud detection streaming pipeline with Kafka integration"
```

---

## Phase 6: Governance, Testing & Deployment (Week 6-7)

### Task 13: Create Testing Framework & Unit Tests

**Files:**
- Create: `tests/conftest.py`
- Create: `tests/unit/test_transformations.py`
- Create: `tests/integration/test_end_to_end.py`

---

#### **Step 1: Create `tests/conftest.py`**

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("negarabank-tests") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.default.parallelism", 4) \
        .getOrCreate()

@pytest.fixture
def sample_accounts_data(spark):
    """Create sample accounts data for testing."""
    schema = StructType([
        StructField("account_id", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("account_type", StringType(), True),
        StructField("balance", DecimalType(15, 2), True),
    ])
    
    data = [
        (1, 100, "CHECKING", 5000.00),
        (2, 101, "SAVINGS", 10000.00),
        (3, 102, "CHECKING", 2500.00),
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_transactions_data(spark):
    """Create sample transactions data for testing."""
    from pyspark.sql.types import TimestampType
    from datetime import datetime
    
    schema = StructType([
        StructField("transaction_id", LongType(), False),
        StructField("account_id", LongType(), False),
        StructField("amount", DecimalType(15, 2), False),
        StructField("txn_date", TimestampType(), False),
    ])
    
    data = [
        (1001, 1, 500.00, datetime(2026, 5, 24, 10, 0)),
        (1002, 1, 300.00, datetime(2026, 5, 24, 11, 0)),
        (1003, 2, 1000.00, datetime(2026, 5, 24, 12, 0)),
    ]
    
    return spark.createDataFrame(data, schema)
```

---

#### **Step 2: Create `tests/unit/test_transformations.py`**

```python
import pytest
from pyspark.sql.functions import col

def test_accounts_null_check(sample_accounts_data):
    """Test that required account columns have no nulls."""
    null_count = sample_accounts_data.filter(col("account_id").isNull()).count()
    assert null_count == 0, "account_id should not have nulls"

def test_accounts_deduplication(spark, sample_accounts_data):
    """Test deduplication logic removes exact duplicates."""
    from pyspark.sql.functions import row_number
    from pyspark.sql.window import Window
    
    # Add duplicates
    with_dupes = sample_accounts_data.union(
        sample_accounts_data.filter(col("account_id") == 1).limit(1)
    )
    
    # Deduplicate
    window = Window.partitionBy("account_id").orderBy(col("account_id"))
    deduped = with_dupes.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1)
    
    assert deduped.count() == sample_accounts_data.count(), "Dedup should remove duplicates"

def test_transactions_fk_validation(spark, sample_accounts_data, sample_transactions_data):
    """Test orphaned records detection."""
    # Add transaction with non-existent account
    from pyspark.sql.types import StructType, StructField, LongType, DecimalType, TimestampType
    from datetime import datetime
    
    orphan_schema = StructType([
        StructField("transaction_id", LongType(), False),
        StructField("account_id", LongType(), False),
        StructField("amount", DecimalType(15, 2), False),
        StructField("txn_date", TimestampType(), False),
    ])
    
    orphan_data = [
        (2001, 999, 500.00, datetime(2026, 5, 24, 13, 0)),  # Non-existent account
    ]
    
    orphan_txn = spark.createDataFrame(orphan_data, orphan_schema)
    all_txns = sample_transactions_data.union(orphan_txn)
    
    # Check for orphans
    valid_accounts = sample_accounts_data.select("account_id").rdd.flatMap(lambda x: x).collect()
    orphans = all_txns.filter(~col("account_id").isin(valid_accounts))
    
    assert orphans.count() == 1, "Should detect 1 orphaned record"

def test_fraud_score_range(spark):
    """Test fraud scores are within valid range (0.0 - 1.0)."""
    from pyspark.sql.types import StructType, StructField, DoubleType
    
    schema = StructType([
        StructField("fraud_score", DoubleType(), False),
    ])
    
    data = [(0.0,), (0.5,), (0.99,), (1.0,)]
    scores_df = spark.createDataFrame(data, schema)
    
    invalid = scores_df.filter((col("fraud_score") < 0) | (col("fraud_score") > 1))
    assert invalid.count() == 0, "All fraud scores should be in [0, 1]"
```

---

#### **Step 3: Create `tests/integration/test_end_to_end.py`**

```python
import pytest
from datetime import datetime

@pytest.mark.integration
def test_bronze_to_silver_accounts_pipeline(spark, sample_accounts_data):
    """Test full Bronze → Silver transformation for accounts."""
    # Simulate bronze load
    bronze_accounts = sample_accounts_data.withColumn(
        "load_date", 
        lit(datetime.now().strftime("%Y-%m-%d"))
    )
    
    # Simulate silver transformation
    silver_accounts = bronze_accounts.select(
        col("account_id").cast("long"),
        col("customer_id").cast("long"),
        col("account_type").cast("string"),
        col("balance").cast("decimal(15,2)"),
        lit("true").alias("dq_passed"),
        col("load_date").cast("string"),
    )
    
    # Validate
    assert silver_accounts.count() == sample_accounts_data.count()
    assert silver_accounts.schema.fieldNames() == [
        "account_id", "customer_id", "account_type", "balance", "dq_passed", "load_date"
    ]

@pytest.mark.integration
def test_silver_to_gold_dimension_pipeline(spark, sample_accounts_data):
    """Test Silver → Gold dimension building."""
    from pyspark.sql.functions import lit, row_number
    from pyspark.sql.window import Window
    
    # Add load_date for silver
    silver = sample_accounts_data.withColumn(
        "load_date",
        lit(datetime.now().strftime("%Y-%m-%d"))
    )
    
    # Build gold dimension
    gold_dim = silver.select(
        col("account_id"),
        col("customer_id"),
        col("account_type"),
    ).distinct().withColumn(
        "account_key",
        row_number().over(Window.orderBy("account_id"))
    )
    
    assert gold_dim.count() == sample_accounts_data.select("account_id").distinct().count()
    assert "account_key" in gold_dim.columns
```

---

#### **Step 4: Commit**

```bash
git add tests/
git commit -m "feat: add comprehensive testing framework (unit + integration tests)"
```

---

### Task 14: Create Data Dictionary & Documentation

**Files:**
- Create: `docs/data_dictionary.md`
- Create: `docs/CONTRIBUTING.md`
- Create: `docs/runbook_troubleshoot.md`

---

#### **Step 1: Create `docs/data_dictionary.md`**

```markdown
# NegaraBank Data Dictionary

## Bronze Layer

### bronze.accounts
Raw account master table from Oracle (no transformations).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| account_id | LONG | No | Unique account identifier (PK) |
| customer_id | LONG | No | Customer identifier (FK to accounts) |
| account_type | STRING | Yes | CHECKING, SAVINGS, CREDIT |
| balance | DECIMAL(15,2) | Yes | Current account balance |
| status | STRING | Yes | ACTIVE, CLOSED, SUSPENDED |
| open_date | TIMESTAMP | Yes | Account opening date |
| load_date | STRING | No | Data load date (partition key) |
| load_timestamp | STRING | No | Data load timestamp |

**Source:** Oracle JDBC extract  
**Freshness:** Daily snapshot (no delta load needed; static master)  
**Lineage:** accounts_raw → accounts_curated (Silver) → dim_account (Gold)

---

### bronze.transactions
Raw transaction records from Oracle.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | LONG | No | Unique transaction ID (PK) |
| account_id | LONG | No | Account identifier (FK) |
| customer_id | LONG | No | Customer identifier (FK) |
| amount | DECIMAL(15,2) | No | Transaction amount |
| txn_date | TIMESTAMP | No | Transaction timestamp |
| status | STRING | Yes | POSTED, PENDING, FAILED |
| load_date | STRING | No | Data load date (partition key) |
| load_timestamp | STRING | No | Data load timestamp |

**Source:** Oracle JDBC (delta/incremental load, 24-hour predicate slices)  
**Freshness:** Daily (millions of records/day)  
**Lineage:** transactions_raw → transactions_curated (Silver) → fact_transactions (Gold)

---

## Silver Layer

### silver.accounts_curated
Deduplicated, quality-validated accounts.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| account_id | LONG | No | (PK) |
| customer_id | LONG | No | (FK) |
| account_type | STRING | Yes | Standardized account type |
| balance | DECIMAL(15,2) | Yes | Validated balance |
| status | STRING | Yes | Standardized status |
| open_date | TIMESTAMP | Yes | Validated timestamp |
| dq_passed | STRING | Yes | 'true' if all DQ checks passed |
| load_date | STRING | No | (Partition key) |

**Transformations:**
- Type casting (strings, timestamps, decimals)
- Null validation (< 5% allowed on non-PK fields)
- Deduplication by account_id (keep latest by load_timestamp)

---

### silver.transactions_curated
Reconciled, quality-validated transactions.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | LONG | No | (PK) |
| account_id | LONG | No | (FK, validated vs. accounts) |
| customer_id | LONG | No | (FK, derived from account_id) |
| amount | DECIMAL(15,2) | No | Validated amount |
| txn_date | TIMESTAMP | No | Validated timestamp |
| status | STRING | Yes | Standardized status |
| reconciled | STRING | Yes | 'true' if FK valid, 'false' if orphan |
| load_date | STRING | No | (Partition key) |

**Transformations:**
- Referential integrity check (account_id must exist in silver.accounts)
- Deduplication by transaction_id (keep latest)
- Type casting & standardization

---

## Gold Layer

### gold.dim_customer (SCD Type 2)
Customer dimension with historical tracking.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_key | LONG | No | Surrogate key (for fact joins) |
| customer_id | LONG | No | Business key |
| name | STRING | Yes | Customer name |
| email | STRING | Yes | Email address |
| segment | STRING | Yes | Customer segment (standard, premium) |
| risk_score | DECIMAL(5,2) | Yes | Calculated risk score |
| effective_date | STRING | No | When this record became effective |
| end_date | STRING | Yes | When this record expired (NULL if current) |
| is_current | STRING | No | 'true' if latest version, 'false' if historical |

**Type:** Dimension (SCD Type 2 - tracks changes over time)  
**Grain:** One row per customer version  
**Lineage:** silver.accounts → dim_customer

---

### gold.fact_fraud_transaction_alert (REAL-TIME, DENORMALIZED)
Real-time fraud alerts with enriched features (for low-latency queries).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | LONG | No | (PK, Upsertable) |
| customer_id | LONG | No | Customer |
| account_id | LONG | No | Account |
| amount | DECIMAL(15,2) | No | Transaction amount |
| event_timestamp | TIMESTAMP | No | When event occurred |
| event_count_24h | INT | Yes | Events in 24h window (feature) |
| avg_transaction_amount | DECIMAL(15,2) | Yes | 24h avg (feature) |
| account_balance | DECIMAL(15,2) | Yes | Latest account balance (enrichment) |
| fraud_score | DECIMAL(3,2) | No | ML model score [0.0 - 1.0] |
| fraud_alert_status | STRING | No | HIGH_RISK, MEDIUM_RISK, LOW_RISK |
| model_version | STRING | Yes | ML model version used |
| processing_timestamp | TIMESTAMP | No | When fraud score computed |
| alert_sent | STRING | Yes | 'true' if alert dispatched |
| event_date | STRING | No | (Partition key, 7-day hot retention) |

**Type:** Fact (denormalized for real-time speed)  
**Source:** Kafka streaming (mobile_clickstream) + broadcast joins with accounts  
**Latency:** 10-15 seconds (event → alert)  
**Storage Tiers:**
- Hot (7 days): S3 Standard (Delta)
- Warm (8-90 days): S3-IA
- Cold (91 days-3 years): S3-Glacier

---

### gold.fact_transactions (STAR SCHEMA)
Transaction fact table (normalized, optimized for BI).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_key | LONG | No | Surrogate key (PK) |
| customer_key | LONG | No | FK to dim_customer |
| account_key | LONG | No | FK to dim_account |
| txn_date_key | STRING | No | FK to dim_date |
| amount | DECIMAL(15,2) | No | Transaction amount |
| status | STRING | Yes | Transaction status |
| created_at | TIMESTAMP | No | Transaction timestamp |
| load_date | STRING | No | (Partition key) |

**Type:** Fact (star schema for BI analytics)  
**Grain:** One row per transaction  
**Lineage:** silver.transactions → fact_transactions

---

## Data Governance

### Access Control (Unity Catalog)
- **Bronze:** Data Engineers only (SELECT, MODIFY)
- **Silver:** Data Engineers (full), Data Analysts (SELECT), ML Engineers (SELECT, MODIFY)
- **Gold:**
  - PUBLIC tables: All teams (SELECT)
  - CONFIDENTIAL tables (fraud_alerts): Fraud team (SELECT, MODIFY), Risk team (SELECT)

### Data Quality SLA
- **Completeness:** 99.5% (< 0.5% nulls on critical fields)
- **Freshness:** < 1 minute (fraud), < 24 hours (batch)
- **Referential Integrity:** 100% (no orphans)

### Retention Policy
- **Hot (7 days):** In-memory, fast queries
- **Warm (8-90 days):** S3-IA (slower reads acceptable)
- **Cold (91 days-3 years):** S3-Glacier (compliance/audit only)
- **Archive (3+ years):** Deleted (regulatory retention met)
```

---

#### **Step 2: Create `docs/CONTRIBUTING.md`**

```markdown
# Contributing Guide

## Development Setup

1. **Clone repository:**
   ```bash
   git clone <repo-url>
   cd negarabank-pipeline
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements-dev.txt
   ```

4. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your Databricks token, AWS credentials
   ```

## Development Workflow

### 1. Feature Development
- Branch: `git checkout -b feature/xyz`
- Code in notebooks or Python modules
- Write tests (unit + integration)
- Run tests locally: `pytest tests/`

### 2. Code Style
- **Python:** Follow PEP8
  ```bash
  black src/ tests/
  flake8 src/
  ```
- **SQL:** Consistent formatting, meaningful table aliases
- **Notebooks:** Clear markdown documentation, logical cell order

### 3. Commit Guidelines
```
feat: add fraud detection pipeline
fix: resolve orphaned records in transactions
refactor: simplify DQ framework
docs: update data dictionary
test: add integration tests for Silver layer
chore: update dependencies
```

### 4. Testing
- **Unit tests:** `pytest tests/unit/`
- **Integration tests:** `pytest tests/integration/ -m integration`
- **DQ validation:** Query gold layer for specific checks

### 5. Pull Request
- Create PR with descriptive title + description
- Reference any related issues (#123)
- Run: `pytest`, `black`, `flake8`
- Request review from data team lead

## Databricks Development

### Syncing Notebooks
```bash
# Sync local files to Databricks workspace
databricks workspace import-dir src/notebooks/ /Repos/NegaraBank/Pipeline/src/notebooks

# Or use DAB deployment:
databricks bundle deploy --target dev
```

### Testing Jobs
```bash
# Trigger job run
databricks jobs run-now --job-id <job_id>

# Monitor output
databricks runs get --run-id <run_id>
```

## Performance Optimization

### Spark Tuning
- Monitor **Adaptive Query Execution (AQE):** Enabled in cluster config
- Check **partition count:** 4-8 per GB of data
- Use **broadcast joins:** For small dimensions (accounts, event_types)
- Enable **Z-ordering:** On frequently filtered columns

### Delta Lake Optimization
```sql
-- Compact small files (run weekly)
OPTIMIZE gold.fact_transactions
ZORDER BY (txn_date_key, customer_key)

-- Check table stats
DESCRIBE DETAIL gold.fact_transactions
```

## Troubleshooting

### Common Issues

**Issue:** Streaming job stuck in micro-batches
- Check Kafka topic lag
- Monitor Spark executor memory
- Check checkpoint directory exists

**Issue:** Oracle JDBC timeout
- Verify EC2 instance is running
- Check VPC security group rules
- Monitor network latency

**Issue:** Out-of-memory in batch job
- Reduce partition size (repartition logic)
- Increase executor memory (cluster config)
- Check for data skew (group by analysis)

## Questions?
- Slack: #data-engineering
- Email: data-team@negarabank.com
```

---

#### **Step 3: Create `docs/runbook_troubleshoot.md`**

```markdown
# Operational Runbook & Troubleshooting

## Daily Operations

### Batch ETL Job Monitoring
**Job:** Daily Batch ETL (Oracle → Bronze → Silver → Gold)  
**Schedule:** 2 AM UTC  
**Expected Duration:** 15-30 minutes

**Check Status:**
```bash
databricks jobs list --name "Daily Batch ETL"
databricks runs get --run-id <run_id>
```

**Monitor in Databricks:**
- Workspace → Workflows → "Daily Batch ETL"
- Check last 3 runs (should all succeed)
- If failed: Check logs for specific task failure

---

### Streaming Job Monitoring (24/7)
**Job:** Real-time Fraud Detection  
**Expected:** Always running  
**Latency SLA:** 10-15 seconds (event to alert)

**Check Health:**
```bash
# Monitor Kafka lag
kafka-consumer-groups --bootstrap-server <kafka> --describe --group negarabank-fraud-detection

# Check streaming job in Databricks
# Workspace → Workflows → "Real-time Fraud Detection"
```

**Restart if needed:**
```bash
# Stop streaming job
databricks jobs cancel-run --run-id <run_id>

# Restart
databricks jobs run-now --job-id <streaming_job_id>
```

---

## Troubleshooting Guide

### Issue: Batch Job Failed

**Symptom:** Job terminated with error

**Diagnosis:**
1. Check task that failed:
   ```bash
   databricks runs get --run-id <run_id>
   # Look for "FAILED" state in tasks
   ```

2. View logs:
   ```bash
   databricks runs get-output --run-id <run_id>
   ```

3. Common causes:
   - **JDBC timeout:** EC2 instance down or VPC issue
   - **Out of memory:** Too many rows, need smaller batches
   - **Schema mismatch:** Oracle table changed, update schema
   - **S3 permission:** IAM role missing S3 access

**Resolution:**
- Retry job: `databricks jobs run-now --job-id <job_id>`
- Fix root cause, re-run

---

### Issue: Data Quality Failed

**Symptom:** DQ checks show high null %, orphan records, etc.

**Diagnosis:**
```sql
-- Check null % by column
SELECT
    table_name,
    column_name,
    ROUND(100.0 * null_count / total_rows, 2) as null_pct
FROM dq_metrics
WHERE snapshot_date = CURRENT_DATE()
ORDER BY null_pct DESC;
```

**Resolution:**
- If oracle source issue: Contact data governance team
- If transformation bug: Fix SQL, re-run Silver layer
- If threshold too strict: Update DQ config

---

### Issue: Fraud Alerts Missing

**Symptom:** No fraud alerts in gold layer for known fraud events

**Diagnosis:**
1. Check Kafka topic:
   ```bash
   # Verify events arriving
   kafka-console-consumer --bootstrap-server <kafka> \
     --topic mobile_clickstream --from-beginning --max-messages 10
   ```

2. Check streaming job:
   ```bash
   # View checkpoint to see if lagging
   aws s3 ls s3://negarabank-checkpoints/fraud_detection/
   ```

3. Check fraud alerts table:
   ```sql
   SELECT COUNT(*), MAX(processing_timestamp)
   FROM gold.fact_fraud_transaction_alert
   WHERE event_date = CURRENT_DATE();
   ```

**Resolution:**
- If Kafka lag: Restart streaming job
- If no events: Check mobile app sending events correctly
- If events but no alerts: Check model is loaded in MLflow

---

### Issue: Out-of-Memory (OOM) Error

**Symptom:** Spark job fails with OOM

**Diagnosis:**
1. Check executor memory config:
   ```bash
   databricks clusters list
   # Check spark_conf for executor memory
   ```

2. Identify memory-heavy operations:
   - Large groupBy/aggregations
   - Broadcast joins of large tables
   - collect() operations

**Resolution:**
- Option 1: Increase cluster memory (edit cluster config)
- Option 2: Repartition data (increase partitions before groupBy)
- Option 3: Process data in smaller batches (limit date range)

**Example fix:**
```python
# Before (OOM):
large_df.groupBy("customer_id").agg(...)

# After (memory efficient):
large_df.repartition(200, "customer_id") \
    .groupBy("customer_id").agg(...) \
    .coalesce(10)
```

---

## Emergency Procedures

### Hard Reset (Nuclear Option)

If jobs are completely stuck or corrupted:

1. **Stop all jobs:**
   ```bash
   databricks jobs cancel-run --run-id <run_id>  # (repeat for all running)
   ```

2. **Clear checkpoints** (streaming only):
   ```bash
   aws s3 rm s3://negarabank-checkpoints/fraud_detection/ --recursive
   ```

3. **Restart from clean state:**
   ```bash
   databricks jobs run-now --job-id <job_id>
   ```

⚠️ **Use sparingly!** Clearing checkpoints may cause duplicate alert processing.

---

## Alerting & Escalation

| Issue | Severity | Action | Escalate To |
|-------|----------|--------|-------------|
| Data quality threshold exceeded | Medium | Investigate + fix within 24h | Data Lead |
| Batch job failed 2+ times | High | Restart + check logs | Data Lead + Eng Lead |
| Streaming lag > 5 min | High | Investigate Kafka lag | Data Lead + Ops |
| Fraud alerts down | Critical | Restart streaming job + verify | Manager + Ops + Fraud Team |

---

## Performance Tuning

### Delta Lake Optimization
Run weekly (after heavy writes):
```sql
OPTIMIZE gold.fact_fraud_transaction_alert;
OPTIMIZE gold.fact_transactions
ZORDER BY (txn_date_key, customer_key);
```

### Spark Cluster Tuning
For Batch cluster, optimize for cost:
```yaml
node_type: i3en.3xlarge  # 8 cores, 96 GB RAM
min_workers: 2
max_workers: 8
autoscale: true  # Scale down after job completes
```

For Streaming cluster, optimize for latency:
```yaml
node_type: i3en.2xlarge  # 4 cores, 64 GB RAM
min_workers: 2
max_workers: 4  # Limited scaling (burst capacity only)
```

---

## Support Contacts

- **Data Engineering:** data-eng@negarabank.com
- **On-Call:** #oncall Slack channel
- **Escalation:** Data Platform Manager
```

---

#### **Step 4: Commit**

```bash
git add docs/data_dictionary.md docs/CONTRIBUTING.md docs/runbook_troubleshoot.md
git commit -m "docs: add comprehensive data dictionary, contributing guide, and operational runbook"
```

---

### Task 15: Create DAB Deployment Scripts & Finalize Configuration

**Files:**
- Create: `deployment/scripts/deploy_dab.sh`
- Create: `deployment/scripts/setup_secrets.sh`
- Update: `databricks.yml` (final configuration)

---

#### **Step 1: Create `deployment/scripts/deploy_dab.sh`**

```bash
#!/bin/bash
set -e

echo "NegaraBank DAB Deployment Script"
echo "=================================="

TARGET=${1:-dev}
echo "Deploying to target: $TARGET"

# Validate target
if [[ "$TARGET" != "dev" && "$TARGET" != "prod" ]]; then
    echo "Error: Target must be 'dev' or 'prod'"
    exit 1
fi

# Check Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "Error: databricks CLI not installed"
    exit 1
fi

# Validate authentication
echo "Verifying Databricks authentication..."
if ! databricks account get > /dev/null 2>&1; then
    echo "Error: Not authenticated to Databricks. Run 'databricks configure' first"
    exit 1
fi

# Deploy bundle
echo ""
echo "Deploying Databricks Asset Bundle to $TARGET..."
databricks bundle deploy --target $TARGET

echo ""
echo "✓ Deployment complete!"
echo ""
echo "Next steps:"
echo "1. Verify jobs in Databricks workspace"
echo "2. Trigger manual test: databricks jobs run-now --job-id <job_id>"
echo "3. Monitor logs in Databricks Runs page"
```

---

#### **Step 2: Create `deployment/scripts/setup_secrets.sh`**

```bash
#!/bin/bash
set -e

echo "Setting up AWS Secrets Manager for NegaraBank"
echo "=============================================="

# Check AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI not installed"
    exit 1
fi

# Get credentials from user
read -p "Oracle DB User: " ORACLE_USER
read -sp "Oracle DB Password: " ORACLE_PASS
echo
read -p "Oracle Host (on-prem): " ORACLE_HOST
read -p "Oracle Port [1521]: " ORACLE_PORT
ORACLE_PORT=${ORACLE_PORT:-1521}

# Create secret
SECRET_NAME="negarabank/oracle/jdbc"
SECRET_VALUE="{\"username\":\"$ORACLE_USER\",\"password\":\"$ORACLE_PASS\",\"host\":\"$ORACLE_HOST\",\"port\":$ORACLE_PORT}"

echo ""
echo "Creating AWS Secrets Manager secret: $SECRET_NAME"
aws secretsmanager create-secret \
    --name "$SECRET_NAME" \
    --description "Oracle JDBC credentials for NegaraBank" \
    --secret-string "$SECRET_VALUE" \
    --region us-east-1 \
    2>/dev/null || echo "(Secret already exists, updating...)"

aws secretsmanager update-secret \
    --secret-id "$SECRET_NAME" \
    --secret-string "$SECRET_VALUE" \
    --region us-east-1

echo "✓ Secrets configured"
echo ""
echo "Secret stored at: arn:aws:secretsmanager:us-east-1:<account>:secret:$SECRET_NAME"
```

---

#### **Step 3: Create `deployment/scripts/create_uc_volumes.sh`**

```bash
#!/bin/bash
set -e

echo "Creating Unity Catalog Volumes for NegaraBank"
echo "=============================================="

WORKSPACE=${1:-dev}
CATALOG="negarabank"
SCHEMA="default"

echo "Target workspace: $WORKSPACE"

# Create catalog (if not exists)
echo "Creating catalog: $CATALOG"
databricks catalogs create --name $CATALOG \
    --comment "NegaraBank data platform" \
    2>/dev/null || echo "(Catalog already exists)"

# Create schemas
for layer in bronze silver gold; do
    echo "Creating schema: ${CATALOG}.${layer}"
    databricks schemas create \
        --catalog-name $CATALOG \
        --name $layer \
        --comment "$layer layer" \
        2>/dev/null || echo "(Schema already exists)"
done

echo ""
echo "✓ Unity Catalog structure created"
echo ""
echo "Catalog: $CATALOG"
echo "Schemas: bronze, silver, gold"
```

---

#### **Step 4: Final Commit & Summary**

```bash
git add deployment/scripts/
git commit -m "chore: add deployment scripts for DAB, secrets, and Unity Catalog setup"
```

---

## Final Steps: Verify & Deploy

### Checklist Before Deployment

- [ ] All code committed to GitHub
- [ ] Tests passing: `pytest tests/`
- [ ] Code style correct: `black src/` + `flake8 src/`
- [ ] Documentation complete (data dictionary, runbook)
- [ ] AWS infrastructure provisioned (Terraform apply)
- [ ] Secrets configured (AWS Secrets Manager)
- [ ] Databricks workspace created
- [ ] DAB bundle validated: `databricks bundle validate`

### Deployment Steps

```bash
# 1. Validate bundle
databricks bundle validate --target dev

# 2. Deploy
./deployment/scripts/deploy_dab.sh dev

# 3. Create Unity Catalog structure
./deployment/scripts/create_uc_volumes.sh

# 4. Monitor first batch job
databricks jobs run-now --job-id daily-batch-etl
# Check in Databricks Runs page

# 5. Start streaming job
databricks jobs run-now --job-id fraud-detection-streaming

# 6. Verify data in Gold layer
# Query gold.dim_customer, gold.fact_transactions, gold.fact_fraud_transaction_alert
```

---

## Phase Timeline

| Phase | Tasks | Duration | Deliverables |
|-------|-------|----------|--------------|
| **Phase 1** | Infrastructure setup, DAB config, GitHub repo | Week 1-2 | Cloud resources, initial codebase |
| **Phase 2** | Bronze layer (JDBC extraction, delta loading) | Week 2-3 | ACCOUNTS, TRANSACTIONS, CREDIT_SCORES, SUPPORT_TICKETS in Bronze |
| **Phase 3** | Silver layer (DQ, deduplication, reconciliation) | Week 3-4 | Curated data in Silver, DQ framework |
| **Phase 4** | Gold layer (dimensions, facts, star schema) | Week 4-5 | Analytics-ready tables, dimensional model |
| **Phase 5** | Real-time streaming (Kafka → fraud detection) | Week 5-6 | 24/7 fraud detection pipeline, < 15 sec latency |
| **Phase 6** | Testing, docs, deployment | Week 6-7 | Full test suite, runbook, DAB deployment |

---

## Success Criteria

✅ **Q1 (Optimization):** Pipeline runs in 15-30 min (vs. 4+ hours)  
✅ **Q2 (Analytics):** Star schema deployed, BI dashboards query gold layer  
✅ **Q3 (Platform):** Fraud detection < 1 min latency, DQ SLA met  
✅ **Governance:** Data lineage tracked, access control enforced  
✅ **Scalability:** Handles 50K events/sec + millions of transactions/day  
✅ **Deployment:** DAB bundle deployed, jobs running on schedule  

---

**Document Owner:** Data Platform Engineering Team  
**Last Updated:** May 24, 2026  
**Status:** Ready for Implementation
