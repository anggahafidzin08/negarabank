# NegaraBank Data Platform Architecture Design

**Document Date:** May 24, 2026  
**Status:** Approved for Implementation  
**Audience:** Data Engineers, ML Engineers, BI Analysts, Platform Architects

---

## Executive Summary

This document outlines the complete data platform architecture for NegaraBank's Assessment Test implementation. The platform addresses three integrated requirements:

- **Q1:** Optimize slow transaction snapshot pipeline (currently 4+ hours → target: minutes)
- **Q2:** Build medallion architecture (Bronze/Silver/Gold) for business analytics
- **Q3:** Design enterprise platform supporting batch + real-time fraud detection

**Design Approach:** Hybrid architecture combining batch ETL (Oracle JDBC) with real-time streaming (Kafka) on Databricks, leveraging Delta Lake for ACID compliance, and dimensionally modeled gold layer for BI/ML.

---

## 1. Technology Stack & Infrastructure

### Core Technologies

| Layer | Technology | Justification |
|-------|-----------|----------------|
| **Cloud Platform** | AWS (EC2, S3, VPC, IAM, Secrets Manager) | Familiar with team, flexible, mature ecosystem |
| **Data Ingestion - Batch** | JDBC (Oracle) on EC2 | Simple, no CDC overhead, 2-3 min startup acceptable |
| **Data Ingestion - Streaming** | Kafka (AWS MSK or self-managed) | Proven for 50K events/sec, fraud detection infra |
| **Stream Processing** | Databricks Structured Streaming | Native Delta Lake integration, auto-scaling |
| **Batch Processing** | Databricks Spark Jobs | Cost-efficient, partition pruning, 10-100x cheaper than streaming |
| **Data Warehouse** | Databricks on Delta Lake (S3 backend) | ACID compliance, schema enforcement, time-travel, unified governance |
| **Warehouse Governance** | Databricks Unity Catalog | Centralized metadata, access control, lineage, compliance |
| **BI Tools** | Tableau / Power BI | DirectQuery on Delta = live dashboards |
| **ML Platform** | Databricks Feature Store + MLflow | Feature lineage, model versioning, real-time serving |
| **Data Quality** | Databricks DQ Framework (Spark SQL) | Validation rules, anomaly detection, SLA monitoring |
| **Governance** | Unity Catalog ACL + Audit Logs | Fine-grained access, PII masking, compliance audit |

### Infrastructure (AWS + Databricks)

```
┌─ AWS Account
│  ├─ VPC (Private networking)
│  │  ├─ Private Subnet (EC2 JDBC Gateway)
│  │  │  ├─ Security Group (Oracle ingress only)
│  │  │  └─ EC2 Instance (t3.large, 2-3 min startup)
│  │  │
│  │  └─ Databricks PrivateLink (secure tunnel to workspace)
│  │
│  ├─ S3 Buckets
│  │  ├─ negarabank-bronze/ (raw data)
│  │  ├─ negarabank-silver/ (curated data)
│  │  ├─ negarabank-gold/ (analytics-ready)
│  │  ├─ negarabank-checkpoints/ (streaming fault tolerance)
│  │  └─ negarabank-features/ (ML feature store)
│  │
│  ├─ Secrets Manager (Oracle credentials, API keys)
│  │
│  ├─ IAM Roles
│  │  ├─ EC2-JDBC-Role (minimal: S3 + Secrets Manager)
│  │  └─ Databricks-Role (S3 full, Unity Catalog admin)
│  │
│  └─ MSK Kafka Cluster (multi-AZ, 3 brokers)
│
└─ Databricks Workspace
   ├─ Batch Cluster (on-demand, 2-8 workers, 2-3 min startup)
   ├─ Streaming Cluster (persistent 24/7, for fraud detection)
   └─ Interactive Cluster (for exploration/testing)
```

### Compute Specifications

**Batch Cluster (Oracle JDBC):**
- Trigger: Scheduled daily job (e.g., 2 AM)
- Node type: i3en.3xlarge (8 cores, 96 GB memory)
- Min workers: 2, Max workers: 8
- Startup time: 2-3 minutes (acceptable per requirements)
- Shutdown: Auto-terminate after job completes (cost optimization)

**Streaming Cluster (Real-time Fraud):**
- Always-on 24/7 (fraud detection SLA = minutes)
- Node type: i3en.2xlarge (4 cores, 64 GB memory)
- Min workers: 2, Max workers: 4
- Autoscaling: Based on Kafka lag

---

## 2. Data Architecture: Medallion + Dimensional Hybrid

### Medallion Layers (Bronze → Silver → Gold)

#### 2.1 Bronze Layer (Raw Data)

**Purpose:** Capture data as-is from source systems (zero transformations).

**Data Sources:**
- Oracle JDBC:
  - ACCOUNTS (static master, daily snapshot)
  - TRANSACTIONS (millions/day, delta loading)
  - CREDIT_SCORES (daily snapshot)
  - SUPPORT_TICKETS (batch ETL)
- Kafka:
  - Mobile clickstream events (50K events/sec)

**Storage:**
- Format: Parquet (Bronze) + Delta (for upserts)
- Location: `s3://negarabank-bronze/{source}/{table}/`
- Partitioning: `load_date` (one partition per batch run, daily)

**Data Quality (Minimal):**
- Schema validation only (column presence, type checking)
- No transformations (keep as-is from source)

**Example Tables:**
```
bronze.accounts_raw
├─ account_id (PK)
├─ customer_id
├─ account_type
├─ balance
├─ open_date
└─ load_date (partition)

bronze.transactions_raw
├─ transaction_id (PK)
├─ account_id
├─ amount
├─ txn_date
├─ status
└─ load_date (partition)

bronze.mobile_events_raw
├─ event_id (PK)
├─ customer_id
├─ event_type
├─ timestamp
├─ event_data (JSON)
└─ load_timestamp (partition by hour)
```

#### 2.2 Silver Layer (Curated Data)

**Purpose:** Data quality, deduplication, reconciliation, schema normalization.

**Transformations:**
1. Data Quality Checks
   - Null/blank validation
   - Type casting (strings → timestamps, decimals)
   - Constraint validation (PK, FK integrity)

2. Deduplication
   - Exact duplicates (row hash)
   - Fuzzy duplicates (mobile events by session_id)
   - Mobile events: deduplicate by session within 5-min window

3. Reconciliation
   - Orphaned records (transactions without matching account)
   - Cross-system consistency checks
   - Account balance sanity checks

4. Schema Normalization
   - Consistent naming (snake_case)
   - Standardized timestamp formats (UTC)
   - Enumeration mapping (status codes → descriptions)

**Storage:**
- Format: Delta Lake (ACID, schema-enforced)
- Location: `s3://negarabank-silver/{source}/{table}/`
- Partitioning: `load_date` (matches Bronze)

**Data Quality Tests:**
- Referential integrity (transactions.account_id exists in accounts)
- Freshness SLA (data < 24 hours old)
- Completeness (< 0.5% nulls on required fields)
- Statistical bounds (amounts within expected range)

**Example Tables:**
```
silver.accounts_curated
├─ account_id
├─ customer_id
├─ account_type
├─ balance
├─ status
├─ open_date
├─ dq_passed (boolean)
└─ load_date

silver.transactions_curated
├─ transaction_id
├─ account_id
├─ amount
├─ txn_date
├─ status
├─ reconciled (boolean, orphan check)
└─ load_date

silver.mobile_events_curated
├─ event_id
├─ session_id (deduplication key)
├─ customer_id
├─ event_type
├─ event_timestamp
├─ is_duplicate (flag for excluded rows)
└─ load_timestamp
```

#### 2.3 Gold Layer (Business-Ready Analytics & ML)

**Purpose:** Dimensional model + real-time fraud features for BI, ML, analysts.

**Two-Path Architecture:**

**Path A: Star Schema (for BI/Analytics)**
```
Dimensions:
├─ dim_customer (SCD Type 2: tracks history)
│  ├─ customer_id (PK)
│  ├─ name, email, phone
│  ├─ segment, risk_score
│  ├─ effective_date, end_date (for SCD tracking)
│  └─ is_current (boolean)
│
├─ dim_account
│  ├─ account_id (PK)
│  ├─ customer_id (FK)
│  ├─ account_type
│  ├─ status
│  └─ open_date
│
├─ dim_date
│  ├─ date_key (PK, YYYYMMDD)
│  ├─ date, month, quarter, year
│  ├─ day_of_week, is_weekend, is_holiday
│  └─ fiscal_period
│
└─ dim_mobile_event_type
   ├─ event_type_id (PK)
   ├─ event_type
   ├─ category
   ├─ is_sensitive
   └─ description

Facts:
├─ fact_transactions
│  ├─ transaction_key (PK)
│  ├─ customer_key (FK → dim_customer)
│  ├─ account_key (FK → dim_account)
│  ├─ date_key (FK → dim_date)
│  ├─ amount
│  ├─ status
│  ├─ created_at
│  └─ load_date (partition)
│
├─ fact_mobile_events
│  ├─ event_key (PK)
│  ├─ customer_key (FK)
│  ├─ date_key (FK)
│  ├─ event_type_key (FK)
│  ├─ event_count
│  ├─ event_timestamp
│  └─ load_date (partition)
│
└─ fact_customer_fraud_risk (Daily Batch)
   ├─ customer_key (PK)
   ├─ date_key (PK)
   ├─ fraud_risk_score (0.0 - 1.0)
   ├─ fraud_indicator (HIGH/MEDIUM/LOW)
   ├─ last_fraud_date
   ├─ fraud_count_12m
   └─ model_version
```

**Path B: Real-time Fraud Table (for Minutes SLA)**
```
fact_fraud_transaction_alert (DENORMALIZED for speed)
├─ transaction_id (PK, upsertable)
├─ customer_id
├─ account_id
├─ amount
├─ event_timestamp
├─ event_count_24h (feature)
├─ avg_transaction_amount (feature)
├─ account_balance
├─ fraud_score (0.0 - 1.0)
├─ fraud_alert_status (HIGH_RISK/MEDIUM_RISK/LOW_RISK)
├─ model_version
├─ processing_timestamp
├─ alert_sent (boolean)
├─ event_date (partition)
└─ load_timestamp (partition by hour, 7-day retention)
```

**Storage Strategy:**

| Table | Storage Tier | Duration | Use Case |
|-------|--------------|----------|----------|
| fact_fraud_transaction_alert | S3 Standard (Delta) | 7 days (hot) | Real-time fraud dashboard |
| fact_fraud_transaction_alert | S3-IA | 8-90 days (warm) | Historical analysis |
| fact_fraud_transaction_alert | S3-Glacier | 91 days-3 years (cold) | Compliance/audit |
| dim_* and fact_transactions | S3 Standard (Delta) | 30 days | Current month analytics |
| fact_transactions | S3-IA | 31 days-7 years | Historical analytics + tax/regulatory |
| dim_customer | S3 Standard (Delta) | Indefinite (SCD) | Always hot, supports SCD Type 2 |

---

## 3. Real-Time Fraud Detection Pipeline

### Architecture

```
Mobile App → Kafka (mobile_clickstream topic)
                ↓
         Databricks Structured Streaming Job
                ├─ Parse & validate events
                ├─ Deduplication (session_id, 5-min window)
                ├─ Broadcast join: dim_customer, dim_account
                ├─ Feature engineering (24h event count, amount anomaly)
                ├─ MLflow model inference (real-time)
                └─ Upsert to fact_fraud_transaction_alert
                ↓
         fact_fraud_transaction_alert (Delta Lake)
                ├─ Schema: [transaction_id, customer_id, fraud_score, alert_status]
                ├─ Partitioned by event_date
                ├─ 7-day retention (hot)
                └─ Upsertable by transaction_id
                ↓
         Downstream:
         ├─ Kafka sink: fraud_alerts topic (for mobile app notifications)
         ├─ REST API: MLflow serving endpoint (/predict)
         └─ Power BI: Live query (1-min auto-refresh)
```

### Latency SLA

**Target:** Minutes (< 5 minutes from event to alert)

| Component | Latency | Notes |
|-----------|---------|-------|
| Kafka ingest | ~100ms | Mobile app → broker |
| Spark micro-batch | 5-10s | Default 5-sec trigger |
| Feature lookups (broadcast) | 1-2s | In-memory join |
| ML inference | 500ms | MLflow single model |
| Delta write (upsert) | 2-3s | Append-only + merge |
| **Total E2E** | **~10-15 seconds** | ✅ Well under SLA |

### Fault Tolerance

- **Checkpoint:** Streaming job saves state to `s3://negarabank-checkpoints/`
- **Recovery:** On failure, resumes from last checkpoint
- **Exactly-once semantics:** No duplicate alerts (transaction_id upsert key)
- **Dead-letter queue:** Failed events → Kafka topic for manual review

### Model Refresh

- **Batch retraining:** Nightly (2 AM, 5-min SLA)
- **MLflow registry:** New version registered automatically
- **Deployment:** Streaming job picks up new version (zero-downtime switch)
- **Rollback:** Previous version always available in MLflow

---

## 4. Data Flow: Batch & Streaming Integration

### Daily Batch ETL (Oracle → Bronze → Silver → Gold)

```
Schedule: 2 AM daily (configurable)

1. Start EC2 JDBC Gateway + Databricks Cluster (~2-3 min)

2. Extract from Oracle (JDBC with delta loading)
   ├─ ACCOUNTS: Full snapshot (small, static)
   ├─ TRANSACTIONS: Incremental load
   │  └─ Use: WHERE txn_date >= {yesterday}
   │  └─ Benefit: 10-100x cost savings vs. full table scan
   ├─ CREDIT_SCORES: Daily snapshot
   └─ SUPPORT_TICKETS: Batch load (last 24 hours)

3. Load to Bronze Layer
   └─ Append to partitioned Parquet/Delta
   └─ Partition key: load_date

4. Transform Bronze → Silver
   ├─ Data quality checks (validation SQL)
   ├─ Deduplication
   ├─ Reconciliation (orphan detection)
   └─ Schema normalization

5. Transform Silver → Gold
   ├─ Build star schema (dim_customer SCD, fact_transactions)
   ├─ Aggregate daily fraud risk (for fact_customer_fraud_risk)
   └─ Partition by date for retention tiers

6. Data Quality Gate
   ├─ Run DQ tests (completeness, freshness, integrity)
   ├─ If 99.5% pass → commit
   ├─ If fail → alert data team + block downstream

7. Shutdown EC2 + Databricks Cluster (cost optimization)
   └─ Auto-terminate after job completes

Duration: 15-30 minutes (vs. 4+ hours in original)
```

### Real-Time Streaming (24/7)

```
Mobile App → Kafka (continuous ingestion)

Streaming Cluster (persistent, 24/7):
├─ Consume from mobile_clickstream
├─ Join with broadcast dim_customer, dim_account (latest)
├─ Enrich with last 24h transaction history
├─ Score with fraud model (real-time inference)
└─ Upsert to fact_fraud_transaction_alert (Delta)

Latency: 10-15 seconds (event → alert)
SLA: Minutes for fraud detection
```

### Convergence at Gold Layer

Both batch and streaming write to the same Gold layer:

```
Batch Path:
Silver.transactions_curated → Gold.fact_transactions
Silver.accounts_curated → Gold.dim_customer (SCD)

Streaming Path:
Kafka events → Gold.fact_fraud_transaction_alert

BI/ML Query Example (joins both paths):
SELECT
  t.transaction_id,
  t.customer_id,
  f.fraud_score,
  f.fraud_alert_status,
  c.customer_segment
FROM gold.fact_transactions t
LEFT JOIN gold.fact_fraud_transaction_alert f
  ON t.transaction_id = f.transaction_id
LEFT JOIN gold.dim_customer c
  ON t.customer_id = c.customer_id
WHERE t.transaction_date = CURRENT_DATE()
```

---

## 5. BI & ML Serving Layer

### BI Tools Integration

**Tableau / Power BI → Databricks SQL (DirectQuery)**

```
Connection: Delta Lake native connector
Latency: Live (5-10 seconds refresh)
Query pushdown: Databricks optimizes queries

Dashboards:
├─ Real-time Fraud Detection
│  ├─ HIGH_RISK count (last 1h)
│  ├─ Fraud trend (24h)
│  └─ Data source: fact_fraud_transaction_alert
│
├─ Customer Behavior Analytics
│  ├─ Active users (daily)
│  ├─ Event types (distribution)
│  └─ Data source: fact_mobile_events + dim_customer
│
├─ Transaction Analytics
│  ├─ Transaction volume by type
│  ├─ Top customers by amount
│  └─ Data source: fact_transactions + dim_account
│
└─ Credit Risk Dashboard
   ├─ Fraud risk distribution
   ├─ High-risk customer segments
   └─ Data source: fact_customer_fraud_risk
```

### ML Platform (Feature Store + MLflow)

**Databricks Feature Store:**
- Online store: Redis (low-latency inference)
- Offline store: Delta Lake (training data)

**Feature Tables:**
```
customer_fraud_profile
├─ fraud_score (latest)
├─ fraud_count_12m
├─ days_since_last_fraud
└─ Lineage: Tracked automatically

customer_transaction_behavior
├─ avg_transaction_amount
├─ transaction_frequency_24h
├─ max_transaction_amount_7d
└─ Lineage: Tracked automatically

customer_mobile_behavior
├─ event_count_24h
├─ device_fingerprint_entropy
├─ location_variance
└─ Lineage: Tracked automatically
```

**MLflow Model Registry:**
```
Fraud Detection Model
├─ Version 1 (Production)
│  ├─ Algorithm: XGBoost
│  ├─ Features: 15 from Feature Store
│  ├─ AUC: 0.92
│  └─ Serving: REST API + Streaming
│
└─ Version 2 (Staging)
   ├─ Algorithm: LightGBM
   ├─ Features: 18 from Feature Store
   ├─ AUC: 0.94 (better, ready for A/B test)
   └─ Serving: Model serving endpoint
```

**Inference Paths:**
1. Real-time (Streaming): Called from streaming job every 5 seconds
2. Batch (Weekly): Score all customers, update fact_customer_fraud_risk
3. API (On-demand): REST endpoint for ad-hoc scoring

---

## 6. Data Governance Layer

### 6.1 Data Quality Framework

**DQ Checks (runs post-load):**

1. Schema Validation
   - Column presence (required fields exist)
   - Type validation (strings/numbers/dates)
   - Constraint enforcement (NOT NULL, length limits)

2. Data Profiling
   - Null % (flag if > 5%)
   - Cardinality (distinct values in expected range)
   - Distribution checks (outliers outside σ3)

3. Referential Integrity
   - Foreign key validation (account_id exists)
   - Orphaned records (transactions without account)

4. Business Rules
   - Fraud score range (0.0 - 1.0)
   - Amount reasonableness (< account balance)
   - Timestamp ordering (no time travel)

**DQ SLA:**
- 99.5% completeness (< 0.5% nulls)
- 99.9% schema validation pass
- 100% referential integrity
- Data freshness: < 1 minute (fraud), < 24 hours (batch)

### 6.2 Metadata & Catalog

**Unity Catalog (Built-in Databricks):**

Every table has:
- Owner (Data Steward)
- Description (business meaning)
- Domain (Fraud, Transactions, CRM)
- Sensitivity (PUBLIC, INTERNAL, CONFIDENTIAL)
- PII columns (flagged for masking)
- Retention policy
- SLA (max acceptable age)
- Tags (searchable)

**Example:**
```
Table: fact_fraud_transaction_alert
├─ Owner: fraud-team@negarabank.com
├─ Description: Real-time fraud alerts from streaming pipeline
├─ Domain: Fraud Detection
├─ Sensitivity: CONFIDENTIAL
├─ PII columns: [customer_id, account_id]
├─ Retention: 7 days hot, 90 days warm, 3 years cold
├─ SLA: Data < 1 minute old
└─ Tags: ["realtime", "ml-ready", "prod"]
```

### 6.3 Data Lineage

**Auto-tracked via SQL (Databricks Lineage):**

```
Upstream (dependencies):
fact_fraud_transaction_alert
├─ Depends on: mobile_events_curated (Silver)
├─ Depends on: accounts_curated (Silver)
├─ Depends on: MLflow fraud model v1

Downstream (consumers):
mobile_events_curated
├─ Used by: fact_fraud_transaction_alert (Gold)
├─ Used by: fact_mobile_events (Gold)
├─ Used by: Tableau "Behavior" dashboard
└─ Used by: MLflow model retraining

Impact Analysis:
If mobile_events_curated changes schema:
├─ fact_fraud_transaction_alert will fail
├─ Alert sent to downstream owners
└─ Tableau dashboard breaks (owner notified)
```

### 6.4 Access Control (Unity Catalog ACL)

**Fine-grained permissions:**

```
BRONZE (Raw):
├─ Data Engineers: SELECT, MODIFY
├─ DBAs: SELECT (audit only)
└─ Others: DENY

SILVER (Curated):
├─ Data Engineers: SELECT, MODIFY
├─ Data Analysts: SELECT (read-only)
├─ ML Engineers: SELECT, MODIFY
└─ BI Teams: SELECT

GOLD (Analytics):
├─ BI Teams: SELECT (all PUBLIC tables)
├─ ML Engineers: SELECT (all PUBLIC tables)
├─ Fraud Team: SELECT, MODIFY (CONFIDENTIAL tables only)
├─ Data Analysts: SELECT (all)
└─ Column masking (customer_ssn → MASKED)
```

### 6.5 Audit & Compliance

**Built-in Audit Logs (Unity Catalog):**

```
Logged Events:
├─ SELECT queries (who, when, which table)
├─ INSERT/UPDATE/DELETE (by whom, what changed)
├─ GRANT/REVOKE (permissions changed)
├─ Schema changes (columns added/removed)
└─ Access denials (unauthorized attempts)

Compliance Use Cases:
├─ PII access report (who accessed customer_ssn?)
├─ Data change audit (who modified fraud_alerts?)
├─ Incident investigation (root cause)
└─ Regulatory reporting (OJK compliance)
```

### 6.6 Data Retention & Archival

**Automated S3 Intelligent Tiering:**

```
Hot (Days 1-7): S3 Standard (Delta Lake)
├─ fact_fraud_transaction_alert
└─ Fast reads for real-time dashboard

Warm (Days 8-90): S3-IA (Infrequent Access)
├─ Historical fraud for trend analysis
└─ Slower reads acceptable

Cold (Days 91-1095): S3-Glacier (3-year retention)
├─ Compliance/audit only
├─ OJK requirement
└─ Automatic purge after 3 years
```

### 6.7 Data Stewardship

```
BRONZE: Data Engineering team
├─ Responsibility: Data quality, SLA
├─ On-call: Weekly rotation
└─ Alert channel: Slack #data-eng-alerts

SILVER: Data Engineering team
├─ Responsibility: Transformation logic, testing
├─ Code review: Required before merge
└─ DQ tests: Must pass before deploy

GOLD - Fraud: Fraud team + Data team
├─ Responsibility: Feature accuracy, model SLA
├─ On-call: Fraud ops (24/7)
└─ Alert channel: #fraud-alerts

GOLD - Analytics: Analytics team
├─ Responsibility: Dashboard accuracy, business logic
├─ Contact: analytics@negarabank.com
└─ Channel: Publish data dictionary
```

---

## 7. Deployment & Operations

### Databricks Asset Bundle (DAB)

**Structure:**
```
negarabank-pipeline/
├─ databricks.yml (workspace config, job definitions)
├─ src/
│  ├─ bronze/ (JDBC extraction jobs)
│  ├─ silver/ (transformation notebooks)
│  ├─ gold/ (star schema building)
│  └─ streaming/ (fraud detection job)
├─ tests/
│  ├─ dq_tests/ (data quality validations)
│  └─ integration_tests/
└─ docs/
   └─ data_dictionary.md
```

**Deployment:**
```bash
# Deploy to dev workspace
databricks bundle deploy --target dev

# Deploy to prod workspace
databricks bundle deploy --target prod
```

**Job Definitions (in DAB):**
- `daily_batch_etl`: Oracle extraction + Bronze → Silver → Gold (2 AM)
- `fraud_detection_streaming`: Real-time Kafka consumer (24/7)
- `data_quality_check`: DQ validations (post-load)
- `model_retraining`: Nightly fraud model update (2 AM)

### Monitoring & Alerting

**Metrics:**
- Job duration (target: 15-30 min for batch)
- Data freshness (data age < SLA)
- Fraud detection latency (< 15 sec)
- Data quality score (target: 99.5%)
- Model accuracy (AUC, precision, recall)

**Alerting:**
- Slack: #data-eng-alerts, #fraud-alerts
- Email: On-call team
- Dashboard: Monitoring dashboard in Tableau

---

## 8. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Streaming cluster failure | Checkpoint recovery, exactly-once semantics |
| Oracle JDBC timeout | EC2 gateway with connection pooling, retry logic |
| Model degradation | Weekly retraining, MLflow versioning, A/B testing |
| Data quality issues | Automated DQ checks, SLA monitoring, alerting |
| PII data exposure | Column-level masking, access control, audit logs |
| Compliance violation | Retention policies, audit trail, stewardship model |

---

## 9. Timeline & Phases

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| **Phase 1: Infrastructure** | Week 1-2 | VPC, EC2, MSK Kafka, IAM roles, Secrets Manager |
| **Phase 2: Batch ETL** | Week 2-3 | Bronze/Silver/Gold layers, JDBC extraction, delta loading |
| **Phase 3: Streaming** | Week 3-4 | Kafka consumer, streaming job, fraud detection |
| **Phase 4: Governance** | Week 4-5 | Unity Catalog, DQ framework, access control, audit |
| **Phase 5: BI/ML Integration** | Week 5-6 | Feature Store, MLflow, Tableau dashboards |
| **Phase 6: Testing & Validation** | Week 6-7 | Load testing, failover testing, compliance validation |

---

## 10. Success Criteria

✅ **Q1 (Optimization):** Pipeline runs in 15-30 minutes (vs. 4+ hours)  
✅ **Q2 (Analytics):** Star schema deployed, BI dashboards live  
✅ **Q3 (Platform):** Fraud detection < 1 minute latency, DQ SLA met  
✅ **Compliance:** Data governance audit-ready (OJK requirements)  
✅ **Scalability:** Handles 50K events/sec + millions of transactions/day  

---

## Appendix: Technology Justifications

### Why Kafka + Databricks Streaming?
- Proven for high-throughput event ingestion (50K events/sec)
- Exactly-once semantics (no duplicate fraud alerts)
- Fault-tolerant with checkpointing
- Integrates natively with Databricks for low latency

### Why Delta Lake?
- ACID compliance (no partial writes on failure)
- Schema enforcement (prevents data quality issues)
- Time-travel (audit trail for compliance)
- Partitioning + Z-ordering (query optimization)
- Unified batch + streaming (single storage format)

### Why Star Schema in Gold?
- BI tools navigate naturally (Tableau/Power BI)
- Slowly Changing Dimensions (SCD Type 2) for history
- Scales better than denormalization for 10+ table joins
- Single source of truth for business metrics

### Why Hybrid (Denormalized + Dimensional)?
- Real-time fraud detection needs speed (single table lookup)
- Analytics needs clean schema (star schema)
- Both paths write to same Gold layer (no duplicate pipelines)
- ML engineers get both fast features + rich context

### Why S3 Intelligent Tiering?
- Automatic cost optimization (moves data between tiers)
- No manual process for hot → warm → cold
- Compliance-friendly (7-year tax retention, 3-year audit retention)
- Native S3 integration (no separate data lake tool needed)

---

**Document Owner:** Data Platform Team  
**Last Updated:** May 24, 2026  
**Next Review:** After Phase 2 implementation
