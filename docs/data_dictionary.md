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
