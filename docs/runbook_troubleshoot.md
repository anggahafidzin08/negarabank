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
