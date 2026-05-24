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
