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
