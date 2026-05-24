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
