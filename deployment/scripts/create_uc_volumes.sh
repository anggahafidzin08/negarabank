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
