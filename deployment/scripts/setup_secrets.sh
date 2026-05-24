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
