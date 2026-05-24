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
