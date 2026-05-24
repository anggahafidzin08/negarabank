-- Silver Layer: Referential Integrity Checks
-- Detect orphaned records (transactions without matching account)

SELECT
    'transactions_fk_accounts' as check_name,
    COUNT(*) as orphan_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM silver.transactions), 2) as orphan_pct,
    MAX(t.load_date) as check_date
FROM silver.transactions t
LEFT JOIN silver.accounts a ON t.account_id = a.account_id
WHERE a.account_id IS NULL
GROUP BY 1

UNION ALL

SELECT
    'transactions_fk_customers' as check_name,
    COUNT(*) as orphan_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM silver.transactions), 2) as orphan_pct,
    MAX(t.load_date) as check_date
FROM silver.transactions t
LEFT JOIN silver.accounts a ON t.customer_id = a.customer_id
WHERE a.customer_id IS NULL
GROUP BY 1;
