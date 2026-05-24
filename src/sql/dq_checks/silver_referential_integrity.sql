-- Silver Layer: Referential Integrity Checks
-- Detect orphaned records (transactions without matching account)

SELECT
    'transactions_fk_accounts' as check_name,
    COUNT(*) as orphan_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM silver.transactions), 2) as orphan_pct,
    MAX(load_date) as check_date
FROM silver.transactions t
WHERE t.account_id NOT IN (SELECT account_id FROM silver.accounts)
GROUP BY 1

UNION ALL

SELECT
    'transactions_fk_customers' as check_name,
    COUNT(*) as orphan_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM silver.transactions), 2) as orphan_pct,
    MAX(load_date) as check_date
FROM silver.transactions t
WHERE t.customer_id NOT IN (SELECT customer_id FROM silver.accounts)
GROUP BY 1;
