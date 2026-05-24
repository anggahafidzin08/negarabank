-- Bronze Layer: Schema Validation
-- Check that required columns exist and have correct types

SELECT
    table_name,
    column_name,
    data_type,
    nullable,
    CASE
        WHEN column_name IN ('account_id', 'customer_id', 'transaction_id') THEN 'required_pk'
        WHEN column_name IN ('load_date', 'load_timestamp') THEN 'required_metadata'
        ELSE 'optional'
    END as column_category,
    CASE
        WHEN nullable = false AND column_category IN ('required_pk', 'required_metadata') THEN 'pass'
        ELSE 'warn'
    END as validation_status
FROM information_schema.columns
WHERE table_schema = 'bronze'
ORDER BY table_name, ordinal_position;
