CREATE OR REPLACE VIEW openpayments.vw_payments_monthly_by_year AS
SELECT
    s3_partition_year AS year,
    payment_month,
    COUNT(*) AS payment_count,
    SUM(payment_amount) AS total_payment_amount,
    AVG(payment_amount) AS avg_payment_amount,
    COUNT(DISTINCT company_name) AS distinct_companies
FROM openpayments.vw_general_payments_typed
WHERE payment_date IS NOT NULL
GROUP BY 1, 2;