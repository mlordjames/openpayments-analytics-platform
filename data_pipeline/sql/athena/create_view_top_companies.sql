CREATE OR REPLACE VIEW openpayments.vw_top_companies AS
SELECT
    company_name,
    s3_partition_year AS year,
    COUNT(*) AS payment_count,
    SUM(payment_amount) AS total_payment_amount,
    AVG(payment_amount) AS avg_payment_amount
FROM openpayments.vw_general_payments_typed
WHERE company_name IS NOT NULL
  AND trim(company_name) <> ''
GROUP BY 1, 2;