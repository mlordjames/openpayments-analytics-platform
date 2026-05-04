-- Top companies overall by total raw payment amount.
SELECT
    applicable_manufacturer_or_applicable_gpo_making_payment_name AS company_name,
    COUNT(*) AS payment_count,
    ROUND(SUM(TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE)), 2) AS total_payment_amount_usd,
    ROUND(AVG(TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE)), 2) AS avg_payment_amount_usd
FROM openpayments.raw_general_payments
GROUP BY 1
ORDER BY total_payment_amount_usd DESC
LIMIT 25;

-- Top companies by year.
SELECT
    year,
    applicable_manufacturer_or_applicable_gpo_making_payment_name AS company_name,
    COUNT(*) AS payment_count,
    ROUND(SUM(TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE)), 2) AS total_payment_amount_usd
FROM openpayments.raw_general_payments
GROUP BY 1, 2
ORDER BY year, total_payment_amount_usd DESC
LIMIT 50;
