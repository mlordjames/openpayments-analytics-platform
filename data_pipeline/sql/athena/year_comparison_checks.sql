-- Year-over-year comparison checks for the 2023 vs 2024 dashboard story.

-- 1) Total rows and total payment amount by year.
SELECT
    year,
    COUNT(*) AS payment_row_count,
    ROUND(SUM(TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE)), 2) AS total_payment_amount_usd
FROM openpayments.raw_general_payments
GROUP BY 1
ORDER BY 1;

-- 2) Monthly totals by year.
SELECT
    year,
    DATE_TRUNC('month', TRY_CAST(date_of_payment AS date)) AS payment_month,
    COUNT(*) AS payment_count,
    ROUND(SUM(TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE)), 2) AS total_payment_amount_usd
FROM openpayments.raw_general_payments
WHERE TRY_CAST(date_of_payment AS date) IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

-- 3) Nature of payment comparison by year.
SELECT
    year,
    nature_of_payment_or_transfer_of_value,
    COUNT(*) AS payment_count,
    ROUND(SUM(TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE)), 2) AS total_payment_amount_usd
FROM openpayments.raw_general_payments
GROUP BY 1, 2
ORDER BY 1, total_payment_amount_usd DESC;

-- 4) Top recipient states by year.
SELECT
    year,
    recipient_state,
    COUNT(*) AS payment_count,
    ROUND(SUM(TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE)), 2) AS total_payment_amount_usd
FROM openpayments.raw_general_payments
WHERE recipient_state IS NOT NULL AND TRIM(recipient_state) <> ''
GROUP BY 1, 2
ORDER BY 1, total_payment_amount_usd DESC;

-- 5) Quick YoY delta summary.
WITH yearly AS (
    SELECT
        year,
        ROUND(SUM(TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE)), 2) AS total_payment_amount_usd
    FROM openpayments.raw_general_payments
    GROUP BY 1
)
SELECT
    a.year AS base_year,
    b.year AS comparison_year,
    a.total_payment_amount_usd AS base_total,
    b.total_payment_amount_usd AS comparison_total,
    ROUND(b.total_payment_amount_usd - a.total_payment_amount_usd, 2) AS absolute_change_usd,
    ROUND(((b.total_payment_amount_usd - a.total_payment_amount_usd) / NULLIF(a.total_payment_amount_usd, 0)) * 100, 2) AS pct_change
FROM yearly a
JOIN yearly b ON a.year = '2023' AND b.year = '2024';
