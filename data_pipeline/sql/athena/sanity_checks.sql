-- 1) Confirm partitions are visible.
SHOW PARTITIONS openpayments.raw_general_payments;

-- 2) Total rows by S3 partition year.
SELECT year, COUNT(*) AS row_count
FROM openpayments.raw_general_payments
GROUP BY 1
ORDER BY 1;

-- 3) Basic sample rows.
SELECT *
FROM openpayments.raw_general_payments
LIMIT 20;

-- 4) Program year inside the files vs S3 partition year.
SELECT year, program_year, COUNT(*) AS row_count
FROM openpayments.raw_general_payments
GROUP BY 1, 2
ORDER BY 1, 2;

-- 5) Min / max payment date by year.
SELECT
    year,
    MIN(TRY_CAST(date_of_payment AS date)) AS min_payment_date,
    MAX(TRY_CAST(date_of_payment AS date)) AS max_payment_date,
    COUNT(*) AS row_count
FROM openpayments.raw_general_payments
GROUP BY 1
ORDER BY 1;

-- 6) Null / blank check on critical columns.
SELECT
    year,
    SUM(CASE WHEN applicable_manufacturer_or_applicable_gpo_making_payment_name IS NULL OR TRIM(applicable_manufacturer_or_applicable_gpo_making_payment_name) = '' THEN 1 ELSE 0 END) AS blank_company_name_rows,
    SUM(CASE WHEN total_amount_of_payment_usdollars IS NULL OR TRIM(total_amount_of_payment_usdollars) = '' THEN 1 ELSE 0 END) AS blank_amount_rows,
    SUM(CASE WHEN date_of_payment IS NULL OR TRIM(date_of_payment) = '' THEN 1 ELSE 0 END) AS blank_payment_date_rows
FROM openpayments.raw_general_payments
GROUP BY 1
ORDER BY 1;

-- 7) Top payment natures overall.
SELECT
    nature_of_payment_or_transfer_of_value,
    COUNT(*) AS payment_count
FROM openpayments.raw_general_payments
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
