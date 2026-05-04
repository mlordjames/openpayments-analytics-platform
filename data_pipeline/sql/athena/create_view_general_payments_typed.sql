CREATE OR REPLACE VIEW openpayments.vw_general_payments_typed AS
SELECT
    year AS s3_partition_year,
    CAST(program_year AS INTEGER) AS program_year,
    record_id,

    applicable_manufacturer_or_applicable_gpo_making_payment_name AS company_name,
    applicable_manufacturer_or_applicable_gpo_making_payment_id AS company_id,

    covered_recipient_type,
    covered_recipient_profile_id,
    covered_recipient_npi,
    covered_recipient_first_name,
    covered_recipient_middle_name,
    covered_recipient_last_name,
    covered_recipient_name_suffix,

    recipient_city,
    recipient_state,
    recipient_country,
    recipient_zip_code,

    covered_recipient_specialty_1 AS recipient_specialty,
    covered_recipient_primary_type_1 AS recipient_primary_type,

    nature_of_payment_or_transfer_of_value AS payment_nature,
    form_of_payment_or_transfer_of_value AS payment_form,

    TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE) AS payment_amount,
    TRY_CAST(date_parse(date_of_payment, '%m/%d/%Y') AS DATE) AS payment_date,
    date_format(TRY_CAST(date_parse(date_of_payment, '%m/%d/%Y') AS DATE), '%Y-%m') AS payment_month,

    physician_ownership_indicator,
    third_party_payment_recipient_indicator,
    charity_indicator,
    dispute_status_for_publication,
    product_indicator,
    payment_publication_date
FROM openpayments.raw_general_payments
WHERE TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE) IS NOT NULL;