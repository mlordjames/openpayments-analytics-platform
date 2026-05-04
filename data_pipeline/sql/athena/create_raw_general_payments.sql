-- Creates one raw Athena table over both 2023 and 2024 S3 partitions.
-- Partition column name must match the S3 folder key: year=2023 / year=2024.
DROP TABLE IF EXISTS openpayments.raw_general_payments;

CREATE EXTERNAL TABLE IF NOT EXISTS openpayments.raw_general_payments (
    change_type string,
    covered_recipient_type string,
    teaching_hospital_ccn string,
    teaching_hospital_id string,
    teaching_hospital_name string,
    covered_recipient_profile_id string,
    covered_recipient_npi string,
    covered_recipient_first_name string,
    covered_recipient_middle_name string,
    covered_recipient_last_name string,
    covered_recipient_name_suffix string,
    recipient_primary_business_street_address_line1 string,
    recipient_primary_business_street_address_line2 string,
    recipient_city string,
    recipient_state string,
    recipient_zip_code string,
    recipient_country string,
    recipient_province string,
    recipient_postal_code string,
    covered_recipient_primary_type_1 string,
    covered_recipient_primary_type_2 string,
    covered_recipient_primary_type_3 string,
    covered_recipient_primary_type_4 string,
    covered_recipient_primary_type_5 string,
    covered_recipient_primary_type_6 string,
    covered_recipient_specialty_1 string,
    covered_recipient_specialty_2 string,
    covered_recipient_specialty_3 string,
    covered_recipient_specialty_4 string,
    covered_recipient_specialty_5 string,
    covered_recipient_specialty_6 string,
    covered_recipient_license_state_code1 string,
    covered_recipient_license_state_code2 string,
    covered_recipient_license_state_code3 string,
    covered_recipient_license_state_code4 string,
    covered_recipient_license_state_code5 string,
    submitting_applicable_manufacturer_or_applicable_gpo_name string,
    applicable_manufacturer_or_applicable_gpo_making_payment_id string,
    applicable_manufacturer_or_applicable_gpo_making_payment_name string,
    applicable_manufacturer_or_applicable_gpo_making_payment_state string,
    applicable_manufacturer_or_applicable_gpo_making_payment_country string,
    total_amount_of_payment_usdollars string,
    date_of_payment string,
    number_of_payments_included_in_total_amount string,
    form_of_payment_or_transfer_of_value string,
    nature_of_payment_or_transfer_of_value string,
    city_of_travel string,
    state_of_travel string,
    country_of_travel string,
    physician_ownership_indicator string,
    third_party_payment_recipient_indicator string,
    name_of_third_party_entity_receiving_payment_or_transfer_of_value string,
    charity_indicator string,
    third_party_equals_covered_recipient_indicator string,
    contextual_information string,
    delay_in_publication_indicator string,
    record_id string,
    dispute_status_for_publication string,
    related_product_indicator string,
    covered_or_noncovered_indicator_1 string,
    indicate_drug_or_biological_or_device_or_medical_supply_1 string,
    product_category_or_therapeutic_area_1 string,
    name_of_drug_or_biological_or_device_or_medical_supply_1 string,
    associated_drug_or_biological_ndc_1 string,
    associated_device_or_medical_supply_pdi_1 string,
    covered_or_noncovered_indicator_2 string,
    indicate_drug_or_biological_or_device_or_medical_supply_2 string,
    product_category_or_therapeutic_area_2 string,
    name_of_drug_or_biological_or_device_or_medical_supply_2 string,
    associated_drug_or_biological_ndc_2 string,
    associated_device_or_medical_supply_pdi_2 string,
    covered_or_noncovered_indicator_3 string,
    indicate_drug_or_biological_or_device_or_medical_supply_3 string,
    product_category_or_therapeutic_area_3 string,
    name_of_drug_or_biological_or_device_or_medical_supply_3 string,
    associated_drug_or_biological_ndc_3 string,
    associated_device_or_medical_supply_pdi_3 string,
    covered_or_noncovered_indicator_4 string,
    indicate_drug_or_biological_or_device_or_medical_supply_4 string,
    product_category_or_therapeutic_area_4 string,
    name_of_drug_or_biological_or_device_or_medical_supply_4 string,
    associated_drug_or_biological_ndc_4 string,
    associated_device_or_medical_supply_pdi_4 string,
    covered_or_noncovered_indicator_5 string,
    indicate_drug_or_biological_or_device_or_medical_supply_5 string,
    product_category_or_therapeutic_area_5 string,
    name_of_drug_or_biological_or_device_or_medical_supply_5 string,
    associated_drug_or_biological_ndc_5 string,
    associated_device_or_medical_supply_pdi_5 string,
    program_year string,
    payment_publication_date string
)
PARTITIONED BY (
    year string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://openpayments-dezoomcamp2026-us-west-1-1f83ec/raw/dataset=general-payments/'
TBLPROPERTIES (
    'skip.header.line.count'='1'
);

-- Load year=2023 and year=2024 partitions from S3.
MSCK REPAIR TABLE openpayments.raw_general_payments;
