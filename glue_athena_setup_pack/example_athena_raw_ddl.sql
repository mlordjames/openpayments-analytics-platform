-- Example starter DDL only.
-- Adjust columns and serde details to match your actual raw files.

CREATE DATABASE IF NOT EXISTS openpayments;

CREATE EXTERNAL TABLE IF NOT EXISTS openpayments.raw_general_payments (
  change_type string,
  covered_recipient_type string,
  teaching_hospital_ccn string,
  teaching_hospital_id string,
  teaching_hospital_name string,
  physician_profile_id string,
  physician_first_name string,
  physician_middle_name string,
  physician_last_name string,
  physician_name_suffix string,
  recipient_primary_business_street_address_line1 string,
  recipient_primary_business_street_address_line2 string,
  recipient_city string,
  recipient_state string,
  recipient_zip_code string,
  recipient_country string,
  recipient_province string,
  recipient_postal_code string,
  physician_primary_type string,
  physician_specialty string,
  physician_license_state_code1 string,
  physician_license_state_code2 string,
  physician_license_state_code3 string,
  physician_license_state_code4 string,
  physician_license_state_code5 string,
  submitting_applicable_manufacturer_or_applicable_gpo_name string,
  applicable_manufacturer_or_applicable_gpo_making_payment_id string,
  applicable_manufacturer_or_applicable_gpo_making_payment_name string,
  applicable_manufacturer_or_applicable_gpo_making_payment_state string,
  applicable_manufacturer_or_applicable_gpo_making_payment_country string,
  total_amount_of_payment_usdollars string,
  date_of_payment string,
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
  name_of_associated_covered_drug_or_biological1 string,
  name_of_associated_covered_device_or_medical_supply1 string,
  reporting_year string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://YOUR-RAW-BUCKET/openpayments/raw/general_payments/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);