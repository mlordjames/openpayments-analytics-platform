# Athena SQL files

These SQL files are aligned to the current project setup:

- bucket: `openpayments-dezoomcamp2026-us-west-1-1f83ec`
- raw path: `s3://openpayments-dezoomcamp2026-us-west-1-1f83ec/raw/dataset=general-payments/`
- partitions: `year=2023/`, `year=2024/`
- database: `openpayments`
- table: `raw_general_payments`

## Run order

1. `create_database.sql`
2. `create_raw_general_payments.sql`
3. `sanity_checks.sql`
4. `top_companies.sql`
5. `year_comparison_checks.sql`

## Important note

The Athena partition column is `year` because the S3 folder keys are `year=2023` and `year=2024`.
If the partition column name and the S3 folder key do not match, `MSCK REPAIR TABLE` will not load partitions.

# Revised Athena SQL pack

This pack rewrites the Athena SQL files using the uploaded sample raw CSV and the confirmed S3 layout.

## Included
- `create_database.sql`
- `create_raw_general_payments.sql`
- `sanity_checks.sql`
- `top_companies.sql`
- `year_comparison_checks.sql`
- `athena_glue_manual_sql_guide.md`




The shortest useful list of Glue/Athena features for this project is:

Glue database
Logical container for tables in the Data Catalog.
Glue crawler
Scans S3 files and creates or updates table schema automatically.
Glue Data Catalog table
Metadata definition that makes your S3 files queryable in Athena.
Athena workgroup
Controls query settings like result location and execution settings.
Athena query result location
S3 path where Athena writes query outputs.
Athena external table
Table definition pointing to files in S3 without moving data.
Athena partitions
Lets Athena prune data by year=2023, year=2024, etc.
MSCK REPAIR TABLE
Loads discovered partitions into Athena for partitioned tables.
Athena views
Saved SQL logic for simplified querying.
Crawler recrawl policy
Controls whether the crawler rechecks everything or only new folders.
Schema change policy
Controls how crawler handles new or changed columns.
Partition projection
Advanced Athena feature to avoid storing partitions explicitly in Glue.
Data preview in Athena console
Fast way to inspect whether the table works.
Saved queries in Athena
Good for storing sanity checks and reusable validation SQL.
Glue ETL jobs
Not needed yet, but could be used later for file conversion or preprocessing.