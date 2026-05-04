# Athena + Glue guide for the current project

This guide matches the current S3 layout:

- `s3://openpayments-dezoomcamp2026-us-west-1-1f83ec/raw/dataset=general-payments/year=2023/`
- `s3://openpayments-dezoomcamp2026-us-west-1-1f83ec/raw/dataset=general-payments/year=2024/`
- `s3://openpayments-dezoomcamp2026-us-west-1-1f83ec/athena-query-results/`

## 1) Use Glue Data Catalog
What it means:
Glue Data Catalog stores the database, table, column, and partition metadata that Athena reads when it queries S3 data.

Why it matters here:
- Athena needs a catalog definition for the raw table.
- The database and table you create in Athena are written to the Glue Data Catalog.
- dbt-athena will later query through this metadata layer.

Project use:
- Database: `openpayments`
- Table: `raw_general_payments`
- Partition column: `s3_year`

## 2) Create Athena tables manually with SQL
What it means:
Instead of relying on a crawler, you define the table schema yourself in Athena using DDL.

Why it matters here:
- Reproducible in Git
- Controlled column names and types
- Easier to document and review
- Better fit for a project repo

Project use:
- Run `create_database.sql`
- Run `create_raw_general_payments.sql`
- Athena writes this metadata into the Glue Data Catalog automatically

Official docs:
- Athena can create tables with DDL in the query editor: `CREATE TABLE` / external table flow.
- Athena uses the Glue Data Catalog to store the resulting metadata.

## 3) Run sanity SQL in Athena
What it means:
Run basic validation queries directly against the raw table before dbt.

Why it matters here:
- Confirms partitions loaded
- Confirms 2023 and 2024 both exist
- Checks dates, nulls, duplicates, top values
- Gives screenshot evidence for Day 1 / Day 2

Project use:
- Run `sanity_checks.sql`
- Run `top_companies.sql`
- Run `year_comparison_checks.sql`

## 4) Query Glue catalog metadata from Athena
What it means:
Athena exposes metadata via `information_schema`, so you can inspect databases, tables, and columns.

Useful examples:
```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'openpayments'
ORDER BY 1, 2;

SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'openpayments'
  AND table_name = 'raw_general_payments'
ORDER BY ordinal_position;
```

Why it matters here:
- Confirms the raw table exists
- Confirms the column definitions match what you expect
- Good for debugging before dbt

## 5) Use partitions in Athena
What it means:
Partitions map known folder structure values like `year=2023` to a partition column.

Why it matters here:
- Lets you query `WHERE s3_year = '2023'`
- Cleaner than separate tables per year
- Better match for year-over-year analysis

Project use:
- Table is partitioned by `s3_year`
- S3 layout already supports this
- `MSCK REPAIR TABLE` loads the discovered partitions

Examples:
```sql
SELECT COUNT(*)
FROM openpayments.raw_general_payments
WHERE s3_year = '2023';
```

## 6) Use partition projection in Athena
What it means:
Athena can derive partitions from table properties instead of reading them from Glue partition metadata.

Why it matters:
- Good for predictable partition structures
- Avoids repeated `MSCK REPAIR TABLE` or manual partition adds
- Useful when new partitions are added regularly

Should we use it now?
Maybe later, but not first.

Reason:
- Your project only has 2023 and 2024 right now
- Standard partitions are simpler to understand and document
- Better to get the table working first, then upgrade to projection if needed

What it would look like:
- Keep `s3_year` as the partition column
- Add projection properties in the table definition
- Athena would infer `2023` and `2024` without loading partition metadata

Good later use case:
- When Airflow keeps adding future years and you want zero partition maintenance

## Recommended order in the Athena console
1. Set query result location to:
   `s3://openpayments-dezoomcamp2026-us-west-1-1f83ec/athena-query-results/`
2. Run `create_database.sql`
3. Run `create_raw_general_payments.sql`
4. Check the table in the Athena left sidebar
5. Run:
   ```sql
   SELECT s3_year, COUNT(*)
   FROM openpayments.raw_general_payments
   GROUP BY 1
   ORDER BY 1;
   ```
6. Run the sanity and comparison SQL files
7. Save screenshots

## Metadata inspection queries to keep
```sql
-- Tables in the project database
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'openpayments'
ORDER BY 1, 2;

-- Columns in the raw table
SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'openpayments'
  AND table_name = 'raw_general_payments'
ORDER BY ordinal_position;
```
