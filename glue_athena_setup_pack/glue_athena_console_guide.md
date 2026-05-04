# Glue + Athena Console Guide for First-Time Setup

This guide assumes:
- your raw data for 2023 and 2024 is already in S3
- you want a clean manual setup
- you are not using a Glue crawler yet
- you will create the raw table with SQL in Athena

## What to create first

### 1) Create or confirm an S3 bucket or prefix for Athena query results
Athena needs a query result location before it can run queries.

Example:
- Bucket: `your-athena-results-bucket`
- Prefix: `query-results/`

This is separate from your raw data bucket.

### 2) Create an Athena workgroup
In the Athena console:
- open Athena
- go to Workgroups
- click Create workgroup
- name it something like `openpayments-wg`
- under query result configuration, set the S3 output location
- save

Use this workgroup for the project so settings stay consistent.

### 3) Create a Glue catalog database
You can create the database either:
- in Glue > Data Catalog > Databases
- or directly in Athena using SQL

Simplest path:
```sql
CREATE DATABASE IF NOT EXISTS openpayments;
```

Athena uses the Glue Data Catalog for databases and tables.

### 4) Create the raw table in Athena
In Athena:
- choose the `openpayments-wg` workgroup
- choose the `openpayments` database
- open the query editor
- run your raw table DDL pointing to the raw S3 location

Example raw location:
- `s3://your-raw-bucket/openpayments/raw/general_payments/`

## Recommended first-time console flow

### Option A: easiest path
1. Create Athena results bucket in S3
2. Create Athena workgroup
3. Open Athena query editor
4. Run `CREATE DATABASE`
5. Run `CREATE EXTERNAL TABLE`
6. Run sanity queries

This is the cleanest path for your project.

### Option B: use Glue console first
1. Open Glue
2. Create database
3. Open Athena
4. Use that database
5. Create the table in Athena

This also works, but Athena-first is simpler for your use case.

## Why manual SQL instead of crawler
For your project, manual SQL is better because:
- you already know the dataset
- it is easier to document in the repo
- it is easier to reproduce
- it avoids crawler guesswork on schema

## What Glue is doing here
Glue is mainly acting as the catalog/metastore:
- database name
- table name
- schema
- S3 location

You do not need to run a Glue job just to query S3 data with Athena.

## What Athena is doing here
Athena is your SQL query layer over the files already stored in S3.

You will use it to:
- validate the raw landing
- query both 2023 and 2024
- support dbt source models
- inspect outputs before the dashboard layer

## What to do right after setup
Run these first checks:
- row count
- min/max payment date
- count by reporting year
- top companies
- nature of payment distribution

## Suggested repo files to add next
- `data_pipeline/sql/athena/create_database.sql`
- `data_pipeline/sql/athena/create_raw_general_payments.sql`
- `data_pipeline/sql/athena/sanity_checks.sql`
- `data_pipeline/sql/athena/top_companies.sql`
- `data_pipeline/sql/athena/year_comparison_checks.sql`