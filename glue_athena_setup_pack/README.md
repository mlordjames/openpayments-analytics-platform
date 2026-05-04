# Glue + Athena Terraform Starter

This folder is for documentation and reproducibility for the AWS Glue Catalog + Athena base setup.

## What it creates
- S3 bucket for Athena query results
- Glue catalog database
- Athena workgroup

## What it does not create
- Glue crawler
- Glue ETL jobs
- Athena tables
- IAM users/roles/policies
- S3 raw-data bucket

Those are intentionally left out so this stays small and easy to understand.

## Usage
```bash
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

After apply:
1. Open Athena
2. Switch to the workgroup created here
3. Confirm the query result location is set
4. Create your raw database/table DDL