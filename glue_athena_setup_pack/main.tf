terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  athena_results_bucket_name = var.athena_results_bucket_name != "" ? var.athena_results_bucket_name : "${var.project_name}-${data.aws_caller_identity.current.account_id}-${var.aws_region}-athena-results"
}

resource "aws_s3_bucket" "athena_results" {
  bucket = local.athena_results_bucket_name

  tags = {
    Project = var.project_name
    Purpose = "athena-query-results"
  }
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_glue_catalog_database" "openpayments" {
  name        = var.glue_database_name
  description = "Glue catalog database for CMS Open Payments analytics platform"

  tags = {
    Project = var.project_name
    Purpose = "glue-catalog-database"
  }
}

resource "aws_athena_workgroup" "openpayments" {
  name = var.athena_workgroup_name

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/${var.athena_results_prefix}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  state = "ENABLED"

  tags = {
    Project = var.project_name
    Purpose = "athena-workgroup"
  }
}

output "glue_database_name" {
  value = aws_glue_catalog_database.openpayments.name
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.openpayments.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.bucket
}

output "athena_results_path" {
  value = "s3://${aws_s3_bucket.athena_results.bucket}/${var.athena_results_prefix}/"
}