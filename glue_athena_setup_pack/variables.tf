variable "aws_region" {
  description = "AWS region for Glue/Athena resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "openpayments"
}

variable "glue_database_name" {
  description = "Glue catalog database name"
  type        = string
  default     = "openpayments"
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name"
  type        = string
  default     = "openpayments-wg"
}

variable "athena_results_bucket_name" {
  description = "Optional custom bucket name for Athena query results. Leave blank to auto-build one."
  type        = string
  default     = ""
}

variable "athena_results_prefix" {
  description = "Prefix inside the Athena results bucket"
  type        = string
  default     = "query-results"
}