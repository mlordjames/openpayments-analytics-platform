variable "aws_region" {
  type    = string
  default = "us-west-1"
}

variable "name_prefix" {
  type    = string
  default = "openpayments-dezoomcamp2026"
}

variable "instance_type" {
  type    = string
  default = "t3.micro"
}

variable "ssh_key_name" {
  type        = string
  description = "Existing EC2 Key Pair name in this region"
  default     = "openpayments-key"
}

variable "ssh_allowed_cidr" {
  type        = string
  description = "Your public IP in CIDR form (example: 197.210.77.10/32). Prefix ranges like /24 are possible if your ISP assigns within that block."
  default     = "0.0.0.0/0"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "public_subnet_cidr" {
  type    = string
  default = "10.20.1.0/24"
}

variable "repo_url" {
  type    = string
  default = "https://github.com/mlordjames/data-zoomcamp.git"
}

variable "repo_dir" {
  type    = string
  default = "/opt/data-zoomcamp"
}

variable "create_bucket" {
  type    = bool
  default = true
}

variable "bucket_name" {
  type        = string
  description = "Optional fixed bucket name. Leave empty to auto-generate with suffix."
  default     = ""
}

variable "bucket_tag_project_value" {
  type    = string
  default = "openpayments"
}
