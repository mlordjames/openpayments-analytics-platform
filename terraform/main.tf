terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_availability_zones" "available" {}

# --- AMI: Amazon Linux 2023
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

# ---------------------------
# Networking (VPC + Public Subnet)
# ---------------------------

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name    = "${var.name_prefix}-vpc"
    Project = var.bucket_tag_project_value
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name    = "${var.name_prefix}-igw"
    Project = var.bucket_tag_project_value
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.public_subnet_cidr
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = {
    Name    = "${var.name_prefix}-public-subnet"
    Project = var.bucket_tag_project_value
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name    = "${var.name_prefix}-public-rt"
    Project = var.bucket_tag_project_value
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# ---------------------------
# Security Group (SSH)
# ---------------------------

resource "aws_security_group" "runner" {
  name        = "${var.name_prefix}-sg"
  description = "Runner SG (SSH from whitelisted CIDR)"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.name_prefix}-sg"
    Project = var.bucket_tag_project_value
  }
}

# ---------------------------
# S3 Bucket (optional)
# ---------------------------

resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  effective_bucket_name = (
    trim(var.bucket_name) != ""
    ? var.bucket_name
    : "${var.name_prefix}-${var.aws_region}-${random_id.suffix.hex}"
  )
}

resource "aws_s3_bucket" "this" {
  count  = var.create_bucket ? 1 : 0
  bucket = local.effective_bucket_name

  tags = {
    Name    = local.effective_bucket_name
    Project = var.bucket_tag_project_value
  }
}

# ---------------------------
# IAM Role + Policy + Instance Profile
# ---------------------------

data "aws_iam_policy_document" "assume_role_ec2" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ec2_role" {
  name               = "${var.name_prefix}-ec2-runner-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role_ec2.json
}

# Tag-scoped "bucket-agnostic" S3 permissions:
# Any bucket tagged Project=openpayments is accessible (list + location + writes).
data "aws_iam_policy_document" "s3_tag_scoped" {
  statement {
    sid       = "ListTaggedBuckets"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::*"]

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.bucket_tag_project_value]
    }
  }

  statement {
    sid       = "BucketLocationForTaggedBuckets"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::*"]

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.bucket_tag_project_value]
    }
  }

  statement {
    sid    = "WriteObjectsToTaggedBuckets"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts"
    ]
    resources = ["arn:aws:s3:::*/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.bucket_tag_project_value]
    }
  }
}

resource "aws_iam_policy" "s3_write" {
  name   = "${var.name_prefix}-s3-tag-scoped-write"
  policy = data.aws_iam_policy_document.s3_tag_scoped.json
}

resource "aws_iam_role_policy_attachment" "attach_s3_write" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = aws_iam_policy.s3_write.arn
}

# Optional: SSM access (nice for ops)
resource "aws_iam_role_policy_attachment" "attach_ssm" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "this" {
  name = "${var.name_prefix}-instance-profile"
  role = aws_iam_role.ec2_role.name
}

# ---------------------------
# EC2 + User Data (fixed for Amazon Linux 2023)
# ---------------------------

locals {
  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail

    LOG=/var/log/user-data.log
    exec > >(tee -a $LOG) 2>&1

    echo "[INFO] Updating packages..."
    dnf -y update

    echo "[INFO] Installing git + docker..."
    dnf -y install git docker

    echo "[INFO] Installing docker compose v2 plugin..."
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

    echo "[INFO] Preparing repo dir: ${var.repo_dir}"
    mkdir -p ${var.repo_dir}
    chown -R ec2-user:ec2-user ${var.repo_dir}

    if [ ! -d "${var.repo_dir}/.git" ]; then
      echo "[INFO] Cloning repo..."
      su - ec2-user -c "git clone ${var.repo_url} ${var.repo_dir}"
    else
      echo "[INFO] Repo already present; skipping clone."
    fi

    echo "[INFO] Done. (Docker not started; you will SSH in and run anything manually.)"
  EOF
}

resource "aws_instance" "runner" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.runner.id]
  key_name                    = var.ssh_key_name
  iam_instance_profile        = aws_iam_instance_profile.this.name
  associate_public_ip_address = true

  user_data = local.user_data

  tags = {
    Name    = "${var.name_prefix}-${var.aws_region}"
    Project = var.bucket_tag_project_value
  }
}

output "bucket_name" {
  value = local.effective_bucket_name
}

output "instance_public_ip" {
  value = aws_instance.runner.public_ip
}
