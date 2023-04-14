terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  profile = "de_project"
  region  = "us-west-1"
}

locals {
  aws_profile           = "de_project" # must be the same as aws profile provider
  project_name          = "de-project"
  resource_name         = "${local.project_name}-${var.env}"
  scrapper_folder       = "../scrapper/data"
  remote_folder         = "/data"
  log_retention_in_days = 3
  tags = {
    project    = local.project_name
    managed_by = "terraform"
  }
}

resource "aws_s3_bucket" "de_project" {
  bucket = local.resource_name
  tags   = local.tags
}

resource "null_resource" "de_project" {
  provisioner "local-exec" {
    command = "aws s3 sync ${local.scrapper_folder} s3://${aws_s3_bucket.de_project.bucket}${local.remote_folder} --profile ${local.aws_profile}"
  }
}

output "bucket" {
  value       = aws_s3_bucket.de_project.bucket
  description = "Bucket name"
}

output "files" {
  value       = local.scrapper_folder
  description = "scrapper_folder"
}
