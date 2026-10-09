terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project     = var.project
      environment = var.environment
      managed_by  = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  name         = "${var.project}-${var.environment}"
  clean_bucket = coalesce(var.clean_bucket_name, "${local.name}-clean-${data.aws_caller_identity.current.account_id}")
}
