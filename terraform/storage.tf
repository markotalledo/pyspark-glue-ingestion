resource "aws_s3_bucket" "clean" {
  bucket        = local.clean_bucket
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_public_access_block" "clean" {
  bucket                  = aws_s3_bucket.clean.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "clean" {
  bucket = aws_s3_bucket.clean.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "clean" {
  bucket = aws_s3_bucket.clean.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "clean" {
  bucket = aws_s3_bucket.clean.id

  rule {
    id     = "expire-rejected"
    status = "Enabled"
    filter {
      prefix = "rejected/"
    }
    expiration {
      days = 30
    }
  }
}
