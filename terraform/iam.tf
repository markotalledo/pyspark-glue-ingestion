data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-clean-events"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

# Logs, metrics and the Data Catalog.
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Read-only on raw, read-write on clean, nothing else.
data "aws_iam_policy_document" "glue_data" {
  statement {
    sid       = "ReadRaw"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.raw_bucket_name}", "arn:aws:s3:::${var.raw_bucket_name}/events/*"]
  }
  statement {
    sid       = "WriteClean"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.clean.arn, "${aws_s3_bucket.clean.arn}/*"]
  }
}

resource "aws_iam_role_policy" "glue_data" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data.json
}
