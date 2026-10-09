data "archive_file" "package" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/build/clean_events.zip"
  excludes    = ["glue_entry.py", "clean_events/__pycache__"]
}

resource "aws_s3_object" "package" {
  bucket = aws_s3_bucket.clean.id
  key    = "_glue/clean_events.zip"
  source = data.archive_file.package.output_path
  etag   = data.archive_file.package.output_md5
}

resource "aws_s3_object" "entry" {
  bucket = aws_s3_bucket.clean.id
  key    = "_glue/glue_entry.py"
  source = "${path.module}/../src/glue_entry.py"
  etag   = filemd5("${path.module}/../src/glue_entry.py")
}

resource "aws_glue_catalog_database" "clean" {
  name = replace("${local.name}_clean", "-", "_")
}

resource "aws_glue_job" "clean_events" {
  name              = "${local.name}-clean-events"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = var.worker_count
  execution_class   = var.execution_class
  timeout           = 30
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.clean.id}/${aws_s3_object.entry.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--extra-py-files"                   = "s3://${aws_s3_bucket.clean.id}/${aws_s3_object.package.key}"
    "--raw_path"                         = "s3://${var.raw_bucket_name}/events"
    "--clean_path"                       = "s3://${aws_s3_bucket.clean.id}"
    "--lookback_days"                    = tostring(var.lookback_days)
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}
