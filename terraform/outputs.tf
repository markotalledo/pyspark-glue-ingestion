output "glue_job_name" {
  value = aws_glue_job.clean_events.name
}

output "clean_bucket" {
  value = aws_s3_bucket.clean.bucket
}

output "catalog_database" {
  value = aws_glue_catalog_database.clean.name
}
