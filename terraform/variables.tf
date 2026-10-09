variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "shop-events"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "raw_bucket_name" {
  description = "Bucket written by events-pipeline-aws-terraform (its raw_bucket output)."
  type        = string
}

variable "clean_bucket_name" {
  description = "Override the clean bucket name. Defaults to <project>-<env>-clean-<account_id>."
  type        = string
  default     = null
}

variable "lookback_days" {
  description = "Event dates rebuilt per run. Must cover the maximum arrival lag of late events."
  type        = number
  default     = 3
}

variable "worker_count" {
  type    = number
  default = 2
}

variable "execution_class" {
  description = "FLEX runs on spare capacity at a lower price; fine for a daily batch that is not on a deadline."
  type        = string
  default     = "FLEX"

  validation {
    condition     = contains(["FLEX", "STANDARD"], var.execution_class)
    error_message = "execution_class must be FLEX or STANDARD."
  }
}
