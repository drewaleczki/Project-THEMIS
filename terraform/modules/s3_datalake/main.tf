variable "project_name" { type = string }
variable "environment" { type = string }

# Bronze Layer
resource "aws_s3_bucket" "bronze" {
  bucket        = "${var.project_name}-${var.environment}-datalake-bronze"
  force_destroy = true
}
resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Silver Layer
resource "aws_s3_bucket" "silver" {
  bucket        = "${var.project_name}-${var.environment}-datalake-silver"
  force_destroy = true
}

# Gold Layer
resource "aws_s3_bucket" "gold" {
  bucket        = "${var.project_name}-${var.environment}-datalake-gold"
  force_destroy = true
}

# Logs Bucket (for EMR and Airflow)
resource "aws_s3_bucket" "logs" {
  bucket        = "${var.project_name}-${var.environment}-datalake-logs"
  force_destroy = true
}

output "bronze_bucket_name" { value = aws_s3_bucket.bronze.id }
output "bronze_bucket_arn" { value = aws_s3_bucket.bronze.arn }

output "silver_bucket_name" { value = aws_s3_bucket.silver.id }
output "silver_bucket_arn" { value = aws_s3_bucket.silver.arn }

output "gold_bucket_name" { value = aws_s3_bucket.gold.id }
output "gold_bucket_arn" { value = aws_s3_bucket.gold.arn }

output "logs_bucket_name" { value = aws_s3_bucket.logs.id }
output "logs_bucket_uri" { value = "s3://${aws_s3_bucket.logs.id}/" }
