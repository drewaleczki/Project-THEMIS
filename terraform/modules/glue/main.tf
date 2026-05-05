variable "project_name" { type = string }
variable "environment" { type = string }
variable "glue_role_arn" { type = string }
variable "gold_bucket_id" { type = string }

resource "aws_glue_catalog_database" "gold_db" {
  name = "themis_gold_db"
}

resource "aws_glue_crawler" "gold_crawler" {
  database_name = aws_glue_catalog_database.gold_db.name
  name          = "${var.project_name}-${var.environment}-gold-crawler"
  role          = var.glue_role_arn

  s3_target {
    path = "s3://${var.gold_bucket_id}/tse/campaign_analytics/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}
