variable "project_name" { type = string }
variable "environment" { type = string }
variable "s3_bronze_arn" { type = string }
variable "s3_silver_arn" { type = string }
variable "s3_gold_arn" { type = string }
variable "s3_logs_arn" { type = string }

# Airflow EC2 Role
resource "aws_iam_role" "airflow_role" {
  name = "${var.project_name}-${var.environment}-airflow-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "airflow_s3_policy" {
  name = "${var.project_name}-${var.environment}-airflow-s3-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          var.s3_bronze_arn, "${var.s3_bronze_arn}/*",
          var.s3_silver_arn, "${var.s3_silver_arn}/*",
          var.s3_gold_arn, "${var.s3_gold_arn}/*",
          var.s3_logs_arn, "${var.s3_logs_arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["elasticmapreduce:RunJobFlow", "elasticmapreduce:AddJobFlowSteps", "elasticmapreduce:DescribeStep", "elasticmapreduce:ListSteps", "elasticmapreduce:TerminateJobFlows", "elasticmapreduce:DescribeCluster", "iam:PassRole"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartCrawler", "glue:GetCrawler"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_s3_attach" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_s3_policy.arn
}

resource "aws_iam_instance_profile" "airflow_profile" {
  name = "${var.project_name}-${var.environment}-airflow-profile"
  role = aws_iam_role.airflow_role.name
}

# EMR Service Role
resource "aws_iam_role" "emr_service_role" {
  name = "${var.project_name}-${var.environment}-emr-service-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_attach" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# EMR EC2 Profile
resource "aws_iam_role" "emr_ec2_role" {
  name = "${var.project_name}-${var.environment}-emr-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2_attach" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_ec2_profile" {
  name = "${var.project_name}-${var.environment}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2_role.name
}

# Glue Service Role
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-${var.environment}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_s3_policy" {
  name = "${var.project_name}-${var.environment}-glue-s3-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [var.s3_gold_arn, "${var.s3_gold_arn}/*"]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

output "airflow_role_name" { value = aws_iam_role.airflow_role.name }
output "airflow_profile_name" { value = aws_iam_instance_profile.airflow_profile.name }
output "emr_service_role_name" { value = aws_iam_role.emr_service_role.name }
output "emr_ec2_role_name" { value = aws_iam_role.emr_ec2_role.name }
output "emr_ec2_profile_name" { value = aws_iam_instance_profile.emr_ec2_profile.name }
output "glue_role_arn" { value = aws_iam_role.glue_role.arn }
