module "networking" {
  source = "./modules/networking"

  vpc_cidr     = var.vpc_cidr
  project_name = var.project_name
  environment  = var.environment
  aws_region   = var.aws_region
}

module "s3_datalake" {
  source = "./modules/s3_datalake"

  project_name = var.project_name
  environment  = var.environment
}

module "iam" {
  source = "./modules/iam"

  project_name  = var.project_name
  environment   = var.environment
  s3_bronze_arn = module.s3_datalake.bronze_bucket_arn
  s3_silver_arn = module.s3_datalake.silver_bucket_arn
  s3_gold_arn   = module.s3_datalake.gold_bucket_arn
  s3_logs_arn   = module.s3_datalake.logs_bucket_arn
}

module "airflow_ec2" {
  source = "./modules/airflow_ec2"

  project_name         = var.project_name
  environment          = var.environment
  instance_type        = var.airflow_instance_type
  subnet_id            = module.networking.public_subnet_id
  vpc_id               = module.networking.vpc_id
  ssh_key_name         = var.ssh_key_name
  airflow_profile_name = module.iam.airflow_profile_name
  rds_endpoint         = module.rds.rds_endpoint
  db_username          = var.db_username
  db_password          = var.db_password
}

module "rds" {
  source = "./modules/rds"

  project_name         = var.project_name
  environment          = var.environment
  vpc_id               = module.networking.vpc_id
  db_subnet_group_name = module.networking.db_subnet_group_name
  airflow_sg_id        = module.airflow_ec2.airflow_sg_id
  db_username          = var.db_username
  db_password          = var.db_password
}

module "glue" {
  source         = "./modules/glue"
  project_name   = var.project_name
  environment    = var.environment
  glue_role_arn  = module.iam.glue_role_arn
  gold_bucket_id = module.s3_datalake.gold_bucket_name
}
