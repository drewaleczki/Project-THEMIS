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
}

module "emr" {
  source = "./modules/emr"

  project_name          = var.project_name
  environment           = var.environment
  subnet_id             = module.networking.public_subnet_id
  vpc_id                = module.networking.vpc_id
  master_instance_type  = var.emr_master_instance_type
  core_instance_type    = var.emr_core_instance_type
  emr_service_role_name = module.iam.emr_service_role_name
  emr_ec2_role_name     = module.iam.emr_ec2_role_name
  emr_ec2_profile_name  = module.iam.emr_ec2_profile_name
  log_uri               = module.s3_datalake.logs_bucket_uri
}
