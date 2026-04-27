variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g., dev, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project Name"
  type        = string
  default     = "themis"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "airflow_instance_type" {
  description = "EC2 instance type for Airflow"
  type        = string
  default     = "t3.medium"
}

variable "db_username" {
  description = "RDS DB Username"
  type        = string
  default     = "airflow_user"
}

variable "db_password" {
  description = "RDS DB Password"
  type        = string
  default     = "SecurePass123!" # Ideally passed via TF_VAR_db_password
}

variable "ssh_key_name" {
  description = "Name of the SSH key pair to access EC2 instances"
  type        = string
  default     = "themis-key"
}
