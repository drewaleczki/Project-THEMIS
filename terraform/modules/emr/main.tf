variable "project_name" { type = string }
variable "environment" { type = string }
variable "subnet_id" { type = string }
variable "vpc_id" { type = string }
variable "master_instance_type" { type = string }
variable "core_instance_type" { type = string }
variable "emr_service_role_name" { type = string }
variable "emr_ec2_role_name" { type = string }
variable "emr_ec2_profile_name" { type = string }
variable "log_uri" { type = string }

resource "aws_security_group" "emr_master" {
  name        = "${var.project_name}-${var.environment}-emr-master-sg"
  description = "Security group for EMR master"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "emr_core" {
  name        = "${var.project_name}-${var.environment}-emr-core-sg"
  description = "Security group for EMR core"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Allow Internal communication between Master and Core
resource "aws_security_group_rule" "emr_internal_master_core" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.emr_core.id
  security_group_id        = aws_security_group.emr_master.id
}

resource "aws_security_group_rule" "emr_internal_core_master" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.emr_master.id
  security_group_id        = aws_security_group.emr_core.id
}

resource "aws_emr_cluster" "cluster" {
  name          = "${var.project_name}-${var.environment}-emr-cluster"
  release_label = "emr-6.15.0"
  applications  = ["Spark", "Hadoop"]

  service_role = var.emr_service_role_name
  log_uri      = var.log_uri

  ec2_attributes {
    subnet_id                         = var.subnet_id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
    instance_profile                  = var.emr_ec2_profile_name
  }

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = 2
  }

  # Scale down behavior to prevent huge bills if left running
  step_concurrency_level = 1

  tags = {
    Name = "${var.project_name}-${var.environment}-emr-cluster"
  }
}

output "emr_cluster_id" {
  value = aws_emr_cluster.cluster.id
}
