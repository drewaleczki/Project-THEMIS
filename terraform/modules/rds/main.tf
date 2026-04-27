variable "project_name" { type = string }
variable "environment" { type = string }
variable "vpc_id" { type = string }
variable "db_subnet_group_name" { type = string }
variable "airflow_sg_id" { type = string }
variable "db_username" { type = string }
variable "db_password" { type = string }

resource "aws_security_group" "rds_sg" {
  name        = "${var.project_name}-${var.environment}-rds-sg"
  description = "Security group for Airflow RDS"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.airflow_sg_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "airflow_db" {
  identifier             = "${var.project_name}-${var.environment}-airflow-db"
  allocated_storage      = 20
  engine                 = "postgres"
  engine_version         = "15.4" # AWS default supported version
  instance_class         = "db.t3.micro"
  username               = var.db_username
  password               = var.db_password
  db_subnet_group_name   = var.db_subnet_group_name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  publicly_accessible    = false
  skip_final_snapshot    = true

  tags = {
    Name = "${var.project_name}-${var.environment}-airflow-db"
  }
}

output "rds_endpoint" {
  value = aws_db_instance.airflow_db.endpoint
}
