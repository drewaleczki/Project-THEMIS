variable "project_name" { type = string }
variable "environment" { type = string }
variable "instance_type" { type = string }
variable "subnet_id" { type = string }
variable "vpc_id" { type = string }
variable "ssh_key_name" { type = string }
variable "airflow_profile_name" { type = string }

resource "aws_security_group" "airflow_sg" {
  name        = "${var.project_name}-${var.environment}-airflow-sg"
  description = "Security group for Airflow EC2 instance"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]
  iam_instance_profile   = var.airflow_profile_name

  user_data = <<-EOF
              #!/bin/bash
              apt-get update -y
              apt-get install -y apt-transport-https ca-certificates curl software-properties-common python3-pip python3-venv git
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
              add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
              apt-get update -y
              apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
              usermod -aG docker ubuntu
              
              mkdir -p /opt/airflow
              cd /opt/airflow
              curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.3/docker-compose.yaml'
              mkdir -p ./dags ./logs ./plugins ./config
              
              # Download pipeline scripts from Git and inject into Airflow
              git clone https://github.com/drewaleczki/Project-THEMIS.git /tmp/Project-THEMIS || echo "Git clone failed (maybe private repo?)"
              if [ -d "/tmp/Project-THEMIS" ]; then
                cp -r /tmp/Project-THEMIS/airflow/dags/* ./dags/
                cp -r /tmp/Project-THEMIS/pipelines ./dags/
                cp -r /tmp/Project-THEMIS/spark_jobs ./dags/
              fi
              
              echo -e "AIRFLOW_UID=$(id -u ubuntu)" > .env
              
              # Wait for docker to be ready
              systemctl enable docker
              systemctl start docker
              
              # Start Airflow
              docker compose up -d
              EOF

  tags = {
    Name = "${var.project_name}-${var.environment}-airflow"
  }
}

output "airflow_public_ip" {
  value = aws_instance.airflow.public_ip
}
