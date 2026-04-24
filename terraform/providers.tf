terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Configure o backend para o GitHub Actions não perder o state
  # Crie este bucket s3 na AWS antes de rodar a pipeline!
  backend "s3" {
    bucket = "themis-terraform-state-bucket-unique-123"
    key    = "themis/terraform.tfstate"
    region = "us-east-1"
    # dynamodb_table = "themis-terraform-locks" # Recomendado para ambientes reais
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "THEMIS"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}
