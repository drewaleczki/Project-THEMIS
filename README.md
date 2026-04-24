# Project THEMIS ⚖️

A Production-Grade Big Data platform for ingesting, processing, and analyzing Brazilian public political data (e.g., Electoral Data, Parliamentary Expenses).

## Data Sources

This project natively consumes public electoral transparency data from the **Tribunal Superior Eleitoral (TSE)**. The Airflow pipelines are configured to extract `.zip` files directly from the [Portal de Dados Abertos do TSE](https://dadosabertos.tse.jus.br/). Datasets consumed include candidates demographics, campaign finance (receipts and expenses), asset declarations, and voting results.

## Architecture

This project is built using **AWS** and managed entirely via **Terraform** using a **Medallion Architecture**.

* **S3 Data Lake**: Segregated into Bronze (Raw), Silver (Cleaned, Parquet format), and Gold (Aggregated Business Logic).
* **Compute Engine**: Apache Spark running on transient Amazon EMR clusters.
* **Orchestration**: Apache Airflow running on a dedicated Amazon EC2 instance (Dockerized).
* **Data Catalog & Querying**: AWS Glue Data Catalog and Amazon Athena.
* **CI/CD**: Fully automated deployment and teardown using GitHub Actions.

## Project Structure

```text
├── .github/
│   └── workflows/
│       ├── terraform-deploy.yml   # CI/CD pipeline for provisioning infrastructure
│       └── terraform-destroy.yml  # Manual pipeline for destroying infrastructure
├── airflow/
│   └── dags/
│       └── themis_dag.py          # Orchestration pipeline
├── pipelines/
│   └── ingest_data.py             # Python script for data ingestion (APIs/CSVs)
├── spark_jobs/
│   ├── bronze_to_silver.py        # PySpark Bronze to Silver transformation
│   ├── data_quality.py            # Data quality checks on the Silver layer
│   └── silver_to_gold.py          # PySpark Silver to Gold aggregation
├── terraform/
│   ├── modules/
│   │   ├── airflow_ec2/           # EC2 instance bootstrapping Airflow
│   │   ├── emr/                   # EMR cluster template and security groups
│   │   ├── iam/                   # IAM Roles for EMR, Airflow, and S3 access
│   │   ├── networking/            # VPC, Subnets, IGW, Route Tables
│   │   └── s3_datalake/           # Bronze, Silver, Gold, and Logs buckets
│   ├── main.tf                    # Root module calling all components
│   ├── providers.tf               # AWS provider and S3 Backend configuration
│   └── variables.tf               # Terraform input variables
├── .gitignore                     # Git configuration ignoring states and credentials
└── README.md
```

## CI/CD with GitHub Actions

This project is configured with automated processes to provision and destroy cloud resources via **GitHub Actions**.

### Prerequisites (Before using Actions)
1. **S3 Backend**: Create an S3 Bucket in your AWS account to store the `terraform.tfstate`. Change the default bucket name in `terraform/providers.tf` (`themis-terraform-state-bucket-unique-123`) to your real bucket name.
2. **GitHub Secrets**: In your repository, go to **Settings > Secrets and variables > Actions**, and create the following Repository Secrets:
   - `AWS_ACCESS_KEY_ID`: Your IAM access key.
   - `AWS_SECRET_ACCESS_KEY`: Your IAM secret key.

> [!WARNING]
> Ensure the `.gitignore` remains intact so you do not accidentally upload your local `.tfstate` or any `.tfvars` files. Required credentials MUST only exist in GitHub Actions Secrets!

### Deploying the Infrastructure
Whenever a **push is made to the `main` branch** modifying files inside `/terraform`, the `Terraform Deploy` workflow will automatically map and deploy the changes to your AWS account.
* You can also trigger it manually by navigating to the **Actions** tab → **Terraform Deploy** → **Run Workflow**.

### Destroying the Infrastructure
To clean up your AWS account and prevent unexpected billing:
1. Go to the **Actions** tab in GitHub.
2. Select the **Terraform Destroy** workflow.
3. Click _Run workflow_.
4. Type `destroy` (lowercase) in the confirmation prompt. The environment will be completely wiped out in a few minutes.

## How to Run the Pipeline (Post-Deploy)

1. Go to your AWS EC2 Console and find the instance named `themis-dev-airflow`.
2. Copy its public IP.
3. Access Airflow at `http://<EC2-PUBLIC-IP>:8080`.
4. Run the DAG `themis_orchestration_pipeline`.

### Pipeline Execution Flow

1. **Ingestion**: `pipelines/ingest_data.py` executes, pulling large datasets from sources into the S3 Bronze bucket.
2. **Cluster Creation**: Airflow provisions a transient EMR PySpark Cluster.
3. **Bronze -> Silver**: Cleans the data, rectifies encodings to `utf-8`, and writes Snappy compressed Parquet.
4. **Data Quality**: Validates that records don't contain null core identifiers or negative values.
5. **Silver -> Gold**: Aggregates campaign expenditures and creates the analytical Parquet layer.
6. **Cluster Termination**: The cluster terminates automatically to save costs.

## Querying Data (Amazon Athena)

Once the data reaches the Gold layer, you can create an AWS Glue Crawler to index the data. To query it, use **Amazon Athena**:

```sql
-- Example query calculating total funds raised per candidate
SELECT nome_candidato, total_arrecadado
FROM "themis_gold_db"."campaigns_analytical"
ORDER BY total_arrecadado DESC
LIMIT 10;
```

## Troubleshooting & Failure Mitigations

* **Schema Changes in Input**: If the input CSV changes columns, the standard `data_quality.py` step will catch missing core columns and fail the Silver step, preventing corruption in Gold.
* **Cost Overruns**: EMR clusters are explicitly configured in Airflow to terminate on the `all_done` trigger rule, guaranteeing shutdown even on failures.
