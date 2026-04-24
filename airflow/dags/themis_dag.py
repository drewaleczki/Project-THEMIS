from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Emr connection defaults
JOB_FLOW_OVERRIDES = {
    "Name": "THEMIS-processing-cluster",
    "ReleaseLabel": "emr-6.15.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "themis-dev-emr-ec2-profile", # from terraform
    "ServiceRole": "themis-dev-emr-service-role", # from terraform
    "LogUri": "s3://themis-dev-datalake-logs/emr/" # this should match terraform outputs
}

def mock_ingestion_task(**kwargs):
    """
    Mock function representing the ingestion of data from API / CSVs directly to Bronze S3.
    """
    logging.info("Starting ingestion into Bronze layer...")
    # Real implementation would call pipelines/ingest_data.py
    logging.info("Ingestion complete.")

def mock_data_quality_task(**kwargs):
    """
    Mock function representing basic data quality assertions.
    """
    logging.info("Starting data quality checks on Silver layer...")
    # Read sample of silver layer or query Athena metrics table
    logging.info("Data Quality checks passed.")

with DAG(
    'themis_orchestration_pipeline',
    default_args=default_args,
    description='End-to-end data pipeline for Project Themis',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['project_themis', 'data_engineering', 'tse'],
) as dag:

    # Task 1: Ingestion Phase (Dynamic Generation)
    # Mapping multiple datasets for parallel processing in Airflow
    TSE_DATASETS = {
        "candidatos": "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2022.zip",
        "bens_candidato": "https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2022.zip",
        "votacao_nominal": "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2022.zip",
        "prestacao_contas": "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2022.zip"
        # Add 'coligacoes', 'vagas', etc., following the same pattern here!
    }

    ingestion_tasks = []
    
    for domain, url in TSE_DATASETS.items():
        task = BashOperator(
            task_id=f'ingest_{domain}_to_bronze',
            bash_command=f'''
                python /opt/airflow/dags/pipelines/ingest_data.py \
                --url "{url}" \
                --domain "{domain}" \
                --year "2022"
            '''
        )
        ingestion_tasks.append(task)

    # Task 2: Create EMR Cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default', # Requires Airflow connection configured
    )

    # Task 3: Wait for EMR cluster to be WAITING
    # In a real scenario, we might use EmrAddStepsOperator and wait for steps, but for simplicity:
    wait_for_cluster = EmrJobFlowSensor(
        task_id='wait_for_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states=['WAITING'],
        failed_states=['TERMINATED', 'TERMINATED_WITH_ERRORS']
    )

    # Task 4 (Mocked PySpark Submit):
    process_silver = PythonOperator(
        task_id='process_bronze_to_silver',
        python_callable=lambda: logging.info("Submitting step: Bronze -> Silver spark job.")
    )

    # Task 5: Data Quality validation gate
    validate_silver = PythonOperator(
        task_id='quality_validation',
        python_callable=mock_data_quality_task
    )

    # Task 6 (Mocked PySpark Submit for Gold):
    process_gold = PythonOperator(
        task_id='process_silver_to_gold',
        python_callable=lambda: logging.info("Submitting step: Silver -> Gold spark job.")
    )

    # Task 7: Terminate EMR Cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        trigger_rule='all_done' # ensure termination even if a step fails
    )

    # Define dependencies
    for ingest_task in ingestion_tasks:
        ingest_task >> create_emr_cluster
        
    create_emr_cluster >> wait_for_cluster >> process_silver
    process_silver >> validate_silver >> process_gold >> terminate_emr_cluster
