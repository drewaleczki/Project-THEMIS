from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
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

    # Task 0: Upload Spark Scripts to S3
    upload_scripts = BashOperator(
        task_id='upload_spark_scripts_to_s3',
        bash_command='aws s3 cp /opt/airflow/dags/spark_jobs/ s3://themis-dev-datalake-logs/scripts/ --recursive'
    )

    ingestion_tasks = []
    previous_task = upload_scripts
    
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
        previous_task >> task
        previous_task = task
        ingestion_tasks.append(task)

    # Task 2: Create EMR Cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default', # Requires Airflow connection configured
    )

    # Task 3: Wait for EMR cluster to be WAITING
    wait_for_cluster = EmrJobFlowSensor(
        task_id='wait_for_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states=['WAITING'],
        failed_states=['TERMINATED', 'TERMINATED_WITH_ERRORS']
    )

    # Generate steps for Bronze -> Silver
    bronze_to_silver_steps = []
    for domain in TSE_DATASETS.keys():
        bronze_to_silver_steps.append({
            'Name': f'Process {domain} Bronze->Silver',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://themis-dev-datalake-logs/scripts/bronze_to_silver.py',
                    '--domain', domain,
                    '--year', '2022',
                    '--bronze_bucket', 'themis-dev-datalake-bronze',
                    '--silver_bucket', 'themis-dev-datalake-silver'
                ]
            }
        })

    # Task 4: Add Silver Steps
    process_silver = EmrAddStepsOperator(
        task_id='add_silver_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=bronze_to_silver_steps,
    )

    # Task 5: Wait for Silver Steps to complete
    wait_for_silver = EmrStepSensor(
        task_id='wait_for_silver',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_silver_steps', key='return_value')[-1] }}",
        aws_conn_id='aws_default',
    )

    # Task 6: Data Quality validation gate
    validate_silver = PythonOperator(
        task_id='quality_validation',
        python_callable=mock_data_quality_task
    )

    # Generate steps for Silver -> Gold
    silver_to_gold_steps = []
    for domain in TSE_DATASETS.keys():
        silver_to_gold_steps.append({
            'Name': f'Process {domain} Silver->Gold',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://themis-dev-datalake-logs/scripts/silver_to_gold.py',
                    '--domain', domain,
                    '--year', '2022',
                    '--silver_bucket', 'themis-dev-datalake-silver',
                    '--gold_bucket', 'themis-dev-datalake-gold'
                ]
            }
        })

    # Task 7: Add Gold Steps
    process_gold = EmrAddStepsOperator(
        task_id='add_gold_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=silver_to_gold_steps,
    )

    # Task 8: Wait for Gold Steps to complete
    wait_for_gold = EmrStepSensor(
        task_id='wait_for_gold',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_gold_steps', key='return_value')[-1] }}",
        aws_conn_id='aws_default',
    )

    # Task 9: Terminate EMR Cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        trigger_rule='all_done' # ensure termination even if a step fails
    )

    # Define dependencies
    for ingest_task in ingestion_tasks:
        ingest_task >> create_emr_cluster
        
    create_emr_cluster >> wait_for_cluster >> process_silver >> wait_for_silver
    wait_for_silver >> validate_silver >> process_gold >> wait_for_gold >> terminate_emr_cluster
