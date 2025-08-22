from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor

# Replace these with your correct values
JOB_ROLE_ARN = "arn:aws:iam::insert_your_role"
S3_LOGS_BUCKET = "s3://s3_path"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"{S3_LOGS_BUCKET}logs/"}
    },
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
                "spark.sql.extensions": "org.apache.sedona.sql.SedonaSqlExtensions"
            }
        }
    ],
}

env_list = ["prd"] # your enviroment

default_args = {
    'owner': 'Raul Leite',
    'on_failure_callback': None,
    'on_success_callback': None,
    'retries': 0
}

with DAG(
    dag_id="geo_pipeline",
    default_args=default_args,
    schedule_interval="00 3 * * 2,4",
    start_date=datetime(2025, 8, 19),
    tags=["tables", "spark"],
    catchup=False,
) as dag:
    
    application_id = "00fuif85ulq2pc09" # Application id from ECR created with the apache sedona image and packages

    spark_submit_parameters = ( 
    "--conf spark.executor.cores=4 "
    "--conf spark.executor.memory=8g "
    "--conf spark.executor.instances=8 "
    "--conf spark.driver.cores=4 "
    "--conf spark.driver.memory=8g "
    )

    spark_job_process_geodata = EmrServerlessStartJobOperator(
        task_id="spark_job_process_geodata",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://s3_path/process_geodata.py", # file to process in s3 path
                "entryPointArguments": env_list,
                "sparkSubmitParameters": spark_submit_parameters,
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    spark_job_process_geodata.wait_for_completion = False

    wait_spark_job_process_geodata_finish = EmrServerlessJobSensor(
    task_id="wait_spark_job_process_geodata_finish",
    application_id=application_id,
    job_run_id=spark_job_process_geodata.output,
    )

    load_to_postgres = EcsRunTaskOperator(
        task_id="load_to_postgres",
        cluster="airflow", # cluster ecs
        task_definition="airflow-task-emr", # ecs task definition
        launch_type="FARGATE",
        overrides={
            # containerOverrides defines container-specific settings for the ECS task
            "containerOverrides": [
                {
                    "name": "airflow_tasks",  # container name defined in the ECS task definition
                    "command": [
                        "/opt/airflow/dags/airflow_tasks/load_to_postgres.py"  # command to be executed in the container (python script)
                    ],
                    # you can add other options like environment, cpu, memory, etc.
                },
            ],
        },
        network_configuration={
            # Specifies the VPC configuration for the ECS task
            "awsvpcConfiguration": {
                # List of subnet IDs where the ECS task will run
                "subnets": ["subnet-", "subnet-", "subnet-"],
                # List of security group IDs to associate with the ECS task
                "securityGroups": ["sg-"],
                # Assigns a public IP address to the ECS task
                "assignPublicIp": "ENABLED",
            },
        },
        tags={
            # Name tag for identifying the ECS task
            "Name": "spark_jobs",
            # Project tag for grouping related resources
            "Project": "airflow_emr",
            # Environment tag to specify the environment (e.g., production)
            "ENV": "prd",
            # Department tag for organizational purposes
            "Department": "data_eng",
        },
        awslogs_stream_prefix=f"airflow_tasks/spark_jobs",
    )

    load_to_postgres.wait_for_completion = True

    chain(
    spark_job_process_geodata,
    wait_spark_job_process_geodata_finish,
    load_to_postgres
    )
