from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for Airflow DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define DAG
with DAG(
    'distributed_image_classification_pipeline',
    default_args=default_args,
    description='Orchestrates PySpark classification job utilizing VGG16 and loading results to ClickHouse',
    schedule_interval=None,  # Scheduled manually or via trigger for testing, can be set to e.g. '*/5 * * * *'
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['image_pipeline', 'spark', 'vgg16', 'clickhouse'],
) as dag:

    # Execute the Spark Job using Python in the Airflow environment
    # The spark volume is mapped to /opt/spark-jobs in docker-compose
    run_spark_classification = BashOperator(
        task_id='run_spark_classification_job',
        bash_command='python /opt/spark-jobs/spark_job.py',
        env={
            'MINIO_ENDPOINT': 'minio:9000',
            'MINIO_ACCESS_KEY': 'minioadmin',
            'MINIO_SECRET_KEY': 'minioadmin',
            'RABBITMQ_HOST': 'rabbitmq',
            'RABBITMQ_USER': 'guest',
            'RABBITMQ_PASS': 'guest',
            'RABBITMQ_QUEUE': 'image_processing_queue',
            'CLICKHOUSE_HOST': 'clickhouse',
            'CLICKHOUSE_PORT': '8123',
            'CLICKHOUSE_DB': 'image_dw'
        }
    )

    run_spark_classification
