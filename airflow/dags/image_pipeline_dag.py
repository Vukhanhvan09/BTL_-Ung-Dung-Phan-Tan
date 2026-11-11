
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="distributed_image_classification_pipeline",
    default_args=default_args,
    description=(
        "Orchestrates distributed traffic image processing "
        "using Spark Cluster and YOLOv8/Mock, "
        "with results stored in ClickHouse traffic_dw"
    ),
    schedule_interval="* * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=[
        "traffic_monitoring",
        "spark",
        "yolov8",
        "clickhouse",
        "traffic_dw",
    ],
) as dag:

    # Always spark-submit to the cluster — never `python spark_job.py` on local[*]
    # Database MUST be traffic_dw (not legacy image_dw)
    run_spark_classification = BashOperator(
        task_id="run_spark_classification_job",
        bash_command="""
set -euo pipefail

export CLICKHOUSE_DB="${CLICKHOUSE_DB:-traffic_dw}"
export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-minio:9000}"
export RABBITMQ_HOST="${RABBITMQ_HOST:-rabbitmq}"
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
export DETECTION_MODE="${DETECTION_MODE:?DETECTION_MODE must be mock or yolo}"

if [[ "$SPARK_MASTER_URL" == local* ]]; then
  echo "Refusing local Spark master: $SPARK_MASTER_URL" >&2
  exit 1
fi

if [[ "$CLICKHOUSE_DB" == "image_dw" ]]; then
  echo "Legacy database image_dw is not supported; use traffic_dw" >&2
  exit 1
fi

# Match cluster capacity: N workers * cores/worker (default 3*2=6)
TOTAL_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-6}"
EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-2}"

"$SPARK_HOME/bin/spark-submit" \
  --master "$SPARK_MASTER_URL" \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --executor-cores "$EXECUTOR_CORES" \
  --total-executor-cores "$TOTAL_CORES" \
  --conf spark.pyspark.python=/opt/bitnami/python/bin/python \
  --conf spark.pyspark.driver.python=/home/airflow/spark-venv/bin/python \
  /opt/spark-jobs/spark_job.py
""",
    )
