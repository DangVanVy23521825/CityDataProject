from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
import subprocess
from docker.types import Mount


# ========= DAG Definition =========
default_args = {
    "owner": "smartcity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="smart_city_pipeline",
    default_args=default_args,
    description="Smart City ETL Orchestration",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 20),
    catchup=False,
    tags=["smartcity", "etl", "kafka", "spark", "glue"],
) as dag:

    # --- Task 1: Ensure Spark Streaming is running (or restart if needed) ---
    spark_streaming_task = BashOperator(
        task_id="check_or_start_spark_streaming",
        bash_command="""
                if docker ps | grep spark-streaming-job; then
                    echo "Spark streaming job already running."
                else
                    echo "Starting Spark streaming job..."
                    docker rm -f spark-streaming-job || true
                    docker run -d --name spark-streaming-job \
                      --network datamasterylab \
                      -v /Users/dangvanvy/Documents/city-data-project/jobs:/opt/bitnami/spark/jobs \
                      custom-spark:3.5.0 \
                      spark-submit \
                        --master spark://spark-master:7077 \
                        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                        /opt/bitnami/spark/jobs/spark.py
                fi
                """,
    )


    # --- Task 2: Glue crawler ---
    glue_crawler_task = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={"Name": "city-data-project"},
        wait_for_completion=True,
    )

    spark_streaming_task >> glue_crawler_task