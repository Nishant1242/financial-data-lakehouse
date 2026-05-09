"""
lakehouse_pipeline.py

Production-grade Airflow DAG for the Financial Data Lakehouse.
Orchestrates the full pipeline: Bronze → Silver → Gold → dbt → MongoDB

Schedule: every hour
Tasks: 7
Author: Nishant Kadam
Version: 1.0.0
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner":             "nishant.kadam",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    dag_id="financial_lakehouse_pipeline",
    description="End-to-end Financial Data Lakehouse pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "financial", "production"],
)


def check_services(**context):
    import os
    from minio import Minio
    import psycopg
    from pymongo import MongoClient

    logger = logging.getLogger(__name__)
    errors = []

    try:
        client = Minio(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )
        client.list_buckets()
        logger.info("MinIO: healthy")
    except Exception as e:
        errors.append(f"MinIO: {str(e)}")

    try:
        conn = psycopg.connect(
            f"host={os.getenv('POSTGRES_HOST')} "
            f"port={os.getenv('POSTGRES_PORT')} "
            f"dbname={os.getenv('POSTGRES_DB')} "
            f"user={os.getenv('POSTGRES_USER')} "
            f"password={os.getenv('POSTGRES_PASSWORD')}"
        )
        conn.close()
        logger.info("PostgreSQL: healthy")
    except Exception as e:
        errors.append(f"PostgreSQL: {str(e)}")

    try:
        client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        client.close()
        logger.info("MongoDB: healthy")
    except Exception as e:
        errors.append(f"MongoDB: {str(e)}")

    if errors:
        raise RuntimeError(f"Service health check failed: {errors}")

    logger.info("All services healthy — pipeline starting")
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


def run_bronze_consumer(**context):
    import json
    import os
    from kafka import KafkaConsumer
    from minio import Minio
    from io import BytesIO
    from datetime import datetime, timezone

    logger = logging.getLogger(__name__)
    logger.info("Starting Bronze consumer batch run")

    KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC_TRADES", "trades")
    BRONZE_BUCKET = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    BATCH_SIZE    = 5
    MAX_RECORDS   = 50

    minio_client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="airflow_bronze_consumer",
        auto_offset_reset="earliest",
        consumer_timeout_ms=30000,
        max_poll_records=BATCH_SIZE
    )

    batch     = []
    batch_num = 0
    total     = 0

    try:
        for message in consumer:
            batch.append(message.value)

            if len(batch) >= BATCH_SIZE:
                batch_num += 1
                total     += len(batch)
                now        = datetime.now(timezone.utc)
                file_path  = (
                    f"trades/year={now.strftime('%Y')}/"
                    f"month={now.strftime('%m')}/"
                    f"day={now.strftime('%d')}/"
                    f"hour={now.strftime('%H')}/"
                    f"trades_{now.strftime('%Y%m%d_%H%M%S')}.json"
                )
                content = "\n".join(json.dumps(t) for t in batch).encode()
                minio_client.put_object(
                    BRONZE_BUCKET, file_path,
                    BytesIO(content), len(content),
                    content_type="application/x-ndjson"
                )
                logger.info(f"Batch {batch_num} written | trades={len(batch)}")
                batch = []

            if total >= MAX_RECORDS:
                logger.info(f"Reached max records ({MAX_RECORDS}), stopping")
                break

        if batch:
            batch_num += 1
            total     += len(batch)
            now        = datetime.now(timezone.utc)
            file_path  = (
                f"trades/year={now.strftime('%Y')}/"
                f"month={now.strftime('%m')}/"
                f"day={now.strftime('%d')}/"
                f"hour={now.strftime('%H')}/"
                f"trades_{now.strftime('%Y%m%d_%H%M%S')}_final.json"
            )
            content = "\n".join(json.dumps(t) for t in batch).encode()
            minio_client.put_object(
                BRONZE_BUCKET, file_path,
                BytesIO(content), len(content),
                content_type="application/x-ndjson"
            )

    finally:
        consumer.close()

    logger.info(f"Bronze consumer complete | total={total} | batches={batch_num}")
    context["ti"].xcom_push(key="bronze_trades_count", value=total)
    return {"trades_collected": total, "batches_written": batch_num}


def run_batch_pipeline(**context):
    import subprocess

    logger = logging.getLogger(__name__)

    bronze_count = context["ti"].xcom_pull(
        task_ids="run_bronze_consumer",
        key="bronze_trades_count"
    )
    logger.info(f"Running batch pipeline | expected_trades={bronze_count}")

    result = subprocess.run(
        [sys.executable, "/opt/airflow/services/processing/batch_pipeline.py"],
        capture_output=True,
        text=True,
        timeout=1800
    )

    if result.returncode != 0:
        logger.error(f"Batch pipeline failed:\n{result.stderr}")
        raise RuntimeError(f"Batch pipeline failed with exit code {result.returncode}")

    logger.info(f"Batch pipeline stdout:\n{result.stdout[-2000:]}")
    return {"status": "success"}


def run_gold_loader(**context):
    import subprocess

    logger = logging.getLogger(__name__)
    logger.info("Running Gold loader")

    result = subprocess.run(
        [sys.executable, "/opt/airflow/services/processing/gold_loader.py"],
        capture_output=True,
        text=True,
        timeout=300
    )

    if result.returncode != 0:
        logger.error(f"Gold loader failed:\n{result.stderr}")
        raise RuntimeError(f"Gold loader failed with exit code {result.returncode}")

    logger.info(f"Gold loader output:\n{result.stdout[-2000:]}")
    return {"status": "success"}


def run_dbt_models(**context):
    import subprocess

    logger = logging.getLogger(__name__)
    logger.info("Running dbt models")

    result = subprocess.run(
        ["dbt", "run",
         "--project-dir", "/opt/airflow/lakehouse_dbt",
         "--profiles-dir", "/home/airflow/.dbt"],
        capture_output=True,
        text=True,
        timeout=300
    )

    logger.info(f"dbt output:\n{result.stdout}")

    if result.returncode != 0:
        logger.error(f"dbt run failed:\n{result.stderr}")
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")

    return {"status": "success", "output": result.stdout[-1000:]}


def run_mongodb_loader(**context):
    import subprocess

    logger = logging.getLogger(__name__)
    logger.info("Running MongoDB loader")

    result = subprocess.run(
        [sys.executable, "/opt/airflow/services/processing/mongodb_loader.py"],
        capture_output=True,
        text=True,
        timeout=300
    )

    if result.returncode != 0:
        logger.error(f"MongoDB loader failed:\n{result.stderr}")
        raise RuntimeError(f"MongoDB loader failed with exit code {result.returncode}")

    logger.info(f"MongoDB loader output:\n{result.stdout[-2000:]}")
    return {"status": "success"}


def pipeline_summary(**context):
    logger = logging.getLogger(__name__)

    bronze_count = context["ti"].xcom_pull(
        task_ids="run_bronze_consumer",
        key="bronze_trades_count"
    ) or 0

    run_id       = context["run_id"]
    dag_id       = context["dag"].dag_id
    logical_date = context["logical_date"]

    logger.info("=" * 60)
    logger.info("  PIPELINE RUN SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  DAG:          {dag_id}")
    logger.info(f"  Run ID:       {run_id}")
    logger.info(f"  Logical date: {logical_date}")
    logger.info(f"  Trades:       {bronze_count}")
    logger.info(f"  Status:       SUCCESS")
    logger.info("=" * 60)

    return {
        "dag_id":   dag_id,
        "run_id":   run_id,
        "trades":   bronze_count,
        "status":   "SUCCESS"
    }


t1_check_services = PythonOperator(
    task_id="check_services",
    python_callable=check_services,
    dag=dag,
)

t2_bronze = PythonOperator(
    task_id="run_bronze_consumer",
    python_callable=run_bronze_consumer,
    dag=dag,
)

t3_silver = PythonOperator(
    task_id="run_batch_pipeline",
    python_callable=run_batch_pipeline,
    dag=dag,
)

t4_gold = PythonOperator(
    task_id="run_gold_loader",
    python_callable=run_gold_loader,
    dag=dag,
)

t5_dbt = PythonOperator(
    task_id="run_dbt_models",
    python_callable=run_dbt_models,
    dag=dag,
)

t6_mongodb = PythonOperator(
    task_id="run_mongodb_loader",
    python_callable=run_mongodb_loader,
    dag=dag,
)

t7_summary = PythonOperator(
    task_id="pipeline_summary",
    python_callable=pipeline_summary,
    dag=dag,
)

t1_check_services >> t2_bronze >> t3_silver >> t4_gold >> t5_dbt >> t6_mongodb >> t7_summary