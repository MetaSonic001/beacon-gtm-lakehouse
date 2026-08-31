"""
Beacon GTM Lakehouse — batch orchestration DAG.

generate_data -> bronze_to_silver -> silver_to_gold -> load_to_postgres
                                            |
                                            +-> log_quality_metrics

Demonstrates: task dependencies, retries with backoff, and an
idempotent design (every task overwrites its output rather than
appending, so re-running a failed DAG run is always safe).

Drop this file into $AIRFLOW_HOME/dags/ (or mount it there via
docker-compose.yml) and it will show up in the Airflow UI.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/beacon"  # adjust if mounted elsewhere inside the Airflow container

default_args = {
    "owner": "shaun",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="beacon_gtm_pipeline",
    description="Bronze -> Silver -> Gold -> Postgres for the Beacon GTM lakehouse",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["beacon", "gtm", "spark"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=(
            f"cd {PROJECT_ROOT}/data_generator && "
            "python generate_data.py --events 2000000 --out ../data/bronze --seed 42"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_ROOT}/spark_jobs && "
            "spark-submit bronze_to_silver.py --bronze ../data/bronze "
            "--silver ../data/silver --quarantine ../data/quarantine"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_ROOT}/spark_jobs && "
            "spark-submit silver_to_gold.py --silver ../data/silver --gold ../data/gold"
        ),
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            f"cd {PROJECT_ROOT}/spark_jobs && "
            "spark-submit --packages org.postgresql:postgresql:42.7.3 "
            "load_gold_to_postgres.py --gold ../data/gold"
        ),
    )

    generate_data >> bronze_to_silver >> silver_to_gold >> load_to_postgres
