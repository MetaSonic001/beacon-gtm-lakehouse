"""
Beacon GTM Lakehouse -- batch orchestration DAG.

Pipeline flow (single-stage, linear):

    copy_actual_data  -->  bronze_to_silver  -->  silver_to_gold  -->  load_to_postgres
        (Python)              (Spark)               (Spark)               (Spark + JDBC)

This DAG:
  1. Copies the actual Maven CRM dataset to the "bronze" layer.
  2. Cleans, deduplicates, and type-casts into the "silver" layer via Spark.
  3. Aggregates silver into business-ready "gold" tables via Spark.
  4. Loads the gold Parquet files into PostgreSQL via spark JDBC.

Design notes:
  - Every task overwrites its output directory, so re-running a failed DAG
    run is always safe (idempotent).
  - Retries use exponential backoff to avoid hammering a flaky service.
  - The DAG file itself is lightweight -- it imports only Airflow; no Spark
    or heavy libraries are imported at parse time, so the DAG shows up in
    the Airflow UI even when Spark is not installed on the scheduler node.
  - All paths are resolved relative to PROJECT_ROOT, which defaults to the
    repository checkout.  Override via the PROJECT_ROOT environment variable
    when mounting the repo at a different path inside a container.

Drop this file into $AIRFLOW_HOME/dags/ (or mount it via docker-compose.yml)
and it will appear in the Airflow UI automatically.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Root of the checked-out repository.  Inside Docker the compose file may
# mount the repo at /opt/beacon, but on a host machine the default
# (current working directory's parent) works fine.  Override with the
# PROJECT_ROOT env var if your layout differs.
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/beacon")

# ---- Postgres connection settings -----------------------------------------
# These are forwarded to load_gold_to_postgres.py as environment variables
# so the Spark JDBC sink can build the connection URL.  Defaults match the
# values in .env / .env.example at the repository root.
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "beacon")
PG_USER     = os.getenv("PG_USER", "beacon")
PG_PASSWORD = os.getenv("PG_PASSWORD", "beacon")

# Bundled into every BashOperator command so the downstream script can
# read them with os.getenv().
PG_ENV_EXPORT = (
    f"export PG_HOST={PG_HOST} "
    f"export PG_PORT={PG_PORT} "
    f"export PG_DB={PG_DB} "
    f"export PG_USER={PG_USER} "
    f"export PG_PASSWORD={PG_PASSWORD} && "
)

# ---------------------------------------------------------------------------
# Default args applied to every task in the DAG
# ---------------------------------------------------------------------------
default_args = {
    # Who to page when a task fails repeatedly.
    "owner": "shaun",
    # Retry a failed task up to 2 times before marking it as failed.
    "retries": 2,
    # Wait 2 minutes before the first retry.
    "retry_delay": timedelta(minutes=2),
    # Double the delay on each subsequent retry (2m -> 4m -> capped at 10m).
    "retry_exponential_backoff": True,
    # Never wait longer than 10 minutes between retries.
    "max_retry_delay": timedelta(minutes=10),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="beacon_gtm_pipeline",
    # Human-readable description shown in the Airflow UI.
    description=(
        "Bronze -> Silver -> Gold -> Postgres "
        "for the Beacon GTM lakehouse"
    ),
    default_args=default_args,
    # NOTE: schedule_interval was deprecated in Airflow 2.4+ in favour of
    # the plain "schedule" parameter.  We use "schedule" here for forward
    # compatibility while remaining compatible with older Airflow versions.
    schedule="@daily",
    # The backfill window start date.  catchup=False means Airflow will
    # NOT materialise every day from 2026-01-01 to today; it will simply
    # schedule the next run.
    start_date=datetime(2026, 1, 1),
    # Do not backfill historical runs on first deployment.
    catchup=False,
    # Tags help group DAGs in the Airflow UI sidebar.
    tags=["beacon", "gtm", "spark"],
    # A docstring rendered as the "Description" tab in the UI.
    doc_md=__doc__,
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 -- Ingest actual Maven CRM dataset to bronze layer
    # -----------------------------------------------------------------------
    # Runs the proper ingestion script that normalizes the actual Maven
    # Analytics CRM Sales Opportunities dataset to the bronze layer.
    copy_actual_data = BashOperator(
        task_id="copy_actual_data",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python scripts/ingest_actual_data.py --out data/bronze/maven"
        ),
        doc_md=(
            "Runs the ingestion script that reads the actual Maven Analytics "
            "CRM Sales Opportunities dataset (accounts, products, sales_teams, "
            "sales_pipeline) from actual_maven_data/ and writes cleaned, "
            "normalized CSVs to the **bronze** data directory. Preserves "
            "natural keys (company names, product names) and applies minimal "
            "normalization: trims strings, casts numbers, parses dates, fixes "
            "known typos (e.g., 'technolgy' -> 'Technology')."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 2 -- Bronze to Silver (Spark)
    # -----------------------------------------------------------------------
    # Cleans raw bronze data: removes duplicates, enforces schemas, casts
    # types, and quarantines rows that fail validation.
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_ROOT}/spark_jobs && "
            "spark-submit bronze_to_silver.py "
            "--bronze ../data/bronze/maven "
            "--silver ../data/silver "
            "--quarantine ../data/quarantine"
        ),
        doc_md=(
            "Reads raw bronze CSVs from data/bronze/maven, applies schema "
            "validation, deduplication, and type-casting, then writes clean "
            "records to the **silver** directory as partitioned Parquet. "
            "Invalid rows are routed to the quarantine folder for later "
            "inspection. Joins on natural keys (account name, product name)."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 3 -- Silver to Gold (Spark)
    # -----------------------------------------------------------------------
    # Aggregates the cleaned silver data into business-level summary
    # tables (the gold layer).
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_ROOT}/spark_jobs && "
            "spark-submit silver_to_gold.py "
            "--silver ../data/silver "
            "--gold ../data/gold"
        ),
        doc_md=(
            "Aggregates the silver-layer data into business-ready "
            "**gold** Parquet files (e.g. daily summaries, KPI tables)."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 4 -- Load Gold into PostgreSQL (Spark + JDBC)
    # -----------------------------------------------------------------------
    # Writes the gold Parquet data into PostgreSQL via Spark's JDBC
    # datasource.  The --packages flag pulls the PostgreSQL JDBC driver
    # (org.postgresql:postgresql) at the specified version so it does not
    # need to be pre-installed on the Spark image.
    #
    # IMPORTANT: The JDBC driver JAR is fetched at runtime by Spark from
    # Maven Central.  If your cluster has no internet access, pre-install
    # postgresql-42.7.3.jar on the driver and classpath instead, and
    # remove the --packages flag.
    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            # Forward Postgres credentials to the child process.
            PG_ENV_EXPORT +
            f"cd {PROJECT_ROOT}/spark_jobs && "
            "spark-submit "
            # Pull the PostgreSQL JDBC driver from Maven Central at runtime.
            "--packages org.postgresql:postgresql:42.7.3 "
            "load_gold_to_postgres.py "
            "--gold ../data/gold"
        ),
        doc_md=(
            "Loads gold Parquet files into PostgreSQL using Spark's JDBC "
            "writer.  Connection details are passed via PG_* environment "
            "variables.  Requires network access to Maven Central for "
            "the JDBC driver JAR, or a pre-installed driver on the classpath."
        ),
    )

    # -----------------------------------------------------------------------
    # Task dependencies (linear pipeline)
    # -----------------------------------------------------------------------
    # Each step must complete before the next begins:
    #   copy -> clean -> aggregate -> load
    copy_actual_data >> bronze_to_silver >> silver_to_gold >> load_to_postgres
