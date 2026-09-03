"""
Beacon GTM Lakehouse — log bronze->silver quality metrics to Postgres.
=======================================================================
After bronze_to_silver.py runs, it prints a summary (input rows, dupes,
passed, quarantined). This small script persists that summary into
Postgres `gold.data_quality_log` so you can *trend data quality over time*.

It uses psycopg2 (a thin Postgres driver) rather than Spark — writing a
few metrics rows doesn't deserve a full Spark job. This is a realistic
pattern: heavy transformations in Spark, light operational writes via a
plain DB driver.

Requires Postgres to be up (docker compose up -d postgres).

Run (usually wired into the Airflow DAG after bronze_to_silver):
    python log_quality_metrics.py \
        --input-rows 1000 --duplicates 5 --passed 950 --quarantined 45 --elapsed 12.3
"""

import argparse
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def run(input_rows, duplicates, passed, quarantined, elapsed):
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "beacon"),
        user=os.getenv("PG_USER", "beacon"),
        password=os.getenv("PG_PASSWORD", "beacon"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO gold.data_quality_log
                    (input_rows, duplicates_removed, passed_validation, quarantined, elapsed_seconds)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (input_rows, duplicates, passed, quarantined, elapsed),
            )
        conn.commit()
        print(f"  logged quality metrics -> gold.data_quality_log "
              f"(in={input_rows}, dup={duplicates}, passed={passed}, quarantined={quarantined})")
    except Exception as e:
        print("\nERROR logging quality metrics (is Postgres up?):")
        print(f"  {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-rows", type=int, required=True)
    ap.add_argument("--duplicates", type=int, default=0)
    ap.add_argument("--passed", type=int, required=True)
    ap.add_argument("--quarantined", type=int, default=0)
    ap.add_argument("--elapsed", type=float, default=0.0)
    args = ap.parse_args()
    run(args.input_rows, args.duplicates, args.passed, args.quarantined, args.elapsed)
