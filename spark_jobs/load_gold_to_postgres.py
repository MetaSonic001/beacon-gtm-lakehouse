"""
Beacon GTM Lakehouse — load gold Parquet tables into Postgres.
===================================================================
This is the final "serving layer" hop: gold/ is what Spark writes as
Parquet, Postgres is what a BI tool or analyst would actually query with
plain SQL. Spark reads each gold Delta/Parquet table and writes it into
the matching `gold.*` Postgres table via the JDBC connector.

Requires the Postgres JDBC driver on the classpath, e.g.:
    spark-submit --packages org.postgresql:postgresql:42.7.3 load_gold_to_postgres.py

The connection details come from .env (loaded via python-dotenv) — see
.env.example. Inside Docker the host would be `postgres`; on the host it
is `localhost`.

Run:
    spark-submit --packages org.postgresql:postgresql:42.7.3 \
        load_gold_to_postgres.py --gold ../data/gold
"""

import argparse
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load .env so PG_* connection settings are read from one place.
load_dotenv()

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "beacon")
PG_USER = os.environ.get("PG_USER", "beacon")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "beacon")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
# The JDBC driver name + Postgres credentials Spark uses to connect.
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

# The gold Parquet tables (also created in Postgres by sql/gold_schema.sql).
TABLES = [
    "account_performance",
    "sales_rep_performance",
    "product_performance",
    "pipeline_funnel",
    "monthly_revenue",
]


def run(gold_path):
    spark = SparkSession.builder.appName("beacon-load-postgres").getOrCreate()

    try:
        for table in TABLES:
            df = spark.read.parquet(f"{gold_path}/{table}")
            row_count = df.count()  # count before write (avoid re-reading after)
            (df.write
                .mode("overwrite")
                .jdbc(JDBC_URL, f"gold.{table}", properties=JDBC_PROPS))
            print(f"  loaded {row_count:,} rows -> gold.{table}")
    except Exception as e:
        # Give a friendly error if Postgres is unreachable (e.g. not started).
        print("\nERROR loading gold tables into Postgres:")
        print(f"  {e}")
        print("\nIs Postgres running? Start it with:  docker compose up -d postgres")
        print("Did you pass the JDBC driver? Add: --packages org.postgresql:postgresql:42.7.3")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", default="../data/gold")
    args = ap.parse_args()
    run(args.gold)
