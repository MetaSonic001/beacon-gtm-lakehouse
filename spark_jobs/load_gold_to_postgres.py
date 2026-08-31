"""
Beacon GTM Lakehouse — load gold Parquet tables into Postgres.

This is the "serving layer" hop: gold/ is what Spark writes, Postgres
is what a BI tool or analyst would actually query with plain SQL.

Requires the Postgres JDBC driver on the classpath, e.g.:
    spark-submit --packages org.postgresql:postgresql:42.7.3 load_gold_to_postgres.py

Set connection details via env vars (see .env.example) or edit PG_* below.
"""

import argparse
import os

from pyspark.sql import SparkSession

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "beacon")
PG_USER = os.environ.get("PG_USER", "beacon")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "beacon")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

TABLES = ["campaign_performance", "sales_pipeline", "customer_activity"]


def run(gold_path):
    spark = SparkSession.builder.appName("beacon-load-postgres").getOrCreate()

    for table in TABLES:
        df = spark.read.parquet(f"{gold_path}/{table}")
        (df.write
            .mode("overwrite")
            .jdbc(JDBC_URL, f"gold.{table}", properties=JDBC_PROPS))
        print(f"  loaded {df.count():,} rows -> gold.{table}")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", default="../data/gold")
    args = ap.parse_args()
    run(args.gold)
