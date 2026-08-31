"""
Beacon GTM Lakehouse — Bronze -> Silver
=========================================
Reads raw CRM + event CSVs from the bronze zone, applies schema
enforcement and data-quality rules, splits records into
silver (clean) vs quarantine (rejected), and writes partitioned
Parquet output.

This is intentionally plain PySpark (no Delta Lake dependency) so it
runs anywhere with just `pip install pyspark` — swap `.parquet()` for
`.format("delta")` if you want to layer Delta Lake on top later.

Run:
    spark-submit bronze_to_silver.py \
        --bronze ../data/bronze --silver ../data/silver --quarantine ../data/quarantine

Data quality rules enforced here (this is the "one key data quality
improvement" story for interviews):
    1. customer_id must be non-null and must exist in dim_customer
    2. event_timestamp must not be in the future
    3. revenue_usd must be >= 0
    4. event_id must be unique (dedup keeps first occurrence)
    5. event_type must be one of the known event types
"""

import argparse
import time

from pyspark.sql import SparkSession, functions as F, Window


KNOWN_EVENT_TYPES = [
    "website_visit", "page_view", "lead_created", "email_sent",
    "email_opened", "email_clicked", "demo_requested",
    "opportunity_created", "opportunity_stage_changed",
    "opportunity_won", "opportunity_lost",
    "subscription_started", "subscription_cancelled",
]


def build_spark(app_name="beacon-bronze-to-silver"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")  # small for local dev; raise for real clusters
        .getOrCreate()
    )


def load_dimensions(spark, bronze_path):
    customers = spark.read.option("header", True).csv(f"{bronze_path}/crm/dim_customer.csv")
    campaigns = spark.read.option("header", True).csv(f"{bronze_path}/crm/dim_campaign.csv")
    reps = spark.read.option("header", True).csv(f"{bronze_path}/crm/dim_sales_rep.csv")
    products = spark.read.option("header", True).csv(f"{bronze_path}/crm/dim_product.csv")
    return customers, campaigns, reps, products


def run(bronze_path, silver_path, quarantine_path):
    spark = build_spark()
    t0 = time.time()

    customers, campaigns, reps, products = load_dimensions(spark, bronze_path)

    raw_events = (
        spark.read.option("header", True)
        .csv(f"{bronze_path}/events/*.csv")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("revenue_usd", F.col("revenue_usd").cast("double"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("campaign_id", F.col("campaign_id").cast("long"))
        .withColumn("sales_rep_id", F.col("sales_rep_id").cast("long"))
    )

    total_in = raw_events.count()

    # --- dedup on event_id, keep first by event_timestamp ---
    w = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").asc_nulls_last())
    deduped = (
        raw_events
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    dup_count = total_in - deduped.count()

    # --- validity checks -> tag each row with a reason instead of silently dropping ---
    known_campaign_ids = [r["campaign_id"] for r in campaigns.select(
        F.col("campaign_id").cast("long")).distinct().collect()]

    checked = (
        deduped
        .withColumn("_err_null_customer", F.col("customer_id").isNull())
        .withColumn("_err_future_ts", F.col("event_timestamp") > F.current_timestamp())
        .withColumn("_err_negative_revenue", F.col("revenue_usd") < 0)
        .withColumn("_err_unknown_type", ~F.col("event_type").isin(KNOWN_EVENT_TYPES))
        .withColumn("_err_orphan_campaign", ~F.col("campaign_id").isin(known_campaign_ids))
    )

    checked = checked.withColumn(
        "_is_bad",
        F.col("_err_null_customer") | F.col("_err_future_ts") | F.col("_err_negative_revenue")
        | F.col("_err_unknown_type") | F.col("_err_orphan_campaign"),
    )

    good = checked.filter(~F.col("_is_bad")).drop(
        "_is_bad", "_err_null_customer", "_err_future_ts",
        "_err_negative_revenue", "_err_unknown_type", "_err_orphan_campaign",
    )
    bad = checked.filter(F.col("_is_bad"))

    good_count = good.count()
    bad_count = bad.count()

    # --- enrich + partition for downstream query performance ---
    silver_events = (
        good
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("year", F.year("event_timestamp"))
        .withColumn("month", F.month("event_timestamp"))
    )

    (silver_events.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{silver_path}/fact_event"))

    (bad.write.mode("overwrite").parquet(f"{quarantine_path}/fact_event_rejected"))

    for name, df in [("dim_customer", customers), ("dim_campaign", campaigns),
                      ("dim_sales_rep", reps), ("dim_product", products)]:
        df.write.mode("overwrite").parquet(f"{silver_path}/{name}")

    elapsed = time.time() - t0

    print("\n===== BRONZE -> SILVER SUMMARY =====")
    print(f"  input rows:        {total_in:,}")
    print(f"  duplicates removed: {dup_count:,}")
    print(f"  passed validation:  {good_count:,}  ({good_count/total_in*100:.2f}%)")
    print(f"  quarantined:        {bad_count:,}  ({bad_count/total_in*100:.2f}%)")
    print(f"  elapsed:            {elapsed:.1f}s")
    print("=====================================\n")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", default="../data/bronze")
    ap.add_argument("--silver", default="../data/silver")
    ap.add_argument("--quarantine", default="../data/quarantine")
    args = ap.parse_args()
    run(args.bronze, args.silver, args.quarantine)
