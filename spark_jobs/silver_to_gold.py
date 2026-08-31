"""
Beacon GTM Lakehouse — Silver -> Gold
========================================
Builds business-ready, aggregated tables from the clean silver layer:

    gold.campaign_performance   - spend, events, conversions, ROI per campaign
    gold.sales_pipeline         - opportunity counts/value by stage and rep
    gold.customer_activity      - per-customer event counts, revenue, last-seen

These are the tables a BI tool / analyst would actually query, which is
the point of a gold layer: pre-aggregated, denormalized, cheap to read.

Run:
    spark-submit silver_to_gold.py --silver ../data/silver --gold ../data/gold
"""

import argparse

from pyspark.sql import SparkSession, functions as F


def build_spark():
    return SparkSession.builder.appName("beacon-silver-to-gold").getOrCreate()


def run(silver_path, gold_path):
    spark = build_spark()

    events = spark.read.parquet(f"{silver_path}/fact_event")
    campaigns = spark.read.parquet(f"{silver_path}/dim_campaign")
    customers = spark.read.parquet(f"{silver_path}/dim_customer")

    # ---- campaign_performance ----
    campaign_events = (
        events.groupBy("campaign_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "lead_created", 1).otherwise(0)).alias("leads_created"),
            F.sum(F.when(F.col("event_type") == "opportunity_won", 1).otherwise(0)).alias("opportunities_won"),
            F.sum("revenue_usd").alias("revenue_usd"),
        )
    )
    campaign_perf = (
        campaigns.select(
            F.col("campaign_id").cast("long").alias("campaign_id"),
            "campaign_name", "channel",
            F.col("budget_usd").cast("double").alias("budget_usd"),
        )
        .join(campaign_events, "campaign_id", "left")
        .fillna(0, subset=["total_events", "leads_created", "opportunities_won", "revenue_usd"])
        .withColumn(
            "cost_per_lead_usd",
            F.when(F.col("leads_created") > 0, F.col("budget_usd") / F.col("leads_created")).otherwise(None),
        )
        .withColumn(
            "roi_pct",
            F.when(F.col("budget_usd") > 0, (F.col("revenue_usd") - F.col("budget_usd")) / F.col("budget_usd") * 100)
            .otherwise(None),
        )
    )
    campaign_perf.write.mode("overwrite").parquet(f"{gold_path}/campaign_performance")

    # ---- sales_pipeline (stage funnel proxy from opportunity events) ----
    opp_events = events.filter(F.col("event_type").startswith("opportunity_"))
    pipeline = (
        opp_events.groupBy("sales_rep_id", "event_type")
        .agg(F.count("*").alias("event_count"), F.sum("revenue_usd").alias("revenue_usd"))
    )
    pipeline.write.mode("overwrite").parquet(f"{gold_path}/sales_pipeline")

    # ---- customer_activity ----
    customer_activity = (
        events.groupBy("customer_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum("revenue_usd").alias("lifetime_revenue_usd"),
            F.max("event_timestamp").alias("last_seen_at"),
            F.min("event_timestamp").alias("first_seen_at"),
        )
        .join(
            customers.select(
                F.col("customer_id").cast("long").alias("customer_id"),
                "company_name", "industry", "country",
            ),
            "customer_id", "left",
        )
    )
    customer_activity.write.mode("overwrite").parquet(f"{gold_path}/customer_activity")

    print("\n===== SILVER -> GOLD SUMMARY =====")
    print(f"  campaign_performance rows: {campaign_perf.count():,}")
    print(f"  sales_pipeline rows:       {pipeline.count():,}")
    print(f"  customer_activity rows:    {customer_activity.count():,}")
    print("===================================\n")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", default="../data/silver")
    ap.add_argument("--gold", default="../data/gold")
    args = ap.parse_args()
    run(args.silver, args.gold)
