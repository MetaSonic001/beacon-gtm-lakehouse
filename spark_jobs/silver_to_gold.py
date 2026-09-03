"""
Beacon GTM Lakehouse — Silver -> Gold
========================================
Builds business-ready, aggregated tables from the clean silver layer.
These are the tables a BI tool / analyst would actually query — the point
of a gold layer is pre-aggregated, denormalized, cheap-to-read data.

Gold tables produced (all keyed on dimensions from the actual Maven CRM schema):
    account_performance     - funnel + win metrics per account (company)
    sales_rep_performance   - funnel + win metrics per sales agent
    product_performance     - funnel + win metrics per product
    pipeline_funnel         - deal-stage funnel with conversion ratios

Note: The actual Maven dataset uses company names (account) and product names
as natural keys instead of surrogate IDs. This pipeline joins on those keys.

Run:
    spark-submit silver_to_gold.py --silver ../data/silver --gold ../data/gold
"""

import argparse

from pyspark.sql import SparkSession, functions as F

# Funnel order used to number stages & compute conversion ratios.
# Actual Maven dataset stages: Prospecting > Engaging > Qualification > Proposal > Negotiation > Won / Lost
FUNNEL_ORDER = ["Prospecting", "Engaging", "Qualification", "Proposal", "Negotiation", "Won"]


def build_spark():
    return SparkSession.builder.appName("beacon-silver-to-gold").getOrCreate()


def run(silver_path, gold_path):
    spark = build_spark()

    # The fact + dimension tables written by bronze_to_silver.py
    # Note: Actual Maven dataset uses 'account' (company name) and 'product' (product name)
    # as keys instead of surrogate IDs
    opps = spark.read.parquet(f"{silver_path}/fact_opportunity")
    accounts = spark.read.parquet(f"{silver_path}/dim_account")
    products = spark.read.parquet(f"{silver_path}/dim_product")
    teams = spark.read.parquet(f"{silver_path}/dim_sales_team")

    # The fact table is used by every aggregation below, so cache it in memory
    # to avoid re-reading + re-executing Parquet 4 times (a real perf win).
    opps.cache()
    # count-before-write trick: compute the count now while it's cached, and
    # reuse the number later instead of re-counting after each write.
    total_opps = opps.count()

    # =========================================================================
    # 1) account_performance — how each customer account performs across the
    #    sales funnel (deals opened, won, lost, value, win rate, days-to-close)
    # =========================================================================
    acct_agg = (
        opps.groupBy("account")
        .agg(
            F.count("*").alias("num_opportunities"),
            F.sum(F.when(F.col("deal_stage") == "Won", 1).otherwise(0)).alias("num_won"),
            F.sum(F.when(F.col("deal_stage") == "Lost", 1).otherwise(0)).alias("num_lost"),
            F.sum(F.when(F.col("is_closed") == False, 1).otherwise(0)).alias("num_open"),
            F.sum(F.when(F.col("deal_stage") == "Won", F.col("close_value")).otherwise(0))
                .alias("won_value"),
            F.avg("days_to_close").alias("avg_days_to_close"),
        )
    )
    account_performance = (
        accounts
        .select("account", "sector", "year_established",
                "revenue", "employees", "office_location", "subsidiary_of")
        .join(acct_agg, "account", "left")
        .fillna(0, subset=["num_opportunities", "num_won", "num_lost",
                           "num_open", "won_value"])
        .withColumn("win_rate",
                    F.when(F.col("num_opportunities") > 0,
                           F.col("num_won") / F.col("num_opportunities")).otherwise(0.0))
        .withColumn("avg_deal_size",
                    F.when(F.col("num_won") > 0,
                           F.col("won_value") / F.col("num_won")).otherwise(0.0))
    )
    acct_rows = account_performance.count()
    account_performance.write.mode("overwrite").parquet(f"{gold_path}/account_performance")

    # =========================================================================
    # 2) sales_rep_performance — per-agent funnel metrics, enriched with
    #    manager + regional_office from the sales_teams dimension
    # =========================================================================
    rep_agg = (
        opps.groupBy("sales_agent")
        .agg(
            F.count("*").alias("num_opportunities"),
            F.sum(F.when(F.col("deal_stage") == "Won", 1).otherwise(0)).alias("num_won"),
            F.sum(F.when(F.col("deal_stage") == "Lost", 1).otherwise(0)).alias("num_lost"),
            F.sum(F.when(F.col("deal_stage") == "Won", F.col("close_value")).otherwise(0))
                .alias("won_value"),
            F.avg("days_to_close").alias("avg_days_to_close"),
        )
    )
    sales_rep_performance = (
        teams.select("sales_agent", "manager", "regional_office")
        .join(rep_agg, "sales_agent", "left")
        .fillna(0, subset=["num_opportunities", "num_won", "num_lost", "won_value"])
        .withColumn("win_rate",
                    F.when(F.col("num_opportunities") > 0,
                           F.col("num_won") / F.col("num_opportunities")).otherwise(0.0))
        .withColumn("avg_deal_size",
                    F.when(F.col("num_won") > 0,
                           F.col("won_value") / F.col("num_won")).otherwise(0.0))
    )
    rep_rows = sales_rep_performance.count()
    sales_rep_performance.write.mode("overwrite").parquet(f"{gold_path}/sales_rep_performance")

    # =========================================================================
    # 3) product_performance — which hardware products sell well & close fast
    # =========================================================================
    prod_agg = (
        opps.groupBy("product")
        .agg(
            F.count("*").alias("num_opportunities"),
            F.sum(F.when(F.col("deal_stage") == "Won", 1).otherwise(0)).alias("num_won"),
            F.sum(F.when(F.col("deal_stage") == "Won", F.col("close_value")).otherwise(0))
                .alias("won_value"),
            F.avg("days_to_close").alias("avg_days_to_close"),
        )
    )
    product_performance = (
        products.select("product", "series", "sales_price")
        .join(prod_agg, "product", "left")
        .fillna(0, subset=["num_opportunities", "num_won", "won_value"])
        .withColumn("win_rate",
                    F.when(F.col("num_opportunities") > 0,
                           F.col("num_won") / F.col("num_opportunities")).otherwise(0.0))
        .withColumn("avg_deal_size",
                    F.when(F.col("num_won") > 0,
                           F.col("won_value") / F.col("num_won")).otherwise(0.0))
    )
    prod_rows = product_performance.count()
    product_performance.write.mode("overwrite").parquet(f"{gold_path}/product_performance")

    # =========================================================================
    # 4) pipeline_funnel — count of deals in each stage + conversion ratio from
    #    the previous funnel stage (how well deals progress stage to stage)
    # =========================================================================
    funnel = (
        opps.groupBy("deal_stage")
        .agg(
            F.count("*").alias("opportunity_count"),
            # `.otherwise(0)` must attach to the WHEN (null close_value -> 0 for
            # open deals), not to the SUM — wrapping the whole SUM breaks the
            # when()/otherwise() contract. coalesce() is the NULL-safe way to
            # default a SUM to 0.
            F.coalesce(F.sum("close_value"), F.lit(0.0)).alias("total_value"),
        )
    )
    # Order the funnel properly & compute % that moved into each stage.
    # Actual Maven dataset stages: Prospecting > Engaging > Qualification > Proposal > Negotiation > Won / Lost
    funnel_ordered = (
        funnel
        .withColumn("stage_index", F.when(F.col("deal_stage") == "Prospecting", 1)
                    .when(F.col("deal_stage") == "Engaging", 2)
                    .when(F.col("deal_stage") == "Qualification", 3)
                    .when(F.col("deal_stage") == "Proposal", 4)
                    .when(F.col("deal_stage") == "Negotiation", 5)
                    .when(F.col("deal_stage") == "Won", 6)
                    .otherwise(0))                     # Lost / unknown at the end
        .orderBy("stage_index")
    )
    # Compute conversion as share of the top-of-funnel (Prospecting) volume — a
    # readable way to show funnel drop-off stage over stage.
    prospecting = funnel.filter(F.col("deal_stage") == "Prospecting") \
        .select("opportunity_count").first()
    top_count = prospecting[0] if prospecting else 0
    pipeline_funnel = (
        funnel_ordered
        .withColumn("conversion_ratio",
                    F.when(F.lit(top_count) > 0, F.col("opportunity_count") / F.lit(top_count))
                        .otherwise(0.0))
    )
    funnel_rows = pipeline_funnel.count()
    pipeline_funnel.write.mode("overwrite").parquet(f"{gold_path}/pipeline_funnel")

    # =========================================================================
    # 5) monthly_revenue — won revenue recognized per close month, for the
    #    revenue-over-time chart. Aggregated on close_date (when a deal is won),
    #    not engage_date, so the line shows "revenue recognized" per month.
    # =========================================================================
    monthly_revenue = (
        opps.filter(F.col("deal_stage") == "Won")
        .withColumn("close_year", F.year("close_date"))
        .withColumn("close_month", F.month("close_date"))
        .groupBy("close_year", "close_month")
        .agg(
            F.count("*").alias("num_won"),
            F.sum("close_value").alias("won_value"),
        )
        .orderBy("close_year", "close_month")
    )
    monthly_rows = monthly_revenue.count()
    monthly_revenue.write.mode("overwrite").parquet(f"{gold_path}/monthly_revenue")

    print("\n===== SILVER -> GOLD SUMMARY =====")
    print(f"  total opportunities (silver): {total_opps:,}")
    print(f"  account_performance rows:     {acct_rows:,}")
    print(f"  sales_rep_performance rows:   {rep_rows:,}")
    print(f"  product_performance rows:     {prod_rows:,}")
    print(f"  pipeline_funnel rows:         {funnel_rows:,}")
    print(f"  monthly_revenue rows:         {monthly_rows:,}")
    print("===================================\n")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", default="../data/silver")
    ap.add_argument("--gold", default="../data/gold")
    args = ap.parse_args()
    run(args.silver, args.gold)
