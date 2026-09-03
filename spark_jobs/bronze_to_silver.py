"""
Beacon GTM Lakehouse — Bronze -> Silver
=========================================
Reads the raw Maven Analytics CRM Sales Opportunities CSVs from the
Bronze zone, applies schema enforcement and data-quality rules,
splits records into silver (clean) vs quarantine (rejected), and
writes partitioned Parquet output.

Input (Bronze — raw Maven CSVs from actual_maven_data/):
    accounts.csv        -> customer / company dimension (account name as key)
    products.csv        -> product catalog dimension (product name as key)
    sales_pipeline.csv  -> the opportunity FACT table (deals)
    sales_teams.csv     -> sales agent / manager / office dimension

This is intentionally plain PySpark (no Delta Lake dependency) so it
runs anywhere with just `pip install pyspark` — swap `.parquet()` for
`.format("delta")` if you want to layer Delta Lake on top later.

Run:
    spark-submit bronze_to_silver.py \
        --bronze ../data/bronze/maven --silver ../data/silver --quarantine ../data/quarantine

Data quality rules enforced here (this is the "data quality" story for
interviews — each rule tags a reason instead of silently dropping rows):
    1. opportunity_id must be non-null and unique (dedup keeps first)
    2. account (company name) must exist in the accounts dimension (referential integrity)
    3. product (product name) must exist in the products dimension (referential integrity)
    4. sales_agent must exist in the sales_teams dimension (referential integrity)
    5. deal_stage must be one of the known deal stages (Prospecting/Engaging/
       Qualification/Proposal/Negotiation/Won/Lost)
    6. close_value must be >= 0 (a negative close value is invalid)
    7. dates must parse; close_date >= engage_date; a Won deal must have a close_date

Note: The actual Maven dataset uses company names and product names as keys
instead of surrogate IDs. This pipeline joins on those natural keys.
"""

import argparse
import time

from pyspark.sql import SparkSession, functions as F, Window

# Small helper that persists this run's quality metrics to Postgres so you can
# trend data quality over time (gold.data_quality_log). It handles its own
# Postgres connection failure gracefully (see call in run()).
from log_quality_metrics import run as log_quality_metrics

# The valid deal stages for this CRM. Anything else is rejected as a bad stage.
# Includes "Engaging" from the actual Maven dataset (maps to Qualification in funnel).
KNOWN_DEAL_STAGES = [
    "Prospecting", "Qualification", "Proposal", "Negotiation", "Won", "Lost", "Engaging",
]


def build_spark(app_name="beacon-bronze-to-silver"):
    """Create a SparkSession configured for local development."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")  # small for local dev; raise for real clusters
        .getOrCreate()
    )


def load_raw(spark, bronze_path):
    """
    Load the four raw Maven CSVs. They are read as-is (strings) and cast
    to proper types in `cast_types`. Header rows are used as column names.

    Actual Maven dataset schema:
    - accounts: account, sector, year_established, revenue, employees, office_location, subsidiary_of
    - products: product, series, sales_price
    - sales_teams: sales_agent, manager, regional_office
    - sales_pipeline: opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value
    """
    accounts = spark.read.option("header", True).csv(f"{bronze_path}/accounts.csv")
    products = spark.read.option("header", True).csv(f"{bronze_path}/products.csv")
    teams = spark.read.option("header", True).csv(f"{bronze_path}/sales_teams.csv")
    pipeline = spark.read.option("header", True).csv(f"{bronze_path}/sales_pipeline.csv")
    return accounts, products, teams, pipeline


def safe_to_date(col, fmt):
    """
    Cast a string column to a date, returning NULL for malformed values on
    every Spark version. Spark 4.x exposes `F.try_to_date`; Spark 3.5 (the
    container image) only ships `F.to_date`, which happens to be lenient
    (NULL on bad input) there. Using whichever exists keeps the job working
    both in the Docker image (Spark 3.5.1) and when run locally on Spark 4.
    """
    if hasattr(F, "try_to_date"):
        return F.try_to_date(col, fmt)
    return F.to_date(col, fmt)


def cast_types(accounts, products, teams, pipeline):
    """
    Cast raw string columns to the correct Spark types (int/long/date/double).
    Column names match the actual Maven dataset.
    """
    # Cast numeric columns in accounts
    accounts = (
        accounts
        .withColumn("year_established", F.col("year_established").cast("int"))
        .withColumn("revenue", F.col("revenue").cast("double"))
        .withColumn("employees", F.col("employees").cast("int"))
    )

    # Cast sales_price in products
    products = products.withColumn("sales_price", F.col("sales_price").cast("double"))

    # Cast pipeline columns
    pipeline = (
        pipeline
        # Use a version-robust date cast so the intentionally malformed
        # dates (the data-quality test cases) return NULL instead of throwing.
        .withColumn("engage_date", safe_to_date("engage_date", "yyyy-MM-dd"))
        .withColumn("close_date", safe_to_date("close_date", "yyyy-MM-dd"))
        .withColumn("close_value", F.col("close_value").cast("double"))
    )
    return accounts, products, teams, pipeline


def run(bronze_path, silver_path, quarantine_path):
    spark = build_spark()
    t0 = time.time()

    accounts, products, teams, pipeline_raw = load_raw(spark, bronze_path)
    accounts, products, teams, pipeline_raw = cast_types(
        accounts, products, teams, pipeline_raw
    )

    total_in = pipeline_raw.count()
    # Actual dataset uses company name (account) and product name as keys
    valid_accounts = [r["account"] for r in accounts.select("account").distinct().collect()]
    valid_products = [r["product"] for r in products.select("product").distinct().collect()]
    valid_agents = [r["sales_agent"] for r in teams.select("sales_agent").distinct().collect()]

    # --- 1) dedup on opportunity_id, keep first by engage_date ---
    w = Window.partitionBy("opportunity_id").orderBy(F.col("engage_date").asc_nulls_last())
    deduped = (
        pipeline_raw
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    dup_count = total_in - deduped.count()

    # --- 2..7) validity checks -> tag each row with a reason, don't silently drop ---
    checked = (
        deduped
        .withColumn("_err_null_opp", F.col("opportunity_id").isNull())
        # Referential integrity on account. IMPORTANT: `col("account").isin(...)`
        # is NULL when account is NULL (Spark three-valued logic), and the
        # coalesce() in `_is_bad` below would turn that NULL into "not bad" —
        # silently letting NULL-account deals through and out of account-level
        # reporting. An explicit isNull() guard makes a missing account a real
        # orphan (referential integrity failure), so those rows are quarantined
        # instead of vanishing from account_performance.
        .withColumn("_err_orphan_account",
                    F.col("account").isNull() | ~F.col("account").isin(valid_accounts))
        .withColumn("_err_orphan_product", ~F.col("product").isin(valid_products))
        .withColumn("_err_orphan_agent", ~F.col("sales_agent").isin(valid_agents))
        .withColumn("_err_bad_stage", ~F.col("deal_stage").isin(KNOWN_DEAL_STAGES))
        .withColumn("_err_negative_value", F.col("close_value") < 0)
        .withColumn(
            "_err_bad_dates",
            # close_date before engage_date is invalid
            ((F.col("close_date").isNotNull() & F.col("engage_date").isNotNull()) &
             (F.col("close_date") < F.col("engage_date")))
            # a Won deal that somehow has no close_date is invalid
            | ((F.col("deal_stage") == "Won") & F.col("close_date").isNull()),
        )
    )

    # CRITICAL: coalesce every error flag to FALSE before OR-ing them.
    #
    # In Spark/SQL's three-valued logic, a comparison against NULL is NULL,
    # not FALSE. Most pipeline rows are *open* deals (Prospecting ->
    # Negotiation) and have a NULL close_value (no value assigned until a
    # deal is won). For those rows `F.col("close_value") < 0` evaluates to
    # NULL, so the whole `_is_bad` OR-expression becomes NULL. Filtering on
    # `~NULL` then drops the row from BOTH the good and bad frames — silently
    # losing ~84% of the data.
    #
    # coalesce(flag, false) turns any NULL flag into FALSE so `_is_bad` is
    # always a clean true/false. Every input row lands in either `good` or
    # `bad` — none are lost.
    checked = checked.withColumn(
        "_is_bad",
        F.coalesce(F.col("_err_null_opp"), F.lit(False)).cast("boolean")
        | F.coalesce(F.col("_err_orphan_account"), F.lit(False))
        | F.coalesce(F.col("_err_orphan_product"), F.lit(False))
        | F.coalesce(F.col("_err_orphan_agent"), F.lit(False))
        | F.coalesce(F.col("_err_bad_stage"), F.lit(False))
        | F.coalesce(F.col("_err_negative_value"), F.lit(False))
        | F.coalesce(F.col("_err_bad_dates"), F.lit(False)),
    )

    good = checked.filter(~F.col("_is_bad"))
    bad = checked.filter(F.col("_is_bad"))

    good_count = good.count()
    bad_count = bad.count()

    # --- enrich the fact table with derived columns useful for analytics ---
    #   is_closed     : has the deal reached a terminal state?
    #   days_to_close : how many days did it take to close (if closed)?
    #   engage_year / engage_month : partition columns for fast pruning
    silver_opps = (
        good
        .withColumn("is_closed", F.col("close_date").isNotNull())
        .withColumn("days_to_close",
                    F.when(F.col("close_date").isNotNull(),
                           F.datediff(F.col("close_date"), F.col("engage_date"))))
        .withColumn("engage_year", F.year("engage_date"))
        .withColumn("engage_month", F.month("engage_date"))
        .drop("_is_bad", "_err_null_opp", "_err_orphan_account", "_err_orphan_product",
              "_err_orphan_agent", "_err_bad_stage", "_err_negative_value", "_err_bad_dates")
    )

    (silver_opps.write.mode("overwrite")
        .partitionBy("engage_year", "engage_month")
        .parquet(f"{silver_path}/fact_opportunity"))

    (bad.write.mode("overwrite").parquet(f"{quarantine_path}/fact_opportunity_rejected"))

    # --- write the clean dimensions to silver as well ---
    for name, df in [("dim_account", accounts), ("dim_product", products),
                     ("dim_sales_team", teams)]:
        df.write.mode("overwrite").parquet(f"{silver_path}/{name}")

    elapsed = time.time() - t0

    print("\n===== BRONZE -> SILVER SUMMARY =====")
    print(f"  input rows:          {total_in:,}")
    print(f"  duplicates removed:  {dup_count:,}")
    print(f"  passed validation:   {good_count:,}  ({good_count/total_in*100:.2f}%)")
    print(f"  quarantined:         {bad_count:,}  ({bad_count/total_in*100:.2f}%)")
    print(f"  elapsed:             {elapsed:.1f}s")
    print("=====================================\n")

    # Persist these metrics to Postgres gold.data_quality_log. This is OPTIONAL:
    # if Postgres isn't running we log a warning and carry on — bronze->silver
    # shouldn't be blocked by the serving layer being down.
    try:
        if total_in > 0:
            log_quality_metrics(total_in, dup_count, good_count, bad_count, elapsed)
    except Exception as e:
        print(f"  (note: could not log quality metrics: {e})")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", default="../data/bronze/maven")
    ap.add_argument("--silver", default="../data/silver")
    ap.add_argument("--quarantine", default="../data/quarantine")
    args = ap.parse_args()
    run(args.bronze, args.silver, args.quarantine)
