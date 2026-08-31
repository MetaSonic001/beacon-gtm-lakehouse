"""
Beacon GTM Lakehouse — Spark Optimization Benchmark
======================================================
This is the "differentiator" script for the project: it runs the SAME
join (fact_event x dim_customer) four different ways and times each,
so you get real numbers for your README instead of made-up ones.

Experiments:
    1. baseline        - default shuffle (sort-merge) join, no tuning
    2. broadcast        - broadcast the small dimension table
    3. skew_handling    - same join but with Adaptive Query Execution +
                          skew join optimization turned on
    4. partition_pruning - filter on a partitioned column (event_date)
                          vs an unpartitioned scan, to show pushdown savings

Run (after bronze_to_silver.py has produced ../data/silver):
    python join_benchmark.py --silver ../data/silver

Note: differences are most visible at higher --events counts when you
regenerate data (10M+). At small dev scale, Spark's own AQE will often
already pick the fast plan — which is itself worth noting in your writeup:
"AQE auto-broadcast kicked in below X MB; I disabled it to show the
uncoalesced behaviour for comparison."
"""

import argparse
import time

from pyspark.sql import SparkSession, functions as F


def build_spark(**confs):
    builder = SparkSession.builder.appName("beacon-join-benchmark")
    for k, v in confs.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def timed(label, fn):
    t0 = time.time()
    result = fn()
    elapsed = time.time() - t0
    print(f"  {label:<28} {elapsed:>8.2f}s")
    return elapsed


def run(silver_path):
    results = {}

    # ---------------- Experiment 1: baseline sort-merge join ----------------
    spark = build_spark(**{
        "spark.sql.autoBroadcastJoinThreshold": "-1",  # force off auto-broadcast
        "spark.sql.adaptive.enabled": "false",
    })
    events = spark.read.parquet(f"{silver_path}/fact_event")
    customers = spark.read.parquet(f"{silver_path}/dim_customer")

    def baseline():
        return events.join(customers, "customer_id").agg(F.count("*")).collect()

    results["baseline_sort_merge"] = timed("1. Baseline (sort-merge)", baseline)
    spark.stop()

    # ---------------- Experiment 2: broadcast join ----------------
    spark = build_spark(**{"spark.sql.adaptive.enabled": "false"})
    events = spark.read.parquet(f"{silver_path}/fact_event")
    customers = spark.read.parquet(f"{silver_path}/dim_customer")

    def broadcast_join():
        return events.join(F.broadcast(customers), "customer_id").agg(F.count("*")).collect()

    results["broadcast"] = timed("2. Broadcast join", broadcast_join)
    spark.stop()

    # ---------------- Experiment 3: AQE + skew join handling ----------------
    spark = build_spark(**{
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    })
    events = spark.read.parquet(f"{silver_path}/fact_event")
    campaigns_dummy = spark.read.parquet(f"{silver_path}/dim_customer")

    def aqe_skew_join():
        # join on campaign_id-equivalent skewed key (customer_id here as proxy;
        # swap to campaign_id join against a small campaign-derived table for a
        # sharper skew demo once you're at 10M+ rows)
        return events.join(campaigns_dummy, "customer_id").agg(F.count("*")).collect()

    results["aqe_skew"] = timed("3. AQE + skew join", aqe_skew_join)
    spark.stop()

    # ---------------- Experiment 4: partition pruning ----------------
    spark = build_spark()
    events = spark.read.parquet(f"{silver_path}/fact_event")

    def unpruned_scan():
        return events.filter(F.col("event_type") == "opportunity_won").count()

    def pruned_scan():
        return events.filter((F.col("year") == 2025) & (F.col("month") == 6)).count()

    results["scan_no_partition_filter"] = timed("4a. Scan w/o partition filter", unpruned_scan)
    results["scan_with_partition_filter"] = timed("4b. Scan w/ partition filter (year/month)", pruned_scan)
    spark.stop()

    print("\n===== BENCHMARK SUMMARY (copy into your README) =====")
    print(f"{'Experiment':<32}{'Runtime (s)':>12}")
    for k, v in results.items():
        print(f"{k:<32}{v:>12.2f}")
    print("=======================================================\n")
    print("Tips for a bigger, more dramatic delta:")
    print("  - Regenerate data with --events 20000000 or higher")
    print("  - Re-run this script and update the table above with real numbers")
    print("  - Screenshot the Spark UI (localhost:4040) during each run for your")
    print("    README: shuffle read/write, stage duration, spill (bytes)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", default="../data/silver")
    args = ap.parse_args()
    run(args.silver)
