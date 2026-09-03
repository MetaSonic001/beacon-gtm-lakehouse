"""
Beacon GTM Lakehouse — Spark Optimization Benchmark
======================================================
Runs the SAME join four different ways and times each, so you get real
numbers instead of made-up ones. It joins the real Silver fact table
(`fact_opportunity`) against the small `dim_sales_team` dimension on the
natural key `sales_agent` — exactly the join the pipeline itself performs.

Experiments:
    1. baseline          - default shuffle (sort-merge) join, no tuning
    2. broadcast          - broadcast the small dimension table
    3. aqe_skew           - same join but with Adaptive Query Execution +
                           skew join optimization turned on
    4. partition_pruning  - filter on the partitioned columns (engage_year /
                           engage_month) vs an unpartitioned scan, to show
                           partition pruning savings

Run (after bronze_to_silver.py has produced ../data/silver):
    python join_benchmark.py --silver ../data/silver

Note: this is a small (~6,000-row) teaching dataset, so absolute runtimes
are tiny and the deltas are modest — the point is demonstrating the
*mechanics* of each strategy. Differences become dramatic at real scale,
where Spark's own AQE auto-broadcast threshold (default 10 MB) often already
picks the fast plan; that auto-behaviour is worth noting in a writeup.
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
    facts = spark.read.parquet(f"{silver_path}/fact_opportunity")
    teams = spark.read.parquet(f"{silver_path}/dim_sales_team")

    def baseline():
        return facts.join(teams, "sales_agent").agg(F.count("*")).collect()

    results["baseline_sort_merge"] = timed("1. Baseline (sort-merge)", baseline)
    spark.stop()

    # ---------------- Experiment 2: broadcast join ----------------
    spark = build_spark(**{"spark.sql.adaptive.enabled": "false"})
    facts = spark.read.parquet(f"{silver_path}/fact_opportunity")
    teams = spark.read.parquet(f"{silver_path}/dim_sales_team")

    def broadcast_join():
        return facts.join(F.broadcast(teams), "sales_agent").agg(F.count("*")).collect()

    results["broadcast"] = timed("2. Broadcast join", broadcast_join)
    spark.stop()

    # ---------------- Experiment 3: AQE + skew join handling ----------------
    spark = build_spark(**{
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    })
    facts = spark.read.parquet(f"{silver_path}/fact_opportunity")
    teams = spark.read.parquet(f"{silver_path}/dim_sales_team")

    def aqe_skew_join():
        return facts.join(teams, "sales_agent").agg(F.count("*")).collect()

    results["aqe_skew"] = timed("3. AQE + skew join", aqe_skew_join)
    spark.stop()

    # ---------------- Experiment 4: partition pruning ----------------
    spark = build_spark()
    facts = spark.read.parquet(f"{silver_path}/fact_opportunity")

    def unpruned_scan():
        return facts.filter(F.col("deal_stage") == "Won").count()

    def pruned_scan():
        return facts.filter(
            (F.col("engage_year") == 2017) & (F.col("engage_month") == 3)
        ).count()

    results["scan_no_partition_filter"] = timed("4a. Scan w/o partition filter", unpruned_scan)
    results["scan_with_partition_filter"] = timed("4b. Scan w/ partition filter (year/month)", pruned_scan)
    spark.stop()

    print("\n===== BENCHMARK SUMMARY (copy into your README) =====")
    print(f"{'Experiment':<32}{'Runtime (s)':>12}")
    for k, v in results.items():
        print(f"{k:<32}{v:>12.2f}")
    print("=======================================================\n")
    print("Notes:")
    print("  - At this dataset's scale the deltas are small; the value is the")
    print("    mechanism (plan + Spark UI), which you can screenshot.")
    print("  - In real deployments, watch shuffle read/write, stage duration,")
    print("    and spill (bytes) in the Spark UI (localhost:4040) per run.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", default="../data/silver")
    args = ap.parse_args()
    run(args.silver)
