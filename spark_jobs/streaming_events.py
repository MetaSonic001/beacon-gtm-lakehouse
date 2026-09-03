"""
Beacon GTM Lakehouse -- Spark Structured Streaming consumer.

Reads live GTM events from a Kafka/Redpanda topic (produced by
ingestion/kafka_producer.py), validates + deduplicates them, computes a
rolling "events per minute per campaign" metric, and writes:

  1. The **raw validated stream** to bronze/streaming_events as Parquet
     (append mode, partitioned by event_date).
  2. A **windowed aggregation** to the console (for demo / debugging --
     swap for a Postgres or Kafka sink in production).

Spark Structured Streaming -- key concepts
==========================================

DataStreamReader (``spark.readStream``)
    The entry point for reading unbounded data.  The ``.format("kafka")``
    call loads the Kafka integration JAR (must be on the --packages
    classpath) and creates a reader that continuously polls the broker
    for new records.

Watermark (``.withWatermark``)
    Streaming data can arrive late.  A watermark tells Spark: "events
    older than ``threshold`` behind the maximum event time seen so far
    will not be tracked for aggregation or deduplication."  This lets
    Spark free internal state and keeps memory bounded.  Without a
    watermark, deduplication and windowed aggregation would require
    unbounded state.

Window aggregation (``F.window``)
    Groups rows into fixed-size time buckets.  ``F.window(ts, "1 minute")``
    produces tumbling windows of 60 seconds each.  Spark assigns every
    row to exactly one window.  Windows that have not received new data
    for longer than the watermark are considered complete and can be
    finalized.

Checkpointing (``.option("checkpointLocation", ...)``)
    Structured Streaming records its progress (offsets, aggregations,
    metadata) to a local directory so that if the job restarts it can
    resume exactly where it left off rather than reprocessing everything.
    Always provide a stable, durable path -- never use /tmp for
    checkpoints in production.

Append output mode (``.outputMode("append")``)
    Only *new* rows are written to the sink.  This works for sinks like
    Parquet where we only need to add new files.  It cannot be used with
    aggregations unless a watermark is in place (see above).

Update output mode (``.outputMode("update")``)
    Only rows that were *updated* since the last trigger are emitted.
    This is the default for aggregation queries and is what we use for
    the console sink.

Run:
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        streaming_events.py --brokers localhost:19092 --topic gtm-events
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

# python-dotenv: load .env so KAFKA_BROKERS / KAFKA_TOPIC are available
# as process-level environment variables.  Wrapped in a try/except so
# the script still works when python-dotenv is not installed (e.g. inside
# a Docker image that sets env vars via other means).
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv is optional -- rely on the real environment


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------
# This schema *must* match the fields emitted by kafka_producer.py's
# make_event() function.  If the producer adds or removes a field, the
# schema here must be updated to match.
#
# NOTE: Only the fields the producer emits are covered.  If a message
# contains extra fields they are silently ignored; if a required field
# is missing or has the wrong type Spark will set the column to null
# rather than raising an error.  There is currently *no dead-letter
# queue* (DLQ) -- parse failures are dropped silently.  Adding a DLQ
# (e.g. writing malformed records to a separate Parquet or Kafka topic)
# would be a valuable extension for production use.
SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("customer_id", LongType()),
    StructField("campaign_id", LongType()),
    StructField("revenue_usd", DoubleType()),
])


# ---------------------------------------------------------------------------
# Core streaming pipeline
# ---------------------------------------------------------------------------

def run(brokers, topic, bronze_out, watermark_minutes, duration_seconds=None):
    """
    Set up and run the Structured Streaming pipeline.

    Parameters
    ----------
    brokers : str
        Comma-separated Kafka broker addresses (e.g. "localhost:19092").
    topic : str
        The Kafka topic to subscribe to.
    bronze_out : str
        Base path for the bronze-layer Parquet output.
    watermark_minutes : int
        How many minutes of lateness to tolerate before Spark stops
        tracking an event for dedup / aggregation.  Higher values use
        more memory but handle later data.
    duration_seconds : int or None
        If set, run the streaming query for this many seconds then stop
        gracefully. If None or 0, run indefinitely.
    """

    # ------------------------------------------------------------------
    # 1. Create (or retrieve) a SparkSession
    # ------------------------------------------------------------------
    # SparkSession is the unified entry point for all Spark functionality.
    # In Structured Streaming it manages the background scheduler that
    # continuously runs micro-batches.
    spark = SparkSession.builder.appName("beacon-streaming-events").getOrCreate()
    # Suppress the noisy INFO logs so the demo output is readable.
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 2. DataStreamReader -- read from Kafka
    # ------------------------------------------------------------------
    # readStream creates a *logical* streaming source.  No data flows
    # until an output query is started.
    #
    # startingOffsets = "latest" means we only see events produced *after*
    # the job starts -- useful for demos.  Change to "earliest" to
    # replay the full topic.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # ------------------------------------------------------------------
    # 3. Parse, validate, watermark, and deduplicate
    # ------------------------------------------------------------------
    # Each Kafka message arrives as a binary ``value`` column.  We cast
    # it to a STRING, then use ``from_json`` with our SCHEMA to extract
    # structured columns.
    #
    # Watermark -- see module docstring for a full explanation.  The
    # ``watermark_minutes`` parameter lets you tune this from the CLI.
    #
    # dropDuplicates -- removes duplicate ``event_id`` values within the
    # watermark window.  This is *not* exact dedup across all time
    # (impossible in a truly unbounded stream without infinite state),
    # but it is good enough for most real-time use cases and is the
    # recommended Spark pattern.
    events = (
        raw.selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json("json", SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withWatermark("event_timestamp", f"{watermark_minutes} minutes")
        .dropDuplicates(["event_id"])
    )

    # ------------------------------------------------------------------
    # 4. Sink 1 -- raw validated stream -> bronze (Parquet, append)
    # ------------------------------------------------------------------
    # Every micro-batch writes new Parquet files under bronze_out,
    # partitioned by event_date so downstream queries (Trino, Athena,
    # Spark batch jobs) can prune partitions efficiently.
    #
    # checkpointLocation is mandatory for stateful / durable streaming.
    # Spark writes the consumed Kafka offsets and internal state there so
    # the job can recover after a restart.
    #
    # processingTime = "30 seconds" tells Spark to run a micro-batch at
    # most every 30 seconds.  If no new data arrives, the batch is
    # skipped (zero-cost).
    raw_query = (
        events.withColumn("event_date", F.to_date("event_timestamp"))
        .writeStream
        .format("parquet")
        .option("path", f"{bronze_out}/streaming_events")
        .option("checkpointLocation", f"{bronze_out}/_checkpoints/streaming_events")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ------------------------------------------------------------------
    # 5. Sink 2 -- windowed aggregation -> console
    # ------------------------------------------------------------------
    # A 1-minute tumbling window groups events by campaign and counts
    # them + sums revenue.  The console sink prints the current result
    # set every trigger -- useful for live demos and debugging.
    #
    # outputMode("update") means only rows whose aggregates *changed*
    # since the last trigger are printed.  This is efficient for
    # windowed queries.
    #
    # TODO: In production, swap ``format("console")`` for a database
    # sink (e.g. JDBC to Postgres, or a Kafka producer sink) so the
    # aggregates are queryable by BI tools.
    agg = (
        events.groupBy(
            F.window("event_timestamp", "1 minute"),
            "campaign_id",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("revenue_usd").alias("revenue_usd"),
        )
    )
    agg_query = (
        agg.writeStream
        .format("console")
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ------------------------------------------------------------------
    # 6. Block until one of the streaming queries terminates
    # ------------------------------------------------------------------
    # awaitAnyTermination() blocks the main thread forever (or until
    # Ctrl+C / an unrecoverable error).  This is the standard way to
    # keep a streaming application alive.
    if duration_seconds and duration_seconds > 0:
        print(f"Running streaming query for {duration_seconds} seconds...")
        import time
        time.sleep(duration_seconds)
        print("Duration elapsed — stopping streaming queries gracefully.")
        raw_query.stop()
        agg_query.stop()
        spark.stop()
    else:
        spark.streams.awaitAnyTermination()


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Spark Structured Streaming consumer for GTM events.",
    )
    ap.add_argument(
        "--brokers",
        default=os.getenv("KAFKA_BROKERS", "localhost:19092"),
        help="Kafka broker addresses "
             "(default: $KAFKA_BROKERS or localhost:19092)",
    )
    ap.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "gtm-events"),
        help="Kafka topic to consume from "
             "(default: $KAFKA_TOPIC or gtm-events)",
    )
    ap.add_argument(
        "--bronze-out",
        default="../data/bronze",
        help="Base path for bronze-layer Parquet output "
             "(default: ../data/bronze)",
    )
    ap.add_argument(
        "--watermark-minutes",
        type=int,
        default=2,
        help="Watermark threshold in minutes -- events older than this "
             "behind the max event time will be dropped from dedup / "
             "aggregation state (default: 2)",
    )
    ap.add_argument(
        "--duration-seconds",
        type=int,
        default=None,
        help="Run the streaming query for this many seconds then stop gracefully. "
             "Omit or set 0 to run indefinitely (default: unbounded).",
    )
    args = ap.parse_args()
    run(args.brokers, args.topic, args.bronze_out, args.watermark_minutes, args.duration_seconds)
