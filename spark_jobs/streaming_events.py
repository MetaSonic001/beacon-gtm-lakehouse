"""
Beacon GTM Lakehouse — Spark Structured Streaming consumer.

Reads live events from Kafka/Redpanda (produced by
ingestion/kafka_producer.py), validates + deduplicates them with a
watermark, computes a rolling "events per minute per campaign" metric,
and writes both the raw stream (to bronze/streaming) and the aggregate
(to console, for demo purposes — swap for a sink of your choice).

Run:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        streaming_events.py --brokers localhost:19092
"""

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("customer_id", LongType()),
    StructField("campaign_id", LongType()),
    StructField("revenue_usd", DoubleType()),
])


def run(brokers, topic, bronze_out):
    spark = SparkSession.builder.appName("beacon-streaming-events").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json("json", SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withWatermark("event_timestamp", "2 minutes")
        .dropDuplicates(["event_id"])  # exactly-once-ish dedup within the watermark window
    )

    # Sink 1: raw validated stream -> bronze (append, partitioned by processing date)
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

    # Sink 2: 1-minute tumbling window aggregate -> console (swap for Postgres/Kafka sink later)
    agg = (
        events.groupBy(
            F.window("event_timestamp", "1 minute"),
            "campaign_id",
        )
        .agg(F.count("*").alias("event_count"), F.sum("revenue_usd").alias("revenue_usd"))
    )
    agg_query = (
        agg.writeStream
        .format("console")
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:19092")
    ap.add_argument("--topic", default="gtm-events")
    ap.add_argument("--bronze-out", default="../data/bronze")
    args = ap.parse_args()
    run(args.brokers, args.topic, args.bronze_out)
