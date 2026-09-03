"""
Beacon GTM Lakehouse -- real-time event producer.

Continuously streams synthetic GTM (Go-To-Market) events onto a Kafka/
Redpanda topic, simulating live website + product activity.  The events
produced here feed spark_jobs/streaming_events.py (Spark Structured
Streaming) and ultimately land in the bronze layer of the lakehouse.

What is a "topic"?
    A Kafka topic is a named, append-only log.  Producers write events
    into a topic; consumers read from it.  Think of it as a durable,
    ordered queue that many readers can consume independently.

What is a "producer"?
    A KafkaProducer batches and sends records to a broker (the server
    that owns the topic).  The python-kafka library handles batching,
    retries, and serialization for us.

What is "rate limiting"?
    To simulate realistic traffic we cap the number of events per second
    with a simple sleep loop: ``time.sleep(1 / rate)``.  Without this
    the producer would blast events as fast as possible, which can
    overwhelm the broker and make demos hard to follow.

Works against the Redpanda service in docker-compose.yml (Kafka
API-compatible, no Zookeeper needed -- much lighter for a laptop demo).

Run:
    pip install kafka-python python-dotenv
    python kafka_producer.py --brokers localhost:19092 --rate 50
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

# python-dotenv reads KEY=VALUE lines from a .env file in the project root
# and injects them into os.environ *before* any other code tries to read
# them.  This means our scripts work out of the box with `docker compose`
# without requiring the user to export env vars manually.
from dotenv import load_dotenv

# kafka-python's KafkaProducer handles connection, batching, retries,
# and serialization to the Kafka/Redpanda broker.
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Load environment variables from .env (project root)
# ---------------------------------------------------------------------------
# load_dotenv() is idempotent -- calling it multiple times is safe and
# it never overwrites variables that are already set in the real
# environment, so CLI flags and system env vars always take precedence.
load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The list of event types our GTM data model tracks.  Each event
# represents something a buyer might do during their journey -- from
# anonymous page views all the way to revenue-generating actions.
EVENT_TYPES = [
    "page_view",
    "email_opened",
    "email_clicked",
    "demo_requested",
    "opportunity_stage_changed",
    "product_feature_used",
]


# ---------------------------------------------------------------------------
# Event factory
# ---------------------------------------------------------------------------

def make_event():
    """
    Generate a single synthetic GTM event.

    Fields:
      * event_id        -- unique identifier (UUID4) for deduplication
      * event_type      -- one of the EVENT_TYPES above
      * event_timestamp -- UTC ISO-8601 timestamp
      * customer_id     -- simulated customer (1 - 50 000)
      * campaign_id     -- simulated campaign (1 - 12)
      * revenue_usd     -- random revenue; only ~5 % of events carry a
                           non-zero value to mimic the real world where
                           most interactions are non-revenue-bearing
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": random.randint(1, 50_000),
        "campaign_id": random.randint(1, 12),
        # Small chance (5 %) that this event carries revenue -- mirrors
        # reality where most touches are top-of-funnel.
        "revenue_usd": round(random.uniform(0, 500), 2) if random.random() < 0.05 else 0,
    }


# ---------------------------------------------------------------------------
# Main entry-point
# ---------------------------------------------------------------------------

def main():
    # ------------------------------------------------------------------
    # CLI arguments
    # ------------------------------------------------------------------
    # Defaults come from the .env file (KAFKA_BROKERS / KAFKA_TOPIC) with
    # hard-coded fallbacks so the script works even without a .env file.
    ap = argparse.ArgumentParser(
        description="Produce synthetic GTM events to a Kafka/Redpanda topic.",
    )
    ap.add_argument(
        "--brokers",
        default=os.getenv("KAFKA_BROKERS", "localhost:19092"),
        help="Comma-separated list of Kafka broker addresses "
             "(default: $KAFKA_BROKERS or localhost:19092)",
    )
    ap.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "gtm-events"),
        help="Kafka topic to publish events to "
             "(default: $KAFKA_TOPIC or gtm-events)",
    )
    ap.add_argument(
        "--rate",
        type=int,
        default=20,
        help="Target number of events to produce per second (default: 20)",
    )
    ap.add_argument(
        "--count",
        type=int,
        default=None,
        help="Total number of events to produce, then exit. "
             "Omit for unbounded (default: unlimited).",
    )
    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Connect to the Kafka / Redpanda broker
    # ------------------------------------------------------------------
    # The value_serializer converts every dict we send into a UTF-8 JSON
    # byte string automatically, so downstream consumers (Spark, etc.)
    # receive well-formed JSON.
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable:
        # Friendly error message instead of a raw traceback -- this is
        # the #1 stumbling block for newcomers to the project.
        print(
            "\nERROR: Could not connect to Kafka broker at "
            f"{args.brokers}.\n\n"
            "Make sure Redpanda is running:\n\n"
            "    docker compose up -d redpanda\n\n"
            "You can verify it is healthy with:\n\n"
            "    docker compose ps\n\n"
            "Then re-run this script.",
            file=sys.stderr,
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Produce events in a loop (rate-limited)
    # ------------------------------------------------------------------
    # The rate limiter is intentionally simple: sleep for 1/rate seconds
    # between events.  For production workloads you would use
    # KafkaProducer's built-in batching and buffer controls instead.
    limit_str = f" (max {args.count:,} events)" if args.count else " (unbounded)"
    print(
        f"Producing to topic '{args.topic}' on {args.brokers} "
        f"at ~{args.rate} events/s{limit_str}.  Ctrl+C to stop."
    )
    sent = 0
    try:
        while args.count is None or sent < args.count:
            event = make_event()

            # producer.send() is non-blocking -- it enqueues the record
            # in an internal buffer and returns immediately.  The actual
            # network I/O happens on a background thread.
            producer.send(args.topic, event)
            sent += 1

            # Progress indicator every 100 events so the terminal is not
            # flooded but you still get feedback.
            if sent % 100 == 0:
                print(f"  sent {sent:,} events...")

            # Rate limiting: sleep just enough to stay near the target
            # throughput.  1 / 20 = 0.05 s between events at 20/s.
            time.sleep(1 / args.rate)

        if args.count:
            print(f"\nReached target count of {args.count:,} events.")

    except KeyboardInterrupt:
        print(f"\nStopped after {sent:,} events.")
    finally:
        # Always flush pending records and close the connection cleanly,
        # even if Ctrl+C or an exception interrupts the loop.
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
