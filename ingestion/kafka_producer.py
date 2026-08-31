"""
Beacon GTM Lakehouse — real-time event producer.

Continuously streams synthetic GTM events onto a Kafka/Redpanda topic,
simulating live website + product activity. This feeds
spark_jobs/streaming_events.py (Spark Structured Streaming).

Works against the Redpanda service in docker-compose.yml (Kafka
API-compatible, no Zookeeper needed — much lighter for a laptop demo).

Run:
    pip install kafka-python
    python kafka_producer.py --brokers localhost:19092 --rate 50
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

EVENT_TYPES = [
    "page_view", "email_opened", "email_clicked", "demo_requested",
    "opportunity_stage_changed", "product_feature_used",
]


def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": random.randint(1, 50_000),
        "campaign_id": random.randint(1, 12),
        "revenue_usd": round(random.uniform(0, 500), 2) if random.random() < 0.05 else 0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:19092")
    ap.add_argument("--topic", default="gtm-events")
    ap.add_argument("--rate", type=int, default=20, help="events per second")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.brokers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing to '{args.topic}' on {args.brokers} at ~{args.rate}/s. Ctrl+C to stop.")
    sent = 0
    try:
        while True:
            event = make_event()
            producer.send(args.topic, event)
            sent += 1
            if sent % 100 == 0:
                print(f"  sent {sent:,} events...")
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print(f"\nStopped after {sent:,} events.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
