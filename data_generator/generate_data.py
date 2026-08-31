"""
Beacon GTM Lakehouse — synthetic data generator
=================================================

Generates two kinds of data for a fictional B2B SaaS company's
Go-To-Market (GTM) org:

1. CRM dimension data (customers, sales reps, campaigns, products)
   -> written once as the "reference data" for the platform.

2. High-volume fact/event data (web events, lead events, opportunity
   events, subscription events) -> this is the "big data" part of the
   project. Volume is controlled by --events, so you can run with
   50K events in 10 seconds to sanity check the pipeline, or with
   50-100M events to produce the real benchmark numbers for your README.

Design notes (why it's built this way):
- Uses only Python's random/datetime + Faker for realism — no external
  API calls, fully offline and reproducible via --seed.
- Deliberately injects bad records (nulls, duplicates, future timestamps,
  orphan foreign keys) at a configurable rate, because a data quality
  story ("X% of records were rejected, here's why") is one of the most
  interview-relevant things you can show.
- Deliberately creates a skewed distribution across campaigns (one
  campaign gets ~55% of events) so the Spark skew-handling benchmark
  in benchmarks/join_benchmark.py has something real to fix.

Usage:
    python generate_data.py --events 2000000 --out ../data/bronze --seed 42
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()

EVENT_TYPES = [
    "website_visit", "page_view", "lead_created", "email_sent",
    "email_opened", "email_clicked", "demo_requested",
    "opportunity_created", "opportunity_stage_changed",
    "opportunity_won", "opportunity_lost",
    "subscription_started", "subscription_cancelled",
]

# Weighted so the funnel looks realistic (lots of page views, few wins)
EVENT_WEIGHTS = [22, 20, 8, 10, 8, 6, 5, 6, 6, 3, 2, 3, 1]

PRODUCTS = ["Beacon Starter", "Beacon Growth", "Beacon Enterprise", "Beacon Add-on: Analytics"]
STAGES = ["Prospecting", "Qualification", "Demo", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
CHANNELS = ["organic_search", "paid_search", "linkedin_ads", "referral", "outbound", "webinar", "content"]


def gen_sales_reps(n=25):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "sales_rep_id": i,
            "rep_name": fake.name(),
            "region": random.choice(["NA", "EMEA", "APAC", "LATAM"]),
            "hire_date": fake.date_between(start_date="-4y", end_date="-30d").isoformat(),
        })
    return rows


def gen_campaigns(n=12):
    rows = []
    for i in range(1, n + 1):
        start = fake.date_between(start_date="-1y", end_date="-30d")
        rows.append({
            "campaign_id": i,
            "campaign_name": f"{random.choice(CHANNELS)}_{fake.bs().split()[0]}_{start.strftime('%Y%m')}",
            "channel": random.choice(CHANNELS),
            "start_date": start.isoformat(),
            "budget_usd": round(random.uniform(2000, 80000), 2),
        })
    return rows


def gen_products(n=len(PRODUCTS)):
    return [{"product_id": i + 1, "product_name": p, "list_price_usd": price}
            for i, (p, price) in enumerate(zip(PRODUCTS, [49, 199, 999, 29]))]


def gen_customers(n=50_000):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "customer_id": i,
            "company_name": fake.company(),
            "industry": fake.job().split(",")[0][:40],
            "employee_count": random.choice([1, 10, 50, 200, 1000, 5000]),
            "country": fake.country(),
            "signup_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
        })
    return rows


def write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  wrote {len(rows):>10,} rows -> {path}")


def skewed_campaign_id(campaign_ids):
    """~55% of events hit campaign #1, the rest spread thinly.
    This is what creates the data-skew benchmark scenario."""
    if random.random() < 0.55:
        return campaign_ids[0]
    return random.choice(campaign_ids)


def gen_events(n, customer_ids, campaign_ids, rep_ids, bad_rate, seed):
    random.seed(seed)
    start = datetime(2025, 1, 1)
    span_days = 600

    for i in range(1, n + 1):
        is_bad = random.random() < bad_rate
        event_time = start + timedelta(
            days=random.randint(0, span_days),
            seconds=random.randint(0, 86400),
        )
        row = {
            "event_id": str(uuid.uuid4()),
            "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
            "event_timestamp": event_time.isoformat(),
            "customer_id": random.choice(customer_ids),
            "campaign_id": skewed_campaign_id(campaign_ids),
            "sales_rep_id": random.choice(rep_ids),
            "revenue_usd": round(random.uniform(0, 15000), 2) if random.random() < 0.08 else 0,
        }

        # --- inject bad records deliberately (the data-quality story) ---
        if is_bad:
            kind = random.choice([
                "null_customer", "future_ts", "negative_revenue",
                "duplicate", "orphan_campaign", "bad_type",
            ])
            if kind == "null_customer":
                row["customer_id"] = ""
            elif kind == "future_ts":
                row["event_timestamp"] = (datetime(2030, 1, 1)).isoformat()
            elif kind == "negative_revenue":
                row["revenue_usd"] = -round(random.uniform(1, 500), 2)
            elif kind == "duplicate":
                row["event_id"] = "DUPLICATE-ID-0001"  # collapses many rows onto one id
            elif kind == "orphan_campaign":
                row["campaign_id"] = 999999  # doesn't exist in dim_campaign
            elif kind == "bad_type":
                row["event_type"] = "unknown_event"

        yield row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=2_000_000,
                     help="Number of fact events to generate (default 2M; use 100_000_000 for the full benchmark run)")
    ap.add_argument("--customers", type=int, default=50_000)
    ap.add_argument("--out", type=str, default="../data/bronze")
    ap.add_argument("--bad-rate", type=float, default=0.03, help="Fraction of events that are deliberately dirty")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--batch-size", type=int, default=1_000_000, help="Rows per output CSV chunk")
    args = ap.parse_args()

    random.seed(args.seed)
    Faker.seed(args.seed)

    print(f"Generating GTM dataset: {args.events:,} events, {args.customers:,} customers")
    print("-- dimension tables --")
    reps = gen_sales_reps()
    campaigns = gen_campaigns()
    products = gen_products()
    customers = gen_customers(args.customers)

    write_csv(f"{args.out}/crm/dim_sales_rep.csv", reps, list(reps[0].keys()))
    write_csv(f"{args.out}/crm/dim_campaign.csv", campaigns, list(campaigns[0].keys()))
    write_csv(f"{args.out}/crm/dim_product.csv", products, list(products[0].keys()))
    write_csv(f"{args.out}/crm/dim_customer.csv", customers, list(customers[0].keys()))

    print("-- fact events (streamed to disk in batches) --")
    customer_ids = [c["customer_id"] for c in customers]
    campaign_ids = [c["campaign_id"] for c in campaigns]
    rep_ids = [r["sales_rep_id"] for r in reps]

    fieldnames = ["event_id", "event_type", "event_timestamp", "customer_id",
                  "campaign_id", "sales_rep_id", "revenue_usd"]

    out_dir = f"{args.out}/events"
    os.makedirs(out_dir, exist_ok=True)

    gen = gen_events(args.events, customer_ids, campaign_ids, rep_ids, args.bad_rate, args.seed)
    batch, batch_num, written = [], 0, 0
    for row in gen:
        batch.append(row)
        if len(batch) >= args.batch_size:
            batch_num += 1
            path = f"{out_dir}/events_part_{batch_num:04d}.csv"
            write_csv(path, batch, fieldnames)
            written += len(batch)
            batch = []
    if batch:
        batch_num += 1
        path = f"{out_dir}/events_part_{batch_num:04d}.csv"
        write_csv(path, batch, fieldnames)
        written += len(batch)

    print(f"\nDone. {written:,} events written across {batch_num} file(s) to {out_dir}/")
    print(f"~{args.bad_rate*100:.1f}% of events are deliberately dirty (nulls, dup ids, future timestamps, "
          f"negative revenue, orphan campaign_ids, unknown event types).")
    print(f"Campaign #{campaign_ids[0]} was deliberately over-weighted (~55% of events) to create data skew.")


if __name__ == "__main__":
    main()
