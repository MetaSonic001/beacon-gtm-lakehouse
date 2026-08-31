# Beacon — a GTM Revenue Intelligence Lakehouse

A local, end-to-end data platform that models the data infrastructure behind
a B2B SaaS company's Go-To-Market org: CRM data, marketing/web events, and
product usage, flowing through a Bronze → Silver → Gold lakehouse into a
queryable analytics layer — batch (Spark/Airflow) **and** streaming
(Kafka/Spark Structured Streaming).

Built as a portfolio project to demonstrate practical data engineering:
pipeline design, data quality enforcement, Spark performance tuning with
real benchmark numbers, and orchestration — not just "downloaded a CSV and
made a chart."

> Runs entirely on your laptop. No AWS/GCP/Azure account required —
> MinIO stands in for S3, Redpanda stands in for a Kafka cluster, Postgres
> is the serving layer.

---

## Why this project exists

Most portfolio data projects are "download NYC taxi data, run a groupby,
make a dashboard." That doesn't demonstrate much. This project is built
around the questions an interviewer actually asks a junior/early-career
data engineer:

- *"Walk me through a pipeline you built."* → Bronze/Silver/Gold below.
- *"How do you handle bad data?"* → deliberately injected dirty records +
  a real quarantine table with a rejection rate (see [Data Quality](#data-quality)).
- *"Tell me about a time you optimized a Spark job."* → the
  [benchmark](#spark-optimization-benchmark) below has real, reproducible
  numbers from a broadcast join vs. sort-merge join.
- *"Batch vs. streaming — when would you use each?"* → this project has
  both, wired to the same lakehouse.

The honest framing for interviews:

> "I don't have several years of professional data engineering experience,
> but I have Spark/Scala experience from my internship at Jio Platforms and
> software engineering experience from Siemens. To prepare for this role, I
> built this end-to-end GTM data platform myself — CRM + event data,
> Bronze/Silver/Gold on Spark, a data quality layer, a Kafka streaming path,
> and I benchmarked and optimized the Spark join strategy, cutting runtime
> from Xs to Ys."

---

## Architecture

```
                              DATA SOURCES
                                   │
                 ┌─────────────────┴─────────────────┐
                 │                                   │
         CRM reference data                  Live event generator
      (customers, reps, campaigns,             (web/product events)
             products)                                │
                 │                                     ▼
                 │                                   Redpanda (Kafka API)
                 │                                     │
                 ▼                                     ▼
           Python ingestion                 Spark Structured Streaming
                 │                                     │
                 ▼                                     ▼
         ┌───────────────────  BRONZE (raw, Parquet/CSV)  ◄──────────────┐
         │                     MinIO (S3-compatible)                     │
         └───────────────────────────┬───────────────────────────────────┘
                                      ▼
                          PySpark batch ETL (bronze_to_silver.py)
                    schema enforcement · dedup · validity checks
                                      │
                        ┌─────────────┴─────────────┐
                        ▼                           ▼
                 SILVER (clean, Parquet)     QUARANTINE (rejected rows
                 partitioned by year/month     + reason flags)
                        │
                        ▼
              PySpark aggregation (silver_to_gold.py)
                        │
                        ▼
                       GOLD
        campaign_performance · sales_pipeline · customer_activity
                        │
                        ▼
                    Postgres (serving layer) ── SQL analytics / BI tool

        Orchestrated end to end by an Airflow DAG (airflow/dags/).
```

### Why MinIO / Redpanda instead of cloud services

The point of the project is to demonstrate the *concepts* (object storage,
Bronze/Silver/Gold, streaming ingestion) without a cloud bill or account
setup getting in the way. MinIO speaks the S3 API, Redpanda speaks the
Kafka API — so everything you write here (`boto3`/`kafka-python` calls,
Spark's `kafka` connector) is the same code you'd point at real S3/MSK
later. That's a legitimate thing to say in an interview: *"I built this
against S3-compatible/Kafka-API-compatible local services so the same
code ports to a managed cloud stack without changes."*

---

## Repo layout

```
beacon-gtm-lakehouse/
├── data_generator/
│   └── generate_data.py         # CRM dims + skewed, dirty event data generator
├── ingestion/
│   └── kafka_producer.py        # streams live synthetic events to Redpanda
├── spark_jobs/
│   ├── bronze_to_silver.py      # PySpark: validate, dedup, quarantine
│   ├── silver_to_gold.py        # PySpark: business aggregates
│   ├── streaming_events.py      # Spark Structured Streaming from Kafka
│   ├── load_gold_to_postgres.py # JDBC load into the serving layer
│   └── scala/                   # one deliberate Scala Spark job (customer LTV)
├── benchmarks/
│   └── join_benchmark.py        # baseline vs broadcast vs AQE/skew vs partition pruning
├── sql/
│   ├── gold_schema.sql          # Postgres DDL
│   └── analytics_queries.sql    # ROI, win rate, LTV, churn-risk queries
├── airflow/dags/
│   └── gtm_pipeline_dag.py      # orchestrates the batch path end to end
├── scripts/
│   └── run_local.sh             # run the whole batch pipeline without Airflow
├── docker-compose.yml           # MinIO + Postgres + Redpanda + Redpanda Console
├── requirements.txt
└── .env.example
```

---

## Quickstart

### 1. Infra (optional for the pure-batch path, required for streaming/Postgres)

```bash
docker compose up -d
# MinIO console:      http://localhost:9001  (beacon / beacon12345)
# Redpanda Console:   http://localhost:8080
# Postgres:            localhost:5432 (beacon / beacon)
```

### 2. Python environment

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the whole batch pipeline in one command

```bash
./scripts/run_local.sh 2000000     # 2M events — runs in ~1-2 minutes on a laptop
```

This generates data, runs Bronze → Silver → Gold, and loads Gold into
Postgres. Then query it:

```bash
psql -h localhost -U beacon -d beacon -f sql/analytics_queries.sql
```

### 4. Run the Spark optimization benchmark

```bash
cd benchmarks && python3 join_benchmark.py --silver ../data/silver
```

### 5. (Optional) Try the streaming path

```bash
# terminal 1
python3 ingestion/kafka_producer.py --brokers localhost:19092 --rate 50

# terminal 2
cd spark_jobs
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    streaming_events.py --brokers localhost:19092
```

### 6. (Optional) Run it via Airflow instead of scripts/run_local.sh

Point Airflow's `dags/` folder at `airflow/dags/gtm_pipeline_dag.py` (see
comments in that file), then trigger `beacon_gtm_pipeline` from the UI.
This is what you demo to show orchestration: retries, scheduling,
dependency graphs, and idempotent re-runs.

---

## Scaling it up for the "real" benchmark numbers

The default `--events 2000000` is sized to run fast on a laptop while you
build and debug. Once the pipeline works end to end, regenerate at a much
larger scale for your actual portfolio numbers:

```bash
python3 data_generator/generate_data.py --events 50000000 --customers 200000 \
    --out data/bronze --batch-size 2000000
```

At that scale, `spark.local[*]` with default memory will likely need a
memory bump (`SPARK_DRIVER_MEMORY=6g`) or you'll want to run against a
small local Spark standalone cluster instead of local mode. Document
whatever you hit — running into "my laptop doesn't have enough memory for
this partition strategy, so I changed X" **is itself a good interview
story about understanding Spark's execution model.**

---

## Data quality

`bronze_to_silver.py` enforces five rules and routes failing rows to a
quarantine table rather than silently dropping them:

| Check | Rule |
|---|---|
| Referential integrity | `customer_id` must be non-null |
| Referential integrity | `campaign_id` must exist in `dim_campaign` |
| Freshness | `event_timestamp` cannot be in the future |
| Range | `revenue_usd` must be >= 0 |
| Enum validity | `event_type` must be a known type |
| Uniqueness | `event_id` deduplicated (first occurrence kept) |

Sample run (200K events, 3% deliberately dirty):

```
input rows:         200,000
duplicates removed:     976
passed validation:  194,025  (97.01%)
quarantined:           4,999  (2.50%)
elapsed:                29.0s
```

The interview version of this: *"I built a quarantine pattern rather than
dropping bad rows — every rejected record keeps its failure reason, so you
can debug upstream data issues instead of just seeing a lower row count."*

---

## Spark optimization benchmark

`benchmarks/join_benchmark.py` runs the same `fact_event ⋈ dim_customer`
join four different ways and times each. Real numbers from a local run
(200K events, `local[*]`, single machine — re-run at 20M+ events for a
more dramatic and representative delta):

| Experiment | Runtime |
|---|---|
| 1. Baseline sort-merge join (AQE off, no broadcast) | 9.58s |
| 2. Broadcast join | 0.84s |
| 3. AQE + skew join handling | 1.72s |
| 4a. Scan without partition filter | 0.50s |
| 4b. Scan with partition filter (`year`/`month`) | 0.20s |

**~11x speedup** from forcing a broadcast join on the small dimension table
instead of a full shuffle. At larger data volumes, re-run this and swap in
real Spark UI screenshots (`localhost:4040`) showing shuffle read/write and
stage duration for each strategy — that's what turns a benchmark table into
a credible interview story instead of a screenshot-free claim.

To push this further for a real skew story: join on `campaign_id` instead
of `customer_id` (the generator deliberately over-weights one campaign to
~55% of events) and compare with/without `spark.sql.adaptive.skewJoin.enabled`.

---

## What's simplified vs. the "full" version

This project intentionally leaves out some things mentioned in more
elaborate GTM-platform designs, to keep the code reviewable and the story
honest:

- **No Delta Lake** — plain partitioned Parquet. Delta adds ACID/time-travel
  semantics; swap `.parquet()` for `.format("delta")` if you want to extend
  this, but it's not needed to demonstrate the Bronze/Silver/Gold pattern.
- **No dbt** — the SQL in `sql/` is plain, hand-written analytical SQL. dbt
  is a good "v2" addition once the core pipeline is solid.
- **No Great Expectations** — data quality rules are implemented directly in
  PySpark (see above), which is easier to explain line-by-line in an
  interview than a YAML-configured framework you didn't write the logic for.
- **Spark isn't containerized** — run it locally via `pip install pyspark`.
  Containerizing `spark-submit` is a reasonable next step once the jobs work.

Call these out explicitly if asked — "I kept it simple on purpose so I can
explain every line" is a stronger answer than pretending to have used every
tool in the ecosystem.

---

## Talking points for interviews

- *Why this dataset/domain?* Recruiters describing "production-grade data
  pipelines for GTM systems" is a real, common JD phrase — this project
  speaks directly to that instead of being generic.
- *Why generate data instead of using only Kaggle CSVs?* Controlling the
  generator means you can engineer specific, realistic problems into the
  data — skew, duplicates, referential integrity violations — and then
  solve them, which is what the job actually is.
- *What would you do differently at real scale?* Partition tuning,
  cluster sizing, Delta Lake for time travel/upserts, a proper feature
  store if this fed ML, streaming exactly-once semantics beyond the
  watermark-based dedup used here.
- *Your Jio/Siemens background:* Jio gives you real Spark/Scala/Hive
  experience to reference; Siemens gives you production engineering
  discipline (CI/CD, debugging distributed systems) that a lot of junior
  DE candidates don't have.

---

## License

MIT — do whatever you want with this for your own portfolio.
