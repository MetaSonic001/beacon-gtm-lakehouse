-- Beacon GTM Lakehouse — Gold layer schema (Postgres serving layer)
-- Load target for spark_jobs/load_gold_to_postgres.py

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.campaign_performance (
    campaign_id         BIGINT PRIMARY KEY,
    campaign_name       TEXT,
    channel             TEXT,
    budget_usd          NUMERIC(12, 2),
    total_events        BIGINT,
    leads_created        BIGINT,
    opportunities_won    BIGINT,
    revenue_usd          NUMERIC(14, 2),
    cost_per_lead_usd    NUMERIC(12, 2),
    roi_pct              NUMERIC(8, 2)
);

CREATE TABLE IF NOT EXISTS gold.sales_pipeline (
    sales_rep_id   BIGINT,
    event_type     TEXT,
    event_count    BIGINT,
    revenue_usd    NUMERIC(14, 2),
    PRIMARY KEY (sales_rep_id, event_type)
);

CREATE TABLE IF NOT EXISTS gold.customer_activity (
    customer_id           BIGINT PRIMARY KEY,
    total_events           BIGINT,
    lifetime_revenue_usd    NUMERIC(14, 2),
    first_seen_at           TIMESTAMP,
    last_seen_at            TIMESTAMP,
    company_name            TEXT,
    industry                 TEXT,
    country                  TEXT
);

CREATE TABLE IF NOT EXISTS gold.data_quality_log (
    run_id           SERIAL PRIMARY KEY,
    run_timestamp     TIMESTAMP DEFAULT now(),
    input_rows        BIGINT,
    duplicates_removed BIGINT,
    passed_validation  BIGINT,
    quarantined         BIGINT,
    elapsed_seconds     NUMERIC(10, 2)
);
