-- =============================================================================
-- Beacon GTM Lakehouse — Gold layer schema (Postgres serving layer)
-- =============================================================================
-- This DDL runs automatically on the Postgres container's first startup
-- (mounted at /docker-entrypoint-initdb.d/01_gold_schema.sql in docker-compose).
-- It defines the analytics-ready tables that spark_jobs/load_gold_to_postgres.py
-- populates via JDBC.
--
-- These tables are the SAME gold tables that silver_to_gold.py computes from
-- the actual Maven CRM data. Analysts query `gold.*` with plain SQL.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------------------
-- account_performance — funnel & win metrics for each customer account/company.
-- Row per account (company name). Enriched with company attributes for slicing.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.account_performance (
    -- NOTE: the column is `account` (natural key). The JDBC loader recreates
    -- this table from the Parquet schema on each run, so the PK below is the
    -- documented intent; Spark's overwrite writes `account` as-is.
    account            TEXT PRIMARY KEY,          -- company name (natural key)
    sector             TEXT,                      -- industry sector
    year_established   INTEGER,                   -- year company was founded
    revenue            NUMERIC(16, 2),            -- annual revenue (in millions USD)
    employees          INTEGER,                   -- number of employees
    office_location    TEXT,                      -- headquarters location
    subsidiary_of      TEXT,                      -- parent company name
    num_opportunities  BIGINT,                    -- total opportunities opened
    num_won            BIGINT,                    -- deals won
    num_lost           BIGINT,                    -- deals lost
    num_open           BIGINT,                    -- deals not yet closed
    won_value          NUMERIC(16, 2),            -- total won deal value
    win_rate           NUMERIC(6, 4),             -- num_won / num_opportunities
    avg_deal_size      NUMERIC(16, 2),            -- avg won deal value
    avg_days_to_close  NUMERIC(8, 2)              -- avg days WON deals took to close
);

-- -----------------------------------------------------------------------------
-- sales_rep_performance — funnel & win metrics per sales agent.
-- Row per sales_agent. Enriched with manager + regional_office.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.sales_rep_performance (
    sales_agent        TEXT PRIMARY KEY,          -- agent name (matches sales_teams)
    manager            TEXT,                      -- agent's manager
    regional_office    TEXT,                      -- office location
    num_opportunities  BIGINT,                    -- total opportunities handled
    num_won            BIGINT,                    -- deals won
    num_lost           BIGINT,                    -- deals lost
    won_value          NUMERIC(16, 2),            -- total won deal value
    win_rate           NUMERIC(6, 4),             -- num_won / num_opportunities
    avg_deal_size      NUMERIC(16, 2),            -- avg won deal value
    avg_days_to_close  NUMERIC(8, 2)              -- avg days to close a deal
);

-- -----------------------------------------------------------------------------
-- product_performance — how well each hardware product sells.
-- Row per product (product name). Enriched with series + price.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.product_performance (
    product            TEXT PRIMARY KEY,          -- product name (natural key)
    series             TEXT,                      -- product line/series
    sales_price        NUMERIC(12, 2),            -- list price
    num_opportunities  BIGINT,                    -- total opportunities
    num_won            BIGINT,                    -- deals won
    won_value          NUMERIC(16, 2),            -- total won value
    win_rate           NUMERIC(6, 4),             -- num_won / num_opportunities
    avg_deal_size      NUMERIC(16, 2),            -- avg won deal value
    avg_days_to_close  NUMERIC(8, 2)              -- avg days won deals took to close
);

-- -----------------------------------------------------------------------------
-- pipeline_funnel — count of opportunities at each deal stage, plus each
-- stage's share of the top-of-funnel (Prospecting) volume. Great for a bar /
-- funnel chart showing where deals drop off.
-- Actual Maven stages: Prospecting > Engaging > Qualification > Proposal > Negotiation > Won / Lost
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.pipeline_funnel (
    deal_stage         TEXT PRIMARY KEY,          -- Prospecting/Engaging/Qualification/Proposal/Negotiation/Won/Lost
    stage_index        INTEGER,                   -- 1..6 ordering of the funnel
    opportunity_count  BIGINT,                    -- deals currently/progressed in this stage
    total_value        NUMERIC(16, 2),            -- sum of close_value in this stage
    conversion_ratio   NUMERIC(6, 4)              -- opportunity_count / top-of-funnel count
);

-- -----------------------------------------------------------------------------
-- monthly_revenue — won revenue recognized per close month (time-series).
-- Row per (close_year, close_month) of deals WON in that month. Drives the
-- revenue-over-time line in the dashboard.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.monthly_revenue (
    close_year         INTEGER NOT NULL,            -- year the deal was won
    close_month        INTEGER NOT NULL,            -- month (1-12) the deal was won
    num_won            BIGINT,                      -- deals won that month
    won_value          NUMERIC(16, 2),              -- revenue recognized that month
    PRIMARY KEY (close_year, close_month)
);

-- -----------------------------------------------------------------------------
-- data_quality_log — records each bronze->silver run's quality metrics.
-- Populated by spark_jobs/log_quality_metrics.py (or bronze_to_silver summary).
-- Lets you trend data quality over time.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.data_quality_log (
    run_id             SERIAL PRIMARY KEY,        -- auto-increment run id
    run_timestamp      TIMESTAMP DEFAULT now(),   -- when the run happened
    input_rows         BIGINT,                    -- rows read from bronze
    duplicates_removed BIGINT,                   -- dedup'd on opportunity_id
    passed_validation  BIGINT,                   -- rows that passed the checks
    quarantined        BIGINT,                    -- rows sent to quarantine
    elapsed_seconds    NUMERIC(10, 2)             -- duration of the step
);

-- -----------------------------------------------------------------------------
-- Indexes for the query patterns in sql/analytics_queries.sql
-- (Postgres uses these to speed up filters/joins/sorts on hot columns).
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_account_perf_sector ON gold.account_performance (sector);
CREATE INDEX IF NOT EXISTS idx_rep_perf_office      ON gold.sales_rep_performance (regional_office);
CREATE INDEX IF NOT EXISTS idx_product_series       ON gold.product_performance (series);
CREATE INDEX IF NOT EXISTS idx_funnel_stage         ON gold.pipeline_funnel (stage_index);