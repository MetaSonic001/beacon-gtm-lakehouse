-- ============================================================================
-- Beacon GTM Lakehouse — analyst-facing queries against the Gold layer
-- ============================================================================
-- Run these against Postgres AFTER spark_jobs/load_gold_to_postgres.py has
-- loaded the gold tables (docker compose up -d postgres first).
--
--   docker compose exec postgres psql -U beacon -d beacon -f sql/analytics_queries.sql
--
-- Each query demonstrates a real SQL concept (aggregations, joins, window
-- functions, conditional logic). A great resource for interview prep.
-- Note: Uses the actual Maven dataset schema (company names, product names as keys)
-- ============================================================================

-- ============================================================================
-- 1. Sales representative leaderboard — who closes the most business?
--    Demonstrates: ORDER BY, LIMIT, numeric ratios.
-- ============================================================================
SELECT sales_agent,
       manager,
       regional_office,
       num_opportunities AS deals,
       num_won           AS won,
       won_value,
       ROUND(100.0 * win_rate, 1) AS win_rate_pct
FROM gold.sales_rep_performance
WHERE num_opportunities > 0
ORDER BY won_value DESC
LIMIT 10;

-- ============================================================================
-- 2. Win rate by regional office — which office converts best?
--    Demonstrates: GROUP BY + aggregate over a joined dimension column.
-- ============================================================================
SELECT regional_office,
       SUM(num_opportunities) AS total_deals,
       SUM(num_won)           AS total_won,
       ROUND(100.0 * SUM(num_won) / NULLIF(SUM(num_opportunities), 0), 1) AS win_rate_pct
FROM gold.sales_rep_performance
GROUP BY regional_office
ORDER BY win_rate_pct DESC NULLS LAST;

-- ============================================================================
-- 3. Won revenue by sector — where is the money coming from?
--    Demonstrates: GROUP BY multiple columns, NULLIF to avoid div-by-zero.
-- ============================================================================
SELECT sector,
       COUNT(*)                          AS accounts,
       SUM(num_opportunities)            AS total_deals,
       SUM(won_value)                    AS total_won_value,
       ROUND(100.0 * SUM(num_won) / NULLIF(SUM(num_opportunities), 0), 1) AS win_rate_pct
FROM gold.account_performance
GROUP BY sector
ORDER BY total_won_value DESC
LIMIT 20;

-- ============================================================================
-- 4. Product performance — which hardware sells the best / fastest?
--    Demonstrates: ORDER BY on computed columns.
-- ============================================================================
SELECT product,
       series,
       sales_price,
       num_opportunities AS deals,
       num_won           AS won,
       ROUND(100.0 * win_rate, 1) AS win_rate_pct,
       ROUND(avg_days_to_close, 0) AS avg_days_to_close
FROM gold.product_performance
WHERE num_opportunities > 0
ORDER BY win_rate DESC
LIMIT 20;

-- ============================================================================
-- 5. Sales funnel — how many deals are at each stage, and what's the
--    conversion from the top of the funnel (Prospecting)?
--    Actual Maven stages: Prospecting > Engaging > Qualification > Proposal > Negotiation > Won / Lost
--    Demonstrates: CASE WHEN ordering, ratio-to-total via a subquery.
-- ============================================================================
SELECT deal_stage,
       opportunity_count,
       total_value,
       ROUND(100.0 * conversion_ratio, 1) AS pct_of_prospecting,
       REPEAT('▓', (opportunity_count * 40 / GREATEST(
                        (SELECT MAX(opportunity_count) FROM gold.pipeline_funnel), 1))) AS bar
FROM gold.pipeline_funnel
ORDER BY stage_index;

-- ============================================================================
-- 6. Manager leaderboard — team total value & win rate by manager.
--    Demonstrates: GROUP BY a dimension column joined into the rep table.
-- ============================================================================
SELECT manager,
       COUNT(*)                 AS reps,
       SUM(num_opportunities)   AS total_deals,
       SUM(won_value)           AS total_won_value,
       ROUND(100.0 * SUM(num_won) / NULLIF(SUM(num_opportunities), 0), 1) AS win_rate_pct
FROM gold.sales_rep_performance
WHERE manager IS NOT NULL
GROUP BY manager
ORDER BY total_won_value DESC;

-- ============================================================================
-- 7. Accounts with the biggest win value (top-quartile customers using a window
--    function) — Demonstrates: NTILE() window function.
-- ============================================================================
SELECT account_name,
       sector,
       num_won,
       won_value,
       NTILE(4) OVER (ORDER BY won_value DESC) AS value_quartile
FROM gold.account_performance
WHERE won_value > 0
ORDER BY won_value DESC
LIMIT 20;

-- ============================================================================
-- 8. Data quality trend — how clean is the pipeline over time? This table is
--    populated each bronze->silver run. Empty until the pipeline has run.
--    Demonstrates: ordering by timestamp, ratio percent.
-- ============================================================================
SELECT run_timestamp,
       input_rows,
       duplicates_removed,
       quarantined,
       passed_validation,
       ROUND(100.0 * quarantined / NULLIF(input_rows, 0), 2) AS quarantine_rate_pct
FROM gold.data_quality_log
ORDER BY run_timestamp DESC
LIMIT 20;