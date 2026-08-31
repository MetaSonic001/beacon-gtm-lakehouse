-- Beacon GTM Lakehouse — analyst-facing queries against the gold layer.
-- Run these against Postgres after spark_jobs/load_gold_to_postgres.py has loaded the tables.
-- These double as good HackerRank/SQL-round practice: window functions, ratios, cohorting.

-- 1. Campaign ROI ranking
SELECT campaign_name, channel, budget_usd, revenue_usd, roi_pct
FROM gold.campaign_performance
ORDER BY roi_pct DESC NULLS LAST
LIMIT 10;

-- 2. Cost per lead by channel
SELECT channel,
       SUM(budget_usd)        AS total_spend,
       SUM(leads_created)      AS total_leads,
       ROUND(SUM(budget_usd) / NULLIF(SUM(leads_created), 0), 2) AS blended_cost_per_lead
FROM gold.campaign_performance
GROUP BY channel
ORDER BY blended_cost_per_lead ASC NULLS LAST;

-- 3. Sales rep performance: win rate proxy (won vs total opportunity events)
SELECT sales_rep_id,
       SUM(CASE WHEN event_type = 'opportunity_won' THEN event_count ELSE 0 END)   AS won,
       SUM(CASE WHEN event_type = 'opportunity_created' THEN event_count ELSE 0 END) AS created,
       ROUND(
         100.0 * SUM(CASE WHEN event_type = 'opportunity_won' THEN event_count ELSE 0 END)
         / NULLIF(SUM(CASE WHEN event_type = 'opportunity_created' THEN event_count ELSE 0 END), 0), 2
       ) AS win_rate_pct
FROM gold.sales_pipeline
GROUP BY sales_rep_id
ORDER BY win_rate_pct DESC NULLS LAST
LIMIT 10;

-- 4. Customer LTV percentiles (window function practice)
SELECT customer_id, company_name, lifetime_revenue_usd,
       NTILE(4) OVER (ORDER BY lifetime_revenue_usd DESC) AS revenue_quartile
FROM gold.customer_activity
WHERE lifetime_revenue_usd > 0
ORDER BY lifetime_revenue_usd DESC
LIMIT 20;

-- 5. Customer recency / churn-risk flag
SELECT customer_id, company_name, last_seen_at,
       CASE
           WHEN last_seen_at < now() - INTERVAL '180 days' THEN 'churn_risk'
           WHEN last_seen_at < now() - INTERVAL '90 days'  THEN 'at_risk'
           ELSE 'active'
       END AS status
FROM gold.customer_activity
ORDER BY last_seen_at ASC
LIMIT 50;

-- 6. Top industries by total pipeline revenue
SELECT industry, COUNT(*) AS customers, SUM(lifetime_revenue_usd) AS total_revenue
FROM gold.customer_activity
GROUP BY industry
ORDER BY total_revenue DESC
LIMIT 10;

-- 7. Data quality trend over pipeline runs
SELECT run_timestamp, input_rows, quarantined,
       ROUND(100.0 * quarantined / NULLIF(input_rows, 0), 2) AS quarantine_rate_pct
FROM gold.data_quality_log
ORDER BY run_timestamp DESC
LIMIT 20;
