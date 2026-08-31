#!/usr/bin/env bash
# Beacon GTM Lakehouse — run the whole batch pipeline locally, no Airflow needed.
# Good for day-to-day development; use the Airflow DAG when you want to show
# orchestration (retries, scheduling, backfills) for the interview story.
#
# Usage:
#   ./scripts/run_local.sh 2000000       # 2M events (fast, ~1-2 min end to end)
#   ./scripts/run_local.sh 20000000      # 20M events (real benchmark numbers)

set -euo pipefail

EVENTS="${1:-2000000}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== 1/4 Generating data (${EVENTS} events) ==="
cd "$ROOT_DIR/data_generator"
python3 generate_data.py --events "$EVENTS" --out ../data/bronze --seed 42

echo "=== 2/4 Bronze -> Silver ==="
cd "$ROOT_DIR/spark_jobs"
python3 bronze_to_silver.py --bronze ../data/bronze --silver ../data/silver --quarantine ../data/quarantine

echo "=== 3/4 Silver -> Gold ==="
python3 silver_to_gold.py --silver ../data/silver --gold ../data/gold

echo "=== 4/4 Load Gold -> Postgres (requires docker-compose up -d postgres) ==="
python3 load_gold_to_postgres.py --gold ../data/gold || \
    echo "  (skipped/failed — is Postgres running? 'docker compose up -d postgres')"

echo ""
echo "Pipeline complete. Try the benchmark next:"
echo "  cd $ROOT_DIR/benchmarks && python3 join_benchmark.py --silver ../data/silver"
