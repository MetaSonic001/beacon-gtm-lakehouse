<#
=============================================================================
Beacon GTM Lakehouse - run the whole batch pipeline end-to-end, locally.
PowerShell version (for native Windows PowerShell / Windows Terminal users).
A Python equivalent lives in scripts/run_pipeline.py (recommended).

Steps covered
  Step 1/5  Ingest actual Maven CRM data         -> data/bronze/maven
  Step 2/5  (optional) Produce streaming events  -> Redpanda topic (commented below)
  Step 3/5  Bronze -> Silver cleanup/validation   -> data/silver
  Step 4/5  Silver -> Gold dimensional/target     -> data/gold
  Step 5/5  Load Gold tables into Postgres        -> postgres

Usage
  .\scripts\run_pipeline.ps1

Design notes
  - Fail-fast: $ErrorActionPreference = 'Stop' (the PS equivalent of
    `set -e`). All paths are anchored to $RootDir so the script runs from
    any directory.
  - We invoke `python` (not `python3`) for consistency with Windows where
    the launcher is usually `python`.
  - This script runs Spark jobs natively (requires pyspark + winutils on Windows).
    For Docker-based runs, use: python scripts/run_pipeline.py --backend docker
#>

[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'   # stop on the first error (like set -euo pipefail)

# ---------------------------------------------------------------------------
# Paths & options - everything is anchored to the repo root ($RootDir) so we
# never depend on the current working directory when the script was launched.
# ---------------------------------------------------------------------------
$RootDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

$DataDir         = Join-Path $RootDir 'data'
$BronzeDir       = Join-Path $DataDir 'bronze'
$MavenBronze     = Join-Path $BronzeDir 'maven'      # actual Maven CSV files land here
$SilverDir       = Join-Path $DataDir 'silver'
$GoldDir         = Join-Path $DataDir 'gold'
$QuarantineDir   = Join-Path $DataDir 'quarantine'

$ScriptsDir      = Join-Path $RootDir 'scripts'
$SparkJobsDir    = Join-Path $RootDir 'spark_jobs'
$IngestionDir    = Join-Path $RootDir 'ingestion'

# ---------------------------------------------------------------------------
# Pre-flight checks: confirm python and pyspark exist BEFORE running steps
# that nothing downstream could consume.
# ---------------------------------------------------------------------------
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: 'python' was not found on PATH. Install Python 3 first." -ForegroundColor Red
    exit 1
}

# Import pyspark from the active python environment; fail with helpful text if not.
python -c "import pyspark" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: 'pyspark' is not installed in the active Python environment." -ForegroundColor Red
    Write-Host "       Try:  pip install -r `"$RootDir\requirements.txt`""
    Write-Host "       Or use Docker backend: python scripts/run_pipeline.py --backend docker"
    exit 1
}

Write-Host "Pre-flight OK: python and pyspark are available." -ForegroundColor Green
Write-Host "Working from: $RootDir"
Write-Host ""

# Make sure output dirs exist.
foreach ($d in @($MavenBronze, $SilverDir, $GoldDir, $QuarantineDir)) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

# ---------------------------------------------------------------------------
# Step 1/5 - Ingest actual Maven CRM dataset (bronze layer)
# ---------------------------------------------------------------------------
Write-Host "=== Step 1/5: Ingest actual Maven CRM data ===" -ForegroundColor Cyan
Push-Location $ScriptsDir
try {
    python ingest_actual_data.py --out $MavenBronze
    if ($LASTEXITCODE -ne 0) { throw "ingest_actual_data.py failed (exit $LASTEXITCODE)" }
} finally {
    Pop-Location
}
Write-Host "  -> bronze Maven files written to: $MavenBronze"
Write-Host ""

# ---------------------------------------------------------------------------
# Step 2/5 - (OPTIONAL) Produce custom streaming / event data
# ---------------------------------------------------------------------------
# The batch pipeline does not depend on this step and it requires Redpanda up.
# To enable the streaming demo, uncomment the block below after starting infra:
#
#   docker compose -f "$RootDir\docker-compose.yml" up -d redpanda redpanda-console
#
# then run the producer (it streams forever, so run it in a second terminal):
#
#   Write-Host "=== Step 2/5: [OPTIONAL] Streaming events -> Redpanda topic 'gtm-events' ==="
#   Push-Location $IngestionDir
#   python kafka_producer.py --brokers localhost:19092 --rate 20
#   Pop-Location
#
# (The downstream consumer is spark_jobs/streaming_events.py.)
Write-Host "=== Step 2/5: [SKIPPED by default] Streaming / event data is optional. ===" -ForegroundColor Cyan
Write-Host "    Uncomment the block in run_pipeline.ps1 to enable it (see comments above)."
Write-Host ""

# ---------------------------------------------------------------------------
# Step 3/5 - Bronze -> Silver (clean, validate, quarantine bad records)
# ---------------------------------------------------------------------------
Write-Host "=== Step 3/5: Bronze -> Silver ===" -ForegroundColor Cyan
Push-Location $SparkJobsDir
try {
    python bronze_to_silver.py --bronze $MavenBronze --silver $SilverDir --quarantine $QuarantineDir
    if ($LASTEXITCODE -ne 0) { throw "bronze_to_silver.py failed (exit $LASTEXITCODE)" }
} finally {
    Pop-Location
}
Write-Host "  -> Cleaned silver at: $SilverDir"
Write-Host "  -> Quarantined rows at: $QuarantineDir"
Write-Host ""

# ---------------------------------------------------------------------------
# Step 4/5 - Silver -> Gold (dimensional model / metrics / targets)
# ---------------------------------------------------------------------------
Write-Host "=== Step 4/5: Silver -> Gold ===" -ForegroundColor Cyan
Push-Location $SparkJobsDir
try {
    python silver_to_gold.py --silver $SilverDir --gold $GoldDir
    if ($LASTEXITCODE -ne 0) { throw "silver_to_gold.py failed (exit $LASTEXITCODE)" }
} finally {
    Pop-Location
}
Write-Host "  -> Gold tables at: $GoldDir"
Write-Host ""

# ---------------------------------------------------------------------------
# Step 5/5 - Load Gold -> Postgres
# ---------------------------------------------------------------------------
# Spark needs the PostgreSQL JDBC driver to reach Postgres; it is NOT bundled
# with Spark. Under spark-submit you'd add:
#     --packages org.postgresql:postgresql:42.7.3
# With plain `python` (like the rest of this script) the driver must already be
# on the Spark classpath. We fail GRACEFULLY here (no script abort) so the rest
# of the pipeline can still report success if Postgres simply isn't running.
$PgPackages = 'org.postgresql:postgresql:42.7.3'
Write-Host "=== Step 5/5: Load Gold -> Postgres (needs JDBC driver: $PgPackages) ===" -ForegroundColor Cyan
Write-Host "    Requires Postgres up: 'docker compose up -d postgres'"
Push-Location $SparkJobsDir
try {
    python load_gold_to_postgres.py --gold $GoldDir
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Postgres load exited with code $LASTEXITCODE. This is expected if Postgres is not running."
        Write-Host "  Start it with:  docker compose -f `"$RootDir\docker-compose.yml`" up -d postgres"
    } else {
        Write-Host "  -> Gold tables loaded into Postgres successfully."
    }
} finally {
    Pop-Location
}
Write-Host ""

# ---------------------------------------------------------------------------
# Done! Hand the user the links to the running UIs.
# ---------------------------------------------------------------------------
Write-Host "==============================================================================" -ForegroundColor Green
Write-Host "Pipeline complete!" -ForegroundColor Green
Write-Host ""
Write-Host "  Bronze (raw)     : $MavenBronze"
Write-Host "  Silver (clean)   : $SilverDir"
Write-Host "  Gold (modeled)   : $GoldDir"
Write-Host ""
Write-Host "Dashboards / consoles (start them with: 'docker compose up -d'):"
Write-Host "  MinIO console    : http://localhost:9001"
Write-Host "  Redpanda console : http://localhost:8080"
Write-Host ""
Write-Host "Next: try the benchmark for the interview numbers:"
Write-Host "  cd `"$RootDir\benchmarks`" && python join_benchmark.py --silver `"$SilverDir`""
Write-Host "==============================================================================" -ForegroundColor Green