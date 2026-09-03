#!/usr/bin/env python3
# =============================================================================
# run_pipeline.py — Beacon GTM Lakehouse: ONE command to run the whole pipeline.
# =============================================================================
# This is the single "just run it" entry point. It drives every step in order:
#
#   Step 1/6  Ingest actual Maven CRM data      -> data/bronze/maven
#   Step 2/6  Bounded streaming demo (30s)      -> data/bronze/streaming_events
#   Step 3/6  Bronze -> Silver (clean/validate) -> data/silver
#   Step 4/6  Silver -> Gold (analytics tables) -> data/gold
#   Step 5/6  Gold -> Postgres serving layer    -> postgres (gold.*)
#   Step 6/6  (Airflow runs daily on schedule)  -> dashboard at localhost:8080
#
# What this adds over running steps by hand:
#   1. Spins up ALL infrastructure (Postgres, MinIO, Redpanda, Airflow, Spark)
#   2. LIVE logs — every line streamed to terminal as it happens.
#   3. PERSISTED logs — identical output teed to logs/pipeline_<timestamp>.log
#   4. Captures Bronze->Silver quality summary -> data/quality/quality_log.csv
#   5. Runs a BOUNDED streaming demo (producer + consumer for 30s) so it completes.
#   6. Leaves all containers running (always-on) for Airflow/dashboard access.
#
# Execution backend for Spark jobs (auto-detected, overridable):
#   • "native" — plain `python` runs Spark jobs locally. Works on Linux/macOS/WSL,
#     or native Windows IF Hadoop (winutils + HADOOP_HOME) is present.
#   • "docker" — Spark jobs run in the repo's Spark container
#     (`docker compose run --rm spark python /app/spark_jobs/...`). Canonical
#     Windows path: the apache/spark base image ships Hadoop natively.
#   The data ingestion (Step 1) and streaming (Step 2) always run via docker.
#
# Usage
# -----
#   python scripts/run_pipeline.py                    # auto backend, starts infra
#   python scripts/run_pipeline.py --no-infra         # assumes infra already up
#   python scripts/run_pipeline.py --backend docker   # force docker backend
#   python scripts/run_pipeline.py --no-postgres      # skip Step 5
#   python scripts/run_pipeline.py --log-dir logs
# =============================================================================

from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Anchoring
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR        = ROOT_DIR / "data"
BRONZE_DIR      = DATA_DIR / "bronze"
MAVEN_BRONZE    = BRONZE_DIR / "maven"
SILVER_DIR      = DATA_DIR / "silver"
GOLD_DIR        = DATA_DIR / "gold"
QUARANTINE_DIR  = DATA_DIR / "quarantine"
QUALITY_DIR     = DATA_DIR / "quality"
QUALITY_CSV     = QUALITY_DIR / "quality_log.csv"

PYTHON = sys.executable              # the interpreter that launched this script
IS_CI  = not sys.stdout.isatty()     # avoid ANSI colour codes when piped

# ANSI colours (only used on a real terminal)
def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if not IS_CI else text

GREEN, YELLOW, RED, CYAN, BOLD, DIM = "32", "33", "31", "36", "1", "90"


# ---------------------------------------------------------------------------
# Log teeing — every line goes to BOTH the terminal and the persisted log.
# ---------------------------------------------------------------------------
class Tee:
    """Duplicates every write to an extra file on top of standard output."""

    def __init__(self, log_path: Path):
        self.log = open(log_path, "w", encoding="utf-8")
        self.lock = threading.Lock()

    def write(self, line: str) -> None:
        with self.lock:
            if self.log.closed:
                return
            self.log.write(line)
            self.log.flush()

    def close(self) -> None:
        with self.lock:
            self.log.close()


_TEE = Tee(Path(os.devnull))

_print = print
def print(*args, **kwargs):
    """print() that also tees to the log file (used for everything we show)."""
    out = " ".join(str(a) for a in args)
    _print(*args, **kwargs)
    _TEE.write(out + "\n")


def banner(title: str) -> None:
    print("")
    print(_c(BOLD, "=" * 74))
    print(_c(BOLD, title))
    print(_c(BOLD, "=" * 74))


# ---------------------------------------------------------------------------
# Backend resolution (native python vs docker spark container)
# ---------------------------------------------------------------------------
def _winutils_present() -> bool:
    """Native Spark on Windows needs winutils.exe + HADOOP_HOME."""
    hh = os.environ.get("HADOOP_HOME")
    if not hh:
        return False
    exe = (Path(hh) / "bin" / "winutils.exe")
    return exe.exists() or shutil.which("winutils.exe") is not None


def resolve_backend(pref: str) -> str:
    if pref in ("native", "docker"):
        return pref
    # auto: native works on unix/WSL; on native Windows it needs winutils.
    if os.name == "nt" and not _winutils_present():
        return "docker"
    return "native"


def docker_image_ready() -> bool:
    try:
        r = subprocess.run(
            ["docker", "compose", "images", "-q", "spark"],
            cwd=ROOT_DIR, capture_output=True, text=True, timeout=60,
        )
        return r.returncode == 0 and bool(r.stdout.strip())
    except Exception:
        return False


def ensure_docker_image(build: bool) -> bool:
    if docker_image_ready():
        return True
    if not build:
        print(_c(RED, "  ✗ Spark container image is not built yet."))
        print(_c(YELLOW, "    Build it once (downloads the apache/spark base + JARs,"))
        print(_c(YELLOW, "    a few minutes on first run), then re-run the pipeline:"))
        print(_c(BOLD, "        python scripts/run_pipeline.py --build-image"))
        return False
    print(_c(YELLOW, "  → Building Spark container image (first run only)..."))
    rc = run_command(
        "Build Spark image",
        ["docker", "compose", "build", "spark"],
        cwd=ROOT_DIR,
    )
    return rc == 0


# ---------------------------------------------------------------------------
# Infrastructure management
# ---------------------------------------------------------------------------
def start_infra() -> bool:
    """Start all Docker Compose services (Postgres, MinIO, Redpanda, Airflow, Spark)."""
    banner(_c(CYAN, "▶ Starting infrastructure (all services)"))
    print(_c(YELLOW, "  $ docker compose up -d"))
    proc = subprocess.Popen(
        ["docker", "compose", "up", "-d"],
        cwd=ROOT_DIR,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line.rstrip("\n"))
    proc.wait()
    if proc.returncode != 0:
        print(_c(RED, "  ✗ Failed to start infrastructure"))
        return False
    print(_c(GREEN, "  ✓ Infrastructure started"))
    return True


def wait_for_healthy(timeout: int = 180) -> bool:
    """Poll docker compose ps until all services report healthy (or timeout)."""
    banner(_c(CYAN, "▶ Waiting for all services to become healthy"))
    start = time.time()
    services = [
        "beacon-postgres", "beacon-minio", "beacon-minio-init",
        "beacon-redpanda", "beacon-redpanda-console",
        "beacon-airflow-init", "beacon-airflow-webserver",
        "beacon-airflow-scheduler", "beacon-airflow-triggerer",
        "beacon-spark",
    ]
    while time.time() - start < timeout:
        try:
            r = subprocess.run(
                ["docker", "compose", "ps", "--format", "json"],
                cwd=ROOT_DIR, capture_output=True, text=True, timeout=10,
            )
            if r.returncode != 0:
                time.sleep(3)
                continue
            import json
            lines = r.stdout.strip().split("\n")
            all_healthy = True
            for line in lines:
                if not line:
                    continue
                info = json.loads(line)
                name = info.get("Name", "")
                state = info.get("State", "")
                health = info.get("Health", "")
                if any(s in name for s in services):
                    if state != "running" or (health and health != "healthy"):
                        all_healthy = False
                        print(_c(DIM, f"  waiting: {name} state={state} health={health}"))
                        break
            if all_healthy:
                print(_c(GREEN, "  ✓ All services healthy"))
                return True
        except Exception as e:
            print(_c(YELLOW, f"  [warn] health check error: {e}"))
        time.sleep(3)
    print(_c(YELLOW, f"  [warn] Timeout waiting for healthy services after {timeout}s"))
    return False


# ---------------------------------------------------------------------------
# Subprocess orchestration
# ---------------------------------------------------------------------------
def run_command(name: str, cmd: list[str], cwd: Path) -> int:
    """Run an arbitrary command, streaming its stdout live AND into the log."""
    banner(name)
    print(_c(YELLOW, f"  $ {' '.join(cmd)}"))
    proc = subprocess.Popen(
        cmd, cwd=cwd,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line.rstrip("\n"))
    proc.wait()
    if proc.returncode != 0:
        print(_c(RED, f"  ✗ {name} FAILED (exit {proc.returncode})"))
    else:
        print(_c(GREEN, f"  ✓ {name} OK"))
    return proc.returncode


QUALITY_PATTERNS = {
    "input_rows":          re.compile(r"^\s*input rows:\s*([\d,]+)"),
    "duplicates_removed":  re.compile(r"^\s*duplicates removed:\s*([\d,]+)"),
    "passed_validation":   re.compile(r"^\s*passed validation:\s*([\d,]+)"),
    "quarantined":         re.compile(r"^\s*quarantined:\s*([\d,]+)"),
    "elapsed_seconds":     re.compile(r"^\s*elapsed:\s*([\d.]+)s"),
}


def run_spark_step(name: str, script: str, flag_args: list[tuple[str, Path]],
                   backend: str, log_quality: bool = False) -> int:
    """Run one Spark pipeline script via the selected backend."""
    banner(_c(CYAN, f"▶ {name}  [{backend} backend]"))

    if backend == "docker":
        # Container paths: the repo's ./data is bind-mounted at /app/data.
        def cpath(p: Path) -> str:
            try:
                rel = p.resolve().relative_to(DATA_DIR.resolve())
                return "/app/data/" + rel.as_posix()
            except ValueError:
                return str(p)
        cmd = ["docker", "compose", "run", "--rm", "spark",
               "python", f"/app/spark_jobs/{script}.py"]
        args = [flag for flag, p in flag_args for flag in (flag, cpath(p))]
        cwd = ROOT_DIR
    else:  # native
        cmd = [PYTHON, str(ROOT_DIR / "spark_jobs" / f"{script}.py")]
        args = [flag for flag, p in flag_args for flag in (flag, str(p))]
        cwd = ROOT_DIR

    print(_c(DIM, "  $ " + " ".join([*cmd, *args][:6]) + " ..."))

    proc = subprocess.Popen(
        [*cmd, *args], cwd=cwd,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    )
    quality: dict[str, str] = {}
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\n")
        print(line)
        if log_quality:
            for key, pat in QUALITY_PATTERNS.items():
                m = pat.search(line)
                if m and key not in quality:
                    quality[key] = m.group(1).replace(",", "")
    proc.wait()

    if proc.returncode != 0:
        print(_c(RED, f"  ✗ {name} FAILED (exit {proc.returncode})"))
    else:
        print(_c(GREEN, f"  ✓ {name} OK"))
        if log_quality and quality:
            _append_quality_row(quality)
    return proc.returncode


# ---------------------------------------------------------------------------
# Quality-log capture (no Postgres required)
# ---------------------------------------------------------------------------
def _ensure_quality_csv_header() -> None:
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    if not QUALITY_CSV.exists():
        QUALITY_CSV.write_text(
            "run_timestamp,input_rows,duplicates_removed,passed_validation,"
            "quarantined,elapsed_seconds\n",
            encoding="utf-8",
        )


def _append_quality_row(quality: dict) -> None:
    try:
        _ensure_quality_csv_header()
        now = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row = ",".join([
            now, quality.get("input_rows", ""), quality.get("duplicates_removed", ""),
            quality.get("passed_validation", ""), quality.get("quarantined", ""),
            quality.get("elapsed_seconds", ""),
        ])
        with open(QUALITY_CSV, "a", encoding="utf-8") as f:
            f.write(row + "\n")
        print(_c(GREEN, "  -> quality metrics appended to data/quality/quality_log.csv"))
    except Exception as e:
        print(_c(YELLOW, f"  [warn] could not write quality log csv: {e}"))


# ---------------------------------------------------------------------------
# Streaming demo (bounded)
# ---------------------------------------------------------------------------
def run_streaming_demo() -> int:
    """Run a bounded streaming demo: produce 500 events, consume for 30s."""
    banner(_c(CYAN, "▶ Step 2/6 — Streaming demo (bounded: 500 events, 30s consumer)"))

    # 1. Produce events (via docker so it uses redpanda:29092)
    print(_c(YELLOW, "  → Producing 500 events to Redpanda..."))
    rc = run_command(
        "Produce streaming events",
        ["docker", "compose", "run", "--rm", "spark",
         "python", "/app/ingestion/kafka_producer.py",
         "--brokers", "redpanda:29092",
         "--topic", "gtm-events",
         "--rate", "50",
         "--count", "500"],
        cwd=ROOT_DIR,
    )
    if rc != 0:
        return rc

    # 2. Run streaming consumer for 30 seconds
    print(_c(YELLOW, "  → Running Spark streaming consumer for 30s..."))
    rc = run_command(
        "Consume streaming events (30s)",
        ["docker", "compose", "run", "--rm", "spark",
         "python", "/app/spark_jobs/streaming_events.py",
         "--brokers", "redpanda:29092",
         "--topic", "gtm-events",
         "--bronze-out", "/app/data/bronze",
         "--watermark-minutes", "2",
         "--duration-seconds", "30"],
        cwd=ROOT_DIR,
    )
    return rc


# ---------------------------------------------------------------------------
# Final summary — where the results live
# ---------------------------------------------------------------------------
def print_results_summary() -> None:
    banner(_c(GREEN, "🏁 PIPELINE COMPLETE — where the results are saved"))
    rows = [
        ("Raw landing (Bronze)",      "data/bronze/maven/*.csv",
         "4 normalized CSVs from actual Maven CRM dataset"),
        ("Streaming bronze (Bronze)", "data/bronze/streaming_events/",
         "Parquet from 30s bounded streaming demo (partitioned by event_date)"),
        ("Clean layer (Silver)",      "data/silver/{dim_account, dim_product, dim_sales_team, fact_opportunity}",
         "validated, deduplicated Parquet (fact is year/month-partitioned)"),
        ("Analytics layer (Gold)",    "data/gold/{account_performance, product_performance, sales_rep_performance, pipeline_funnel}",
         "pre-aggregated BI-ready Parquet tables"),
        ("Rejected rows (quarantine)", "data/quarantine/fact_opportunity_rejected",
         "rows that failed validation + reason flags"),
        ("Serving layer (Postgres)",  "gold.* schema in Postgres",
         "same 4 Gold tables, queryable with SQL / BI tools"),
        ("Data lake (MinIO/S3)",      "buckets beacon-lakehouse / beacon-raw / beacon-curated / beacon-gold",
         "MinIO console: http://localhost:9001"),
        ("Run logs",                  "logs/pipeline_<timestamp>.log",
         "this exact output, persisted to a file"),
        ("Quality trend",             "data/quality/quality_log.csv",
         "one row per run — input / dupes / passed / quarantined / elapsed"),
        ("Airflow UI",                "http://localhost:8080 (admin/admin)",
         "DAG 'beacon_gtm_pipeline' runs daily; trigger manually anytime"),
        ("Dashboard",                 "streamlit run dashboard/app.py",
         "interactive explanation + results viewer"),
    ]
    for label, path, desc in rows:
        print(f"  • {_c(BOLD, label):<28} {path}")
        print(f"    {_c(DIM, '→')} {desc}")
    print("")


# ---------------------------------------------------------------------------
# CLI + main
# ---------------------------------------------------------------------------
def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run the full Beacon GTM Lakehouse pipeline in one command — "
                    "spins up infra, runs batch + bounded streaming, leaves infra up.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  python scripts/run_pipeline.py                    # full run, starts infra\n"
               "  python scripts/run_pipeline.py --no-infra         # infra already up\n"
               "  python scripts/run_pipeline.py --backend docker   # force docker backend\n"
               "  python scripts/run_pipeline.py --no-postgres      # skip PG load\n",
    )
    p.add_argument("--backend", choices=["auto", "native", "docker"], default="auto",
                   help="how to run Spark jobs. auto = docker on native Windows "
                        "without winutils, else native.")
    p.add_argument("--build-image", action="store_true",
                   help="with --backend docker: build the Spark image if missing")
    p.add_argument("--no-postgres", action="store_true",
                   help="skip Step 5 (load Gold into Postgres)")
    p.add_argument("--no-infra", action="store_true",
                   help="skip docker compose up -d / health checks (assume already running)")
    p.add_argument("--log-dir", type=str, default=None,
                   help="directory for the persisted log (default: logs/)")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()

    # Open the persisted log FIRST so nothing we print is lost.
    log_dir = Path(args.log_dir or (ROOT_DIR / "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"pipeline_{ts}.log"
    global _TEE
    _TEE = Tee(log_path)

    backend = resolve_backend(args.backend)

    print(_c(BOLD, "BEACON GTM LAKEHOUSE"))
    print(f"  working dir : {ROOT_DIR}")
    print(f"  run started : {ts}")
    print(f"  log file    : {log_path}")
    print(f"  backend     : {backend}")
    print(_c(YELLOW, "  (all output below is streamed live AND persisted to the above file)"))
    print("")

    # Pre-flight checks BEFORE generating data that nothing could consume.
    if PYTHON is None:
        print(_c(RED, "ERROR: no Python interpreter found."))
        return 1
    if importlib.util.find_spec("pyspark") is None and backend == "native":
        print(_c(RED, "ERROR: 'pyspark' is not installed in the active Python "
                      "environment for the native backend."))
        print(_c(YELLOW, "       Try:  pip install -r requirements.txt"))
        print(_c(YELLOW, "       Or run with the docker backend: "
                         "python scripts/run_pipeline.py --backend docker"))
        return 1
    if backend == "docker" and not ensure_docker_image(args.build_image):
        return 1

    # Start infrastructure (unless --no-infra)
    if not args.no_infra:
        if not start_infra():
            return 1
        if not wait_for_healthy():
            print(_c(YELLOW, "  [warn] Some services not healthy — continuing anyway..."))

    # Make sure output dirs exist.
    for d in (MAVEN_BRONZE, SILVER_DIR, GOLD_DIR, QUARANTINE_DIR):
        d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Step 1/6 — Ingest actual Maven CRM data (via docker, no Spark)
    # ------------------------------------------------------------------
    rc = run_command(
        "Step 1/6 — Ingest actual Maven CRM data (normalized to Bronze schema)",
        ["docker", "compose", "run", "--rm", "spark",
         "python", "/app/scripts/ingest_actual_data.py",
         "--out", "/app/data/bronze/maven"],
        cwd=ROOT_DIR,
    )
    if rc != 0:
        return rc
    print(_c(GREEN, f"  -> bronze Maven files written to: {MAVEN_BRONZE}"))

    # ------------------------------------------------------------------
    # Step 2/6 — Bounded streaming demo
    # ------------------------------------------------------------------
    rc = run_streaming_demo()
    if rc != 0:
        print(_c(YELLOW, "  [warn] Streaming demo had issues — continuing with batch pipeline..."))
        # Don't return; batch pipeline is the main value

    # ------------------------------------------------------------------
    # Step 3/6 — Bronze -> Silver (validates, dedups, quarantines)
    # ------------------------------------------------------------------
    rc = run_spark_step(
        "Step 3/6 — Bronze -> Silver (clean + validate + quarantine)",
        "bronze_to_silver",
        [("--bronze", MAVEN_BRONZE), ("--silver", SILVER_DIR),
         ("--quarantine", QUARANTINE_DIR)],
        backend,
        log_quality=True,
    )
    if rc != 0:
        return rc

    # ------------------------------------------------------------------
    # Step 4/6 — Silver -> Gold (business aggregations)
    # ------------------------------------------------------------------
    rc = run_spark_step(
        "Step 4/6 — Silver -> Gold (analytics-ready aggregates)",
        "silver_to_gold",
        [("--silver", SILVER_DIR), ("--gold", GOLD_DIR)],
        backend,
    )
    if rc != 0:
        return rc

    # ------------------------------------------------------------------
    # Step 5/6 — Load Gold -> Postgres (fail-soft if Postgres is down)
    # ------------------------------------------------------------------
    if args.no_postgres:
        banner(_c(CYAN, "▶ Step 5/6 — [SKIPPED] Gold -> Postgres (--no-postgres)"))
    else:
        rc = run_spark_step(
            "Step 5/6 — Load Gold -> Postgres (serving layer)",
            "load_gold_to_postgres",
            [("--gold", GOLD_DIR)],
            backend,
        )
        if rc != 0:
            print(_c(YELLOW, "  [warn] Postgres load failed — expected if Postgres isn't "
                             "up. Start it with:  docker compose up -d postgres"))
            print("")

    # ------------------------------------------------------------------
    # Step 6/6 — Airflow (already running)
    # ------------------------------------------------------------------
    banner(_c(CYAN, "▶ Step 6/6 — Airflow (always-on)"))
    print("  Airflow webserver: http://localhost:8080 (admin/admin)")
    print("  DAG 'beacon_gtm_pipeline' scheduled daily; trigger manually from UI.")
    print("")

    print_results_summary()
    print(_c(BOLD, f"\nFull log saved to: {log_path}"))
    print(_c(BOLD, "Infrastructure left running. Stop with: docker compose down"))
    _TEE.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())