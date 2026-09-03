#!/usr/bin/env python3
"""
Beacon GTM Lakehouse — Ingest Actual Maven CRM Data
====================================================
Reads the real Maven Analytics "CRM Sales Opportunities" dataset from
actual_maven_data/CRM+Sales+Opportunities/ and copies it to the bronze layer
with minimal normalization (type casting, basic cleaning).

Source schema (actual_maven_data/CRM+Sales+Opportunities/*.csv):
  accounts.csv:
    account, sector, year_established, revenue, employees, office_location, subsidiary_of

  products.csv:
    product, series, sales_price

  sales_pipeline.csv:
    opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value

  sales_teams.csv:
    sales_agent, manager, regional_office

Target schema (what bronze_to_silver.py expects - SAME as source, just cleaned):
  accounts.csv:
    account, sector, year_established, revenue, employees, office_location, subsidiary_of

  products.csv:
    product, series, sales_price

  sales_pipeline.csv:
    opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value

  sales_teams.csv:
    sales_agent, manager, regional_office

Normalization rules:
  - Strip whitespace from string columns
  - Cast numeric columns to proper types (int/double)
  - Parse dates to YYYY-MM-DD format
  - Fix known typo: "technolgy" -> "Technology" in sector
  - Fill empty subsidiary_of with empty string (not NULL)
  - Ensure all string columns are strings (not NaN)

Usage:
  python scripts/ingest_actual_data.py [--out data/bronze/maven] [--limit N]

The --limit flag truncates the pipeline table for faster iteration during development.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Anchoring
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
ACTUAL_DATA_DIR = ROOT_DIR / "actual_maven_data" / "CRM+Sales+Opportunities"
DEFAULT_OUT = ROOT_DIR / "data" / "bronze" / "maven"


# ---------------------------------------------------------------------------
# Sector normalization (fixes typos, standardizes names)
# ---------------------------------------------------------------------------
SECTOR_NORMALIZATION = {
    "technolgy": "Technology",  # typo in actual data
    "technology": "Technology",
    "software": "Software",
    "medical": "Medical",
    "retail": "Retail",
    "marketing": "Marketing",
    "finance": "Finance",
    "entertainment": "Entertainment",
    "telecommunications": "Telecommunications",
    "services": "Services",
    "employment": "Employment",
}


def normalize_sector(sector: str) -> str:
    """Lowercase, strip, map known typos, title-case."""
    if pd.isna(sector):
        return "Unknown"
    s = str(sector).strip().lower()
    return SECTOR_NORMALIZATION.get(s, s.title())


# ---------------------------------------------------------------------------
# Deal stage mapping: keep actual stages but normalize case
# ---------------------------------------------------------------------------
VALID_DEAL_STAGES = [
    "Prospecting",
    "Engaging",
    "Qualification",
    "Proposal",
    "Negotiation",
    "Won",
    "Lost",
]


def normalize_deal_stage(stage: str) -> str:
    """Normalize deal stage to valid values."""
    if pd.isna(stage):
        return "Unknown"
    s = str(stage).strip()
    # Map case-insensitively
    for valid in VALID_DEAL_STAGES:
        if valid.lower() == s.lower():
            return valid
    return s  # return as-is if not matched (will be caught by validation in bronze_to_silver)


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------
def run(out_dir: Path, limit: int | None = None) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Load raw CSVs ---
    print(f"Reading actual Maven data from: {ACTUAL_DATA_DIR}")
    accounts_raw = pd.read_csv(ACTUAL_DATA_DIR / "accounts.csv")
    products_raw = pd.read_csv(ACTUAL_DATA_DIR / "products.csv")
    teams_raw = pd.read_csv(ACTUAL_DATA_DIR / "sales_teams.csv")
    pipeline_raw = pd.read_csv(ACTUAL_DATA_DIR / "sales_pipeline.csv")

    print(f"  accounts:  {len(accounts_raw)} rows")
    print(f"  products:  {len(products_raw)} rows")
    print(f"  teams:     {len(teams_raw)} rows")
    print(f"  pipeline:  {len(pipeline_raw)} rows")

    if limit and limit < len(pipeline_raw):
        pipeline_raw = pipeline_raw.head(limit)
        print(f"  [limited pipeline to {limit} rows]")

    # --- Normalize accounts ---
    # Source: account, sector, year_established, revenue, employees, office_location, subsidiary_of
    # Target: SAME columns, just cleaned
    accounts_out = pd.DataFrame()
    accounts_out["account"] = accounts_raw["account"].astype(str).str.strip()
    accounts_out["sector"] = accounts_raw["sector"].apply(normalize_sector)
    accounts_out["year_established"] = pd.to_numeric(accounts_raw["year_established"], errors="coerce").astype("Int64")
    accounts_out["revenue"] = pd.to_numeric(accounts_raw["revenue"], errors="coerce")
    accounts_out["employees"] = pd.to_numeric(accounts_raw["employees"], errors="coerce").astype("Int64")
    accounts_out["office_location"] = accounts_raw["office_location"].astype(str).str.strip()
    accounts_out["subsidiary_of"] = accounts_raw["subsidiary_of"].fillna("").astype(str).str.strip()

    # --- Normalize products ---
    # Source: product, series, sales_price
    products_out = pd.DataFrame()
    products_out["product"] = products_raw["product"].astype(str).str.strip()
    products_out["series"] = products_raw["series"].astype(str).str.strip()
    products_out["sales_price"] = pd.to_numeric(products_raw["sales_price"], errors="coerce")

    # --- Normalize sales_teams (minimal cleaning) ---
    teams_out = pd.DataFrame()
    teams_out["sales_agent"] = teams_raw["sales_agent"].fillna("").astype(str).str.strip()
    teams_out["manager"] = teams_raw["manager"].fillna("").astype(str).str.strip()
    teams_out["regional_office"] = teams_raw["regional_office"].fillna("").astype(str).str.strip()

    # --- Normalize sales_pipeline ---
    # Source: opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value
    pipeline_out = pd.DataFrame()
    pipeline_out["opportunity_id"] = pipeline_raw["opportunity_id"].astype(str).str.strip()
    pipeline_out["sales_agent"] = pipeline_raw["sales_agent"].fillna("").astype(str).str.strip()
    pipeline_out["product"] = pipeline_raw["product"].fillna("").astype(str).str.strip()
    pipeline_out["account"] = pipeline_raw["account"].fillna("").astype(str).str.strip()
    pipeline_out["deal_stage"] = pipeline_raw["deal_stage"].apply(normalize_deal_stage)
    # Parse dates to YYYY-MM-DD format
    pipeline_out["engage_date"] = pd.to_datetime(pipeline_raw["engage_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    pipeline_out["close_date"] = pd.to_datetime(pipeline_raw["close_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    pipeline_out["close_value"] = pd.to_numeric(pipeline_raw["close_value"], errors="coerce")

    # --- Write CSVs ---
    accounts_path = out_dir / "accounts.csv"
    products_path = out_dir / "products.csv"
    teams_path = out_dir / "sales_teams.csv"
    pipeline_path = out_dir / "sales_pipeline.csv"

    accounts_out.to_csv(accounts_path, index=False)
    products_out.to_csv(products_path, index=False)
    teams_out.to_csv(teams_path, index=False)
    pipeline_out.to_csv(pipeline_path, index=False)

    print(f"\nWrote 4 cleaned CSVs to: {out_dir}")
    print(f"  accounts.csv       rows: {len(accounts_out):,}")
    print(f"  products.csv       rows: {len(products_out):,}")
    print(f"  sales_teams.csv    rows: {len(teams_out):,}")
    print(f"  sales_pipeline.csv rows: {len(pipeline_out):,}")

    # Quick quality summary
    print("\n=== QUALITY SUMMARY (after normalization) ===")
    print(f"  Distinct accounts in pipeline: {pipeline_out['account'].nunique():,}")
    print(f"  Pipeline rows with empty account: {(pipeline_out['account'] == '').sum():,}")
    print(f"  Distinct products in pipeline: {pipeline_out['product'].nunique():,}")
    print(f"  Pipeline rows with empty product: {(pipeline_out['product'] == '').sum():,}")
    print(f"  Distinct agents in pipeline: {pipeline_out['sales_agent'].nunique():,}")
    print(f"  Deal stages present: {sorted(pipeline_out['deal_stage'].dropna().unique().tolist())}")
    print(f"  Null engage_date: {pipeline_out['engage_date'].isna().sum():,}")
    print(f"  Null close_date:  {pipeline_out['close_date'].isna().sum():,}")
    print(f"  Null close_value: {pipeline_out['close_value'].isna().sum():,}")
    print(f"  Sectors in accounts: {sorted(accounts_out['sector'].dropna().unique().tolist())}")

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Copy and lightly clean actual Maven CRM data to the Bronze layer (keeps actual schema)."
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output directory for the 4 cleaned CSVs (default: {DEFAULT_OUT})",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional: limit pipeline rows for faster iteration (default: all)",
    )
    args = ap.parse_args()

    return run(args.out, args.limit)


if __name__ == "__main__":
    sys.exit(main())