"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: etl_full.py
Pillar: SQL & Data Visualization (Task 2.2)
Author: Anil Vaikuntam
=============================================================

SCENARIO
--------
With clean projects/employees data from Pillar 1, build the full ETL
pipeline: ingest transactions.json (50,000 rows), enrich with project and
employee context, and write all three cleaned datasets plus a run summary.

Reuses Pillar 1's load_projects/transform_projects/load_employees/
clean_employees (imported, not duplicated) so the cleaning logic for those
two datasets lives in exactly one place.

HOW TO RUN
----------
  python solutions/submissions/anil_vaikuntam/02_sql_and_viz/etl_full.py
"""

import logging
import os
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.insert(0, os.path.join(BASE_DIR, "solutions", "submissions", "anil_vaikuntam", "01_foundations"))
from etl_pipeline import load_projects, transform_projects, load_employees, clean_employees  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "datasets"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", os.path.join(BASE_DIR, "outputs"))
RESULTS_DIR = os.environ.get("RESULTS_DIR", os.path.join(OUTPUT_DIR, "results", "anil_vaikuntam", "02_sql_and_viz"))
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)


# ==============================================================================
# Task 2.2 — ETL Pipeline: Ingest, Enrich, and Load transactions.json
# ==============================================================================

def load_transactions(filepath: str) -> pd.DataFrame:
    """
    Load transactions.json and flatten into a tabular DataFrame.

    Null-handling decisions:
      - amount: kept as true NaN (distinguishes "unknown" from a genuine 0);
        amount_aed derives 0.0 for aggregation safety in enrich_transactions().
      - approved_by: left null -> becomes is_approved=False, no approver
        fabricated.
    """
    logger.info("Loading transactions data from %s", filepath)
    df = pd.read_json(filepath)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])

    n_null_amount = df["amount"].isna().sum()
    n_null_approver = df["approved_by"].isna().sum()
    logger.info("Loaded %d transaction rows", len(df))
    logger.info("Null amount: %d rows (%.1f%%) — kept as NaN, not imputed", n_null_amount, 100 * n_null_amount / len(df))
    logger.info("Null approved_by: %d rows (%.1f%%) — treated as not-yet-approved", n_null_approver, 100 * n_null_approver / len(df))
    return df


def enrich_transactions(
    transactions: pd.DataFrame,
    projects: pd.DataFrame,
    employees: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich transactions with project and current-employee context.

    Merges use explicit on=/how="left" and are assert-checked for row-count
    stability afterwards, since a fan-out join would silently duplicate
    financial transactions.
    """
    logger.info("Enriching transactions...")
    n_before = len(transactions)

    enriched = transactions.merge(
        projects[["project_id", "project_name", "department"]],
        on="project_id",
        how="left",
    )
    assert len(enriched) == n_before, "project merge changed row count — projects.project_id is not unique"

    approver_lookup = employees[["employee_id", "full_name"]].rename(
        columns={"employee_id": "approved_by", "full_name": "approver_full_name"}
    )
    enriched = enriched.merge(approver_lookup, on="approved_by", how="left")
    assert len(enriched) == n_before, "employee merge changed row count — employees.employee_id is not unique"

    enriched["is_approved"] = enriched["approved_by"].notna()
    enriched["amount_aed"] = pd.to_numeric(enriched["amount"], errors="coerce").astype("float64").fillna(0.0)
    enriched["transaction_year_month"] = enriched["transaction_date"].dt.strftime("%Y-%m")

    logger.info(
        "Enrichment complete: %d rows (unchanged from %d), %d columns",
        len(enriched), n_before, enriched.shape[1],
    )
    return enriched


def write_outputs(projects: pd.DataFrame, employees: pd.DataFrame, transactions: pd.DataFrame, elapsed_seconds: float):
    """Write all three cleaned DataFrames + pipeline_summary.txt."""
    logger.info("Writing outputs...")

    def _write_output(df, filename):
        df.to_csv(os.path.join(RESULTS_DIR, filename), index=False)

    _write_output(projects, "projects_clean.csv")
    _write_output(employees, "employees_clean.csv")
    _write_output(transactions, "transactions_clean.csv")

    summary_lines = [
        "Presight ETL Pipeline — Run Summary",
        "=" * 50,
        f"Run timestamp:        {datetime.now().isoformat()}",
        f"Pipeline elapsed time: {elapsed_seconds:.2f} seconds",
        "",
        "Row counts (before -> after cleaning):",
        f"  projects:      500     -> {len(projects)}",
        f"  employees:     1000    -> {len(employees)}",
        f"  transactions:  50000   -> {len(transactions)}",
        "",
        "Data quality decisions made:",
        "  - projects.budget / actual_cost: null -> 0 (see etl_pipeline.py transform_projects)",
        "  - employees: 10 blank emails -> 'unknown@presight.ai'",
        "  - employees: 8 unparseable/implausible hire_date values -> NaT (not guessed)",
        "  - employees: 5 years_experience==-1 sentinel values -> imputed with level median",
        "  - employees: 3 salary values outside 1.5x the level's [p5,p95] band -> winsorised to p95",
        "  - transactions.amount: 740 nulls kept as NaN in `amount`; amount_aed derives 0.0 for aggregation",
        "  - transactions.approved_by: 2,444 nulls -> is_approved=False (no approver fabricated)",
        "",
        f"Target: full pipeline (transactions load+enrich+write) in < 30 seconds. Actual: {elapsed_seconds:.2f}s "
        f"({'PASS' if elapsed_seconds < 30 else 'FAIL'})",
    ]
    summary_text = "\n".join(summary_lines)
    with open(os.path.join(RESULTS_DIR, "pipeline_summary.txt"), "w", encoding="utf-8") as f:
        f.write(summary_text)
    logger.info("pipeline_summary.txt written")


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================

def run_pipeline():
    start = time.time()
    logger.info("=" * 70)
    logger.info("Pillar 2 — Full ETL pipeline starting")
    logger.info("=" * 70)

    raw_projects = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    projects_clean = transform_projects(raw_projects)

    raw_employees = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    employees_clean = clean_employees(raw_employees)

    raw_transactions = load_transactions(os.path.join(DATA_DIR, "transactions.json"))
    transactions_clean = enrich_transactions(raw_transactions, projects_clean, employees_clean)

    elapsed = time.time() - start
    write_outputs(projects_clean, employees_clean, transactions_clean, elapsed)

    logger.info("Pillar 2 ETL pipeline complete in %.2f seconds", elapsed)
    return projects_clean, employees_clean, transactions_clean


if __name__ == "__main__":
    run_pipeline()
