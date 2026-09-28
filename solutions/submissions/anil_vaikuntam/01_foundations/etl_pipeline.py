"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: etl_pipeline.py
Pillar: Foundations (Tasks 1.1, 1.3)
Author: Anil Vaikuntam
=============================================================

SCENARIO
--------
You are a Data Engineer at Presight. The project management team has provided
raw data extracts from their operational systems:
  - datasets/projects.csv                    → project records
  - datasets/employees.csv                   → employee/HR records
  - datasets/employees_salary_history.csv    → salary/role change history

Your job is to clean, transform, and load this data into a structured format
ready for analytics and reporting.

TASKS COVERED BY THIS FILE
---------------------------
  Task 1.1 → Clean and transform projects.csv using Pandas
  Task 1.3 → Identify and fix data quality issues in employees.csv

HOW TO RUN
----------
  python solutions/submissions/anil_vaikuntam/01_foundations/etl_pipeline.py

OUTPUT
------
  Cleaned CSVs written to: outputs/
"""

import json
import logging
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
# DATA_DIR/OUTPUT_DIR are environment-overridable (needed for Task 4.1's
# containerised ETL and for running this same module inside the Airflow
# container in Pillar 3), with the host repo layout as the default.
BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    )
)
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "datasets"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", os.path.join(BASE_DIR, "outputs"))
RESULTS_DIR = os.environ.get(
    "RESULTS_DIR", os.path.join(OUTPUT_DIR, "results", "anil_vaikuntam", "01_foundations")
)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)


# ==============================================================================
# TASK 1.1 — Load and transform projects.csv
# ==============================================================================


def load_projects(filepath: str) -> pd.DataFrame:
    """Load projects.csv into a DataFrame with explicit dtypes and parsed dates."""
    logger.info("Loading projects data from %s", filepath)

    dtype_map = {
        "project_id": "string",
        "project_name": "string",
        "department": "string",
        "status": "string",
        "project_manager_id": "string",
        "priority": "string",
        "region": "string",
    }
    df = pd.read_csv(
        filepath,
        dtype=dtype_map,
        parse_dates=["start_date", "end_date"],
    )
    logger.info("Loaded %d project rows, %d columns", len(df), df.shape[1])
    return df


def transform_projects(df: pd.DataFrame) -> pd.DataFrame:
    """Apply business-logic transformations to the projects DataFrame."""
    logger.info("Transforming projects data...")
    df = df.copy()

    # Replace missing budget/actual_cost with 0 before deriving anything from
    # them, so budget_variance/is_over_budget/budget_utilisation_pct are never
    # NaN for rows that started with a missing value.
    null_budget = df["budget"].isna().sum()
    null_actual = df["actual_cost"].isna().sum()
    logger.info(
        "Null budget: %d rows | Null actual_cost: %d rows -> filled with 0",
        null_budget,
        null_actual,
    )
    df["budget"] = df["budget"].fillna(0)
    df["actual_cost"] = df["actual_cost"].fillna(0)

    # Derived column: budget_variance
    df["budget_variance"] = df["actual_cost"] - df["budget"]

    # Derived column: is_over_budget
    df["is_over_budget"] = df["actual_cost"] > df["budget"]

    # Derived column: duration_days (only where both dates exist)
    has_both_dates = df["start_date"].notna() & df["end_date"].notna()
    df["duration_days"] = np.where(
        has_both_dates, (df["end_date"] - df["start_date"]).dt.days, np.nan
    )

    # Derived column: budget_utilisation_pct (guard div-by-zero)
    df["budget_utilisation_pct"] = np.where(
        df["budget"] > 0, (df["actual_cost"] / df["budget"]) * 100, np.nan
    )

    # Standardise status (strip whitespace, consistent casing)
    df["status"] = df["status"].str.strip().str.title()

    # Map status -> status_category
    status_map = {
        "In Progress": "Active",
        "Completed": "Closed",
        "Not Started": "Pending",
        "On Hold": "Pending",
    }
    df["status_category"] = df["status"].map(status_map)
    unmapped = df["status_category"].isna().sum()
    if unmapped:
        logger.warning(
            "%d rows had a status value outside the expected 4 categories", unmapped
        )

    # Derived column: risk_level
    # .to_numpy(dtype=bool) forces a plain bool ndarray — `priority` is
    # pandas' nullable "string" dtype, so the comparison below returns
    # nullable "boolean" dtype, which np.select rejects on some
    # pandas/numpy version combinations.
    conditions = [
        ((df["priority"] == "Critical") | (df["is_over_budget"])).to_numpy(dtype=bool),
        ((df["priority"] == "High") | (df["budget_utilisation_pct"] > 90)).to_numpy(
            dtype=bool
        ),
    ]
    choices = ["High", "Medium"]
    df["risk_level"] = np.select(conditions, choices, default="Low")

    logger.info("Transform complete: %d rows, %d columns", len(df), df.shape[1])
    logger.info(
        "risk_level distribution: %s", df["risk_level"].value_counts().to_dict()
    )
    return df


# ==============================================================================
# TASK 1.3 — Data quality issues in employees.csv
# ==============================================================================


def load_employees(filepath: str) -> pd.DataFrame:
    """Load employees.csv and log a null-count summary. Fixes happen in clean_employees()."""
    logger.info("Loading employees data from %s", filepath)
    df = pd.read_csv(filepath, dtype={"employee_id": "string"})
    logger.info("Loaded %d employee rows, %d columns", len(df), df.shape[1])
    null_counts = df.isna().sum()
    logger.info("Null counts per column:\n%s", null_counts[null_counts > 0].to_string())
    return df


def clean_employees(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect and fix data quality issues in employees.csv.

    All detection is vectorised (boolean masks, groupby aggregations) — found
    by profiling the full 1,000-row file rather than sampling.
    """
    logger.info("Cleaning employees data...")
    df = df.copy()
    quality_summary = {}

    # Issue 1: Missing values — blank email addresses
    missing_email = df["email"].isna() | (df["email"].astype(str).str.strip() == "")
    n_missing_email = int(missing_email.sum())
    logger.info("Issue 1 [Missing values] — blank emails: %d rows", n_missing_email)
    df.loc[missing_email, "email"] = "unknown@presight.ai"
    quality_summary["missing_email_fixed"] = n_missing_email

    # Issue 2: Invalid date format — hire_date values that aren't date-shaped
    # at all (e.g. "-999")
    hire_date_str = df["hire_date"].astype(str)
    non_date_shaped = hire_date_str.str.match(r"^-?\d{1,4}$")
    n_bad_format = int(non_date_shaped.sum())
    logger.info(
        "Issue 2 [Invalid date format] — non-date-shaped hire_date: %d rows",
        n_bad_format,
    )
    df.loc[non_date_shaped, "hire_date"] = pd.NA
    quality_summary["invalid_hire_date_format_fixed"] = n_bad_format

    # Issue 3: Implausible dates — hire_date is date-shaped (YYYY-MM-DD) but
    # lands on an impossible year (e.g. "99999-01-01")
    parsed_hire = pd.to_datetime(df["hire_date"], errors="coerce")
    today = pd.Timestamp.today().normalize()
    reasonable_window = (parsed_hire >= pd.Timestamp("1990-01-01")) & (
        parsed_hire <= today
    )
    implausible = df["hire_date"].notna() & parsed_hire.notna() & ~reasonable_window
    implausible_pattern = hire_date_str.str.match(r"^\d{5,}-\d{2}-\d{2}$")
    implausible = implausible | (implausible_pattern & df["hire_date"].notna())
    n_implausible = int(implausible.sum())
    logger.info(
        "Issue 3 [Implausible dates] — hire_date outside 1990-today: %d rows",
        n_implausible,
    )
    df.loc[implausible, "hire_date"] = pd.NA
    df["hire_date"] = pd.to_datetime(df["hire_date"], errors="coerce")
    quality_summary["implausible_hire_date_fixed"] = n_implausible

    # Issue 4: Numeric out-of-range — years_experience outside [0, 50].
    # Imputed with the level's median rather than dropped.
    yrs = pd.to_numeric(df["years_experience"], errors="coerce")
    out_of_range = (yrs < 0) | (yrs > 50)
    n_out_of_range = int(out_of_range.sum())
    logger.info(
        "Issue 4 [Numeric out-of-range] — years_experience outside [0,50]: %d rows",
        n_out_of_range,
    )
    df.loc[out_of_range, "years_experience"] = np.nan
    df["years_experience"] = pd.to_numeric(df["years_experience"], errors="coerce")
    level_median = df.groupby("level")["years_experience"].transform("median")
    df["years_experience"] = df["years_experience"].fillna(level_median)
    quality_summary["years_experience_out_of_range_fixed"] = n_out_of_range

    # Issue 5: Logical inconsistency — salary far outside the normal band for
    # the employee's level (p5/p95 per level). Winsorised to p95 rather than
    # dropped, since the rest of the record is otherwise valid.
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce").astype("float64")
    salary = df["salary"]
    p95_by_level = df.groupby("level")["salary"].transform(lambda s: s.quantile(0.95))
    p05_by_level = df.groupby("level")["salary"].transform(lambda s: s.quantile(0.05))
    salary_anomaly = (salary > p95_by_level * 1.5) | (salary < p05_by_level * 0.5)
    n_salary_anomaly = int(salary_anomaly.sum())
    logger.info(
        "Issue 5 [Logical inconsistency] — salary inconsistent with level band: %d rows",
        n_salary_anomaly,
    )
    df["salary_flagged_outlier"] = salary_anomaly
    df.loc[salary_anomaly, "salary"] = p95_by_level[salary_anomaly]
    quality_summary["salary_level_mismatch_fixed"] = n_salary_anomaly

    # Issue 6: Status conflicts — an Active employee reporting to an Inactive
    # manager. Guardrail check; finds zero on this dataset.
    inactive_ids = set(df.loc[df["status"] == "Inactive", "employee_id"])
    status_conflict = df["manager_id"].isin(inactive_ids) & (df["status"] == "Active")
    n_status_conflict = int(status_conflict.sum())
    logger.info(
        "Issue 6 [Status conflict] — Active employee reporting to an Inactive manager: %d rows",
        n_status_conflict,
    )
    quality_summary["status_conflicts_found"] = n_status_conflict

    logger.info("Data quality summary: %s", quality_summary)
    df.attrs["quality_summary"] = quality_summary
    return df


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================


def _write_output(df: pd.DataFrame, filename: str):
    """Writes to outputs/results/anil_vaikuntam/... — the submission-folder
    convention, so multiple trainees' outputs in the shared repo don't
    collide."""
    df.to_csv(os.path.join(RESULTS_DIR, filename), index=False)


def _write_pipeline_summary(
    n_projects_raw: int,
    n_projects_clean: int,
    n_employees_raw: int,
    n_employees_clean: int,
    quality_summary: dict,
    elapsed_seconds: float,
):
    """Run-summary artifact — same shape as Pillar 2's etl_full.py, scoped to
    Pillar 1's own load+transform+clean+write time."""
    lines = [
        "Presight ETL Pipeline — Pillar 1 (Foundations) Run Summary",
        "=" * 50,
        f"Run timestamp:        {datetime.now().isoformat()}",
        f"Pipeline elapsed time: {elapsed_seconds:.2f} seconds",
        "",
        "Row counts (before -> after cleaning):",
        f"  projects:   {n_projects_raw:<6} -> {n_projects_clean}",
        f"  employees:  {n_employees_raw:<6} -> {n_employees_clean}",
        "",
        "Data quality decisions made:",
        "  - projects.budget / actual_cost: null -> 0 (see transform_projects)",
        f"  - employees: {quality_summary.get('missing_email_fixed', 0)} blank emails -> 'unknown@presight.ai'",
        f"  - employees: {quality_summary.get('invalid_hire_date_format_fixed', 0)} non-date-shaped + "
        f"{quality_summary.get('implausible_hire_date_fixed', 0)} implausible hire_date values -> null",
        f"  - employees: {quality_summary.get('years_experience_out_of_range_fixed', 0)} years_experience "
        "out-of-range values -> imputed with level median",
        f"  - employees: {quality_summary.get('salary_level_mismatch_fixed', 0)} salary values outside the "
        "level's [p5,p95] band -> winsorised to p95",
        f"  - employees: {quality_summary.get('status_conflicts_found', 0)} Active-under-Inactive-manager "
        "conflicts found (guardrail, kept even at 0)",
        "",
        f"Target: full Pillar 1 pipeline (projects + employees) in < 30 seconds. "
        f"Actual: {elapsed_seconds:.2f}s ({'PASS' if elapsed_seconds < 30 else 'FAIL'})",
    ]
    with open(
        os.path.join(RESULTS_DIR, "pipeline_summary.txt"), "w", encoding="utf-8"
    ) as f:
        f.write("\n".join(lines))
    logger.info("pipeline_summary.txt written")


def run_pipeline():
    """
    Orchestrates the Pillar 1 pipeline end to end.
    """
    start = time.time()
    logger.info("=" * 70)
    logger.info("Pillar 1 — Foundations pipeline starting")
    logger.info("=" * 70)

    # Step 1: Task 1.1 — projects
    raw_projects = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    projects_clean = transform_projects(raw_projects)
    _write_output(projects_clean, "projects_clean.csv")
    logger.info("Wrote projects_clean.csv (%d rows x %d cols)", *projects_clean.shape)

    # Step 2: Task 1.3 — employees
    raw_employees = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    employees_clean = clean_employees(raw_employees)
    _write_output(employees_clean, "employees_clean.csv")
    logger.info("Wrote employees_clean.csv (%d rows x %d cols)", *employees_clean.shape)

    quality_summary = employees_clean.attrs.get("quality_summary", {})
    with open(
        os.path.join(RESULTS_DIR, "employees_quality_summary.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(quality_summary, f, indent=2)

    elapsed = time.time() - start
    _write_pipeline_summary(
        len(raw_projects),
        len(projects_clean),
        len(raw_employees),
        len(employees_clean),
        quality_summary,
        elapsed,
    )

    logger.info(
        "Pillar 1 pipeline complete in %.2f seconds. Outputs written to %s",
        elapsed,
        RESULTS_DIR,
    )
    return projects_clean, employees_clean


if __name__ == "__main__":
    run_pipeline()
