"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: dq_framework.py
Pillar: Infrastructure & Governance (Task 4.3)
Author: Anil Vaikuntam
=============================================================

SCENARIO
--------
Configurable data quality check framework — new rules are added by editing
DQ_CONFIG, not by touching this file's code. Every check function reads its
parameters from the config dict for the dataset being checked.

Imported by:
  - solutions/submissions/anil_vaikuntam/03_big_data/airflow_dag.py
    (the validate_data_quality DQ-gate task)
  - This pillar's own outputs/dq_report_{dataset}.md generation
"""

import logging
import os
import sys

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", os.path.join(BASE_DIR, "outputs"))
RESULTS_DIR = os.environ.get("RESULTS_DIR", os.path.join(OUTPUT_DIR, "results", "anil_vaikuntam", "04_infrastructure"))


# ==============================================================================
# Configuration — the only place rules/thresholds are defined
# ==============================================================================

DQ_CONFIG = {
    "projects": {
        "completeness_threshold": 0.90,
        "pk_columns": ["project_id"],
        "numeric_ranges": {
            "budget": {"min": 0, "max": 10_000_000},
            "actual_cost": {"min": 0, "max": 10_000_000},
        },
        "date_columns": ["start_date", "end_date"],
        "consistency_rules": [
            {"type": "before", "columns": ["start_date", "end_date"]},
            {"type": "non_negative", "column": "actual_cost"},
        ],
        "foreign_keys": {
            "project_manager_id": ("employees", "employee_id"),
        },
        "distribution_check_columns": ["department", "status", "region"],
        "outlier_check_columns": ["budget", "actual_cost"],
    },
    "employees": {
        "completeness_threshold": 0.85,
        "pk_columns": ["employee_id"],
        "numeric_ranges": {
            "salary": {"min": 10000, "max": 100000},
            "years_experience": {"min": 0, "max": 50},
        },
        "date_columns": ["hire_date"],
        "consistency_rules": [
            {"type": "non_negative", "column": "salary"},
            # Same p5/p95-per-level band used by etl_pipeline.py's clean_employees()
            # (Issue 5) — one shared definition of "salary matches level" across
            # the cleaning step and this independent DQ check, not two competing
            # thresholds that could disagree with each other.
            {"type": "salary_level_band", "salary_column": "salary", "level_column": "level"},
        ],
        "foreign_keys": {
            "manager_id": ("employees", "employee_id"),  # self-referential
        },
        "distribution_check_columns": ["department", "level", "region"],
        "outlier_check_columns": ["salary"],
    },
    "transactions": {
        "completeness_threshold": 0.80,  # amount/approved_by have known, documented nulls (Task 2.2)
        "pk_columns": ["transaction_id"],
        "numeric_ranges": {
            "amount": {"min": 0, "max": 1_000_000},
        },
        "date_columns": ["transaction_date"],
        "consistency_rules": [
            {"type": "non_negative", "column": "amount"},
        ],
        "foreign_keys": {
            "project_id": ("projects", "project_id"),
            "approved_by": ("employees", "employee_id"),
        },
        # currency deliberately excluded: it's single-valued (AED) by design
        # in this dataset, not a loading error — including it would be a
        # permanent false positive rather than a useful check.
        "distribution_check_columns": ["category", "payment_status"],
        "outlier_check_columns": ["amount"],
        "freshness_days": 30,
        "freshness_column": "transaction_date",
    },
}


# ==============================================================================
# Individual checks — each reads its rules from `config`, none hardcode names
# ==============================================================================
#% non-null per column; flag below threshold

def _check_completeness(df: pd.DataFrame, config: dict) -> dict:
    threshold = config["completeness_threshold"]
    details = {col: round(1 - df[col].isna().mean(), 4) for col in df.columns}
    failed_columns = [col for col, pct in details.items() if pct < threshold]
    return {
        "status": "FAIL" if failed_columns else "PASS",
        "details": details,
        "failed_columns": failed_columns,
    }

#PK columns contain no duplicates
def _check_uniqueness(df: pd.DataFrame, config: dict) -> dict:
    pk_columns = config["pk_columns"]
    total = len(df)
    unique = df.drop_duplicates(subset=pk_columns).shape[0]
    status = "PASS" if unique == total else "FAIL"
    cols = "+".join(pk_columns)
    return {"status": status, "details": f"{cols}: {unique} unique / {total} total"}

#Values within expected min/max range
def _check_validity_numeric(df: pd.DataFrame, config: dict) -> dict:
    ranges = config.get("numeric_ranges", {})
    messages = []
    failed = False
    for col, bounds in ranges.items():
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce")
        below = int((values < bounds["min"]).sum())
        above = int((values > bounds["max"]).sum())
        if below or above:
            failed = True
        messages.append(f"{col}: {below} values below minimum ({bounds['min']}), {above} values above maximum ({bounds['max']})")
    return {"status": "FAIL" if failed else "PASS", "details": "; ".join(messages) if messages else "no numeric_ranges configured"}

#	Dates valid, non-future where expected
def _check_validity_date(df: pd.DataFrame, config: dict) -> dict:
    date_columns = config.get("date_columns", [])
    messages = []
    failed = False
    today = pd.Timestamp.today().normalize()
    for col in date_columns:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        invalid = int((df[col].notna() & parsed.isna()).sum())
        future = int((parsed > today).sum())
        if invalid or future:
            failed = True
        messages.append(f"{col}: {invalid} unparseable, {future} future-dated")
    return {"status": "FAIL" if failed else "PASS", "details": "; ".join(messages) if messages else "no date_columns configured"}

#start_date < end_date; actual_cost >= 0; salary level matches role level

def _check_consistency(df: pd.DataFrame, config: dict) -> dict:
    rules = config.get("consistency_rules", [])
    messages = []
    failed = False
    for rule in rules:
        if rule["type"] == "before":
            c1, c2 = rule["columns"]
            if c1 not in df.columns or c2 not in df.columns:
                continue
            d1, d2 = pd.to_datetime(df[c1], errors="coerce"), pd.to_datetime(df[c2], errors="coerce")
            violations = int(((d1.notna()) & (d2.notna()) & (d1 > d2)).sum())
            if violations:
                failed = True
            messages.append(f"{c1} <= {c2}: {violations} violations")
        elif rule["type"] == "non_negative":
            col = rule["column"]
            if col not in df.columns:
                continue
            values = pd.to_numeric(df[col], errors="coerce")
            violations = int((values < 0).sum())
            if violations:
                failed = True
            messages.append(f"{col} >= 0: {violations} violations")
        elif rule["type"] == "salary_level_band":
            # Same p5/p95-per-level band as etl_pipeline.py's clean_employees()
            # Issue 5 — reuses that definition of "matches the level" rather
            # than inventing a second, possibly-disagreeing threshold here.
            salary_col, level_col = rule["salary_column"], rule["level_column"]
            if salary_col not in df.columns or level_col not in df.columns:
                continue
            salary = pd.to_numeric(df[salary_col], errors="coerce")
            p95 = salary.groupby(df[level_col]).transform(lambda s: s.quantile(0.95))
            p05 = salary.groupby(df[level_col]).transform(lambda s: s.quantile(0.05))
            violations = int(((salary > p95 * 1.5) | (salary < p05 * 0.5)).sum())
            if violations:
                failed = True
            messages.append(f"{salary_col} matches {level_col} band: {violations} violations")
    return {"status": "FAIL" if failed else "PASS", "details": "; ".join(messages) if messages else "no consistency_rules configured"}

#	FKs exist in referenced table (e.g. project_manager_id exists in employees)

def _check_referential_integrity(df: pd.DataFrame, config: dict, reference_tables: dict) -> dict:
    fks = config.get("foreign_keys", {})
    messages = []
    failed = False
    for col, (ref_table_name, ref_col) in fks.items():
        if col not in df.columns or ref_table_name not in reference_tables:
            messages.append(f"{col} -> {ref_table_name}.{ref_col}: SKIPPED (reference table not provided)")
            continue
        ref_values = set(reference_tables[ref_table_name][ref_col].dropna())
        actual = df[col].dropna()
        orphans = int((~actual.isin(ref_values)).sum())
        if orphans:
            failed = True
        messages.append(f"{col} -> {ref_table_name}.{ref_col}: {orphans} orphaned values")
    return {"status": "FAIL" if failed else "PASS", "details": "; ".join(messages) if messages else "no foreign_keys configured"}


# ---- Bonus checks -----------------------------------------------------------
#Distribution check — flag if > 30% of a column has the same value (data loading error)

def _check_distribution(df: pd.DataFrame, config: dict) -> dict:
    columns = config.get("distribution_check_columns", [])
    messages = []
    failed = False
    for col in columns:
        if col not in df.columns or df[col].dropna().empty:
            continue
        top_share = df[col].value_counts(normalize=True, dropna=True).iloc[0]
        if top_share > 0.30:
            failed = True
        messages.append(f"{col}: top value = {top_share:.1%} of rows")
    return {"status": "FAIL" if failed else "PASS", "details": "; ".join(messages) if messages else "no distribution_check_columns configured"}

#Freshness check — flag if max transaction_date older than 30 days

def _check_freshness(df: pd.DataFrame, config: dict) -> dict:
    col = config.get("freshness_column")
    max_age_days = config.get("freshness_days")
    if not col or col not in df.columns:
        return {"status": "PASS", "details": "no freshness_column configured"}
    parsed = pd.to_datetime(df[col], errors="coerce")
    if parsed.dropna().empty:
        return {"status": "PASS", "details": f"{col}: no valid dates to check"}
    age_days = (pd.Timestamp.today().normalize() - parsed.max()).days
    status = "FAIL" if age_days > max_age_days else "PASS"
    return {"status": status, "details": f"{col}: most recent value is {age_days} days old (threshold {max_age_days})"}

# Outlier check — flag if > 3 std dev from mean (z-score) for numeric columns

def _check_outliers(df: pd.DataFrame, config: dict) -> dict:
    columns = config.get("outlier_check_columns", [])
    messages = []
    failed = False
    for col in columns:
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce").dropna()
        if values.std(ddof=0) == 0 or values.empty:
            messages.append(f"{col}: 0 outliers (no variance)")
            continue
        z = (values - values.mean()) / values.std(ddof=0)
        n_outliers = int((z.abs() > 3).sum())
        if n_outliers:
            failed = True
        messages.append(f"{col}: {n_outliers} values beyond 3 std dev")
    return {"status": "FAIL" if failed else "PASS", "details": "; ".join(messages) if messages else "no outlier_check_columns configured"}


# ==============================================================================
# Orchestrator
# ==============================================================================

def run_data_quality_checks(df: pd.DataFrame, dataset_name: str, reference_tables: dict = None) -> dict:
    """
    Run all configured checks for `dataset_name` against `df`.

    reference_tables: dict of {table_name: DataFrame} used for the
    referential-integrity check, e.g. {"employees": employees_df}.
    """
    if dataset_name not in DQ_CONFIG:
        raise ValueError(f"No DQ_CONFIG entry for dataset '{dataset_name}' — add one rather than hardcoding a check.")
    config = DQ_CONFIG[dataset_name]
    reference_tables = reference_tables or {}

    logger.info("Running data quality checks on: %s", dataset_name)

    checks = {
        "completeness": _check_completeness(df, config),
        "uniqueness": _check_uniqueness(df, config),
        "validity_numeric": _check_validity_numeric(df, config),
        "validity_date": _check_validity_date(df, config),
        "consistency": _check_consistency(df, config),
        "referential_integrity": _check_referential_integrity(df, config, reference_tables),
        "distribution": _check_distribution(df, config),
        "freshness": _check_freshness(df, config),
        "outliers": _check_outliers(df, config),
    }

    for check_name, result in checks.items():
        if result["status"] == "FAIL":
            logger.warning("DQ check FAILED — dataset=%s check=%s details=%s", dataset_name, check_name, result["details"])

    checks_run = len(checks)
    checks_passed = sum(1 for r in checks.values() if r["status"] == "PASS")

    return {
        "dataset_name": dataset_name,
        "checks_run": checks_run,
        "checks_passed": checks_passed,
        "checks_failed": checks_run - checks_passed,
        "results": checks,
    }


def write_dq_report_markdown(dq_result: dict, output_dir: str = None):
    output_dir = output_dir or RESULTS_DIR
    dataset_name = dq_result["dataset_name"]
    lines = [
        f"# Data Quality Report — {dataset_name}",
        "",
        f"**Checks run:** {dq_result['checks_run']} | **Passed:** {dq_result['checks_passed']} | **Failed:** {dq_result['checks_failed']}",
        "",
        "| Check | Status | Details |",
        "|---|---|---|",
    ]
    for check_name, result in dq_result["results"].items():
        details = result["details"]
        if isinstance(details, dict):
            details = "; ".join(f"{k}={v}" for k, v in list(details.items())[:8]) + (" ..." if len(details) > 8 else "")
        status_icon = "PASS" if result["status"] == "PASS" else "**FAIL**"
        lines.append(f"| {check_name} | {status_icon} | {details} |")

    text = "\n".join(lines)
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, f"dq_report_{dataset_name}.md"), "w") as f:
        f.write(text)
    return text


# ==============================================================================
# Standalone entry point — regenerates dq_report_{dataset}.md for all three
# datasets, using the same loaders (load_projects/load_employees/
# load_transactions) and self-referential reference_tables pattern as
# airflow_dag.py's validate_data_quality task, so results are directly
# comparable to the DAG's live DQ gate numbers instead of drifting stale.
# ==============================================================================

def main():
    sys.path.insert(0, os.path.join(BASE_DIR, "solutions", "submissions", "anil_vaikuntam", "01_foundations"))
    sys.path.insert(0, os.path.join(BASE_DIR, "solutions", "submissions", "anil_vaikuntam", "02_sql_and_viz"))
    from etl_pipeline import load_projects, load_employees
    from etl_full import load_transactions

    data_dir = os.path.join(BASE_DIR, "datasets")
    raw = {
        "projects": load_projects(os.path.join(data_dir, "projects.csv")),
        "employees": load_employees(os.path.join(data_dir, "employees.csv")),
        "transactions": load_transactions(os.path.join(data_dir, "transactions.json")),
    }

    for name, df in raw.items():
        # Includes raw[name] itself — employees.manager_id is a
        # self-referential FK, so the reference set for a dataset's own FK
        # checks has to contain the dataset being validated (see the same
        # comment in airflow_dag.py's validate_data_quality task).
        result = run_data_quality_checks(df, name, reference_tables=raw)
        logger.info("%s -> %d/%d checks passed", name, result["checks_passed"], result["checks_run"])
        write_dq_report_markdown(result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    main()
