"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: airflow_dag.py
Pillar: Big Data Processing (Task 3.3)
Author: Anil Vaikuntam
=============================================================

SCENARIO
--------
Airflow DAG orchestrating the Pillar 1/2 ETL pipeline daily, with a data
quality gate that blocks downstream tasks if a critical check fails.

DEPLOYMENT NOTE
----------------
Deployed into the Airflow container's /opt/airflow/dags directory alongside
three sibling modules — etl_pipeline.py, etl_full.py, dq_framework.py —
copied in at deploy time (see deploy_dag.ps1). Airflow puts dags_folder on
sys.path, so `import etl_pipeline` resolves without a package structure.
docker-compose.yml mounts ./datasets -> /opt/airflow/datasets and
./outputs -> /opt/airflow/outputs, which is why DATA_DIR/OUTPUT_DIR default
there; those imported modules read DATA_DIR/OUTPUT_DIR from the environment
so the same code runs unmodified on the host and inside this container.

HOW TO RUN
----------
  See deploy_dag.ps1, then trigger from the Airflow UI (localhost:8081) or:
  airflow dags trigger presight_etl_pipeline
"""

from datetime import timedelta
import logging
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from etl_pipeline import load_projects, load_employees, transform_projects, clean_employees
from etl_full import load_transactions, enrich_transactions, write_outputs, RESULTS_DIR as ETL_RESULTS_DIR
from dq_framework import run_data_quality_checks, write_dq_report_markdown

logger = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/datasets")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/airflow/outputs")
# outputs/results/anil_vaikuntam/... — the submission-folder convention, so
# multiple trainees' outputs in the shared repo don't collide. etl_full.py's
# write_outputs() already lands the CSVs here on its own (it computes the
# same path independently); this DAG's own pipeline_report goes here too.
RESULTS_DIR = os.environ.get("RESULTS_DIR", os.path.join(OUTPUT_DIR, "results", "anil_vaikuntam", "03_big_data"))

# Critical-failure threshold for the DQ gate — completeness below this on a
# dataset's own primary key (the one column that must never be missing)
# blocks every downstream task.
CRITICAL_COMPLETENESS_THRESHOLD = 0.80
KEY_COLUMNS = {"projects": "project_id", "employees": "employee_id", "transactions": "transaction_id"}


# ==============================================================================
# Task 3.3a — DAG configuration
# ==============================================================================

default_args = {
    "owner": "anil_vaikuntam",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ==============================================================================
# Task 3.3b — Task functions
# ==============================================================================

def task_extract_projects(**context):
    ti = context["ti"]
    df = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    ti.xcom_push(key="projects_raw_count", value=len(df))
    logger.info("Extracted %d project rows", len(df))
    return f"Extracted {len(df)} project rows"


def task_extract_employees(**context):
    ti = context["ti"]
    df = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    ti.xcom_push(key="employees_raw_count", value=len(df))
    logger.info("Extracted %d employee rows", len(df))
    return f"Extracted {len(df)} employee rows"


def task_extract_transactions(**context):
    ti = context["ti"]
    df = load_transactions(os.path.join(DATA_DIR, "transactions.json"))
    ti.xcom_push(key="transactions_raw_count", value=len(df))
    logger.info("Extracted %d transaction rows", len(df))
    return f"Extracted {len(df)} transaction rows"


def task_validate_data_quality(**context):
    """
    DQ GATE. Reloads all three raw datasets and runs the Task 4.3 framework
    on each, writing dq_report_{dataset}.md per dataset via dq_framework's
    own write_dq_report_markdown(). Raises ValueError — failing this task
    and blocking every downstream task — if completeness on a dataset's
    primary key drops below 80%.
    """
    ti = context["ti"]

    raw = {
        "projects": load_projects(os.path.join(DATA_DIR, "projects.csv")),
        "employees": load_employees(os.path.join(DATA_DIR, "employees.csv")),
        "transactions": load_transactions(os.path.join(DATA_DIR, "transactions.json")),
    }

    dq_results = {}
    critical_failures = []
    for name, df in raw.items():
        # Includes `raw[name]` itself — employees.manager_id is a
        # self-referential FK (must resolve to another row in the same
        # table), so the reference set for a dataset's own FK checks has to
        # contain the dataset being validated, not just the other two.
        result = run_data_quality_checks(df, name, reference_tables=raw)
        dq_results[name] = result
        write_dq_report_markdown(result)

        key_col = KEY_COLUMNS[name]
        completeness = result["results"]["completeness"]["details"].get(key_col, 0)
        if completeness < CRITICAL_COMPLETENESS_THRESHOLD:
            critical_failures.append(f"{name}.{key_col} completeness {completeness:.1%} < {CRITICAL_COMPLETENESS_THRESHOLD:.0%}")

    ti.xcom_push(key="dq_results", value={
        name: {"checks_run": r["checks_run"], "checks_passed": r["checks_passed"], "checks_failed": r["checks_failed"]}
        for name, r in dq_results.items()
    })

    if critical_failures:
        raise ValueError(f"DQ gate FAILED — critical completeness failures: {'; '.join(critical_failures)}")

    logger.info("DQ gate PASSED for all 3 datasets: %s", {n: f"{r['checks_passed']}/{r['checks_run']}" for n, r in dq_results.items()})
    return "DQ gate passed"


def task_transform_and_enrich(**context):
    ti = context["ti"]

    raw_projects = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    raw_employees = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    raw_transactions = load_transactions(os.path.join(DATA_DIR, "transactions.json"))

    clean_projects = transform_projects(raw_projects)
    clean_emp = clean_employees(raw_employees)
    enriched_txn = enrich_transactions(raw_transactions, clean_projects, clean_emp)

    ti.xcom_push(key="projects_clean_count", value=len(clean_projects))
    ti.xcom_push(key="employees_clean_count", value=len(clean_emp))
    ti.xcom_push(key="transactions_clean_count", value=len(enriched_txn))

    # DataFrames aren't XCom'd (too large) — load_to_output recomputes them.
    # XCom carries metadata only; large payloads go task-to-task via disk.
    logger.info("Transform complete: projects=%d employees=%d transactions=%d", len(clean_projects), len(clean_emp), len(enriched_txn))
    return "Transform complete"


def task_load_to_output(**context):
    raw_projects = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    raw_employees = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    raw_transactions = load_transactions(os.path.join(DATA_DIR, "transactions.json"))

    clean_projects = transform_projects(raw_projects)
    clean_emp = clean_employees(raw_employees)
    enriched_txn = enrich_transactions(raw_transactions, clean_projects, clean_emp)

    write_outputs(clean_projects, clean_emp, enriched_txn, elapsed_seconds=0.0)
    logger.info("Wrote outputs to %s", ETL_RESULTS_DIR)
    return f"Outputs written to {ETL_RESULTS_DIR}"


def task_generate_pipeline_report(**context):
    ti = context["ti"]
    execution_date = context["execution_date"]

    projects_raw = ti.xcom_pull(task_ids="extract_projects", key="projects_raw_count")
    employees_raw = ti.xcom_pull(task_ids="extract_employees", key="employees_raw_count")
    transactions_raw = ti.xcom_pull(task_ids="extract_transactions", key="transactions_raw_count")
    dq_results = ti.xcom_pull(task_ids="validate_data_quality", key="dq_results")
    projects_clean = ti.xcom_pull(task_ids="transform_and_enrich", key="projects_clean_count")
    employees_clean = ti.xcom_pull(task_ids="transform_and_enrich", key="employees_clean_count")
    transactions_clean = ti.xcom_pull(task_ids="transform_and_enrich", key="transactions_clean_count")

    lines = [
        "Presight ETL Pipeline — DAG Run Report",
        "=" * 50,
        f"DAG run date: {execution_date}",
        "",
        "Row counts (raw -> clean):",
        f"  projects:     {projects_raw} -> {projects_clean}",
        f"  employees:    {employees_raw} -> {employees_clean}",
        f"  transactions: {transactions_raw} -> {transactions_clean}",
        "",
        "DQ gate results:",
    ]
    for name, r in (dq_results or {}).items():
        lines.append(f"  {name}: {r['checks_passed']}/{r['checks_run']} checks passed")
    lines += [
        "",
        f"Files written to: {ETL_RESULTS_DIR}",
        "  projects_clean.csv, employees_clean.csv, transactions_clean.csv, pipeline_summary.txt",
    ]

    os.makedirs(RESULTS_DIR, exist_ok=True)
    report_path = os.path.join(RESULTS_DIR, f"pipeline_report_{execution_date.date()}.txt")
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    logger.info("Pipeline report written to %s", report_path)
    return report_path


# ==============================================================================
# Task 3.3a — DAG definition
# ==============================================================================

with DAG(
    dag_id="presight_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for Presight project management data",
    schedule_interval="0 6 * * *",  # 06:00 in the DAG's timezone (Asia/Dubai, set via start_date below)
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Dubai"),
    catchup=False,
    max_active_runs=1,
    tags=["presight", "etl", "assessment"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_projects = PythonOperator(task_id="extract_projects", python_callable=task_extract_projects)
    extract_employees = PythonOperator(task_id="extract_employees", python_callable=task_extract_employees)
    extract_transactions = PythonOperator(task_id="extract_transactions", python_callable=task_extract_transactions)

    validate_dq = PythonOperator(task_id="validate_data_quality", python_callable=task_validate_data_quality)
    transform_enrich = PythonOperator(task_id="transform_and_enrich", python_callable=task_transform_and_enrich)
    load_output = PythonOperator(task_id="load_to_output", python_callable=task_load_to_output)
    pipeline_report = PythonOperator(task_id="generate_pipeline_report", python_callable=task_generate_pipeline_report)

    # ===========================================================================
    # Task 3.3c — dependency chain: extract tasks run in parallel, all three
    # must complete before the DQ gate; gate must pass before transform/load/report.
    # ===========================================================================
    start >> [extract_projects, extract_employees, extract_transactions] >> validate_dq
    validate_dq >> transform_enrich >> load_output >> pipeline_report >> end
