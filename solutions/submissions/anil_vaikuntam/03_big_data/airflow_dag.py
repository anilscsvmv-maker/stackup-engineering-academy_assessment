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
Mounted into the Airflow container's /opt/airflow/dags directory alongside
three sibling modules — etl_pipeline.py, etl_full.py, dq_framework.py — by
the docker-compose.airflow.yml override (see deploy_dag.ps1), which replaces
the base starter_files/ mount so starter_files/ is never touched. Airflow puts
dags_folder on sys.path, so `import etl_pipeline` resolves without a package
structure. docker-compose.yml mounts ./datasets -> /opt/airflow/datasets and
./outputs -> /opt/airflow/outputs, which is why DATA_DIR/OUTPUT_DIR default
there; the override sets the same env vars for the imported modules so the
same code runs unmodified on the host and inside this container.

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
# outputs/results/anil_vaikuntam/03_big_data — the submission-folder
# convention, so multiple trainees' outputs in the shared repo don't collide.
# The pipeline report is written to OUTPUT_DIR (the Task 3.3e path) with a
# copy here. The cleaned CSVs are NOT written here: etl_full.py's
# write_outputs() lands them in its own .../02_sql_and_viz folder
# (ETL_RESULTS_DIR below).
RESULTS_DIR = os.environ.get("RESULTS_DIR", os.path.join(OUTPUT_DIR, "results", "anil_vaikuntam", "03_big_data"))

# Critical-failure threshold for the DQ gate — completeness below this on a
# dataset's own primary key (the one column that must never be missing)
# blocks every downstream task.
CRITICAL_COMPLETENESS_THRESHOLD = 0.80
KEY_COLUMNS = {"projects": "project_id", "employees": "employee_id", "transactions": "transaction_id"}

# Files etl_full.write_outputs() writes into ETL_RESULTS_DIR.
OUTPUT_FILES = ["projects_clean.csv", "employees_clean.csv", "transactions_clean.csv", "pipeline_summary.txt"]


# ==============================================================================
# Task 3.3a — DAG configuration
# ==============================================================================
#   dag_id:            presight_etl_pipeline
#   Schedule:          daily at 06:00 UAE time (Asia/Dubai = UTC+4)
#   catchup:           False
#   max_active_runs:   1
#   retries:           2, retry_delay = 5 minutes
#   Tags:              ['presight', 'etl', 'assessment']

DAG_ID = "presight_etl_pipeline"
DAG_TIMEZONE = "Asia/Dubai"  # UAE time, UTC+4
SCHEDULE = "0 6 * * *"  # 06:00 in DAG_TIMEZONE (applied via start_date's tz)
START_DATE = pendulum.datetime(2025, 1, 1, tz=DAG_TIMEZONE)
CATCHUP = False
MAX_ACTIVE_RUNS = 1
TAGS = ["presight", "etl", "assessment"]


def alert_on_failure(context):
    """
    Task 3.3d failure alerting (log variant). Fires once a task has exhausted
    its retries; email_on_failure stays False because the local Airflow stack
    has no SMTP server configured.
    """
    ti = context["ti"]
    logger.error(
        "ALERT: DAG %s task %s FAILED after all retries (run %s): %s. Log: %s",
        ti.dag_id, ti.task_id, context["run_id"], context.get("exception"), ti.log_url,
    )


default_args = {
    "owner": "anil_vaikuntam",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": alert_on_failure,
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
    paths = [os.path.join(ETL_RESULTS_DIR, name) for name in OUTPUT_FILES]
    for path in paths:
        logger.info("Wrote %s", path)
    context["ti"].xcom_push(key="files_written", value=paths)
    return f"{len(paths)} files written to {ETL_RESULTS_DIR}"


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
    files_written = ti.xcom_pull(task_ids="load_to_output", key="files_written") or []

    # Execution time = DAG run start -> now (this is the last real task).
    run_started = context["dag_run"].start_date
    now = pendulum.now("UTC")
    elapsed = (now - run_started).total_seconds() if run_started else None

    lines = [
        "Presight ETL Pipeline — DAG Run Report",
        "=" * 50,
        f"DAG run date:       {execution_date}",
        f"Run started:        {run_started}",
        f"Report generated:   {now}",
        f"Execution time:     {elapsed:.1f} seconds" if elapsed is not None else "Execution time:     n/a",
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
    lines += ["", "Files written:"]
    lines += [f"  {path}" for path in files_written]

    # Task 3.3e path: outputs/pipeline_report_{execution_date}.txt, plus a copy
    # in the trainee's results folder (README submission convention).
    report_name = f"pipeline_report_{execution_date.date()}.txt"
    report_paths = [os.path.join(OUTPUT_DIR, report_name), os.path.join(RESULTS_DIR, report_name)]
    for report_path in report_paths:
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        logger.info("Pipeline report written to %s", report_path)
    return report_paths[0]


# ==============================================================================
# DAG definition — all settings come from the Task 3.3a configuration block above
# ==============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Daily ETL pipeline for Presight project management data",
    schedule_interval=SCHEDULE,
    start_date=START_DATE,
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=TAGS,
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
