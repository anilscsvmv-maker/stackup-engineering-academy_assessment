# Quick Run Reference

Run from repo root. Full details: [HOW_TO_RUN.md](HOW_TO_RUN.md)

**Before any `python ...` command below:** check that `python` is the
interpreter with the project's packages — on a machine with more than one
Python it can resolve to an unrelated install and fail with
`ModuleNotFoundError`.
```powershell
python -c "import pandas, duckdb, pyspark, kafka, psycopg2"
```
This doesn't apply to the Docker-based steps (they run inside containers).

| # | Task | Run via | Command | Input | Purpose (short) | What it actually does | Output | View results interactively |
|---|---|---|---|---|---|---|---|---|
| 0 | — | Terminal | `docker compose up -d` | — | Start Kafka, Airflow, Postgres | Starts the local Docker services these later steps depend on, in the background. | — | Kafka UI at `localhost:8080`, Airflow UI at `localhost:8081` (both up as soon as their containers are healthy — no need to wait for steps 7/8) |
| 1 | 1.1 Clean and transform projects.csv, 1.3 Find and fix issues in employees.csv | Terminal | `python solutions/submissions/anil_vaikuntam/01_foundations/etl_pipeline.py` | `datasets/projects.csv`<br>`datasets/employees.csv` | Clean projects/employees | Reads the two raw CSVs, fixes data quality issues (bad dates, missing emails, salary outliers, etc.), and writes cleaned versions plus a summary report. | `outputs/results/anil_vaikuntam/01_foundations/` | `foundations_explorer.ipynb` (real before/after tables per fix), or open the CSVs in Excel/VS Code |
| 2 | 2.2 ETL Pipeline: Ingest, transform, and load | Terminal | `python solutions/submissions/anil_vaikuntam/02_sql_and_viz/etl_full.py` | `datasets/projects.csv`<br>`datasets/employees.csv`<br>`datasets/transactions.json` | Clean transactions | Reads all 50,000 raw transactions, joins in the project name and who approved each one, and writes the cleaned/enriched result. | `outputs/results/anil_vaikuntam/02_sql_and_viz/` | Open the CSV in Excel/VS Code |
| 3 | 1.2 Star schema with SCD Type 2 | DuckDB CLI (or any SQL client), or Python | `duckdb outputs\presight_warehouse.duckdb -c ".read solutions/submissions/anil_vaikuntam/01_foundations/data_model.sql"`<br>No DuckDB CLI? Same build via Python: `python -c "import duckdb; s=open('solutions/submissions/anil_vaikuntam/01_foundations/data_model.sql', encoding='utf-8').read(); duckdb.connect('outputs/presight_warehouse.duckdb').execute(s[:s.index('-- SECTION 2')])"` (runs Section 1 only, so no IO Error) | Step 1's clean CSVs +<br>`datasets/employees_salary_history.csv` | Build star schema + SCD2 `dim_employee` | Creates the warehouse tables and builds `dim_employee` so it tracks an employee's salary/role/level history over time, not just their current values. **Expected:** ends with an `IO Error: __OUTPUTS__/projects_clean.csv not found` — harmless, it just means this command also tried the file's Section 2 (which needs a path substitution step 4 does properly); Section 1 above it — the actual schema + SCD2 build — already succeeded by then. | `outputs/presight_warehouse.duckdb` | `warehouse_explorer.ipynb`, or DBeaver/SQLTools on the `.duckdb` file |
| 4 | 2.1 Answer six business questions | Terminal | `python solutions/submissions/anil_vaikuntam/02_sql_and_viz/run_queries.py` | `outputs/presight_warehouse.duckdb` (needs steps 2 & 3 done first) | Load warehouse + run 6 business questions | Finishes loading the warehouse, then runs and prints the answers to all six required business questions (e.g. budget performance, vendor risk). | printed to terminal | `warehouse_explorer.ipynb`, or DBeaver/SQLTools on the `.duckdb` file |
| 5 | 2.3 Query Optimisation | Terminal (or psql/any Postgres client) | `python solutions/submissions/anil_vaikuntam/02_sql_and_viz/run_optimization.py` | Step 1 & 2's clean CSVs (`employees_clean.csv`, `projects_clean.csv`, `transactions_clean.csv`) | Query optimisation benchmark | Runs a slow query and a faster rewritten version, and prints the real timing/plan comparison between the two. | printed to terminal | `warehouse_explorer.ipynb`, or DBeaver/SQLTools on the `.duckdb` file |
| 6 | 3.1 Apache Spark: Process events at scale | Terminal (env vars set first) | `$env:JAVA_HOME = "$env:LOCALAPPDATA\Programs\Eclipse Adoptium\jdk-17.0.20.101-hotspot"`<br>`$env:HADOOP_HOME = "C:\hadoop"`<br>`$env:Path = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"`<br>`Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue`<br>`$env:PYSPARK_PYTHON = (Get-Command python).Source`<br>`$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source`<br>`python solutions\submissions\anil_vaikuntam\03_big_data\spark_pipeline.py` | `datasets/events_stream/*.jsonl` (12 files) | Spark event processing → 5 tables | The env-var lines tell Spark which Java/Hadoop/Python to use (and stop a separately-installed Spark's jars from shadowing pip PySpark's); the last line then processes all 100K platform events with Spark and writes 5 summary tables. | `outputs/artifacts/anil_vaikuntam/03_big_data/spark/` | `big_data_explorer.ipynb` (reads the Parquet tables via DuckDB) |
| 7 | 3.2 Apache Kafka: Real-time streaming | Terminal | `python solutions/submissions/anil_vaikuntam/03_big_data/kafka_streaming.py --mode both` | `datasets/events_stream/events_2025_01.jsonl` | Kafka producer + consumer | Streams one month of events into a Kafka topic one at a time (simulating live traffic), then a consumer reads them back and forwards urgent escalations to a second topic. | `outputs/results/anil_vaikuntam/03_big_data/kafka/summary.json` | Kafka UI at `localhost:8080` |
| 8 | 3.3 Apache Airflow: Orchestrate the pipeline | Terminal (deploy) + Airflow UI or terminal (trigger) | `.\solutions\submissions\anil_vaikuntam\03_big_data\deploy_dag.ps1`<br>`docker exec presight-airflow-webserver airflow dags trigger presight_etl_pipeline` | `datasets/projects.csv`, `employees.csv`, `transactions.json` (read inside the DAG) | Deploy + run scheduled DAG with DQ gate | Line 1 copies the pipeline code into the Airflow containers and restarts them (note: `/opt/airflow/dags` is bind-mounted to the repo's `starter_files/`, so this also deletes `starter_files/airflow_dag_starter.py` on the host — see HOW_TO_RUN.md for a scratch-folder `airflow dags test` alternative). Line 2 starts a real run of the pipeline end-to-end, including a data-quality check that can stop the run if data looks bad. | `outputs/results/anil_vaikuntam/03_big_data/pipeline_report_<date>.txt`<br>`outputs/results/anil_vaikuntam/04_infrastructure/dq_report_*.md` | Airflow UI at `localhost:8081` |
| 9 | 4.1 Docker: Containerise the ETL pipeline | Terminal / Docker Desktop | **Two separate commands — run them one after the other, in order:**<br>`docker build -t presight-etl .`<br>`docker run --rm -v ${PWD}/outputs:/app/outputs presight-etl`<br>⚠️ Common mistake: leaving the 2nd command as `docker build --rm -v ...` instead of switching to `docker run` — fails with `unknown shorthand flag: 'v' in -v` (`-v` only exists on `docker run`). If `${PWD}` doesn't expand in your shell, use the full path: `docker run --rm -v "C:\Users\anil.vaikuntam\OneDrive - G42\Documents\Stack Up Enginerring\stackup-engineering-academy_assessment\outputs:/app/outputs" presight-etl` | `datasets/projects.csv`, `employees.csv`, `transactions.json` (same as step 3) | Run Pillar 2 ETL in container | Builds a Docker image containing the pipeline, then runs it in an isolated container to prove it works outside your local machine too. | `outputs/results/anil_vaikuntam/02_sql_and_viz/` | Open the CSV in Excel/VS Code |
| — | 2.4 Dashboard Design | Terminal (after step 4) | `python solutions/submissions/anil_vaikuntam/02_sql_and_viz/build_dashboard.py` | Executive dashboard mockup | Power BI Desktop isn't available here, so this renders a one-page dashboard mockup PDF from real data (`projects_clean.csv`, `transactions_clean.csv`, and `queries.sql`'s Q5 against the warehouse). | `outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_mockup.pdf` | Open the PDF |
| — | 4.2 Data Governance Document | — (static document, nothing to run) | — | Data governance document | N/A — a written document, not a script. | `outputs/results/anil_vaikuntam/04_infrastructure/data_governance_document.md` | Open the `.md` file |
| — | 4.3 Data Quality Framework | Automatic (via step 8's Airflow trigger), or Python (manual) | Automatic: none — runs as part of step 8's `airflow dags trigger`. Manual: `python solutions/submissions/anil_vaikuntam/04_infrastructure/dq_framework.py` | Configurable DQ framework | `airflow_dag.py`'s `validate_data_quality` task now calls both `run_data_quality_checks()` (for its pass/fail gate and XCom numbers) and `write_dq_report_markdown()`, so every DAG run regenerates `dq_report_*.md` automatically. `dq_framework.py`'s own `main()` does the same thing standalone, using the same loaders/reference_tables, for checking DQ without running the full DAG. | `outputs/results/anil_vaikuntam/04_infrastructure/dq_report_*.md` | Open the `.md` reports |

**Order:** 1 (1.1 + 1.3) → 2 (2.2) → 3 (1.2) → 4 (2.1) → 5 (2.3). Step 0 before 6-9. 6-8 need Step 0 running.

---

## Run everything end-to-end

Steps 0-9 above chained into one script — `run_all.ps1` runs the whole
project, all 4 pillars, in the correct order, instead of running each
command by hand. It doesn't do anything the table above doesn't already
document; it's just automation of the same sequence.

```powershell
.\solutions\submissions\anil_vaikuntam\run_all.ps1
```

**Runtime:** ~15 minutes total on this machine, dominated by step 7's Kafka
run (~7 min — 8,333 events at the task's own scripted 50ms/event delay, not
something the script can speed up without breaking the spec) and step 6's
Spark run (~4.5 min, most of it writing 5,088 date partitions into a
OneDrive-synced folder).

**Skip switches** for re-runs where you don't need everything (combine as
needed):

| Switch | Skips |
|---|---|
| `-SkipSpark` | Step 6 — Task 3.1 Spark |
| `-SkipKafka` | Step 7 — Task 3.2 Kafka (saves ~7 min) |
| `-SkipAirflow` | Step 8 — Task 3.3 Airflow deploy + trigger |
| `-SkipDocker` | Step 9 — Task 4.1 Docker build + run |

Example — re-run everything except the two slow steps:
```powershell
.\solutions\submissions\anil_vaikuntam\run_all.ps1 -SkipKafka -SkipDocker
```

**Included:** 2.4 Dashboard Design (`build_dashboard.py`, right after
step 5). **Not included:** 4.2 Data Governance Document (a static `.md`
file, nothing to run) — the script prints a reminder when it reaches that
point.
