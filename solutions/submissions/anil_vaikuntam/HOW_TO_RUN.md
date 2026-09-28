# How to Run This Submission

Exact commands to execute all 4 pillars, in order, from a clean clone. All
commands run from the **repo root**. For the design story behind each
pillar, see [SUBMISSION_NOTES.md](SUBMISSION_NOTES.md).

Each step below names the **tool/interface** to run it with — most are a
plain terminal, two are SQL-client-or-terminal (your choice), two are
Jupyter notebooks, and Pillar 3/4 need Docker. Every step that writes a
file also says how to check it actually worked.

## Prerequisites

- Python 3.11+ (verified on 3.13) + `pip install -r requirements.txt`.
  **Note:** if more than one Python is installed, plain `python` can
  resolve to the wrong one — verify with `python -c "import pandas, duckdb"`
  before running anything; if that fails, call the correct interpreter by
  full path.
- Docker Desktop running (Pillar 3 services + Pillar 4 container)
- Java 17 on PATH/`JAVA_HOME`, plus Hadoop native libs (`HADOOP_HOME`,
  Windows only) — required for Pillar 3's Spark job (exact env vars below)
- A DuckDB CLI or SQL client for the `.sql` files — see Pillar 1/2 below
- Jupyter support in your editor (e.g. VS Code's Jupyter extension) for the
  two `.ipynb` exploration notebooks
- Pillars run in order: 2 depends on 1's cleaned CSVs, 3's DAG imports 1 and
  2's pipeline functions, 4 containerises 2's pipeline.

## Credentials

| Service                            | URL                   | Username   | Password      |
| ---------------------------------- | --------------------- | ---------- | ------------- |
| Airflow UI                         | http://localhost:8081 | `admin`    | `admin`       |
| Kafka UI                           | http://localhost:8080 | — (none)   | —             |
| PostgreSQL (Airflow's metadata DB) | `localhost:5432`      | `presight` | `presight123` |

All defined in `docker-compose.yml` — only relevant if you're inspecting
services directly rather than through the pipeline scripts.

## Step 0 — Start Docker services

Needed before Pillar 3 or Pillar 4. Safe to run now regardless — Pillar 1/2
don't need it.

```
docker compose up -d
docker compose ps
```

Wait until `presight-kafka`, `presight-kafka-ui`, `presight-postgres`,
`presight-airflow-webserver`, and `presight-airflow-scheduler` all show
`Up` (the webserver takes ~20–30s longer than the others to report
healthy). Ignore the `version is obsolete` warning — cosmetic, from an
unrelated Compose file field.

Stop everything later with `docker compose down`.

## Pillar 1 — Foundations (Tasks 1.1–1.3)

**Task 1.1 (projects) + Task 1.3 (employees) — Tool: terminal**
```
python solutions/submissions/anil_vaikuntam/01_foundations/etl_pipeline.py
```
Writes `projects_clean.csv` / `employees_clean.csv` / `employees_quality_summary.json` /
`pipeline_summary.txt` to `outputs/results/anil_vaikuntam/01_foundations/`.

**Check it worked:**
```
python -c "import pandas as pd; print(pd.read_csv('outputs/results/anil_vaikuntam/01_foundations/projects_clean.csv').shape)"
type outputs\results\anil_vaikuntam\01_foundations\pipeline_summary.txt
```
Expect `(500, 17)`, and `Actual: <n>s (PASS)` in the summary — target is under 30 seconds.

**Task 1.2 — Tool: DuckDB CLI (or any SQL client) against `outputs/presight_warehouse.duckdb`**
Run `01_foundations/data_model.sql`'s Section 1 (star schema DDL + SCD2
`dim_employee` build + validation). It has no path placeholders, so it runs
as-is — no substitution needed. It's also self-re-runnable: `CREATE TABLE
IF NOT EXISTS` + a `DELETE FROM` reset block at the top mean running it
twice in a row against the same file just rebuilds cleanly.
```
duckdb outputs\presight_warehouse.duckdb
.read solutions/submissions/anil_vaikuntam/01_foundations/data_model.sql
```
**Expected trailing error:** `.read` executes the whole file, so it also
reaches Section 2 (load), which uses an `__OUTPUTS__` path placeholder that
only `run_queries.py` substitutes. You'll see `IO Error:
__OUTPUTS__/projects_clean.csv not found` at the end — harmless. Section 1
(the schema + SCD2 build + validation queries above) has already completed
and printed its results by that point; Section 2 gets loaded correctly later
by `run_queries.py` (Pillar 2, step below).
(A DuckDB CLI build lives at `%LOCALAPPDATA%\duckdb-cli\duckdb.exe` if not
already on PATH — add that folder to PATH, or call it by full path.)

**Check it worked:** the last few statements in Section 1 *are* the
validation queries — watch their output directly. Q1 (duplicate current),
Q3 (overlaps), Q5 (gaps) should return 0 rows; Q4 should show matching
`actual_rows`/`expected_rows`. Or query it yourself:
```
duckdb outputs\presight_warehouse.duckdb -readonly -c "SELECT COUNT(*) FROM dim_employee;"
```
Expect `2231`.

**No DuckDB CLI installed?** The same Section 1 build runs through Python's
`duckdb` package (identical engine, no path placeholder to trip over):
```
python -c "import duckdb; s=open('solutions/submissions/anil_vaikuntam/01_foundations/data_model.sql', encoding='utf-8').read(); duckdb.connect('outputs/presight_warehouse.duckdb').execute(s[:s.index('-- SECTION 2')])"
```

**Tool: Jupyter notebook** — [foundations_explorer.ipynb](01_foundations/notebooks/foundations_explorer.ipynb)
walks through `etl_pipeline.py` step by step — real before/after tables for
each of the 6 employee data-quality fixes, not just the code. Same
Python 3 kernel as the Pillar 2/3 notebooks. Open it in
VS Code, run cells top to bottom.

See [01_foundations/other files/ASSUMPTIONS.md](<01_foundations/other files/ASSUMPTIONS.md>) and
[01_foundations/other files/VALIDATION_EVIDENCE.md](<01_foundations/other files/VALIDATION_EVIDENCE.md>)
for the reasoning and full evidence behind this pillar's numbers.

## Pillar 2 — SQL & Data Visualization (Tasks 2.1–2.4)

**Task 2.2 (ETL) + Task 2.1 (business questions) + Task 2.3 (optimisation) — Tool: terminal**
```
python solutions/submissions/anil_vaikuntam/02_sql_and_viz/etl_full.py
python solutions/submissions/anil_vaikuntam/02_sql_and_viz/run_queries.py
python solutions/submissions/anil_vaikuntam/02_sql_and_viz/run_optimization.py
```
`etl_full.py` (**Task 2.2**) writes `transactions_clean.csv` + `pipeline_summary.txt`.
`run_queries.py` (**Task 2.1**) loads Section 2 of `data_model.sql` (remaining warehouse
tables) and runs `queries.sql`'s six business questions, printing real
result rows. `run_optimization.py` (**Task 2.3**) runs `query_optimization.sql`'s
original/rewritten queries against real PostgreSQL (the `presight-postgres`
container — `docker-compose up -d postgres` first) and prints the Task 2.3
EXPLAIN ANALYZE + benchmark output.

**Check it worked:**
```
type outputs\results\anil_vaikuntam\02_sql_and_viz\pipeline_summary.txt
```
Look for `Actual: <n>s (PASS)` — target is under 30 seconds.

**Task 1.2 + Task 2.1 — Tool: DuckDB CLI (or any SQL client)** — `01_foundations/data_model.sql`
(**Task 1.2**) and `02_sql_and_viz/queries.sql` (**Task 2.1**) are plain runnable SQL; open either and run
statements directly against the warehouse. `queries.sql` needs Section 2
already loaded first (via `run_queries.py` above, or manually).

**Task 2.3 — Tool: psql (or any Postgres client)** —
`02_sql_and_viz/query_optimization.sql` runs against real PostgreSQL, not
DuckDB — its own setup block creates plain `employees`/`projects`/
`transactions` tables and loads them via `COPY`, which is server-side, so
the CSVs need to exist inside the container first. `run_optimization.py`
above does all of this automatically; to run it manually:
```
docker cp outputs/results/anil_vaikuntam/01_foundations/employees_clean.csv presight-postgres:/tmp/employees_clean.csv
docker cp outputs/results/anil_vaikuntam/01_foundations/projects_clean.csv   presight-postgres:/tmp/projects_clean.csv
docker cp outputs/results/anil_vaikuntam/02_sql_and_viz/transactions_clean.csv presight-postgres:/tmp/transactions_clean.csv
docker exec presight-postgres psql -U presight -d postgres -c "CREATE DATABASE presight_practice;"
docker exec -i presight-postgres psql -U presight -d presight_practice -f - < solutions/submissions/anil_vaikuntam/02_sql_and_viz/query_optimization.sql
```

**Tool: Jupyter notebook** — [warehouse_explorer.ipynb](02_sql_and_viz/notebooks/warehouse_explorer.ipynb)
gives the same warehouse a live, editable query surface with results
rendered as tables, useful for a demo. Open it in VS Code, select a Python 3
kernel that has pandas/duckdb installed, run cells top to bottom.

**Task 2.4 — Dashboard — Tool: terminal** (run after `run_queries.py`, which
loads the warehouse it reads Q5 from):
```
python solutions/submissions/anil_vaikuntam/02_sql_and_viz/build_dashboard.py
```
Power BI Desktop isn't available on this machine, so per the setup guide's
documented alternative this renders a one-page executive dashboard mockup
(real matplotlib charts off `projects_clean.csv`, `transactions_clean.csv`
and `queries.sql`'s own Q5 against the warehouse) to
`outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_mockup.pdf`.
`other files/export_dashboard_data.sql` holds per-visual extract queries
for rebuilding the same dashboard in Power BI — see that file's own header.

See [02_sql_and_viz/other files/VALIDATION_EVIDENCE.md](<02_sql_and_viz/other files/VALIDATION_EVIDENCE.md>)
for the real row counts, query results, and optimisation benchmark numbers
behind this pillar.

## Pillar 3 — Big Data Processing (Tasks 3.1–3.3)

Requires Step 0 (Docker services) above already running.

**Task 3.1 — Tool: terminal, with Spark env vars set first** (Windows PowerShell shown
— these don't persist between terminal sessions, set them each time or add
to your profile):
```powershell
$env:JAVA_HOME = "$env:LOCALAPPDATA\Programs\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:HADOOP_HOME = "C:\hadoop"                    # folder containing bin\winutils.exe + bin\hadoop.dll
$env:Path = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"
Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue   # use pip PySpark's own jars, not a separate Spark install
$py = (Get-Command python).Source
$env:PYSPARK_PYTHON = $py                          # pin driver+worker to the same interpreter
$env:PYSPARK_DRIVER_PYTHON = $py

python solutions\submissions\anil_vaikuntam\03_big_data\spark_pipeline.py
```
`JAVA_HOME` above points at this machine's per-user Temurin 17 install. On another
machine, don't have a JDK 17 yet? `winget install --id EclipseAdoptium.Temurin.17.JDK -e`
installs one (usually lands at `C:\Program Files\Eclipse Adoptium\jdk-17.x.x-hotspot`);
if that install hangs on a UAC prompt, download the portable zip from
[adoptium.net](https://adoptium.net/temurin/releases/?version=17) instead and
extract it anywhere — no admin rights needed.
PySpark is installed in the same Python as everything else (verified with
PySpark 4.2 on Python 3.13). If `SPARK_HOME` points at a separately-installed
Spark of a different version, `getOrCreate()` fails with `'JavaPackage'
object is not callable` — hence the `Remove-Item Env:SPARK_HOME` line.
`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`
matter specifically because `escalation_log()` uses `applyInPandas`, which
spawns real worker processes; if those resolve to a *different* Python than
the driver (e.g. picked up from PATH), the job fails with a version-mismatch
error. That Python also needs `pandas`, `pyarrow`, and a working `numpy` — if
imports fail with an ABI/binary mismatch, `pip install --force-reinstall
--no-cache-dir numpy pandas pyarrow` fixes it.

Writes the 5 Parquet tables to `outputs/artifacts/anil_vaikuntam/03_big_data/spark/`
— gitignored (a regenerable binary build output, unlike the CSV/JSON/markdown
deliverables tracked under `outputs/results/`).

**Check it worked:** the run ends by printing a "PERFORMANCE BASELINE"
block — total time, per-table timings, total rows (expect `99996`), and
events/sec. Or:
```
dir outputs\artifacts\anil_vaikuntam\03_big_data\spark
```
Expect 5 subfolders, one per table.

**Task 3.2 — Tool: terminal**
```
python solutions/submissions/anil_vaikuntam/03_big_data/kafka_streaming.py --mode both
```
Producer streams all 8,333 events from `events_2025_01.jsonl` at 50ms/message
(~7 min — this is expected, not a hang), then the consumer drains them and
writes `outputs/results/anil_vaikuntam/03_big_data/kafka/summary.json`.

**Check it worked:**
```
type outputs\results\anil_vaikuntam\03_big_data\kafka\summary.json
```
Expect `total_messages_consumed` = 8333 and `critical_escalations_forwarded` > 0.
Confirm topics in Kafka UI (http://localhost:8080): `presight.project.events`,
`presight.escalations.critical`.

**Tool: Jupyter notebook** — [big_data_explorer.ipynb](03_big_data/notebooks/big_data_explorer.ipynb)
explores all 5 Spark Parquet tables plus the Kafka summary via DuckDB's
`read_parquet()` (no pyarrow/pandas-parquet path needed for reading). Same
Python 3 kernel as the Pillar 2 notebook.

**Task 3.3 — Tool: terminal (deploy) + Airflow web UI or terminal (trigger)**
```
.\solutions\submissions\anil_vaikuntam\03_big_data\deploy_dag.ps1
```
Copies the DAG + sibling modules into the Airflow containers and restarts
them — re-run this any time `etl_pipeline.py`/`etl_full.py`/`dq_framework.py`/
`airflow_dag.py` change, since it's a one-time copy, not a live mount.
**Side effect to know about:** `docker-compose.yml` bind-mounts
`./starter_files` to the containers' `/opt/airflow/dags`, so this script
deletes the tracked `starter_files/airflow_dag_starter.py` and leaves four
copied files in `starter_files/` on the host. To test the DAG without
touching the repo, run it from a scratch folder inside the container instead
(bash shown; in Git Bash prefix with `MSYS_NO_PATHCONV=1`):
```
docker exec presight-airflow-webserver mkdir -p /tmp/presight_dags
docker cp solutions/submissions/anil_vaikuntam/01_foundations/etl_pipeline.py presight-airflow-webserver:/tmp/presight_dags/etl_pipeline.py
docker cp solutions/submissions/anil_vaikuntam/02_sql_and_viz/etl_full.py presight-airflow-webserver:/tmp/presight_dags/etl_full.py
docker cp solutions/submissions/anil_vaikuntam/04_infrastructure/dq_framework.py presight-airflow-webserver:/tmp/presight_dags/dq_framework.py
docker cp solutions/submissions/anil_vaikuntam/03_big_data/airflow_dag.py presight-airflow-webserver:/tmp/presight_dags/presight_etl_pipeline.py
docker exec -e PYTHONPATH=/tmp/presight_dags presight-airflow-webserver airflow dags test presight_etl_pipeline <yyyy-mm-dd> -S /tmp/presight_dags
```
After `deploy_dag.ps1`, either trigger from the UI (`http://localhost:8081`, `admin`/`admin` —
search `presight_etl_pipeline`, toggle it **on**, click **Trigger DAG**) or:
```
docker exec presight-airflow-webserver airflow dags trigger presight_etl_pipeline
```

**Check it worked:** wait for every task to go green in the UI, or:
```
docker exec presight-airflow-webserver airflow dags list-runs -d presight_etl_pipeline
```
should show `success`. Then:
```
type outputs\results\anil_vaikuntam\03_big_data\pipeline_report_<today's date>.txt
```

See [03_big_data/other files/VALIDATION_EVIDENCE.md](<03_big_data/other files/VALIDATION_EVIDENCE.md>) for
real Spark/Kafka/Airflow run numbers behind this pillar.

## Pillar 4 — Infrastructure & Governance (Tasks 4.1–4.3)

**Task 4.1 — Tool: Docker Desktop / terminal**
```
docker build -t presight-etl .
docker run --rm -v ${PWD}/outputs:/app/outputs presight-etl
```
(or `docker compose run --rm etl`, using the `etl` service in
`docker-compose.override.yml`). Runs the same Task 2.2 pipeline inside the
container. **Task 4.3**'s DQ framework (`04_infrastructure/dq_framework.py`) is imported
by `airflow_dag.py`'s quality-gate task, which calls both `run_data_quality_checks()` (its
`checks_run`/`checks_passed`/`checks_failed` numbers feed `pipeline_report_<date>.txt` in
Pillar 3) and `write_dq_report_markdown()`, so every DAG run regenerates the
`dq_report_{dataset}.md` files in `outputs/results/anil_vaikuntam/04_infrastructure/`.
`python solutions/submissions/anil_vaikuntam/04_infrastructure/dq_framework.py` produces the
same reports standalone.
**Task 4.2**'s `data_governance.md` is a static document, no script to run.

**Check it worked:**
```
dir outputs\results\anil_vaikuntam\02_sql_and_viz\transactions_clean.csv
type outputs\results\anil_vaikuntam\02_sql_and_viz\pipeline_summary.txt
```
Same file the container writes as Pillar 2's own `etl_full.py` run — look
for the same `Actual: <n>s (PASS)` line, under 30 seconds.
