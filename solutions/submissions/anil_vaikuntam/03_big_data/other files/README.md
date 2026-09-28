# Pillar 3 — Big Data Processing

## Files

- `spark_pipeline.py` — Task 3.1. Loads all 12 monthly event files with an explicit schema, cleans/dedupes, writes 5 aggregated Parquet tables.
- `kafka_streaming.py` — Task 3.2. Producer/consumer against a live Kafka broker, with severity-based escalation forwarding.
- `airflow_dag.py` — Task 3.3. DAG `presight_etl_pipeline`: parallel extract → DQ gate → transform → load → report. Imports `01_foundations/etl_pipeline.py`, `02_sql_and_viz/etl_full.py`, and `04_infrastructure/dq_framework.py` as sibling modules rather than duplicating their logic.
- `deploy_dag.ps1` — copies this DAG plus its three imported modules into the Airflow containers (the base `docker-compose.yml` only bind-mounts `starter_files/` into the container's DAGs folder, not `solutions/`, so the real files have to be copied in). Re-run this any time the DAG or its imported modules change.
- `big_data_explorer.ipynb` — Jupyter notebook exploring the Spark Parquet output and the Kafka run summary via DuckDB's `read_parquet()`.

## Outputs

| File | Path |
|---|---|
| 5 Spark Parquet tables | `outputs/artifacts/anil_vaikuntam/03_big_data/spark/` — **gitignored**: regenerable binary build output, re-created by re-running `spark_pipeline.py` |
| `summary.json` (Kafka) | `outputs/results/anil_vaikuntam/03_big_data/kafka/` |
| `pipeline_report_<date>.txt` (Airflow) | `outputs/results/anil_vaikuntam/03_big_data/` |

## Local UIs

| UI | URL | Credentials | Use for |
|---|---|---|---|
| Kafka UI | http://localhost:8080 | none | Browse `presight.project.events` / `presight.escalations.critical` topics |
| Airflow UI | http://localhost:8081 | `admin` / `admin` | Trigger `presight_etl_pipeline`, view task logs/XCom |

## How to run

See [HOW_TO_RUN.md](../HOW_TO_RUN.md) — Pillar 3 section.

See [VALIDATION_EVIDENCE.md](VALIDATION_EVIDENCE.md) for real Spark/Kafka/Airflow
run numbers behind this pillar's claims.
