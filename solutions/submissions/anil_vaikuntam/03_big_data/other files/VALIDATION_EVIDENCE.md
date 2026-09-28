# Pillar 3 — Validation Evidence

Real numbers from actually running Spark, Kafka, and the Airflow DAG against live
Docker services — not restated claims. Commands to reproduce each check are included.

## Task 3.1 — Spark (`spark_pipeline.py`)

| Check | Result |
|---|---|
| Raw event rows loaded (all 12 monthly files) | 99,996 |
| Rows dropped (null `event_id`/`user_id`) | 0 |
| Rows dropped (duplicate `event_id`) | 0 |
| Total execution time | 264.30s (local mode, PySpark 4.2 / Java 17; most of it is writing `daily_event_volume`'s 5,088 date partitions into a OneDrive-synced folder) |
| Throughput | 378 events/sec |

Per-table output, all 5 written as Parquet under
`outputs/artifacts/anil_vaikuntam/03_big_data/spark/`:

| Table | Rows | Build time |
|---|---|---|
| `project_activity_summary` | 500 | 3.91s |
| `user_activity_summary` | 960 | 3.11s |
| `escalation_log` (partitioned by `severity`) | 1,969 | 24.27s |
| `daily_event_volume` (partitioned by `event_date`) | 5,088 | 2.44s |
| `peak_usage_analysis` | 20 | 2.35s |

(Build times exclude the Parquet writes, which account for the rest of the total.)

```powershell
$env:JAVA_HOME = "$env:LOCALAPPDATA\Programs\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"
Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
python solutions\submissions\anil_vaikuntam\03_big_data\spark_pipeline.py
dir outputs\artifacts\anil_vaikuntam\03_big_data\spark   # 5 subfolders
```

## Task 3.2 — Kafka (`kafka_streaming.py --mode both`)

The full January file (`events_2025_01.jsonl`, 8,333 events) streamed live against
`presight-kafka`/`presight-kafka-ui` (no mocked broker):

| Check | Result |
|---|---|
| Messages produced | 8,333 |
| Messages consumed | 8,333 (matches produced — no loss) |
| Critical escalations forwarded to `presight.escalations.critical` | 14 |
| Producer elapsed time | ~7 min (scripted 50ms/message delay) |
| Consumer elapsed time | 13.8s |
| Consumer throughput | 603.95 messages/sec |
| `escalation_raised` events in the stream | 178 |
| `escalation_resolved` events in the stream | 158 |

```powershell
python solutions/submissions/anil_vaikuntam/03_big_data/kafka_streaming.py --mode both
type outputs\results\anil_vaikuntam\03_big_data\kafka\summary.json
```

`total_messages_consumed` (8,333) == `Total sent: 8333` logged by the producer confirms
no message loss end-to-end; `critical_escalations_forwarded` > 0 confirms the
severity-based forwarding logic actually fired, not just parsed.

## Task 3.3 — Airflow (`presight_etl_pipeline` DAG)

Run `manual__2026-09-24T00:00:00+00:00` via `airflow dags test` inside the
`presight-airflow-webserver` container (Airflow 2.7.3) — every task actually executed
against the real datasets, from a scratch copy of the DAG + its three sibling modules in
`/tmp/presight_dags` (see HOW_TO_RUN.md for why not `deploy_dag.ps1`):

| Check | Result |
|---|---|
| DAG run state | `success` — all 9 tasks |
| Run duration | ~7s |
| Row counts (raw → clean) | projects 500→500, employees 1000→1000, transactions 50000→50000 |

DQ gate results (`validate_data_quality` task, run against **raw** data before
cleaning — this is the gate, not a report on the cleaned output):

| Dataset | Checks passed |
|---|---|
| projects | 6/9 |
| employees | 3/9 |
| transactions | 5/9 (the `freshness` check is time-sensitive — it flags data older than 30 days, so this count depends on the run date) |

None of these failures block the DAG — only primary-key completeness below 80% would
(`CRITICAL_COMPLETENESS_THRESHOLD` in `airflow_dag.py`), and all three datasets pass
that specific check. The failing checks here are the same known raw-data issues
Pillar 1 documents and fixes downstream (e.g. employees' 8 unparseable/implausible
`hire_date` values, 5 out-of-range `years_experience`, 3 salary/level mismatches) —
the gate is reporting them, not missing them.

**Gate blocks bad data — tested, not assumed.** Same scratch copy, with
`CRITICAL_COMPLETENESS_THRESHOLD` temporarily set to an impossible `1.01`:
`validate_data_quality` raised `ValueError: DQ gate FAILED — critical completeness
failures: projects.project_id completeness 100.0% < 101%; ...` and went to
`up_for_retry` (per `retries=2`); `transform_and_enrich`, `load_to_output` and
`generate_pipeline_report` never ran. The threshold was then set back to `0.80`.

```powershell
type outputs\results\anil_vaikuntam\03_big_data\pipeline_report_2026-09-24.txt
```

**`04_infrastructure`'s `dq_report_*.md` files** are written by the same
`validate_data_quality` task (it calls `dq_framework.write_dq_report_markdown()` for each
dataset), so the run above regenerated them — `dq_report_employees.md` shows the same 3/9
as the gate. `python solutions/submissions/anil_vaikuntam/04_infrastructure/dq_framework.py`
regenerates them standalone.
