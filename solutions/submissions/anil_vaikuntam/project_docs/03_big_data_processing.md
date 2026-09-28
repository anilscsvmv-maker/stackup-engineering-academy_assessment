# Big Data Processing: Spark, Kafka, and a Quality-Gated Airflow Pipeline

## 1. Executive Summary

Presight's platform produces a continuous stream of events — logins, escalations, file uploads. At 100,000 events across 12 monthly files, the Pandas approach used in the earlier pillars was no longer the right tool for this volume.

This pillar builds a PySpark pipeline that turns the event stream into five summary tables, a Kafka producer/consumer that simulates real-time ingestion with severity-based routing, and an Airflow pipeline that runs the whole thing daily with a data-quality gate — tested directly to confirm it actually blocks bad data instead of just assuming the code works. Every part ran against real infrastructure — a live Kafka broker, an actual Airflow container — and the edge cases below are the ones that only show up that way.

## 2. Business Problem

Two problems. The immediate one: nobody could query the event stream to answer questions like which projects have the most escalations, when usage peaks, or how long escalations stay open. The longer-term one: the existing Pandas pipeline had no path to real-time processing, ran only when someone remembered to run it manually, and had no schedule or protection against loading bad data.

## 3. Project Scope

**In scope:** Spark batch processing of all 12 files into five summary tables; a Kafka producer/consumer that forwards critical escalations to their own topic; an Airflow pipeline (extract → data-quality check → clean → load → report) on a daily schedule.

**Out of scope:** a multi-node Spark cluster — 100,000 rows doesn't need one, and the goal was to prove the pipeline logic works, not to run it at production scale; Spark Structured Streaming — this is a batch producer/consumer simulation, not true streaming; a production Kafka deployment — a single local broker is used instead.

**A practical constraint:** developed on Windows, where PySpark needs Hadoop's native libraries to read files — these don't come with `pip install pyspark` and had to be installed separately.

## 4. Technology Stack

| Technology | Role in the Project | Why It Was Chosen |
|---|---|---|
| PySpark | Distributed processing into 5 aggregated Parquet tables | Where vectorised pandas stops being the right fit |
| Apache Kafka | Real-time ingestion simulation, topic routing | Standard decoupling pattern, the natural next step past daily batch |
| Apache Airflow 2.7 | Daily orchestration, retries, DQ gate, XCom reporting | Built for dependency-ordered scheduled tasks with visible failures |
| Java 17 (Temurin) | JVM runtime Spark depends on | Not optional — Spark is JVM-based regardless of the Python wrapper |
| Docker Compose | Local Kafka, Zookeeper, Airflow, Postgres | Reproducible infra without hand-installing four services |

## 5. High-Level Architecture & Data Flow

```mermaid
flowchart TD
    subgraph EventFiles["events_stream/*.jsonl (12 files, ~100K events)"]
        F1["events_2025_01.jsonl"]
        Fn["... 11 more monthly files"]
    end

    subgraph SparkJob["spark_pipeline.py"]
        Load["Explicit StructType schema\n(no inferSchema)"]
        Validate["Drop nulls, dedupe event_id,\nderive event_date/hour/month"]
        Agg["5 aggregations:\nproject activity, user activity,\nescalation log, daily volume,\npeak usage"]
    end

    EventFiles --> Load --> Validate --> Agg --> Parquet[("5 Parquet tables\npartitioned by event_date / severity")]

    F1 -.simulated real-time.-> Producer["Kafka producer\n50ms/msg, keyed by event_type"]
    Producer --> TopicA["presight.project.events\n(3 partitions)"]
    TopicA --> Consumer["Kafka consumer\ngroup: presight-assessment-consumer"]
    Consumer -- "Critical severity" --> TopicB["presight.escalations.critical"]
    Consumer --> Summary[("kafka summary.json\nthroughput + counts")]

    subgraph DAG["Airflow: presight_etl_pipeline (daily 06:00 Dubai)"]
        Extract["extract_projects\nextract_employees\nextract_transactions\n(parallel)"]
        Gate{"validate_data_quality\nDQ GATE"}
        Transform["transform_and_enrich"]
        LoadOut["load_to_output"]
        Report["generate_pipeline_report\n(via XCom)"]
    end

    Extract --> Gate
    Gate -- "pass" --> Transform --> LoadOut --> Report
    Gate -- "fail: raise ValueError" --> Blocked["Downstream tasks\ndo NOT run"]
```

## 6. Key Engineering Decisions

**Explicit schema over `inferSchema`.** Inference means an extra full pass just to guess types, and the nested `payload` field can't be inferred reliably anyway. Defined the `StructType` up front instead.

**Partitioning tied to actual query patterns.** `daily_event_volume` partitioned by `event_date` (date-range queries); `escalation_log` by `severity` ("show me the Critical ones"). Partitioning has overhead, so each choice matches how the table actually gets queried.

**Environment-variable-driven `DATA_DIR`/`OUTPUT_DIR`.** The same pipeline code runs on the dev machine and inside the Airflow container, which has a different filesystem layout. Path resolution reads from env vars instead of maintaining two versions — and this same mechanism ended up solving Pillar 4's containerisation requirement for free.

**DAG dependencies deployed as sibling modules, not a package.** Airflow puts `dags/` on `sys.path`, so copying `etl_pipeline.py`/`etl_full.py`/`dq_framework.py` alongside the DAG lets it `import etl_pipeline` directly — no packaging step.

## 7. Challenges & Fixes

**Getting PySpark to run on Windows.** Spark needs Hadoop's Windows-native file-handling library (`winutils.exe`/`hadoop.dll`) to list and write files, and `pip install pyspark` doesn't include it — `HADOOP_HOME` points at `C:\hadoop`. A second, quieter trap: with `SPARK_HOME` pointing at a separately-installed Spark 3.5.9 while pip's PySpark is 4.2, `getOrCreate()` fails with `'JavaPackage' object is not callable` because the jars don't match — the run clears `SPARK_HOME` so PySpark uses its own jars. One more: the partitioned Parquet output paths exceed Windows' 260-character limit under this OneDrive folder, which DuckDB and pyarrow can't open without the `\\?\` long-path prefix (Spark itself writes them fine).

**Matching escalations by time, not position.** Pairing the 1st "raised" event with the 1st "resolved" event per project only works if they alternate perfectly, which isn't true when a project has more than one escalation open at once — positional pairing produces impossible negative resolution times. `escalation_log` instead matches each "resolved" event to the earliest still-open "raised" event before it. On this run: 1,969 escalations, 1,098 matched as resolved, 0 negative durations (minimum 9.99 hours).

**Letting the Kafka producer do the encoding.** The producer is configured with `value_serializer`/`key_serializer`, so the critical-escalation forwarder hands it plain dicts — encoding to bytes first as well would double-encode. Confirmed by a full 8,333-message run against the live broker with all 14 Critical escalations forwarded and no errors.

**Testing that the data-quality check actually blocks bad data.** Rather than just trust that the check would work as written, it was tested directly: with `CRITICAL_COMPLETENESS_THRESHOLD` set to an impossible `1.01` in a scratch copy of the DAG, `validate_data_quality` raised `DQ gate FAILED` and went to retry, and `transform_and_enrich`/`load_to_output`/`generate_pipeline_report` never ran; set back to `0.80`, the full run passes.

## 8. Data Quality, Reliability, Security & Performance

`validate_data_quality` is a hard gate: reloads all three datasets, runs the Pillar 4 DQ framework, raises if completeness on a primary key drops below 80%. With `retries=2`/`max_active_runs=1`, a bad extract gets caught and retried instead of propagating.

Real numbers: Spark processed all 99,996 events in 264.3s (~378 events/sec, local mode — most of that is writing 5,088 date partitions into a OneDrive-synced folder). Kafka consumed all 8,333 messages at ~604 msg/sec, forwarding 14 Critical escalations correctly. The Airflow DAG runs green in ~7s inside the Airflow 2.7.3 container.

## 9. Outcome & Business Value

The event stream went from unqueryable to five analytics-ready tables, reproducible on any future file drop. Kafka proves out the real-time ingestion pattern for whatever comes after daily batch. And the pipeline stopped depending on someone remembering to run it — it's scheduled, retried, gated, and proven to actually block bad data rather than just claiming to.
