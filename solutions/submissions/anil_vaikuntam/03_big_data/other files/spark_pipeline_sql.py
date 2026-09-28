"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Reference File: spark_pipeline_sql.py
Pillar: Big Data Processing (Task 3.1) — Spark SQL variant
Author: Anil Vaikuntam
=============================================================

Same 5 output tables as spark_pipeline.py, but the aggregation logic is
expressed as spark.sql() queries over temp views instead of the DataFrame
API — kept here as a reference for the SQL-on-Spark style (CTEs, window
functions, CASE WHEN) rather than as a second graded submission.

escalation_log is the one exception: matching each escalation_resolved
event to the EARLIEST still-unclaimed escalation_raised event before it is
a greedy, order-dependent algorithm (see _match_project_escalations in
spark_pipeline.py for the full rationale) — there's no set-based SQL
formulation of "claim then remove from the candidate pool", so that step
stays a groupBy().applyInPandas() call. Everything upstream of it (splitting
raised/resolved rows out of the event stream) is still plain SQL.

HOW TO RUN
----------
  python "solutions\\submissions\\anil_vaikuntam\\03_big_data\\other files\\spark_pipeline_sql.py"
"""

import glob
import logging
import os
import sys
import time

# Must be set before the JVM's Python worker processes are spawned (i.e.
# before SparkSession creation) — escalation_log's applyInPandas step
# forks worker subprocesses that otherwise resolve "python" off PATH,
# which can be a different interpreter than the one running this script;
# PySpark refuses to run driver/worker on mismatched minor versions.
# Pinning both to sys.executable keeps worker == driver always.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType, DoubleType, BooleanType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
EVENTS_DIR = os.path.join(BASE_DIR, "datasets", "events_stream")
# Separate from spark_pipeline.py's output dir on purpose — this is a
# reference/teaching variant, not a second copy of the graded deliverable,
# so it must never overwrite outputs/artifacts/.../spark/.
RESULTS_DIR = os.path.join(BASE_DIR, "outputs", "artifacts", "anil_vaikuntam", "03_big_data", "spark_sql")


# ==============================================================================
# STEP 1 — Spark session
# ==============================================================================

def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PresightEventsProcessingSQL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", os.cpu_count() or 8)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==============================================================================
# STEP 2 — Load events + register as a temp view
# ==============================================================================

EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("payload", MapType(StringType(), StringType()), True),
])


def load_events(spark: SparkSession, events_dir: str):
    """
    Expand the events_*.jsonl glob in Python and pass Spark an explicit file
    list, rather than a wildcard path. A wildcard path forces Spark's
    Hadoop-based Globber to call RawLocalFileSystem.listStatus, which on
    Windows without winutils.exe throws UnsatisfiedLinkError from
    NativeIO$Windows.access0 inside an async ForkJoinPool worker — a fatal
    Error that Scala's Future treats as NonFatal-only-catching, so the
    Promise never completes and the read hangs indefinitely instead of
    failing. An explicit path list skips the Globber entirely.
    """
    path_pattern = os.path.join(events_dir, "events_*.jsonl")
    paths = sorted(glob.glob(path_pattern))
    if not paths:
        raise FileNotFoundError(f"No files matched {path_pattern}")
    df = spark.read.schema(EVENTS_SCHEMA).json([p.replace("\\", "/") for p in paths])
    df.createOrReplaceTempView("raw_events")
    n = df.count()
    logger.info("Loaded %d raw event rows from %d files in %s", n, len(paths), events_dir)
    return df


# ==============================================================================
# STEP 3 — Validate and clean, via SQL
# ==============================================================================

VALIDATE_SQL = """
WITH not_null AS (
    SELECT *
    FROM raw_events
    WHERE event_id IS NOT NULL AND user_id IS NOT NULL
),
-- ROW_NUMBER() over a window, not a plain DISTINCT/GROUP BY, so we control
-- which duplicate survives: first occurrence per event_id by timestamp.
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY timestamp ASC) AS rn
    FROM not_null
)
SELECT
    event_id, event_type, project_id, user_id, timestamp, payload,
    CAST(timestamp AS DATE)          AS event_date,
    HOUR(timestamp)                  AS event_hour,
    DATE_FORMAT(timestamp, 'yyyy-MM') AS event_month
FROM deduped
WHERE rn = 1
"""


def validate_events(spark: SparkSession):
    n_raw = spark.table("raw_events").count()
    clean = spark.sql(VALIDATE_SQL)
    clean.createOrReplaceTempView("events")
    n_clean = clean.count()
    logger.info("Validate: %d raw -> %d clean rows (%d dropped: null keys or duplicate event_id)",
                n_raw, n_clean, n_raw - n_clean)
    return clean


# ==============================================================================
# STEP 4 — Aggregations, each a single spark.sql() query over `events`
# ==============================================================================

PROJECT_ACTIVITY_SQL = """
SELECT
    project_id,
    COUNT(*)                                                              AS total_events,
    SUM(CASE WHEN event_type = 'escalation_raised' THEN 1 ELSE 0 END)     AS escalation_count,
    SUM(CASE WHEN event_type = 'task_completed'    THEN 1 ELSE 0 END)     AS task_completions,
    SUM(CASE WHEN event_type = 'document_uploaded' THEN 1 ELSE 0 END)     AS document_uploads,
    MAX(timestamp)                                                        AS last_event_timestamp,
    COUNT(DISTINCT user_id)                                               AS unique_users,
    COUNT(DISTINCT event_type)                                            AS unique_event_types
FROM events
WHERE project_id IS NOT NULL  -- logins etc. have no project_id
GROUP BY project_id
ORDER BY total_events DESC
"""

USER_ACTIVITY_SQL = """
SELECT
    user_id,
    SUM(CASE WHEN event_type = 'login'  THEN 1 ELSE 0 END)                          AS login_count,
    SUM(CASE WHEN event_type = 'logout' THEN 1 ELSE 0 END)                          AS logout_count,
    SUM(CASE WHEN event_type NOT IN ('login', 'logout') THEN 1 ELSE 0 END)          AS actions_taken,
    COUNT(DISTINCT CASE WHEN project_id IS NOT NULL THEN project_id END)            AS projects_touched,
    MIN(timestamp)                                                                  AS first_active,
    MAX(timestamp)                                                                  AS last_active,
    COUNT(DISTINCT event_date)                                                      AS active_days
FROM events
GROUP BY user_id
ORDER BY actions_taken DESC
"""

DAILY_EVENT_VOLUME_SQL = """
WITH daily AS (
    SELECT event_date, event_type, COUNT(*) AS event_count
    FROM events
    GROUP BY event_date, event_type
)
SELECT
    event_date, event_type, event_count,
    SUM(event_count) OVER (
        PARTITION BY event_type ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_count
FROM daily
ORDER BY event_date ASC, event_count DESC
"""

PEAK_USAGE_SQL = """
SELECT
    event_date, event_hour,
    COUNT(*)                     AS total_events,
    COUNT(DISTINCT user_id)      AS unique_users,
    COUNT(DISTINCT event_type)   AS event_types_per_hour
FROM events
GROUP BY event_date, event_hour
ORDER BY total_events DESC
LIMIT 20
"""


def project_activity_summary(spark: SparkSession):
    return spark.sql(PROJECT_ACTIVITY_SQL)


def user_activity_summary(spark: SparkSession):
    return spark.sql(USER_ACTIVITY_SQL)


def daily_event_volume(spark: SparkSession):
    return spark.sql(DAILY_EVENT_VOLUME_SQL)


def peak_usage_analysis(spark: SparkSession):
    return spark.sql(PEAK_USAGE_SQL)


# ---- escalation_log: SQL for the split, applyInPandas for the match -------

RAISED_SQL = """
SELECT
    project_id, 'raised' AS _kind, timestamp AS ts, event_id,
    user_id AS raised_by, payload['severity'] AS severity,
    CAST(NULL AS STRING) AS resolved_by
FROM events
WHERE event_type = 'escalation_raised'
"""

RESOLVED_SQL = """
SELECT
    project_id, 'resolved' AS _kind, timestamp AS ts,
    CAST(NULL AS STRING) AS event_id, CAST(NULL AS STRING) AS raised_by,
    CAST(NULL AS STRING) AS severity, payload['resolved_by'] AS resolved_by
FROM events
WHERE event_type = 'escalation_resolved'
"""

_ESCALATION_LOG_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("project_id", StringType()),
    StructField("raised_by", StringType()),
    StructField("raised_at", TimestampType()),
    StructField("severity", StringType()),
    StructField("resolved", BooleanType()),
    StructField("resolved_by", StringType()),
    StructField("resolved_at", TimestampType()),
    StructField("resolution_time_hours", DoubleType()),
])


def _match_project_escalations(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Runs once per project_id group. For each escalation_resolved event, in
    chronological order, claims the EARLIEST still-unclaimed
    escalation_raised event at or before it. Not expressible as a set-based
    SQL query: each claim removes that row from the pool available to later
    resolved events, so the result depends on processing order, not just
    row values. Group sizes are small (~4 escalations/project), so the O(n^2)
    scan is negligible.
    """
    raised = pdf[pdf["_kind"] == "raised"].sort_values("ts").reset_index(drop=True)
    resolved = pdf[pdf["_kind"] == "resolved"].sort_values("ts").reset_index(drop=True)

    claimed_by = {}
    claimed = [False] * len(raised)
    for _, r in resolved.iterrows():
        for i in range(len(raised)):
            if not claimed[i] and raised.loc[i, "ts"] <= r["ts"]:
                claimed[i] = True
                claimed_by[i] = r
                break

    rows = []
    for i, rw in raised.iterrows():
        if i in claimed_by:
            rr = claimed_by[i]
            rows.append({
                "event_id": rw["event_id"], "project_id": rw["project_id"],
                "raised_by": rw["raised_by"], "raised_at": rw["ts"], "severity": rw["severity"],
                "resolved": True, "resolved_by": rr["resolved_by"], "resolved_at": rr["ts"],
                "resolution_time_hours": (rr["ts"] - rw["ts"]).total_seconds() / 3600.0,
            })
        else:
            rows.append({
                "event_id": rw["event_id"], "project_id": rw["project_id"],
                "raised_by": rw["raised_by"], "raised_at": rw["ts"], "severity": rw["severity"],
                "resolved": False, "resolved_by": None, "resolved_at": None,
                "resolution_time_hours": None,
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[f.name for f in _ESCALATION_LOG_SCHEMA])


def escalation_log(spark: SparkSession):
    raised = spark.sql(RAISED_SQL)
    resolved = spark.sql(RESOLVED_SQL)
    return (
        raised.unionByName(resolved)
        .groupBy("project_id")
        .applyInPandas(_match_project_escalations, schema=_ESCALATION_LOG_SCHEMA)
    )


# ==============================================================================
# STEP 5 — Write outputs
# ==============================================================================

def write_parquet(df, name: str, output_dir: str, row_count: int, partition_by=None, coalesce_to=None):
    path = os.path.join(output_dir, name).replace("\\", "/")
    writer = df
    if coalesce_to:
        writer = writer.coalesce(coalesce_to)
    w = writer.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.parquet(path)
    logger.info("Wrote %s -> %s (%d rows%s)", name, path, row_count, f", partitioned by {partition_by}" if partition_by else "")
    return row_count


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================

def run_pipeline():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    pipeline_start = time.time()
    spark = get_spark_session()

    t0 = time.time()
    load_events(spark, EVENTS_DIR)
    clean = validate_events(spark)
    clean.cache()
    total_rows = clean.count()
    t_load = time.time() - t0
    logger.info("Load+validate: %.2fs, %d clean rows", t_load, total_rows)

    timings = {}

    def timed(name, fn, *args):
        t0 = time.time()
        result = fn(*args).cache()
        result_count = result.count()
        timings[name] = time.time() - t0
        logger.info("%s: %.2fs, %d rows", name, timings[name], result_count)
        return result, result_count

    proj_summary, proj_n = timed("project_activity_summary", project_activity_summary, spark)
    user_summary, user_n = timed("user_activity_summary", user_activity_summary, spark)
    esc_log, esc_n = timed("escalation_log", escalation_log, spark)
    daily_vol, daily_n = timed("daily_event_volume", daily_event_volume, spark)
    peak_usage, peak_n = timed("peak_usage_analysis", peak_usage_analysis, spark)

    write_parquet(proj_summary, "project_activity_summary", RESULTS_DIR, proj_n, coalesce_to=1)
    write_parquet(user_summary, "user_activity_summary", RESULTS_DIR, user_n, coalesce_to=1)
    write_parquet(esc_log, "escalation_log", RESULTS_DIR, esc_n, partition_by="severity")
    write_parquet(daily_vol, "daily_event_volume", RESULTS_DIR, daily_n, partition_by="event_date")
    write_parquet(peak_usage, "peak_usage_analysis", RESULTS_DIR, peak_n, coalesce_to=1)

    total_elapsed = time.time() - pipeline_start
    events_per_sec = total_rows / total_elapsed if total_elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("PERFORMANCE BASELINE (Spark SQL variant)")
    logger.info("Total execution time: %.2fs", total_elapsed)
    for name, secs in timings.items():
        logger.info("  %-28s %.2fs", name, secs)
    logger.info("Total rows processed: %d", total_rows)
    logger.info("Throughput: %.0f events/sec", events_per_sec)
    logger.info("=" * 60)

    spark.stop()
    return total_elapsed, total_rows, events_per_sec


if __name__ == "__main__":
    run_pipeline()
