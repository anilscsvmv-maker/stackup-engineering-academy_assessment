"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: spark_pipeline.py
Pillar: Big Data Processing (Task 3.1)
Author: Anil Vaikuntam
=============================================================

SCENARIO
--------
Process 100K platform events across 12 monthly files into 5 aggregated
Parquet output tables: project_activity_summary, user_activity_summary,
escalation_log, daily_event_volume, peak_usage_analysis.

HOW TO RUN
----------
  (Java 17 required on PATH/JAVA_HOME, HADOOP_HOME=C:\\hadoop on Windows, and
  SPARK_HOME unset so pip PySpark uses its own jars — see HOW_TO_RUN.md)
  python solutions\\submissions\\anil_vaikuntam\\03_big_data\\spark_pipeline.py
"""

import logging
import os
import time

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType, DoubleType, BooleanType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
EVENTS_DIR = os.path.join(BASE_DIR, "datasets", "events_stream")
# outputs/artifacts/, not outputs/results/ — Parquet is a large, regenerable
# binary build output (re-running this script recreates it byte-for-byte
# from source), unlike the CSV/JSON/markdown deliverables elsewhere in
# outputs/results/, which are worth tracking in git. outputs/artifacts/ is
# gitignored.
RESULTS_DIR = os.path.join(BASE_DIR, "outputs", "artifacts", "anil_vaikuntam", "03_big_data", "spark")


# ==============================================================================
# STEP 1 — Spark session
# ==============================================================================

def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PresightEventsProcessing")
        .master("local[*]")
        # Default 200 shuffle partitions is tuned for cluster-scale data, not
        # ~100K local rows — that many tiny tasks means per-task JVM/scheduler
        # overhead dominates actual work. Matching the local core count (every
        # groupBy/window/join here is a shuffle) is the single biggest lever
        # for a small local-mode job like this one.
        .config("spark.sql.shuffle.partitions", os.cpu_count() or 8)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==============================================================================
# STEP 2 — Load events
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
    Load all 12 events_2025_*.jsonl files with an explicit schema —
    inferSchema would force a full extra read pass, and payload (varying keys
    per event_type) can't be inferred reliably anyway.
    """
    path_pattern = os.path.join(events_dir, "events_*.jsonl").replace("\\", "/")
    df = spark.read.schema(EVENTS_SCHEMA).json(path_pattern)
    n = df.count()
    logger.info("Loaded %d raw event rows from %s", n, path_pattern)
    return df


# ==============================================================================
# STEP 3 — Validate and clean
# ==============================================================================

def validate_events(df):
    n_raw = df.count()

    df = df.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
    n_after_null_drop = df.count()
    logger.info("Dropped %d rows with null event_id/user_id (%d -> %d)", n_raw - n_after_null_drop, n_raw, n_after_null_drop)

    # Keep first occurrence per event_id by timestamp (dedupe), via row_number()
    # over a window rather than dropDuplicates() — dropDuplicates() offers no
    # control over WHICH duplicate survives; this guarantees "first by time".
    w = Window.partitionBy("event_id").orderBy(F.col("timestamp").asc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    n_after_dedupe = df.count()
    logger.info("Dropped %d duplicate event_id rows (%d -> %d)", n_after_null_drop - n_after_dedupe, n_after_null_drop, n_after_dedupe)

    df = (
        df.withColumn("event_date", F.to_date("timestamp"))
          .withColumn("event_hour", F.hour("timestamp"))
          .withColumn("event_month", F.date_format("timestamp", "yyyy-MM"))
    )
    return df


# ==============================================================================
# STEP 4 — Aggregations
# ==============================================================================

# Table 1: project_activity_summary — INSTRUCTIONS.md requirement:
# For each project: total_events (all events linked to this project),
# escalation_count (event_type='escalation_raised'), task_completions
# (event_type='task_completed'), document_uploads
# (event_type='document_uploaded'), last_event_timestamp (most recent
# timestamp), unique_users (distinct users who triggered events),
# unique_event_types (distinct event types touching this project).
# Exclude null project_ids (logins).
def project_activity_summary(df):
    """Table 1. Excludes null project_ids (logins have no project)."""
    scoped = df.filter(F.col("project_id").isNotNull())
    return (
        scoped.groupBy("project_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "escalation_raised", 1).otherwise(0)).alias("escalation_count"),
            F.sum(F.when(F.col("event_type") == "task_completed", 1).otherwise(0)).alias("task_completions"),
            F.sum(F.when(F.col("event_type") == "document_uploaded", 1).otherwise(0)).alias("document_uploads"),
            F.max("timestamp").alias("last_event_timestamp"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("event_type").alias("unique_event_types"),
        )
        .orderBy(F.col("total_events").desc())
    )


# Table 2: user_activity_summary — INSTRUCTIONS.md requirement:
# For each user: login_count (event_type='login'), logout_count
# (event_type='logout'), actions_taken (all events excluding
# login/logout), projects_touched (distinct projects, excluding nulls),
# first_active (earliest timestamp), last_active (latest timestamp),
# active_days (distinct event_date count).
def user_activity_summary(df):
    """Table 2."""
    return (
        df.groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "login", 1).otherwise(0)).alias("login_count"),
            F.sum(F.when(F.col("event_type") == "logout", 1).otherwise(0)).alias("logout_count"),
            F.sum(F.when(~F.col("event_type").isin("login", "logout"), 1).otherwise(0)).alias("actions_taken"),
            F.countDistinct(F.when(F.col("project_id").isNotNull(), F.col("project_id"))).alias("projects_touched"),
            F.min("timestamp").alias("first_active"),
            F.max("timestamp").alias("last_active"),
            F.countDistinct("event_date").alias("active_days"),
        )
        .orderBy(F.col("actions_taken").desc())
    )

#-------------------------------------------------------------------------

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
   - Processes one project’s escalation events at a time.
   - Handles resolution events from earliest to latest.
   - Matches each resolution to the **oldest escalation still open**.
   - Only matches an escalation raised before or at the resolution time.
   - Each raised escalation can be matched only once.
   - This ensures resolution duration is never negative.
   - Uses nested loops (`O(n²)`), which are manageable because each project has few escalations.
   - **Assumption:** the oldest open escalation is resolved first; the data does not confirm the actual match.
    """
    raised = pdf[pdf["_kind"] == "raised"].sort_values("ts").reset_index(drop=True)
    resolved = pdf[pdf["_kind"] == "resolved"].sort_values("ts").reset_index(drop=True)

    claimed_by = {}  # index into `raised` -> matching row of `resolved`
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


# Table 3: escalation_log — INSTRUCTIONS.md requirement:

# Build a complete escalation log by joining escalation_raised and
# escalation_resolved events. Extract severity from raised events'
# payload, resolved_by from resolved events' payload. Compute
# resolution_time_hours = (resolved_at - raised_at) / 3600. Set
# resolved = True/False based on existence of a matching resolution. For
# unresolved escalations, set resolved_at and resolved_by to NULL. (Hint
# given: left join between raised and resolved DataFrames on project_id,
# ordered by timestamp — see the docstring below for why this
# implementation matches nearest-prior instead of a positional join.)
def escalation_log(df):
    """
    Table 3. Matches each escalation_resolved event to the raise it actually
    closed (see _match_project_escalations), not a positional Nth-to-Nth
    pairing — verified against this data that raised/resolved events overlap
    heavily (a new escalation often opens before an older one closes), so a
    same-rank join frequently attaches the wrong resolution to the wrong
    raise. An earlier version did exactly that and produced negative
    resolution_time_hours on roughly half of all "resolved" pairs. There's no
    explicit escalation-correlation ID in the source schema, so nearest-prior
    temporal matching is the best available ground truth.
    """
    raised = (
        df.filter(F.col("event_type") == "escalation_raised")
        .select(
            F.col("project_id"),
            F.lit("raised").alias("_kind"),
            F.col("timestamp").alias("ts"),
            F.col("event_id"),
            F.col("user_id").alias("raised_by"),
            F.col("payload").getItem("severity").alias("severity"),
            F.lit(None).cast("string").alias("resolved_by"),
        )
    )
    resolved = (
        df.filter(F.col("event_type") == "escalation_resolved")
        .select(
            F.col("project_id"),
            F.lit("resolved").alias("_kind"),
            F.col("timestamp").alias("ts"),
            F.lit(None).cast("string").alias("event_id"),
            F.lit(None).cast("string").alias("raised_by"),
            F.lit(None).cast("string").alias("severity"),
            F.col("payload").getItem("resolved_by").alias("resolved_by"),
        )
    )

    return (
        raised.unionByName(resolved)
        .groupBy("project_id")
        .applyInPandas(_match_project_escalations, schema=_ESCALATION_LOG_SCHEMA)
    )


# Table 4: daily_event_volume — INSTRUCTIONS.md requirement:
# Group by event_date and event_type, count events per group, add
# cumulative_count per event type (running total). Sort by event_date
# ascending, event_count descending. (Hint given: Window.partitionBy
# ("event_type").orderBy("event_date") with sum().over(...) for
# cumulative.)
def daily_event_volume(df):
    """Table 4. Cumulative running total per event_type via a window sum."""
    daily = df.groupBy("event_date", "event_type").agg(F.count("*").alias("event_count"))
    w = Window.partitionBy("event_type").orderBy("event_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    daily = daily.withColumn("cumulative_count", F.sum("event_count").over(w))
    return daily.orderBy(F.col("event_date").asc(), F.col("event_count").desc())


# Table 5: peak_usage_analysis — INSTRUCTIONS.md requirement:
# For each event_date x event_hour combination: total_events (count),
# unique_users (distinct users active), event_types_per_hour (distinct
# event types). Sort by total_events descending and return top 20 hours.
# This helps identify peak usage windows.
def peak_usage_analysis(df):
    """Table 5. Top 20 date x hour combinations by total_events."""
    return (
        df.groupBy("event_date", "event_hour")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("event_type").alias("event_types_per_hour"),
        )
        .orderBy(F.col("total_events").desc())
        .limit(20)
    )


# ==============================================================================
# STEP 5 — Write outputs
# ==============================================================================

def write_parquet(df, name: str, output_dir: str, row_count: int, partition_by=None, coalesce_to=None):
    """
    `row_count` is passed in rather than recomputed here — the caller
    already cached+counted `df` once (see run_pipeline's `timed()`), so
    reusing that count avoids a second full pass over the aggregation just
    to log a number already known.
    """
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
    raw = load_events(spark, EVENTS_DIR)
    clean = validate_events(raw)
    clean.cache()
    total_rows = clean.count()
    t_load = time.time() - t0
    logger.info("Load+validate: %.2fs, %d clean rows", t_load, total_rows)

    timings = {}

    def timed(name, fn, *args):
        """
        Caches the aggregation result before counting it, so the count below
        (needed for timing/logging) and both write_parquet calls later reuse
        the same materialised data instead of recomputing the aggregation
        from `clean` on every one of those actions.
        """
        t0 = time.time()
        result = fn(*args).cache()
        result_count = result.count()
        timings[name] = time.time() - t0
        logger.info("%s: %.2fs, %d rows", name, timings[name], result_count)
        return result, result_count

    proj_summary, proj_n = timed("project_activity_summary", project_activity_summary, clean)
    user_summary, user_n = timed("user_activity_summary", user_activity_summary, clean)
    esc_log, esc_n = timed("escalation_log", escalation_log, clean)
    daily_vol, daily_n = timed("daily_event_volume", daily_event_volume, clean)
    peak_usage, peak_n = timed("peak_usage_analysis", peak_usage_analysis, clean)

    write_parquet(proj_summary, "project_activity_summary", RESULTS_DIR, proj_n, coalesce_to=1)
    write_parquet(user_summary, "user_activity_summary", RESULTS_DIR, user_n, coalesce_to=1)
    write_parquet(esc_log, "escalation_log", RESULTS_DIR, esc_n, partition_by="severity")
    write_parquet(daily_vol, "daily_event_volume", RESULTS_DIR, daily_n, partition_by="event_date")
    write_parquet(peak_usage, "peak_usage_analysis", RESULTS_DIR, peak_n, coalesce_to=1)

    total_elapsed = time.time() - pipeline_start
    events_per_sec = total_rows / total_elapsed if total_elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("PERFORMANCE BASELINE")
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
