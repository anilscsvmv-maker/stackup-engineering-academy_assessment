"""
Driver for Pillar 2, Task 2.3 (query_optimization.sql) — parses the setup
block, original query, rewritten query, and indexes straight out of that
.sql file (no queries duplicated in Python), stages the clean CSVs into
the presight-postgres container, loads plain (unindexed) employees/
projects/transactions tables, then benchmarks original vs. rewritten,
then again after adding the indexes. Runs against real PostgreSQL (the
presight-postgres container from docker-compose.yml), not DuckDB — see
query_optimization.sql's header for why.
"""

import os
import subprocess
import time

import psycopg2

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
SQL_FILE = os.path.join(BASE_DIR, "solutions", "submissions", "anil_vaikuntam", "02_sql_and_viz", "query_optimization.sql")

CONTAINER = "presight-postgres"
PG_USER = "presight"
PG_PASSWORD = "presight123"
PG_DB = "presight_practice"
PG_HOST = "localhost"
PG_PORT = 5432

CSV_FILES = {
    "employees_clean.csv": os.path.join(BASE_DIR, "outputs", "results", "anil_vaikuntam", "01_foundations", "employees_clean.csv"),
    "projects_clean.csv": os.path.join(BASE_DIR, "outputs", "results", "anil_vaikuntam", "01_foundations", "projects_clean.csv"),
    "transactions_clean.csv": os.path.join(BASE_DIR, "outputs", "results", "anil_vaikuntam", "02_sql_and_viz", "transactions_clean.csv"),
}


def stage_csvs_and_database():
    """Copy the clean CSVs into the container (COPY below is server-side)
    and make sure presight_practice exists, without touching the airflow
    metadata database that container also hosts."""
    for name, path in CSV_FILES.items():
        subprocess.run(["docker", "cp", path, f"{CONTAINER}:/tmp/{name}"], check=True)

    check = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", PG_USER, "-d", "postgres", "-tAc",
         f"SELECT 1 FROM pg_database WHERE datname='{PG_DB}'"],
        capture_output=True, text=True, check=True,
    )
    if check.stdout.strip() != "1":
        subprocess.run(
            ["docker", "exec", CONTAINER, "psql", "-U", PG_USER, "-d", "postgres",
             "-c", f"CREATE DATABASE {PG_DB};"],
            check=True,
        )


def split_sections(sql_text: str):
    setup_end = sql_text.index("-- ORIGINAL QUERY (unmodified from the starter file)")
    orig_end = sql_text.index("-- REWRITTEN QUERY — before indexes")
    rewritten_end = sql_text.index("-- INDEXES —")
    indexes_end = sql_text.index("-- REWRITTEN QUERY — after indexes")
    bonus_start = sql_text.index("-- BONUS")
    return (
        sql_text[:setup_end],
        sql_text[setup_end:orig_end],
        sql_text[orig_end:rewritten_end],
        sql_text[rewritten_end:indexes_end],
        sql_text[bonus_start:],
    )


def bonus_index_sql(bonus_block: str) -> str:
    """Pull the two live SQL statements (CREATE INDEX + ANALYZE) out of the
    BONUS section, skipping the comment lines that document its captured
    EXPLAIN ANALYZE output."""
    return "\n".join(
        line for line in bonus_block.split("\n")
        if line.strip() and not line.strip().startswith("--")
    )


def bare_query(block: str) -> str:
    """Strip the leading comment lines and EXPLAIN ANALYZE so the block can
    be run as a plain, timed query."""
    text = block.split("EXPLAIN ANALYZE", 1)[1] if "EXPLAIN ANALYZE" in block else block
    return text.strip()


def bench(cur, query, n=5):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        cur.execute(query)
        rows = cur.fetchall()
        times.append(time.perf_counter() - t0)
    return rows, times


def main():
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_text = f.read()

    setup_sql, original_block, rewritten_block, index_sql, bonus_block = split_sections(sql_text)
    original_query = bare_query(original_block)
    rewritten_query = bare_query(rewritten_block)
    bonus_sql = bonus_index_sql(bonus_block)

    stage_csvs_and_database()

    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB)
    conn.autocommit = True
    cur = conn.cursor()

    # \timing is a psql meta-command, not valid SQL — strip it before sending
    # the setup block to psycopg2.
    cur.execute(setup_sql.replace("\\timing on", ""))
    cur.execute("SELECT COUNT(*) FROM employees")
    n_emp = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM projects")
    n_proj = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM transactions")
    n_txn = cur.fetchone()[0]
    print("Loaded: employees=%d projects=%d transactions=%d" % (n_emp, n_proj, n_txn))

    print("\n--- EXPLAIN ANALYZE: original query ---")
    cur.execute("EXPLAIN ANALYZE " + original_query)
    for row in cur.fetchall():
        print(row[0])
    result, times = bench(cur, original_query)
    print(f"Original: {len(result)} rows | best={min(times)*1000:.2f}ms | runs(ms)={[round(t*1000,2) for t in times]}")

    print("\n--- EXPLAIN ANALYZE: rewritten query ---")
    cur.execute("EXPLAIN ANALYZE " + rewritten_query)
    for row in cur.fetchall():
        print(row[0])
    result2, times2 = bench(cur, rewritten_query)
    print(f"Rewritten: {len(result2)} rows | best={min(times2)*1000:.2f}ms | runs(ms)={[round(t*1000,2) for t in times2]}")

    assert len(result) == len(result2), "row count mismatch between original and rewritten query!"
    print(f"\nRow counts match: {len(result)} == {len(result2)}")
    print(f"Speedup (best-of-5): {min(times)/min(times2):.2f}x")

    print("\n--- With indexes ---")
    cur.execute(index_sql)
    result3, times3 = bench(cur, rewritten_query)
    print(f"Rewritten+indexed: {len(result3)} rows | best={min(times3)*1000:.2f}ms | runs(ms)={[round(t*1000,2) for t in times3]}")
    print(f"Speedup vs original (best-of-5): {min(times)/min(times3):.2f}x")

    print("\n--- With bonus (payment_status, amount) index ---")
    cur.execute(bonus_sql)
    result4, times4 = bench(cur, rewritten_query)
    print(f"Rewritten+amount-indexed: {len(result4)} rows | best={min(times4)*1000:.2f}ms | runs(ms)={[round(t*1000,2) for t in times4]}")
    print(f"Speedup vs original (best-of-5): {min(times)/min(times4):.2f}x")
    print(f"Speedup vs status+project_id index (best-of-5): {min(times3)/min(times4):.2f}x")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
