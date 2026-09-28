"""
Driver for Pillar 2: loads Section 2 of 01_foundations/data_model.sql (the
remaining warehouse tables) into the DuckDB file created in Pillar 1, then
runs queries.sql's six Task 2.1 business questions and prints real result
rows. The SQL itself lives entirely in the two .sql files — this script only
substitutes paths and executes/prints.
"""

import os
import re

import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
SQL_FILE = os.path.join(BASE_DIR, "solutions", "submissions", "anil_vaikuntam", "01_foundations", "data_model.sql")
QUERIES_FILE = os.path.join(BASE_DIR, "solutions", "submissions", "anil_vaikuntam", "02_sql_and_viz", "queries.sql")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
RESULTS_DIR = os.path.join(BASE_DIR, "outputs", "results", "anil_vaikuntam", "02_sql_and_viz")
DB_PATH = os.path.join(OUTPUT_DIR, "presight_warehouse.duckdb")


def main():
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_text = f.read()
    with open(QUERIES_FILE, "r", encoding="utf-8") as f:
        queries_text = f.read()

    outputs_path = RESULTS_DIR.replace("\\", "/")
    sql_text = sql_text.replace("__OUTPUTS__", outputs_path)

    section2 = sql_text[sql_text.index("-- SECTION 2"):]

    con = duckdb.connect(DB_PATH)

    print("=" * 70)
    print("SECTION 2 — loading dim_date, dim_project, dim_vendor,")
    print("            bridge_employee_project, fact_transactions")
    print("=" * 70)
    # Delete in reverse FK-dependency order (fact/bridge before the
    # dimensions they reference) so a re-run doesn't trip a constraint
    # violation on a still-referenced dim_date/dim_project/dim_vendor row.
    for table in ["fact_transactions", "bridge_employee_project", "dim_date", "dim_project", "dim_vendor"]:
        con.execute(f"DELETE FROM {table}")  # idempotent re-run
    con.execute(section2)
    for table in ["dim_date", "dim_project", "dim_employee", "dim_vendor", "bridge_employee_project", "fact_transactions"]:
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:28s} {n:>8} rows")

    print("\n" + "=" * 70)
    print("SECTION 3 — Task 2.1 business questions")
    print("=" * 70)

    # Split on "-- Qn — <title>" header lines; DuckDB ignores the remaining
    # "--" comment lines inside each chunk, so the chunk can be executed as-is.
    parts = re.split(r"\n-- (Q\d [^\n]+)\n", queries_text)
    labeled = list(zip(parts[1::2], parts[2::2]))

    for label, chunk in labeled:
        print(f"\n{label}")
        print("-" * 70)
        result = con.execute(chunk).fetchdf()
        print(f"({len(result)} rows)")
        with __import__("pandas").option_context("display.max_columns", None, "display.width", 160):
            print(result.head(10).to_string(index=False))

    con.close()


if __name__ == "__main__":
    main()
