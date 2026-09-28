# Pillar 2 — SQL & Data Visualization

## Files

- `etl_full.py` — Task 2.2. Full ETL: loads `transactions.json`, enriches with project/employee context, writes `transactions_clean.csv` + `pipeline_summary.txt`. This is the **canonical** ETL — `03_big_data/airflow_dag.py` and the repo-root `Dockerfile` both import/run this exact file, not a copy.
- `queries.sql` — Task 2.1. The six business questions, standalone runnable SQL against the warehouse `01_foundations/data_model.sql` builds.
- `query_optimization.sql` — Task 2.3. Original query, captured `EXPLAIN ANALYZE`, rewrite, indexes, and benchmark — runs against real PostgreSQL (not DuckDB, unlike the rest of this pillar), since the indexes showed nothing on DuckDB and needed verifying on a genuine row-store. Self-contained (its own setup block creates and loads the plain tables the starter query needs), but requires the `presight-postgres` container running.
- `build_dashboard.py` — Task 2.4. Power BI Desktop isn't available on this machine, so per the setup guide's documented alternative this renders a one-page executive dashboard mockup PDF from real data (`projects_clean.csv`, `transactions_clean.csv`, and `queries.sql`'s own Q5 run against the warehouse).
- `export_dashboard_data.sql` — supporting per-visual extract queries for rebuilding the same dashboard in Power BI (Task 2.4). Not what `build_dashboard.py` reads; kept as a documented alternative — see the file's own header for why its numbers deliberately differ from `queries.sql`'s Q1/Q3.
- `run_queries.py`, `run_optimization.py` — Python drivers that execute the `.sql` files above and print real captured output (`run_queries.py` substitutes path placeholders for DuckDB; `run_optimization.py` stages the CSVs into the Postgres container and benchmarks against it). Not required reading to understand the SQL itself — the `.sql` files are plain runnable SQL on their own.
- `warehouse_explorer.ipynb` — Jupyter notebook for live, editable querying of the warehouse (results render as tables). A UI convenience on top of the same warehouse, not a separate data path.

## Outputs

| File | Path |
|---|---|
| `transactions_clean.csv` | `outputs/results/anil_vaikuntam/02_sql_and_viz/` |
| `pipeline_summary.txt` | `outputs/results/anil_vaikuntam/02_sql_and_viz/` |
| `dashboard_mockup.pdf` | `outputs/results/anil_vaikuntam/02_sql_and_viz/` |
| Q1–Q6 results, optimisation benchmark | printed by `run_queries.py`/`run_optimization.py`, not written to a file — the `.sql` files themselves are the deliverable |

## How to run

See [HOW_TO_RUN.md](../HOW_TO_RUN.md) — Pillar 2 section.

See [VALIDATION_EVIDENCE.md](VALIDATION_EVIDENCE.md) for real row counts, query
results, and the optimisation benchmark numbers behind this pillar's claims.
