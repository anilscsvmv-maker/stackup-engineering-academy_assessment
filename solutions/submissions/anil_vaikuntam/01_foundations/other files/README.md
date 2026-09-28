# Pillar 1 — Foundations

## Files

- `etl_pipeline.py` — Task 1.1 (`load_projects`/`transform_projects`) and Task 1.3 (`load_employees`/`clean_employees`). Imported by `02_sql_and_viz/etl_full.py`, `03_big_data/airflow_dag.py`, and the Docker image — not duplicated there.
- `data_model.sql` — Task 1.2. Sections 1+2: star schema DDL, the SCD2 `dim_employee` build (pure SQL, no Python), and validation queries. Sections 3 (business questions) and 4 (query optimisation) live in `02_sql_and_viz/` instead — see that folder's README for why.

## Input datasets

- `datasets/projects.csv`
- `datasets/employees.csv`
- `datasets/employees_salary_history.csv`

## Outputs

| File | Path |
|---|---|
| `projects_clean.csv` | `outputs/results/anil_vaikuntam/01_foundations/` |
| `employees_clean.csv` | `outputs/results/anil_vaikuntam/01_foundations/` |
| `employees_quality_summary.json` | `outputs/results/anil_vaikuntam/01_foundations/` |
| `pipeline_summary.txt` | `outputs/results/anil_vaikuntam/01_foundations/` |
| `dim_employee` (in the star schema) | `outputs/presight_warehouse.duckdb` — single shared warehouse file, not namespaced per pillar |

See [ASSUMPTIONS.md](ASSUMPTIONS.md) and [VALIDATION_EVIDENCE.md](VALIDATION_EVIDENCE.md) for the reasoning behind the less obvious decisions and the real numbers backing them.

## How to run

See [HOW_TO_RUN.md](../HOW_TO_RUN.md) — Pillar 1 section.
