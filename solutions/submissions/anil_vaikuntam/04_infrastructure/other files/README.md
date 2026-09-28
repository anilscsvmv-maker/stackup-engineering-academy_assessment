# Pillar 4 — Infrastructure & Governance

## Files

- `data_governance.md` — Task 4.2. PII/classification, ownership, retention, access control, and lineage for all 4 datasets. Packaged copy at `outputs/results/anil_vaikuntam/04_infrastructure/data_governance_document.md`.
- `dq_framework.py` — Task 4.3. Configurable data-quality framework (`DQ_CONFIG` dict + 9 checks). Imported by `03_big_data/airflow_dag.py`'s DQ-gate task — not duplicated there.

**`Dockerfile` and `.dockerignore` are at the repo root, not in this folder** — `tasks/04_infrastructure/INSTRUCTIONS.md` explicitly requires `Dockerfile` at the repo root (Docker build context has to include `solutions/` and `datasets/`, both siblings of this folder, so it can't build correctly from inside `04_infrastructure/` itself).

## Outputs

| File | Path |
|---|---|
| `data_governance_document.md` | `outputs/results/anil_vaikuntam/04_infrastructure/` |
| `dq_report_projects.md`, `dq_report_employees.md`, `dq_report_transactions.md` | `outputs/results/anil_vaikuntam/04_infrastructure/` |
| Dockerised ETL output (`projects_clean.csv` etc.) | `outputs/results/anil_vaikuntam/02_sql_and_viz/` — the container runs Pillar 2's `etl_full.py`, so its outputs land in that pillar's folder, not here |

## How to run

See [HOW_TO_RUN.md](../HOW_TO_RUN.md) — Pillar 4 section.
