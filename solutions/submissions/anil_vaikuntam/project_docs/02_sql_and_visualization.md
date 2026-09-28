# SQL & Visualization: Turning Clean Data into Answers Finance Can Trust

## 1. Executive Summary

With Pillar 1's clean data ready, the next problem was access: Finance and Operations had recurring questions and no reliable way to answer them without manually pulling numbers each time.

This pillar builds the warehouse (a star schema in DuckDB), writes and checks six business questions as SQL, benchmarks and rewrites a slow query with real timing evidence, and builds an executive dashboard from the live data. The full ETL — loading and enriching 50,000 transactions — runs in under a second against a 30-second target.

## 2. Business Problem

Finance and Operations had six recurring questions: budget performance, manager workload, vendor concentration, unresolved disputes, spend trends, and compensation history. Answering any of them meant manually joining CSV files together — slow, and had to be redone from scratch each time.

There was also a specific performance problem: a query that runs "hundreds of times a day" in production was flagged as slow and needed an actual measured fix, not a guess at what might help.

## 3. Project Scope

**In scope:** loading Pillar 1's output plus a flattened `transactions.json` (50,000 rows) into a 6-table warehouse; six checked business questions; benchmarking and rewriting the slow query with reasoned indexes; a one-page executive dashboard.

**Out of scope:** a production Postgres deployment — the optimisation is benchmarked on DuckDB and on the local `presight-postgres` container (PostgreSQL 15), with the indexing strategy written down for where it would apply in production.

**Data volumes:** 500 projects, 1,000 employees, about 1,800 salary-history rows, 50,000 transactions.

## 4. Technology Stack

| Technology | Role in the Project | Why It Was Chosen |
|---|---|---|
| DuckDB | Warehouse engine, all six queries, optimisation benchmarking | Embedded, vectorised, native `EXPLAIN ANALYZE` — a real warehouse with no server |
| SQL (window functions, CTEs) | Running totals, MoM deltas, SCD2 self-joins | One pass over a sorted partition beats a self-join, for both speed and readability |
| Pandas | Flattening and enriching transactions.json | Still pandas-scale (50K rows); reuses Pillar 1's cleaning functions instead of duplicating them |
| Matplotlib (PDF) | Executive dashboard mockup | Power BI Desktop isn't available here; the setup guide's documented alternative is a mockup PDF built from real numbers — `build_dashboard.py` reads the clean CSVs and runs `queries.sql`'s Q5 against the warehouse |

## 5. High-Level Architecture & Data Flow

```mermaid
flowchart TD
    subgraph P1out["From Pillar 1"]
        PC[("projects_clean.csv")]
        EC[("employees_clean.csv")]
    end
    HIST["employees_salary_history.csv"]

    TXN["transactions.json (50,000 rows)"] --> Load["load_transactions()"]
    Load --> Enrich["enrich_transactions()\n+ project/employee context"]
    PC --> Enrich
    EC --> Enrich
    Enrich --> TC[("transactions_clean.csv")]

    subgraph WH["DuckDB warehouse (data_model.sql, pure SQL)"]
        DateDim["dim_date\n(generated, 11,323 rows)"]
        ProjDim["dim_project"]
        EmpDim["dim_employee (SCD2)\nwindow-function build"]
        VendDim["dim_vendor"]
        Bridge["bridge_employee_project"]
        Fact["fact_transactions\npoint-in-time employee resolution"]
    end

    PC --> ProjDim
    EC --> EmpDim
    HIST --> EmpDim
    TC --> Fact
    TC --> VendDim
    ProjDim --> Bridge
    EmpDim --> Bridge
    ProjDim --> Fact
    VendDim --> Fact
    DateDim --> Fact
    EmpDim -. "valid_from/valid_to BETWEEN" .-> Fact

    Fact --> Q["Q1-Q6 business questions"]
    Q --> Dash["Executive dashboard mockup (PDF)"]
    Fact --> Opt["Query optimisation:\nbenchmark -> rewrite -> index -> re-benchmark"]
```

The key design choice: `fact_transactions` joins `dim_employee` on a **point-in-time** match (`transaction_date BETWEEN valid_from AND valid_to`), not a simple FK. A 2022 transaction resolves to whoever was valid in 2022, not today's row — the whole reason `dim_employee` is SCD2.

## 6. Key Engineering Decisions

**Point-in-time resolution for `employee_key`.** A naive join resolves every transaction to whoever that employee is *today* — wrong if they've since been promoted. `BETWEEN valid_from AND valid_to` gets it right by construction, and it's the clearest illustration of why SCD2 exists here.

**Window functions over self-joins (Q5).** `SUM() OVER (PARTITION BY ... ORDER BY ...)` computes the running total in one pass; a self-join re-scans per row. Faster, and the intent is stated directly instead of buried in a join condition.

**Reported the honest optimisation result, not a fabricated one.** The brief expected 10x+. On DuckDB at this scale, both queries ran in ~9-10ms (best-of-5: 9.60ms vs 8.63ms), because DuckDB's optimiser already rewrites the comma-join and the "correlated" subquery turned out not to be correlated at all. On PostgreSQL the indexes give a real 2.04x (27.42ms → 13.43ms) — still not 10x at 50K rows. The write-up says what happened and why, plus where it'd actually matter (production Postgres, 10-50M rows) — that answer survives a follow-up question; a fabricated number doesn't.

## 7. Challenges & Fixes

**Checking that joins don't quietly duplicate rows.** If a join accidentally matches more than one row on the other side, it silently duplicates financial rows with no error message. Added a row-count check after each join in `enrich_transactions()`, so if this ever happens the pipeline fails loudly instead of quietly shipping a wrong dashboard.

**Two business questions that genuinely return no rows.** Q2 and Q3 came back empty. Before assuming this was a bug, checked the raw numbers directly — the highest active-project count for any one manager is 3, and the highest vendor spend share is about 4.4%, both below what the queries are checking for. The queries are correct; the condition just doesn't occur in this data. This is written down rather than loosening the thresholds just to force some output.

**Keeping the dashboard's numbers identical to the SQL answers.** A dashboard built from its own separately-computed extract can quietly drift from the business-question results. `build_dashboard.py` reads the same `projects_clean.csv`/`transactions_clean.csv` the warehouse loads, and its monthly trend chart runs `queries.sql`'s own Q5 against the warehouse rather than re-implementing it — so the chart and Q5 can't disagree.

## 8. Data Quality, Reliability, Security & Performance

`amount` nulls (1.5%) stay as true `NaN`; the derived `amount_aed` used in aggregates defaults to 0.0 so a `SUM()` doesn't break. `approved_by` nulls (4.9%) become `is_approved = False`, no fabricated approver.

The warehouse load surfaced a real finding: 25 transactions have an approver whose `hire_date` is *after* the transaction date. The point-in-time join correctly leaves `employee_key` null for those instead of misattributing them.

Performance: full 50,000-row ETL in 0.84s against a 30s target.

## 9. Outcome & Business Value

Finance and Operations get six reliable, repeatable answers instead of manual spreadsheet work, backed by a warehouse that threads historical employee context through every transaction. The dashboard gives a single executive view built from real numbers. And the optimisation write-up shows the actual skill needed here — reading and reasoning about a real execution plan, not just running a benchmark.
