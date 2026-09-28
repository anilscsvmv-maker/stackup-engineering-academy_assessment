-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Solution File: query_optimization.sql
-- Pillar: SQL & Data Visualization (Task 2.3) — Query optimisation
-- Author: Anil Vaikuntam | Engine: DuckDB
-- =============================================================================
--
-- PREREQUISITES
-- -------------
-- Uses plain employees/projects/transactions tables, not data_model.sql's
-- star schema. Needs employees_clean.csv, projects_clean.csv,
-- transactions_clean.csv on disk first — the setup block below loads them.
--
-- HOW TO USE
-- ----------
-- Substitute __RESULTS__ with the real outputs path and run in any SQL
-- client. (run_optimization.py benchmarks the PostgreSQL version,
-- query_optimization.sql — not this file.)
--
-- Running the setup block below as-is (with the literal __RESULTS__
-- placeholder) fails with: IO Error: No files found that match the
-- pattern "__RESULTS__/employees_clean.csv" — it's not a real path.
-- On this machine, __RESULTS__ resolves to:
--   C:/Users/anil.vaikuntam/OneDrive - G42/Documents/Stack Up Enginerring/stackup-engineering-academy_assessment/outputs/results/anil_vaikuntam/02_sql_and_viz
-- so to run manually in a SQL client, replace the setup block below with:
--   CREATE OR REPLACE TABLE employees AS SELECT * FROM read_csv_auto('C:/Users/anil.vaikuntam/OneDrive - G42/Documents/Stack Up Enginerring/stackup-engineering-academy_assessment/outputs/results/anil_vaikuntam/02_sql_and_viz/employees_clean.csv');
--   CREATE OR REPLACE TABLE projects AS SELECT * FROM read_csv_auto('C:/Users/anil.vaikuntam/OneDrive - G42/Documents/Stack Up Enginerring/stackup-engineering-academy_assessment/outputs/results/anil_vaikuntam/02_sql_and_viz/projects_clean.csv');
--   CREATE OR REPLACE TABLE transactions AS SELECT * FROM read_csv_auto('C:/Users/anil.vaikuntam/OneDrive - G42/Documents/Stack Up Enginerring/stackup-engineering-academy_assessment/outputs/results/anil_vaikuntam/02_sql_and_viz/transactions_clean.csv');
-- (forward slashes even on Windows — DuckDB expects them.)
--
-- RESULT SUMMARY: at this scale (1,000 employees / 500 projects / 50,000
-- transactions), original and rewritten both run ~9-10ms on DuckDB 1.5.5
-- (best-of-5: 9.60ms vs 8.63ms, 923 rows each) — no 10x+ speedup here.
-- See 4a for why, and 4c for where it'd matter.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Setup — plain unindexed tables matching the starter query's table names
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE employees AS SELECT * FROM read_csv_auto('__RESULTS__/employees_clean.csv');
CREATE OR REPLACE TABLE projects AS SELECT * FROM read_csv_auto('__RESULTS__/projects_clean.csv');
CREATE OR REPLACE TABLE transactions AS SELECT * FROM read_csv_auto('__RESULTS__/transactions_clean.csv');


-- ---------------------------------------------------------------------------
-- ORIGINAL QUERY (unmodified from the starter file)
-- ---------------------------------------------------------------------------
SELECT
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    t.transaction_date
FROM employees e, projects p, transactions t
WHERE e.employee_id = p.project_manager_id
AND   p.project_id  = t.project_id
AND   p.status NOT IN ('Completed', 'On Hold')
AND   t.payment_status = 'Pending'
AND   t.amount > (
        SELECT AVG(amount)
        FROM transactions
        WHERE payment_status = 'Pending'
      )
ORDER BY e.department, t.amount DESC;


-- ---------------------------------------------------------------------------
-- 4a) EXPLAIN ANALYZE — original query, real captured output
-- ---------------------------------------------------------------------------
-- Total Time: 0.0135s (13.5ms, single profiled run) -> 923 rows returned
--   ORDER_BY (department ASC, amount DESC)              0.00s   923 rows
--     HASH_JOIN employee_id = project_manager_id         0.00s   923 rows
--       TABLE_SCAN employees                              0.00s   147 rows
--         Dynamic Filters: employee_id BETWEEN 'EMP0002' AND 'EMP0991'
--       HASH_JOIN project_id = project_id                  0.00s   923 rows
--         TABLE_SCAN projects                               0.00s   239 rows
--           Filters: status NOT IN ('Completed','On Hold')
--         NESTED_LOOP_JOIN amount > SUBQUERY                 0.00s 2,127 rows
--           TABLE_SCAN transactions                           0.00s 2,127 rows
--             Filters: payment_status='Pending'
--             Dynamic Filters: amount > 64552.30  (subquery result, already
--                                                   resolved to a constant)
--           UNGROUPED_AGGREGATE avg(amount)                   0.00s     1 row
--             TABLE_SCAN transactions (payment_status='Pending')  8,927 rows
--
-- Bottleneck analysis:
--   1. Join bottleneck? None — every join is 0.00s. The 13.5ms is scan
--      I/O on transactions, not join cost.
--   2. Correlated subquery re-run per row? No — AVG(amount) doesn't
--      reference the outer query, so DuckDB evaluates it once and folds
--      it into a literal filter (a single UNGROUPED_AGGREGATE node).
--   3. Missing indexes? DuckDB uses zone maps + dynamic filters instead
--      of B-tree indexes here, so no — 4c covers a Postgres deployment.
--   4. Implicit FROM A,B,C is still risky (a dropped WHERE silently
--      becomes a cartesian product), even though DuckDB's optimizer
--      already produces the same hash-join plan. Rewritten in 4b anyway.


-- ---------------------------------------------------------------------------
-- 4b) REWRITTEN QUERY
-- ---------------------------------------------------------------------------
-- 4 changes (none move the needle on DuckDB — see 4d — but they're the
-- right shape for a row-oriented production database):
--   1. Implicit FROM A,B,C -> explicit JOIN...ON
--   2. Subquery -> CTE
--   3. Status filter pushed onto JOIN...ON (early predicate pushdown)
--   4. No SELECT * — every column named
WITH pending_avg AS (
    SELECT AVG(amount) AS avg_pending_amount
    FROM transactions
    WHERE payment_status = 'Pending'
)
SELECT
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    t.transaction_date
FROM transactions t
JOIN projects p
    ON p.project_id = t.project_id
    AND p.status NOT IN ('Completed', 'On Hold')
JOIN employees e
    ON e.employee_id = p.project_manager_id
CROSS JOIN pending_avg
WHERE t.payment_status = 'Pending'
  AND t.amount > pending_avg.avg_pending_amount
ORDER BY e.department, t.amount DESC;


-- ---------------------------------------------------------------------------
-- 4c) Indexes — for a production PostgreSQL deployment
-- ---------------------------------------------------------------------------
-- DuckDB doesn't need these (see 4a.3), but the task frames this as a
-- query run "hundreds of times a day" in production — these target that.

-- Speeds up the manager->employee join + status filter. status listed
-- second — it's the lower-selectivity predicate (4 values).
CREATE INDEX idx_projects_manager_status ON projects (project_manager_id, status);

-- Speeds up the project_id join + payment_status filter. payment_status
-- first, so Postgres can seek the ~18% Pending rows before the join.
CREATE INDEX idx_transactions_status_project ON transactions (payment_status, project_id);

-- employee_id is already this table's PK in a real deployment — no new
-- index needed there.

-- Trade-off: write overhead on INSERT/UPDATE for read speed on a
-- read-heavy, high-frequency query — worth it here. Wouldn't index a
-- low-query-value column like `notes`.


-- ---------------------------------------------------------------------------
-- 4d) Benchmark — rewritten query, real captured output
-- ---------------------------------------------------------------------------
-- Best-of-5 (after 3 warm-up runs), 923 rows each:
--   original            9.60ms  (runs 9.60-15.70ms)
--   rewritten           8.63ms  (runs 8.63-15.59ms) — 1.11x, within noise
--   rewritten + 4c idx 15.96ms  (runs 15.96-19.86ms) — no gain (0.60x)
-- Same physical plan as 4a — DuckDB's optimizer already found it for the
-- original syntax. The indexes don't help: the bottleneck is scan I/O,
-- not joins or missing indexes.
--
-- Where this WOULD show 10x+: production volume (10-50M rows, where the
-- 4c indexes start mattering), or an engine that re-evaluates a
-- correlated subquery per row — 4b's CTE stays fast either way.
--
-- This got tested for real, not just reasoned about: see
-- query_optimization.sql, which runs the same original/rewritten/
-- indexed queries against actual PostgreSQL (the presight-postgres
-- container already in docker-compose.yml). There, the 4c indexes DO show
-- a real ~2x speedup (27.42ms -> 13.43ms, best-of-5, via
-- run_optimization.py) — because Postgres is a row-store and DuckDB isn't.
