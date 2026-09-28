-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Solution File: query_optimization.sql
-- Pillar: SQL & Data Visualization (Task 2.3) — Query optimisation
-- Author: Anil Vaikuntam | Engine: PostgreSQL 15
-- =============================================================================
--
-- SCENARIO
-- --------
-- A query used on the finance dashboard, run hundreds of times a day, is
-- slow. Diagnose it, rewrite it, and add indexes where they'd help in
-- production.
--
-- This was first measured on DuckDB, where the rewrite and indexes showed
-- no real speedup (1.11x for the rewrite, none from the indexes — see
-- query_optimization_duckdb.sql; DuckDB is columnar with zone maps, so it
-- doesn't need a B-tree index to skip rows cheaply). That result was
-- reasoned about but not left unverified: it's tested here for real
-- against PostgreSQL, a genuine row-store, to check whether the reasoning
-- actually holds on the kind of engine the production indexes are written
-- for.
--
-- HOW TO RUN
-- ----------
-- Uses the presight-postgres container already defined in docker-compose.yml
-- (docker-compose up -d postgres) — against a dedicated presight_practice
-- database, not the airflow metadata database that container also hosts.
-- COPY below is server-side, so the CSVs need to exist inside the container
-- first (run_optimization.py does all of this automatically):
--
--   docker cp outputs/results/anil_vaikuntam/01_foundations/employees_clean.csv presight-postgres:/tmp/employees_clean.csv
--   docker cp outputs/results/anil_vaikuntam/01_foundations/projects_clean.csv   presight-postgres:/tmp/projects_clean.csv
--   docker cp outputs/results/anil_vaikuntam/02_sql_and_viz/transactions_clean.csv presight-postgres:/tmp/transactions_clean.csv
--   docker exec presight-postgres psql -U presight -d postgres -c "CREATE DATABASE presight_practice;"
--   docker exec -i presight-postgres psql -U presight -d presight_practice -f - < solutions/submissions/anil_vaikuntam/02_sql_and_viz/query_optimization.sql
-- =============================================================================

\timing on

-- ---------------------------------------------------------------------------
-- Setup — plain unindexed tables matching the starter query's table names
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS employees, projects, transactions;

CREATE TABLE employees (
    employee_id             TEXT,
    full_name                TEXT,
    email                     TEXT,
    department                 TEXT,
    role                        TEXT,
    level                         TEXT,
    hire_date                      DATE,
    salary                           NUMERIC,
    manager_id                        TEXT,
    region                              TEXT,
    status                               TEXT,
    years_experience                      NUMERIC,
    salary_flagged_outlier                 BOOLEAN
);

CREATE TABLE projects (
    project_id                TEXT,
    project_name                TEXT,
    department                    TEXT,
    status                          TEXT,
    start_date                        DATE,
    end_date                            DATE,
    budget                                NUMERIC,
    actual_cost                            NUMERIC,
    project_manager_id                       TEXT,
    priority                                   TEXT,
    region                                       TEXT,
    budget_variance                                NUMERIC,
    is_over_budget                                   BOOLEAN,
    duration_days                                      NUMERIC,
    budget_utilisation_pct                               NUMERIC,
    status_category                                        TEXT,
    risk_level                                               TEXT
);

CREATE TABLE transactions (
    transaction_id              TEXT,
    project_id                    TEXT,
    vendor_id                       TEXT,
    vendor_name                       TEXT,
    category                            TEXT,
    amount                                NUMERIC,
    currency                               TEXT,
    transaction_date                         DATE,
    approved_by                                TEXT,
    payment_status                               TEXT,
    invoice_ref                                    TEXT,
    notes                                            TEXT,
    project_name                                       TEXT,
    department                                           TEXT,
    approver_full_name                                     TEXT,
    is_approved                                              BOOLEAN,
    amount_aed                                                 NUMERIC,
    transaction_year_month                                       TEXT
);

COPY employees    FROM '/tmp/employees_clean.csv'    WITH (FORMAT csv, HEADER true);
COPY projects     FROM '/tmp/projects_clean.csv'     WITH (FORMAT csv, HEADER true);
COPY transactions FROM '/tmp/transactions_clean.csv' WITH (FORMAT csv, HEADER true);

-- select *from employees

-- ---------------------------------------------------------------------------
-- ORIGINAL QUERY (unmodified from the starter file)
-- ---------------------------------------------------------------------------
EXPLAIN ANALYZE
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
-- EXPLAIN ANALYZE output — original query, real captured run (steady-state,
-- no indexes; one representative sample — see "repeated runs" note below
-- for why single captures on this query vary run to run)
-- ---------------------------------------------------------------------------
--                                                               QUERY PLAN
-- --------------------------------------------------------------------------------------------------------------------------------------
--  Sort  (cost=4253.30..4256.88 rows=1430 width=116) (actual time=24.469..24.506 rows=923 loops=1)
--    Sort Key: e.department, t.amount DESC
--    Sort Method: quicksort  Memory: 178kB
--    InitPlan 1 (returns $0)
--      ->  Aggregate  (cost=1984.43..1984.44 rows=1 width=32) (actual time=12.514..12.517 rows=1 loops=1)
--            ->  Seq Scan on transactions  (cost=0.00..1962.00 rows=8972 width=6) (actual time=0.002..11.851 rows=8927 loops=1)
--                  Filter: (payment_status = 'Pending'::text)
--                  Rows Removed by Filter: 41073
--    ->  Hash Join  (cost=61.74..2193.92 rows=1430 width=116) (actual time=12.919..23.518 rows=923 loops=1)
--          Hash Cond: (p.project_manager_id = e.employee_id)
--          ->  Hash Join  (cost=20.24..2132.75 rows=1430 width=87) (actual time=12.677..22.970 rows=923 loops=1)
--                Hash Cond: (t.project_id = p.project_id)
--                ->  Seq Scan on transactions t  (cost=0.00..2087.00 rows=2991 width=34) (actual time=12.527..22.431 rows=2127 loops=1)
--                      Filter: ((amount > $0) AND (payment_status = 'Pending'::text))
--                      Rows Removed by Filter: 47873
--                ->  Hash  (cost=17.25..17.25 rows=239 width=69) (actual time=0.128..0.130 rows=239 loops=1)
--                      Buckets: 1024  Batches: 1  Memory Usage: 33kB
--                      ->  Seq Scan on projects p  (cost=0.00..17.25 rows=239 width=69) (actual time=0.018..0.094 rows=239 loops=1)
--                            Filter: (status <> ALL ('{Completed,"On Hold"}'::text[]))
--                            Rows Removed by Filter: 261
--          ->  Hash  (cost=29.00..29.00 rows=1000 width=45) (actual time=0.235..0.236 rows=1000 loops=1)
--                Buckets: 1024  Batches: 1  Memory Usage: 86kB
--                ->  Seq Scan on employees e  (cost=0.00..29.00 rows=1000 width=45) (actual time=0.006..0.109 rows=1000 loops=1)
--  Planning Time: 0.368 ms
--  Execution Time: 24.571 ms
--
-- Summary — where this 24.6ms sample actually goes (self-time per step, approx.):
--   Step                         | Scan Type  | Rows In -> Out   | Time     | %
--   ------------------------------+-----------+------------------+----------+-----
--   AVG(amount) InitPlan          | Seq Scan  | 50,000 -> 8,927  | 11.85 ms | 48%
--   transactions filter (t)       | Seq Scan  | 50,000 -> 2,127  |  9.90 ms | 40%
--   Hash Join x2 + Sort           | (memory)  | -> 923 rows      |  2.82 ms | 12%
--   ------------------------------+-----------+------------------+----------+-----
--   TOTAL                                                         24.57 ms | 100%
--
-- Bottleneck analysis:
--   Joins used: two Hash Joins — transactions/projects on project_id, then
--   that result/employees on project_manager_id = employee_id. Both are
--   cheap (build a small in-memory hash table from projects/employees,
--   239 and 1000 rows) and are NOT the bottleneck — neither shows up as
--   expensive in "actual time".
--
--   What IS consuming the time: two separate full Seq Scans on
--   transactions (50,000 rows each), because the non-correlated AVG(amount)
--   subquery is pulled out as its own step (InitPlan 1) and evaluated
--   BEFORE the main query runs, not folded into the main scan's filter:
--     1. InitPlan 1's scan (~11.9ms this sample) — reads all 50,000 rows to
--        filter payment_status='Pending' (41,073 rows removed), just to
--        compute one number: the average.
--     2. The main Seq Scan on transactions t (~9.9ms this sample) — reads
--        all 50,000 rows AGAIN, this time filtering both
--        payment_status='Pending' AND amount > $0 (47,873 rows removed),
--        to get the 2,127 rows that actually matter.
--   Together these two full-table scans account for ~88% of this sample's
--   total — the joins, sort, and employees/projects scans are all
--   sub-millisecond by comparison. Neither scan has an index to seek
--   through the ~18% Pending rows directly; both must read and discard
--   most of the table. This is exactly what idx_transactions_status_project
--   (4c below) targets — it lets both scans become Bitmap Index Scans
--   instead of Seq Scans.


-- ---------------------------------------------------------------------------
-- REWRITTEN QUERY — before indexes
-- ---------------------------------------------------------------------------
-- 4 changes: implicit FROM A,B,C -> explicit JOIN...ON (a dropped WHERE on
-- a comma-join silently becomes a cartesian product); repeated subquery ->
-- CTE, referenced as a scalar subquery (not CROSS JOIN — nothing here
-- needs pending_avg's columns in the SELECT list, so a scalar subquery
-- expresses "compare against one precomputed value" more directly, and
-- lets Postgres plan it the same way as the original's InitPlan instead
-- of forcing an extra Nested Loop + Join Filter); status filter pushed
-- onto JOIN...ON; no SELECT *.
EXPLAIN ANALYZE
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
WHERE t.payment_status = 'Pending'
  AND t.amount > (SELECT avg_pending_amount FROM pending_avg)
ORDER BY e.department, t.amount DESC;


-- ---------------------------------------------------------------------------
-- EXPLAIN ANALYZE output — rewritten query, before indexes, real captured run
-- ---------------------------------------------------------------------------
--                                                              QUERY PLAN
-- -------------------------------------------------------------------------------------------------------------------------------------
--  Sort  (cost=4253.31..4256.89 rows=1430 width=116) (actual time=23.756..23.791 rows=923 loops=1)
--    Sort Key: e.department, t.amount DESC
--    Sort Method: quicksort  Memory: 178kB
--    InitPlan 1 (returns $0)
--      ->  Aggregate  (cost=1984.43..1984.44 rows=1 width=32) (actual time=8.605..8.607 rows=1 loops=1)
--            ->  Seq Scan on transactions  (cost=0.00..1962.00 rows=8972 width=6) (actual time=0.004..7.959 rows=8927 loops=1)
--                  Filter: (payment_status = 'Pending'::text)
--                  Rows Removed by Filter: 41073
--    ->  Hash Join  (cost=61.74..2193.92 rows=1430 width=116) (actual time=9.916..22.721 rows=923 loops=1)
--          Hash Cond: (p.project_manager_id = e.employee_id)
--          ->  Hash Join  (cost=20.24..2132.75 rows=1430 width=87) (actual time=9.130..21.650 rows=923 loops=1)
--                Hash Cond: (t.project_id = p.project_id)
--                ->  Seq Scan on transactions t  (cost=0.00..2087.00 rows=2991 width=34) (actual time=8.627..20.781 rows=2127 loops=1)
--                      Filter: ((amount > $0) AND (payment_status = 'Pending'::text))
--                      Rows Removed by Filter: 47873
--                ->  Hash  (cost=17.25..17.25 rows=239 width=69) (actual time=0.481..0.481 rows=239 loops=1)
--                      Buckets: 1024  Batches: 1  Memory Usage: 33kB
--                      ->  Seq Scan on projects p  (cost=0.00..17.25 rows=239 width=69) (actual time=0.021..0.432 rows=239 loops=1)
--                            Filter: (status <> ALL ('{Completed,"On Hold"}'::text[]))
--                            Rows Removed by Filter: 261
--          ->  Hash  (cost=29.00..29.00 rows=1000 width=45) (actual time=0.775..0.776 rows=1000 loops=1)
--                Buckets: 1024  Batches: 1  Memory Usage: 86kB
--                ->  Seq Scan on employees e  (cost=0.00..29.00 rows=1000 width=45) (actual time=0.018..0.497 rows=1000 loops=1)
--  Planning Time: 0.618 ms
--  Execution Time: 23.899 ms
--
-- Summary — where this 23.9ms sample actually goes (self-time per step, approx.):
--   Step                         | Scan Type  | Rows In -> Out   | Time     | %
--   ------------------------------+-----------+------------------+----------+-----
--   AVG(amount) InitPlan          | Seq Scan  | 50,000 -> 8,927  |  7.96 ms | 33%
--   transactions filter (t)       | Seq Scan  | 50,000 -> 2,127  | 12.15 ms | 51%
--   Hash Join x2 + Sort           | (memory)  | -> 923 rows      |  3.79 ms | 16%
--   ------------------------------+-----------+------------------+----------+-----
--   TOTAL                                                         23.90 ms | 100%
--
-- Analysis: same plan as the original, same bottleneck. Removing CROSS
-- JOIN lets Postgres fold the CTE back into an InitPlan — the identical
-- shape (InitPlan -> Hash Join x2 -> Sort) as the original comma-join
-- query. Same two Seq Scans on transactions dominate the time either way.
-- The CTE here is a readability change, not a performance one — the real
-- gain comes from the indexes below. (Best-of-5 via run_optimization.py:
-- original 27.42ms, rewritten 30.97ms — 0.89x, i.e. run-to-run noise.)


-- ---------------------------------------------------------------------------
-- INDEXES — for the production deployment this query actually runs in
-- ---------------------------------------------------------------------------
-- Speeds up the manager->employee join + status filter. status listed
-- second — it's the lower-selectivity predicate (4 values).
CREATE INDEX idx_projects_manager_status ON projects (project_manager_id, status);

-- Speeds up the project_id join + payment_status filter. payment_status
-- first, so Postgres can seek the ~18% Pending rows before the join.
CREATE INDEX idx_transactions_status_project ON transactions (payment_status, project_id);

-- employee_id is already this table's PK in a real deployment — no new
-- index needed there.

ANALYZE employees;
ANALYZE projects;
ANALYZE transactions;


-- ---------------------------------------------------------------------------
-- REWRITTEN QUERY — after indexes
-- ---------------------------------------------------------------------------
EXPLAIN ANALYZE
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
WHERE t.payment_status = 'Pending'
  AND t.amount > (SELECT avg_pending_amount FROM pending_avg)
ORDER BY e.department, t.amount DESC;


-- ---------------------------------------------------------------------------
-- EXPLAIN ANALYZE output — rewritten query, after indexes, real captured run
-- ---------------------------------------------------------------------------
--                                                                           QUERY PLAN
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Sort  (cost=3308.03..3311.54 rows=1404 width=116) (actual time=7.902..7.940 rows=923 loops=1)
--    Sort Key: e.department, t.amount DESC
--    Sort Method: quicksort  Memory: 178kB
--    InitPlan 1 (returns $0)
--      ->  Aggregate  (cost=1573.83..1573.84 rows=1 width=32) (actual time=3.195..3.197 rows=1 loops=1)
--            ->  Bitmap Heap Scan on transactions  (cost=104.61..1551.79 rows=8815 width=6) (actual time=0.449..2.265 rows=8927 loops=1)
--                  Recheck Cond: (payment_status = 'Pending'::text)
--                  Heap Blocks: exact=1337
--                  ->  Bitmap Index Scan on idx_transactions_status_project  (cost=0.00..102.40 rows=8815 width=0) (actual time=0.333..0.334 rows=8927 loops=1)
--                        Index Cond: (payment_status = 'Pending'::text)
--    ->  Hash Join  (cost=166.50..1660.78 rows=1404 width=116) (actual time=4.099..6.854 rows=923 loops=1)
--          Hash Cond: (t.project_id = p.project_id)
--          ->  Bitmap Heap Scan on transactions t  (cost=103.14..1572.36 rows=2938 width=34) (actual time=3.663..5.994 rows=2127 loops=1)
--                Recheck Cond: (payment_status = 'Pending'::text)
--                Filter: (amount > $0)
--                Rows Removed by Filter: 6800
--                Heap Blocks: exact=1337
--                ->  Bitmap Index Scan on idx_transactions_status_project  (cost=0.00..102.40 rows=8815 width=0) (actual time=0.333..0.333 rows=8927 loops=1)
--                      Index Cond: (payment_status = 'Pending'::text)
--          ->  Hash  (cost=60.38..60.38 rows=239 width=98) (actual time=0.429..0.431 rows=239 loops=1)
--                Buckets: 1024  Batches: 1  Memory Usage: 40kB
--                ->  Hash Join  (cost=20.24..60.38 rows=239 width=98) (actual time=0.135..0.372 rows=239 loops=1)
--                      Hash Cond: (e.employee_id = p.project_manager_id)
--                      ->  Seq Scan on employees e  (cost=0.00..29.00 rows=1000 width=45) (actual time=0.003..0.094 rows=1000 loops=1)
--                      ->  Hash  (cost=17.25..17.25 rows=239 width=69) (actual time=0.129..0.129 rows=239 loops=1)
--                            Buckets: 1024  Batches: 1  Memory Usage: 33kB
--                            ->  Seq Scan on projects p  (cost=0.00..17.25 rows=239 width=69) (actual time=0.005..0.086 rows=239 loops=1)
--                                  Filter: (status <> ALL ('{Completed,"On Hold"}'::text[]))
--                                  Rows Removed by Filter: 261
--  Planning Time: 0.271 ms
--  Execution Time: 8.007 ms
--
-- What improved (23.90ms -> 8.01ms) — same InitPlan/Hash Join/Sort shape
-- before and after, just cheaper leaf scans:
--   Step                | Before (Seq Scan) | After (Bitmap Index) | Note
--   --------------------+--------------------+----------------------+------------------
--   InitPlan AVG scan   | 7.96 ms            | 2.27 ms              | index seeks ~8,927 Pending rows directly
--   Join-side scan      | 12.15 ms           | 2.33 ms              | still Filters amount > $0 after the fetch (6,800 rows removed)
--   --------------------+--------------------+----------------------+------------------
-- Best-of-5 via run_optimization.py: 27.42ms original -> 13.43ms rewritten
-- + indexed (2.04x).




-- ---------------------------------------------------------------------------
-- BONUS — does indexing `amount` too help further? Tested, yes.
-- ---------------------------------------------------------------------------
-- idx_transactions_status_project (payment_status, project_id) narrows to
-- the Pending rows via the index, but still needs a post-fetch Filter for
-- amount > $0 (the average) since amount isn't in that index — Filter:
-- amount > $0, Rows Removed by Filter: 6800 in the earlier capture.
--
-- Swapping the trailing column for amount instead of project_id lets
-- Postgres push that comparison into the index seek itself:

CREATE INDEX idx_transactions_status_amount ON transactions (payment_status, amount);
ANALYZE transactions;
--
-- Real captured plan, same query as "REWRITTEN QUERY — after indexes"
-- above, with this index also present:
--                                                                           QUERY PLAN
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Sort  (cost=3195.13..3198.64 rows=1404 width=116) (actual time=11.963..12.009 rows=923 loops=1)
--    Sort Key: e.department, t.amount DESC
--    Sort Method: quicksort  Memory: 178kB
--    InitPlan 1 (returns $0)
--      ->  Aggregate  (cost=1573.72..1573.73 rows=1 width=32) (actual time=6.947..6.949 rows=1 loops=1)
--            ->  Bitmap Heap Scan on transactions  (cost=104.57..1551.69 rows=8810 width=6) (actual time=1.201..4.913 rows=8927 loops=1)
--                  Recheck Cond: (payment_status = 'Pending'::text)
--                  Heap Blocks: exact=1337
--                  ->  Bitmap Index Scan on idx_transactions_status_project  (cost=0.00..102.37 rows=8810 width=0) (actual time=1.052..1.053 rows=8927 loops=1)
--                        Index Cond: (payment_status = 'Pending'::text)
--    ->  Hash Join  (cost=141.88..1547.99 rows=1404 width=116) (actual time=8.765..10.772 rows=923 loops=1)
--          Hash Cond: (t.project_id = p.project_id)
--          ->  Bitmap Heap Scan on transactions t  (cost=78.52..1459.57 rows=2937 width=34) (actual time=7.734..9.115 rows=2127 loops=1)
--                Recheck Cond: ((payment_status = 'Pending'::text) AND (amount > $0))
--                Heap Blocks: exact=1084
--                ->  Bitmap Index Scan on idx_transactions_status_amount  (cost=0.00..77.79 rows=2937 width=0) (actual time=7.584..7.584 rows=2127 loops=1)
--                      Index Cond: ((payment_status = 'Pending'::text) AND (amount > $0))
--          ->  Hash  (cost=60.38..60.38 rows=239 width=98) (actual time=1.013..1.017 rows=239 loops=1)
--                Buckets: 1024  Batches: 1  Memory Usage: 40kB
--                ->  Hash Join  (cost=20.24..60.38 rows=239 width=98) (actual time=0.507..0.885 rows=239 loops=1)
--                      Hash Cond: (e.employee_id = p.project_manager_id)
--                      ->  Seq Scan on employees e  (cost=0.00..29.00 rows=1000 width=45) (actual time=0.011..0.173 rows=1000 loops=1)
--                      ->  Hash  (cost=17.25..17.25 rows=239 width=69) (actual time=0.489..0.491 rows=239 loops=1)
--                            Buckets: 1024  Batches: 1  Memory Usage: 33kB
--                            ->  Seq Scan on projects p  (cost=0.00..17.25 rows=239 width=69) (actual time=0.012..0.338 rows=239 loops=1)
--                                  Filter: (status <> ALL ('{Completed,"On Hold"}'::text[]))
--                                  Rows Removed by Filter: 261
--  Planning Time: 0.533 ms
--  Execution Time: 12.109 ms
--
-- What changed, and what didn't:
--   Step              | With (status,project_id) | With (status,amount)
--   ------------------+---------------------------+----------------------
--   InitPlan AVG scan  | Bitmap Index+Heap, 2.27ms | Bitmap Index+Heap on idx_transactions_status_project, 4.91ms
--   Join-side scan     | Bitmap + Filter, 2.33ms   | Bitmap on idx_transactions_status_amount, no Filter, 1.38ms
--   ------------------+---------------------------+----------------------
--   This capture total | 8.01 ms                   | 12.11 ms
--   Best-of-5 (run_optimization.py) | 13.43 ms     | 11.16 ms (1.20x)
--
--   1. The join-side scan's Filter disappears — amount > $0 is now part
--      of Index Cond, so the index returns exactly the 2,127 qualifying
--      rows directly instead of fetching 8,927 and discarding 6,800
--      afterward.
--   2. The InitPlan does NOT switch to an Index Only Scan here — the
--      planner keeps using idx_transactions_status_project for the AVG.
--      Single captures swing either way at this size (12.11ms here vs
--      8.01ms without the extra index); best-of-5 shows a modest 1.20x,
--      so the extra index is a marginal gain on this data, not a clear one.
--
-- Trade-off (why this isn't unconditionally "just do this instead"):
--   This index is narrowly useful — it helps only this exact filter shape
--   (payment_status + amount range). idx_transactions_status_project
--   (payment_status, project_id) stays valuable for other queries that
--   join on project_id. Adding this as a THIRD index on transactions
--   means every INSERT/UPDATE to that table now maintains three indexes
--   instead of two — worth it only if this query is genuinely hot enough
--   in production to justify the extra write cost on what's likely the
--   fastest-growing table in the system.
