-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Solution File: data_model.sql
-- Pillar: Foundations (Task 1.2) — star schema DDL, SCD2 dim_employee build,
--         and the staging load that Pillar 2's queries run against
-- Author: Anil Vaikuntam | Engine: DuckDB
-- =============================================================================
--
-- SCENARIO
-- --------
-- Presight runs a project management platform. This file designs and loads
-- the analytics warehouse that powers dashboards for Finance, Operations,
-- and Executive leadership. The business questions and query-optimisation
-- exercise that query this warehouse live in the sibling 02_sql_and_viz/
-- files: queries.sql (Task 2.1) and query_optimization.sql (Task 2.3).
--
-- PREREQUISITES
-- -------------
-- Run everything from the repo root (relative paths below assume it) and
-- have Pillar 1's Task 1.1/1.3 outputs on disk first — Section 1 reads
-- employees_clean.csv, Section 2 reads projects_clean.csv + transactions_
-- clean.csv. Full run order: see HOW_TO_RUN.md.
--
-- HOW TO USE
-- ----------
-- Section 1 has no path placeholders — run it directly in any SQL client
-- (DuckDB CLI, SQLTools against outputs/presight_warehouse.duckdb, etc.).
-- If the warehouse file already has these tables, delete
-- outputs/presight_warehouse.duckdb first for a clean rebuild (CREATE TABLE
-- fails on a table that already exists).
-- Section 2 (load) still uses an __OUTPUTS__ path placeholder, substituted
-- and run via:
--   solutions/submissions/anil_vaikuntam/02_sql_and_viz/run_queries.py
-- Then, against the loaded warehouse, see queries.sql and
-- query_optimization.sql in 02_sql_and_viz/.
-- =============================================================================
-- ===========================================================================
-- SECTION 1 — TASK 1.2: Design the data model (Star Schema)
-- ===========================================================================
--
-- Classic Kimball star schema: one fact table (fact_transactions) at the
-- centre, surrounded by conformed dimensions. Every dimension uses a
-- surrogate INTEGER key (*_key) as its primary key and keeps the original
-- source-system identifier (project_id, employee_id, ...) as a natural key.
-- Surrogates over natural keys everywhere, for three reasons:
--   1. dim_employee is SCD2 — employee_id legitimately repeats across
--      multiple rows over time, so it can't be a PK on its own.
--   2. Integer surrogate keys join and index faster than string natural keys.
--   3. Natural keys can be reused/retired by source systems; surrogates never
--      change once assigned, so historical fact rows stay stable.
-- ---------------------------------------------------------------------------
-- dim_date
-- ---------------------------------------------------------------------------
-- Design decision: a full calendar dimension, not a raw DATE column on the
-- fact table, so every downstream query groups/filters by year, quarter,
-- weekday etc. via a cheap integer join instead of re-deriving those with
-- EXTRACT()/date-math in every query. date_key is YYYYMMDD as an INTEGER
-- (e.g. 20250115) rather than the DATE itself — it sorts naturally, joins
-- as a 4-byte int instead of an 8-byte date/string, and is what most BI
-- tools expect for a date dimension's surrogate key.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    -- surrogate, formatted YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    -- natural key
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR NOT NULL,
    week INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    -- 1=Monday ... 7=Sunday
    day_name VARCHAR NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
-- ---------------------------------------------------------------------------
-- dim_project
-- ---------------------------------------------------------------------------
-- Design decision: this is a Type 1 (overwrite) dimension — we only need
-- the current state of a project, not its history, so there's no valid_from/
-- valid_to here the way there is on dim_employee. The Task 1.1 derived
-- columns (budget_variance, is_over_budget, budget_utilisation_pct,
-- risk_level, status_category) are stored on the dimension rather than
-- recomputed in every query — they're deterministic functions of budget/
-- actual_cost/status/priority, so materialising them once here means Q1 and
-- the dashboard don't each need their own copy of that business logic.
-- project_manager_id is kept as a plain natural-key attribute, not an FK to
-- dim_employee: since dim_employee is versioned, "who manages this project"
-- is resolved at query time via dim_employee.is_current (see fact_transactions
-- below and Q2), not frozen to whichever employee version existed at load time.
CREATE TABLE IF NOT EXISTS dim_project (
    project_key INTEGER PRIMARY KEY,
    project_id VARCHAR NOT NULL UNIQUE,
    -- natural key
    project_name VARCHAR NOT NULL,
    department VARCHAR,
    status VARCHAR,
    status_category VARCHAR,
    start_date DATE,
    end_date DATE,
    budget DOUBLE,
    actual_cost DOUBLE,
    budget_variance DOUBLE,
    is_over_budget BOOLEAN,
    duration_days INTEGER,
    budget_utilisation_pct DOUBLE,
    risk_level VARCHAR,
    project_manager_id VARCHAR,
    -- resolved via dim_employee at query time
    priority VARCHAR,
    region VARCHAR
);
-- ---------------------------------------------------------------------------
-- dim_employee — SCD Type 2
-- ---------------------------------------------------------------------------
-- Design decision: employees_salary_history.csv proves salary/role/level
-- change over time (up to 5 versions for the most-changed employees), and
-- Q6 plus fact_transactions' approver resolution both need point-in-time
-- correctness — "what was true about this person when this happened", not
-- just what's true today. A Type 1 dimension would silently destroy that
-- history on every reload. Population logic (staging tables, window
-- functions, validation) is below, after all six DDL statements.
CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key INTEGER PRIMARY KEY,
    -- surrogate, unique PER VERSION
    employee_id VARCHAR NOT NULL,
    -- natural key, repeats across versions
    full_name VARCHAR,
    email VARCHAR,
    department VARCHAR,
    role VARCHAR,
    level VARCHAR,
    salary DOUBLE,
    hire_date DATE,
    manager_id VARCHAR,
    region VARCHAR,
    status VARCHAR,
    years_experience DOUBLE,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    -- sentinel 9999-12-31 for current
    is_current BOOLEAN NOT NULL,
    change_reason VARCHAR
);
-- ---------------------------------------------------------------------------
-- dim_vendor
-- ---------------------------------------------------------------------------
-- Design decision: vendors only appear inside transactions.json (vendor_id +
-- vendor_name per line), so this dimension is derived by de-duplicating
-- those two fields rather than sourced from a standalone vendor master.
-- Type 1, not Type 2 — vendor names aren't expected to change in a way worth
-- tracking historically at this assessment's scope, and Q3 (concentration
-- risk) only needs current vendor identity, not a version history of it.
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key INTEGER PRIMARY KEY,
    vendor_id VARCHAR NOT NULL UNIQUE,
    -- natural key
    vendor_name VARCHAR NOT NULL
);
-- ---------------------------------------------------------------------------
-- bridge_employee_project
-- ---------------------------------------------------------------------------
-- Design decision: an employee can work across multiple projects, and a
-- project can have multiple associated employees (a manager, and separately
-- whoever approves its transactions) — a many-to-many that a star schema
-- can't express as a plain FK on either dimension. project_role records
-- *why* the pair is linked (e.g. 'Manager', sourced from projects.csv) so
-- the same bridge table can later carry other relationship types (e.g.
-- 'Approver') without a schema change.
CREATE TABLE IF NOT EXISTS bridge_employee_project (
    bridge_key INTEGER PRIMARY KEY,
    employee_key INTEGER NOT NULL REFERENCES dim_employee(employee_key),
    project_key INTEGER NOT NULL REFERENCES dim_project(project_key),
    project_role VARCHAR -- e.g. 'Manager', 'Team Member'
);
-- ---------------------------------------------------------------------------
-- fact_transactions
-- ---------------------------------------------------------------------------
-- Design decision: grain is one row per transaction (transaction_id) — the
-- most granular event available, so nothing downstream needs a coarser
-- pre-aggregation. employee_key resolves to the SCD2 version of the
-- approver that was CURRENT ON THE TRANSACTION DATE (see the point-in-time
-- join in Section 2), not whoever holds that employee_id today — this is
-- the entire reason dim_employee needed to be SCD2 rather than Type 1.
-- amount/currency/category/payment_status/invoice_ref/notes stay as
-- degenerate fact attributes rather than their own dimensions — none of
-- them have enough independent cardinality or attached metadata to justify
-- the extra join.
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_key INTEGER PRIMARY KEY,
    transaction_id VARCHAR NOT NULL UNIQUE,
    -- natural key
    project_key INTEGER REFERENCES dim_project(project_key),
    employee_key INTEGER REFERENCES dim_employee(employee_key),
    -- the approver
    vendor_key INTEGER REFERENCES dim_vendor(vendor_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    amount DOUBLE,
    currency VARCHAR,
    category VARCHAR,
    payment_status VARCHAR,
    invoice_ref VARCHAR,
    notes VARCHAR
);
-- ===========================================================================
-- Reset — makes this whole file safely re-runnable
-- ===========================================================================
-- CREATE TABLE IF NOT EXISTS above means re-running this file against a
-- warehouse that already has the schema no longer errors — but every table
-- would still be reloaded from empty, so anything already inside them needs
-- clearing first (a second run would otherwise violate the PRIMARY KEY /
-- UNIQUE constraints below, or double every row). Cleared in reverse FK
-- order — fact_transactions and bridge_employee_project reference the four
-- dim_* tables, so they're emptied first; deleting a dim_* row a child
-- table still points to would fail the FOREIGN KEY constraint otherwise.
DELETE FROM fact_transactions;
DELETE FROM bridge_employee_project;
DELETE FROM dim_date;
DELETE FROM dim_project;
DELETE FROM dim_employee;
DELETE FROM dim_vendor;
-- ===========================================================================
-- Populate dim_employee (SCD Type 2)
-- ===========================================================================
-- Source: outputs/employees_clean.csv (Task 1.3 output — NOT the raw
-- datasets/employees.csv) + datasets/employees_salary_history.csv.
--
-- DESIGN DECISIONS:
--
-- 1. Only role/level/salary are versioned (Type 2). Everything else
--    (name, email, dept, etc.) carries forward from employees_clean.csv —
--    the history file has nothing else to version against.
--
-- 2. 594 of 1,000 employees have salary history (2-5 versions each). Each
--    past row becomes a closed version via LEAD(effective_date); the
--    CURRENT version's role/level/salary always comes from
--    employees_clean.csv, not the last history row — it's the documented
--    "current state" source of truth. Two employees (EMP0356, EMP0900)
--    show a pre-existing gap between their last history row and
--    employees_clean.csv — a source-data discrepancy, not a build bug.
--
-- 3. The remaining 406 employees have no history: one row, valid_from =
--    hire_date, valid_to = 9999-12-31, is_current = TRUE.
--
-- 4. 8 employees have neither salary history nor a usable hire_date
--    (nulled by Task 1.3 as unrecoverable) — they get a 1900-01-01
--    sentinel valid_from instead of NULL, so Q3/Q5's date math doesn't break.
--
-- 5. The final INSERT joins tmp_* to stg_employees ONCE (not a subquery
--    per column) for the carried-forward columns — faster, shorter to read.
--
-- 6. Same-day duplicates: EMP0084 had two salary changes on 2025-03-02.
--    valid_to is computed as the next version's valid_from - 1 day, so two
--    same-day versions produce an inverted interval for the earlier one —
--    caught by Q5's gap check, not Q3's overlap check. Fixed by collapsing
--    same-day rows to their terminal state first (previous_salary =
--    new_salary chain).
-- ===========================================================================
CREATE OR REPLACE TABLE stg_employees AS
SELECT *
FROM read_csv_auto(
        'outputs/results/anil_vaikuntam/01_foundations/employees_clean.csv'
    );
CREATE OR REPLACE TABLE stg_salary_history AS
SELECT *
FROM read_csv_auto('datasets/employees_salary_history.csv');
-- ---------------------------------------------------------------------------
-- Same-day dedup: EMP0084 has two history rows dated 2025-03-02 (Annual
-- Raise then Promotion). Two versions sharing one valid_from would give
-- the earlier one an inverted range (valid_to before valid_from) — missed
-- by Q3's overlap check, caught by Q5's gap check. Fix: keep only the
-- terminal state per day. A row is dropped when another same-day row's
-- previous_salary matches its new_salary (15217->16314->23354 for
-- EMP0084). Employees without same-day ties are untouched.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_salary_history_dedup AS
SELECT h.*
FROM stg_salary_history h
WHERE NOT EXISTS (
        SELECT 1
        FROM stg_salary_history h2
        WHERE h2.employee_id = h.employee_id
            AND h2.effective_date = h.effective_date
            AND h2.previous_salary = h.new_salary
    );


-- ---------------------------------------------------------------------------
-- Closed (historical) versions: every history row except each employee's
-- most recent. valid_to is chained from the NEXT row's effective_date via
-- LEAD(), so consecutive versions are gapless and non-overlapping.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp_dim_employee_hist AS WITH ranked AS (
        SELECT h.*,
            ROW_NUMBER() OVER (
                PARTITION BY employee_id
                ORDER BY effective_date
            ) AS rn,
            COUNT(*) OVER (PARTITION BY employee_id) AS total_versions,
            LEAD(effective_date) OVER (
                PARTITION BY employee_id
                ORDER BY effective_date
            ) AS next_effective_date
        FROM stg_salary_history_dedup h
    )
SELECT employee_id,
    new_role AS role,
    new_level AS level,
    new_salary AS salary,
    change_reason,
    effective_date AS valid_from,
    next_effective_date - INTERVAL 1 DAY AS valid_to,
    FALSE AS is_current
FROM ranked
WHERE rn < total_versions;
-- ---------------------------------------------------------------------------
-- Current version for employees WITH history — role/level/salary sourced
-- from employees_clean.csv, not the last history row (see design decision 2).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp_dim_employee_current_with_hist AS
SELECT e.employee_id,
    e.role,
    e.level,
    e.salary,
    NULL AS change_reason,
    lh.last_effective_date AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current
FROM stg_employees e
    JOIN (
        SELECT employee_id,
            MAX(effective_date) AS last_effective_date
        FROM stg_salary_history
        GROUP BY employee_id
    ) lh ON lh.employee_id = e.employee_id;
-- ---------------------------------------------------------------------------
-- Single current version for employees WITHOUT history — 1900-01-01
-- sentinel for the 8 employees whose hire_date was also nulled (design
-- decision 4).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE tmp_dim_employee_no_hist AS
SELECT e.employee_id,
    e.role,
    e.level,
    e.salary,
    NULL AS change_reason,
    COALESCE(e.hire_date, DATE '1900-01-01') AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current
FROM stg_employees e
WHERE e.employee_id NOT IN (
        SELECT DISTINCT employee_id
        FROM stg_salary_history
    );
-- ---------------------------------------------------------------------------
-- Final assembly: union the three tmp tables, join once to stg_employees
-- for the carried-forward columns, assign the surrogate key.
-- ---------------------------------------------------------------------------
INSERT INTO dim_employee
SELECT ROW_NUMBER() OVER (
        ORDER BY t.employee_id,
            t.valid_from
    ) AS employee_key,
    t.employee_id,
    e.full_name,
    e.email,
    e.department,
    t.role,
    t.level,
    t.salary,
    e.hire_date,
    e.manager_id,
    e.region,
    e.status,
    e.years_experience,
    t.valid_from,
    t.valid_to,
    t.is_current,
    t.change_reason
FROM (
        SELECT *
        FROM tmp_dim_employee_hist
        UNION ALL
        SELECT *
        FROM tmp_dim_employee_current_with_hist
        UNION ALL
        SELECT *
        FROM tmp_dim_employee_no_hist
    ) t
    JOIN stg_employees e ON e.employee_id = t.employee_id;
DROP TABLE tmp_dim_employee_hist;
DROP TABLE tmp_dim_employee_current_with_hist;
DROP TABLE tmp_dim_employee_no_hist;
-- stg_employees / stg_salary_history are deliberately NOT dropped — they're
-- small (1,000 and ~1,826 rows) and useful to have on hand for Q4 below and
-- for anyone auditing the build later.
-- ===========================================================================
-- SCD2 validation queries (Task 1.2 requirement)
-- ===========================================================================
-- ---------------------------------------------------------------------------
-- Q1: No employee has more than one current record. Must return zero rows.
-- ---------------------------------------------------------------------------
SELECT employee_id,
    COUNT(*) AS current_count
FROM dim_employee
WHERE is_current = TRUE
GROUP BY employee_id
HAVING COUNT(*) > 1;
-- ---------------------------------------------------------------------------
-- Q2: Employees with the most version history. Expect employees with 2-5
-- versions (matches the ~60% of employees who appear in
-- employees_salary_history.csv).
-- ---------------------------------------------------------------------------
SELECT employee_id,
    COUNT(*) AS version_count
FROM dim_employee
GROUP BY employee_id
ORDER BY version_count DESC
LIMIT 10;
-- ---------------------------------------------------------------------------
-- Q3: Self-join to detect overlapping valid_from/valid_to periods for the
-- same employee. Must return zero rows.
-- ---------------------------------------------------------------------------
SELECT a.employee_id,
    a.employee_key AS version_a,
    b.employee_key AS version_b,
    a.valid_from AS a_from,
    a.valid_to AS a_to,
    b.valid_from AS b_from,
    b.valid_to AS b_to
FROM dim_employee a
    JOIN dim_employee b ON a.employee_id = b.employee_id
    AND a.employee_key < b.employee_key
    AND a.valid_from <= b.valid_to
    AND a.valid_to >= b.valid_from;
-- ---------------------------------------------------------------------------
-- Q4: Row-count sanity check — dim_employee's total row count should equal
-- (one row per salary-history record) + (one row per employee with no
-- history), computed independently of the INSERT logic above. Catches a
-- whole class of bug (dropped/duplicated employees) that Q1-Q3 wouldn't.
-- Counts against stg_salary_history_dedup, not stg_salary_history — one
-- employee (EMP0084) has two same-day history rows that collapse to one
-- version (see the dedup step above), so dim_employee has one fewer row
-- than raw stg_salary_history would suggest.
-- expected_rows and actual_rows must match.
-- ---------------------------------------------------------------------------
SELECT (
        SELECT COUNT(*)
        FROM dim_employee
    ) AS actual_rows,
    (
        SELECT COUNT(*)
        FROM stg_salary_history_dedup
    ) + (
        SELECT COUNT(*)
        FROM stg_employees
        WHERE employee_id NOT IN (
                SELECT DISTINCT employee_id
                FROM stg_salary_history
            )
    ) AS expected_rows;
-- ---------------------------------------------------------------------------
-- Q5: Gap check — the complement of Q3. Q3 proves versions never overlap;
-- this proves they never leave a gap either, i.e. every employee's coverage
-- is fully contiguous from their first version to 9999-12-31. For each
-- non-final version, valid_to + 1 day must equal the next version's
-- valid_from. Must return zero rows.
-- ---------------------------------------------------------------------------
WITH ordered AS (
    SELECT employee_id,
        employee_key,
        valid_to,
        LEAD(valid_from) OVER (
            PARTITION BY employee_id
            ORDER BY valid_from
        ) AS next_valid_from
    FROM dim_employee
)
SELECT employee_id,
    employee_key,
    valid_to,
    next_valid_from
FROM ordered
WHERE next_valid_from IS NOT NULL
    AND next_valid_from <> valid_to + INTERVAL 1 DAY;
-- ===========================================================================
-- SECTION 2 — Load staging data into the star schema
-- ===========================================================================
-- Loads the cleaned Pillar 1/2 outputs into the remaining five tables.
-- dim_employee is already populated (Section 1, above) before this runs.
-- ---------------------------------------------------------------------------
-- dim_date
-- ---------------------------------------------------------------------------
-- Design decision: generated with generate_series rather than hand-populated
-- or loaded from a CSV, since every column here is a pure function of the
-- date itself — there's no source data to load, only a range to materialise.
-- 2000-2030 comfortably covers every date in the four source datasets
-- (transaction dates, hire dates, project dates) with headroom either side.
INSERT INTO dim_date
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
    d AS full_date,
    year(d) AS year,
    quarter(d) AS quarter,
    month(d) AS month,
    strftime(d, '%B') AS month_name,
    week(d) AS week,
    day(d) AS day,
    isodow(d) AS day_of_week,
    strftime(d, '%A') AS day_name,
    isodow(d) IN (6, 7) AS is_weekend
FROM generate_series(
        DATE '2000-01-01',
        DATE '2030-12-31',
        INTERVAL 1 DAY
    ) AS t(d);
-- ---------------------------------------------------------------------------
-- dim_project
-- ---------------------------------------------------------------------------
-- Design decision: a straight 1:1 load from Pillar 1's projects_clean.csv —
-- no transformation needed here because Task 1.1's cleaning already produced
-- exactly the columns this dimension needs, in the shape it needs them.
-- project_key is assigned by ROW_NUMBER() ordered on the natural key, so the
-- surrogate is stable and reproducible across re-runs of this script.
INSERT INTO dim_project
SELECT ROW_NUMBER() OVER (
        ORDER BY project_id
    ) AS project_key,
    project_id,
    project_name,
    department,
    status,
    status_category,
    start_date,
    end_date,
    budget,
    actual_cost,
    budget_variance,
    is_over_budget,
    duration_days,
    budget_utilisation_pct,
    risk_level,
    project_manager_id,
    priority,
    region
FROM read_csv_auto('__OUTPUTS__/projects_clean.csv');
-- ---------------------------------------------------------------------------
-- dim_vendor
-- ---------------------------------------------------------------------------
-- Design decision: vendors aren't a separate source file, so this dimension
-- is built by de-duplicating vendor_id/vendor_name straight out of the
-- transaction data — confirmed during Pillar 1/2 profiling that every
-- vendor_id maps to exactly one vendor_name (no dirty near-duplicates to
-- reconcile), so a plain DISTINCT is sufficient.
INSERT INTO dim_vendor
SELECT ROW_NUMBER() OVER (
        ORDER BY vendor_id
    ) AS vendor_key,
    vendor_id,
    vendor_name
FROM (
        SELECT DISTINCT vendor_id,
            vendor_name
        FROM read_csv_auto('__OUTPUTS__/transactions_clean.csv')
    );
-- ---------------------------------------------------------------------------
-- bridge_employee_project
-- ---------------------------------------------------------------------------
-- Design decision: the only employee<->project edge guaranteed by the
-- source data is the project manager relationship (see dim_project's
-- comment above on the bridge table's purpose). Resolved against the
-- CURRENT dim_employee version per manager (is_current = TRUE), since a
-- bridge row here represents "who manages this project as of today", not a
-- historical snapshot — that's a deliberately different resolution rule
-- than fact_transactions' point-in-time join below.
INSERT INTO bridge_employee_project
SELECT ROW_NUMBER() OVER (
        ORDER BY dp.project_key
    ) AS bridge_key,
    de.employee_key,
    dp.project_key,
    'Manager' AS project_role
FROM dim_project dp
    JOIN dim_employee de ON de.employee_id = dp.project_manager_id
    AND de.is_current = TRUE;
-- ---------------------------------------------------------------------------
-- fact_transactions
-- ---------------------------------------------------------------------------
-- Design decision: employee_key resolves via a point-in-time join
-- (t.transaction_date BETWEEN de.valid_from AND de.valid_to) instead of a
-- plain equi-join to the current employee row — a transaction from 2022
-- must resolve to whichever dim_employee version was valid in 2022, not to
-- today's row. This is the single design choice that makes dim_employee's
-- SCD2 shape actually pay off, rather than being modelling for its own sake.
--
-- Data quality note surfaced by this join: 25 transactions have a non-null
-- approved_by whose hire_date is AFTER the transaction_date — someone
-- "approved" a transaction before they were employed, per the source data.
-- The BETWEEN join correctly leaves employee_key NULL for these rather than
-- silently matching them to a later SCD2 version, which would misrepresent
-- who was actually at the company on that date.
INSERT INTO fact_transactions
SELECT ROW_NUMBER() OVER (
        ORDER BY t.transaction_id
    ) AS transaction_key,
    t.transaction_id,
    dp.project_key,
    de.employee_key,
    dv.vendor_key,
    CAST(
        strftime(t.transaction_date, '%Y%m%d') AS INTEGER
    ) AS date_key,
    t.amount_aed AS amount,
    t.currency,
    t.category,
    t.payment_status,
    t.invoice_ref,
    t.notes
FROM read_csv_auto('__OUTPUTS__/transactions_clean.csv') t
    LEFT JOIN dim_project dp ON dp.project_id = t.project_id
    LEFT JOIN dim_vendor dv ON dv.vendor_id = t.vendor_id
    LEFT JOIN dim_employee de ON de.employee_id = t.approved_by
    AND t.transaction_date BETWEEN de.valid_from AND de.valid_to;