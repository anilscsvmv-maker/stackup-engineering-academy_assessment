-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Solution File: queries.sql
-- Pillar: SQL & Data Visualization (Task 2.1) — Six business questions
-- Author: Anil Vaikuntam | Engine: DuckDB
-- =============================================================================
--
-- PREREQUISITES
-- -------------
-- Run against the warehouse built by 01_foundations/data_model.sql — Section
-- 1 (schema + dim_employee) and Section 2 (load) must already be loaded, so
-- dim_date/dim_project/dim_employee/dim_vendor/bridge_employee_project/
-- fact_transactions are all populated.
--
-- HOW TO USE
-- ----------
-- Run standalone in any SQL client (DuckDB CLI, SQLTools, etc.) against
-- outputs/presight_warehouse.duckdb, or via:
--   solutions/submissions/anil_vaikuntam/02_sql_and_viz/run_queries.py
-- (loads Section 2, then runs every query below and prints real result rows)
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Q1 — Department budget performance

--Which departments have spent more than 90% of their total allocated budget? 
--Include departments that are over budget.

-- Aggregate budget/actual_cost off dim_project, then filter with HAVING
-- since the 90% threshold applies to the aggregated total.
-- ---------------------------------------------------------------------------
SELECT
    department,
    SUM(budget) AS total_budget,
    SUM(actual_cost) AS total_actual_cost,
    ROUND(100.0 * SUM(actual_cost) / NULLIF(SUM(budget), 0), 2) AS spend_percentage,
    SUM(actual_cost) > SUM(budget) AS over_budget
FROM dim_project
GROUP BY department
HAVING SUM(actual_cost) / NULLIF(SUM(budget), 0) > 0.90
ORDER BY spend_percentage DESC;


-- ---------------------------------------------------------------------------
-- Q2 — Project manager workload (current employee data)

--Which managers are currently overseeing more than three active projects?
-- A manager with too many active projects is a delivery risk.

-- Join dim_project to dim_employee where is_current=TRUE, restrict to
-- active projects, filter for >3 in HAVING.
-- Verified result: 0 rows on this dataset — max concurrent active projects
-- per manager is 3 (checked against the raw distribution directly).
-- ---------------------------------------------------------------------------
SELECT
    de.full_name,
    de.email,
    COUNT(*) AS active_project_count,
    SUM(dp.budget) AS combined_budget_responsibility,
    SUM(dp.actual_cost) AS combined_actual_spend
FROM dim_project dp
JOIN dim_employee de
    ON de.employee_id = dp.project_manager_id
    AND de.is_current = TRUE
WHERE dp.status_category = 'Active'
GROUP BY de.full_name, de.email
HAVING COUNT(*) > 3
ORDER BY active_project_count DESC;


-- ---------------------------------------------------------------------------
-- Q3 — Vendor concentration risk

--Identify vendors who account for more than 5% of total transaction spend. 
--With 50,000 transactions, even a 5% share represents meaningful concentration.

-- A scalar subquery computes the grand total spend once in the SELECT list;
-- it's a single value, so a subquery here reads more directly than a CTE +
-- CROSS JOIN would for the same result.
-- Verified result: 0 rows — spend is evenly distributed across all 25
-- vendors (max observed share ~4.4%).
-- ---------------------------------------------------------------------------
WITH vendor_spend AS (
    SELECT
        dv.vendor_name,
        SUM(ft.amount) AS total_spend,
        COUNT(*) AS transaction_count
    FROM fact_transactions ft
    JOIN dim_vendor dv ON dv.vendor_key = ft.vendor_key
    GROUP BY dv.vendor_name
)
SELECT
    vendor_name,
    total_spend,
    transaction_count,
    ROUND(100.0 * total_spend / (SELECT SUM(total_spend) FROM vendor_spend), 2) AS percentage_of_total_spend,
    CASE
        WHEN 100.0 * total_spend / (SELECT SUM(total_spend) FROM vendor_spend) > 10 THEN 'HIGH'
        WHEN 100.0 * total_spend / (SELECT SUM(total_spend) FROM vendor_spend) > 5  THEN 'MEDIUM'
        ELSE 'NORMAL'
    END AS risk_flag
FROM vendor_spend
WHERE 100.0 * total_spend / (SELECT SUM(total_spend) FROM vendor_spend) > 5
ORDER BY percentage_of_total_spend DESC;


-- ---------------------------------------------------------------------------
-- Q4 — Projects with open financial issues

--Find all projects with pending or disputed transactions totalling more than 50,000 AED.
 --These need finance team attention.

-- Filter fact_transactions to Pending/Disputed, aggregate per project,
-- then HAVING > 50,000 AED.
-- ---------------------------------------------------------------------------
SELECT
    dp.project_id,
    dp.project_name,
    dp.department,
    dp.status AS project_status,
    COUNT(*) AS open_transaction_count,
    SUM(ft.amount) AS open_transaction_value
FROM fact_transactions ft
JOIN dim_project dp ON dp.project_key = ft.project_key
WHERE ft.payment_status IN ('Pending', 'Disputed')
GROUP BY dp.project_id, dp.project_name, dp.department, dp.status
HAVING SUM(ft.amount) > 50000
ORDER BY open_transaction_value DESC;


-- ---------------------------------------------------------------------------
-- Q5 — Monthly spend trend with running total

--Show total transaction spend per month, per category, with a running total accumulating within each category over time. 
--This view drives the Finance dashboard.

-- SUM() OVER (PARTITION BY category ORDER BY year_month) gives the running
-- total in one pass; LAG() gives month-over-month % change.
-- ---------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        strftime(dd.full_date, '%Y-%m') AS year_month,
        ft.category,
        SUM(ft.amount) AS monthly_spend
    FROM fact_transactions ft
    JOIN dim_date dd ON dd.date_key = ft.date_key
    GROUP BY year_month, ft.category
)
SELECT
    year_month,
    category,
    monthly_spend,
    SUM(monthly_spend) OVER (
        PARTITION BY category ORDER BY year_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    ROUND(
        100.0 * (monthly_spend - LAG(monthly_spend) OVER (PARTITION BY category ORDER BY year_month))
        / NULLIF(LAG(monthly_spend) OVER (PARTITION BY category ORDER BY year_month), 0),
        2
    ) AS month_over_month_pct_change
FROM monthly
ORDER BY category, year_month ASC;


-- ---------------------------------------------------------------------------
-- Q6 — Employee compensation history analysis (SCD2 self-join)

--Using dim_employee SCD2 data, identify employees who received the largest 
--single salary increase (in absolute AED terms).

-- Self-join dim_employee to itself, matching each version to the next by
-- valid_to = valid_from - 1 day (the adjacency data_model.sql's SCD2 build
-- constructs, and its own Q5 validation independently verifies). Top 20 by
-- increase_amount.
-- ---------------------------------------------------------------------------
SELECT
    old.employee_id,
    new.full_name,
    new.valid_from AS change_date,
    old.salary AS previous_salary,
    new.salary AS new_salary,
    new.salary - old.salary AS increase_amount,
    ROUND(100.0 * (new.salary - old.salary) / NULLIF(old.salary, 0), 2) AS increase_pct
FROM dim_employee old
JOIN dim_employee new
    ON old.employee_id = new.employee_id
    AND new.valid_from = old.valid_to + INTERVAL 1 DAY
ORDER BY increase_amount DESC
LIMIT 20;
