-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Solution File: export_dashboard_data.sql
-- Pillar: SQL & Data Visualization (Task 2.4) — Power BI data source extracts
-- Author: Anil Vaikuntam | Engine: DuckDB
-- =============================================================================
--
-- PREREQUISITES
-- -------------
-- Run against the fully loaded warehouse (data_model.sql Sections 1+2 must
-- already be populated) — same prerequisite as queries.sql.
--
-- HOW TO USE
-- ----------
-- Run standalone in any SQL client (DuckDB CLI, SQLTools, etc.) against
-- outputs/presight_warehouse.duckdb. The target folder
-- (outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_data/) must already
-- exist — COPY TO writes files, it doesn't create directories.
--
-- WHY THESE AREN'T JUST Q1/Q3 FROM queries.sql, VERBATIM
-- --------------------------------------------------------
-- Task 2.4 names "Q1 results" / "Q3 results" as the data source for two
-- visuals, but both those queries are answers to Task 2.1's specific
-- business questions, not dashboard-ready extracts:
--   - Q1 filters to departments over 90% budget utilisation — on this data
--     that's a single row (Legal). A bar chart titled "actual spend vs
--     budget by department" needs every department to be a meaningful
--     comparison, so this file re-aggregates the same budget/actual_cost
--     logic Q1 uses, without the >90% filter.
--   - Q3 flags vendors whose spend share exceeds a 5%/10% risk threshold —
--     it isn't a top-5 ranking. The donut chart spec explicitly asks for
--     "top 5 + Other", which needs the vendors ranked by spend directly.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- kpi_cards — single-row summary: total budget, total actual spend,
-- % over-budget projects, total transactions
-- ---------------------------------------------------------------------------
COPY (
    SELECT
        SUM(budget) AS total_budget,
        SUM(actual_cost) AS total_actual_spend,
        ROUND(100.0 * SUM(CASE WHEN is_over_budget THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_over_budget_projects,
        (SELECT COUNT(*) FROM fact_transactions) AS total_transactions
    FROM dim_project
) TO 'outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_data/kpi_cards.csv' (HEADER, DELIMITER ',');


-- ---------------------------------------------------------------------------
-- department_budget_vs_actual — bar chart source. All departments (not just
-- Q1's >90%-utilisation subset — see note above), same aggregation logic.
-- ---------------------------------------------------------------------------
COPY (
    SELECT
        department,
        SUM(budget) AS total_budget,
        SUM(actual_cost) AS total_actual_cost,
        ROUND(100.0 * SUM(actual_cost) / NULLIF(SUM(budget), 0), 2) AS spend_percentage,
        SUM(actual_cost) > SUM(budget) AS over_budget
    FROM dim_project
    GROUP BY department
    ORDER BY spend_percentage DESC
) TO 'outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_data/department_budget_vs_actual.csv' (HEADER, DELIMITER ',');


-- ---------------------------------------------------------------------------
-- monthly_spend_by_category — line chart source. Same as queries.sql's Q5,
-- unfiltered by construction, so reused verbatim.
-- ---------------------------------------------------------------------------
COPY (
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
    ORDER BY category, year_month ASC
) TO 'outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_data/monthly_spend_by_category.csv' (HEADER, DELIMITER ',');


-- ---------------------------------------------------------------------------
-- top10_budget_variance — table source. Largest overspend first
-- (budget_variance = actual_cost - budget, so DESC = worst overrun).
-- Carries region/status/priority so it can also back a slicer.
-- ---------------------------------------------------------------------------
COPY (
    SELECT
        project_id, project_name, department, region, status, status_category,
        priority, budget, actual_cost, budget_variance, is_over_budget, risk_level
    FROM dim_project
    ORDER BY budget_variance DESC
    LIMIT 10
) TO 'outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_data/top10_budget_variance.csv' (HEADER, DELIMITER ',');


-- ---------------------------------------------------------------------------
-- vendor_concentration_top5_other — donut chart source. Top 5 vendors by
-- spend, remainder collapsed into one "Other" row.
-- ---------------------------------------------------------------------------
COPY (
    WITH vendor_spend AS (
        SELECT dv.vendor_name, SUM(ft.amount) AS total_spend
        FROM fact_transactions ft
        JOIN dim_vendor dv ON dv.vendor_key = ft.vendor_key
        GROUP BY dv.vendor_name
    ),
    ranked AS (
        SELECT vendor_name, total_spend, ROW_NUMBER() OVER (ORDER BY total_spend DESC) AS rn
        FROM vendor_spend
    )
    SELECT vendor_name, total_spend FROM ranked WHERE rn <= 5
    UNION ALL
    SELECT 'Other' AS vendor_name, SUM(total_spend) AS total_spend FROM ranked WHERE rn > 5
    ORDER BY total_spend DESC
) TO 'outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_data/vendor_concentration_top5_other.csv' (HEADER, DELIMITER ',');


-- ---------------------------------------------------------------------------
-- all_projects — every project with region/status/priority/year, so
-- slicers can filter across the whole dashboard, not just the top-10 table.
-- project_year is derived from start_date — the task doesn't specify which
-- date "year" means, and start_date is the one every project has.
-- ---------------------------------------------------------------------------
COPY (
    SELECT
        project_id, project_name, department, region, status, status_category,
        priority, EXTRACT(YEAR FROM start_date) AS project_year,
        budget, actual_cost, budget_variance, is_over_budget, risk_level
    FROM dim_project
    ORDER BY project_id
) TO 'outputs/results/anil_vaikuntam/02_sql_and_viz/dashboard_data/all_projects.csv' (HEADER, DELIMITER ',');
