# Pillar 1 — Assumptions

Explicit assumptions and judgment calls made while building Pillar 1, beyond what the task instructions state outright.

## Task 1.1 — `transform_projects()`

1. Null `budget`/`actual_cost` are filled with `0` **before** deriving `budget_variance`/`is_over_budget`/`budget_utilisation_pct` — deriving first would propagate `NaN` into every downstream metric for those rows.
2. `budget_utilisation_pct` is left `NaN` when `budget` is `0` (the `np.where(budget > 0, ...)` guard), not `inf` or a misleading `0%` — avoids a divide-by-zero while keeping the row.
3. Unmapped `status` values would fall through `status_category` as-is rather than being force-mapped to a guessed category — didn't come up on this dataset (all values map cleanly), but the code doesn't assume a closed set.

## Task 1.3 — `clean_employees()`

1. Blank/missing email is replaced with a placeholder (`unknown@presight.ai`), not regenerated from the employee's name — a fabricated-but-plausible email is arguably worse than an obviously-placeholder one, since it could be mistaken for real contact data downstream.
2. Unparseable or implausible (pre-1990, or clearly corrupted like `"-999"`) `hire_date` values are set to null, not guessed or defaulted to today.
3. Negative `years_experience` is treated as a genuine data error (not a valid signed value) and imputed with the employee's **level median** — chosen over leaving it null, since `years_experience` feeds no downstream join/key, so a reasonable estimate is more useful than a gap.
4. Salary values outside 1.5×/0.5× the p5–p95 band for the employee's level are **winsorised** (capped to p95), not just flagged — this is a deliberate choice to force-correct rather than leave for human review, on the reasoning that salary is used directly in Q6 (compensation trend) and the SCD2 build, so an uncorrected outlier would propagate into every downstream number derived from it.
5. "Active employee reporting to an Inactive manager" is checked but never triggers a fix on this dataset (0 rows found) — kept in as a guardrail for future data, not removed as dead code.

## Task 1.2 — `dim_employee` SCD2 build (`data_model.sql`, Section 1)

1. Only `role`/`level`/`salary` are tracked as true Type 2 (versioned) attributes. Every other column (`full_name`, `department`, `manager_id`, etc.) carries forward from the current `employees_clean.csv` snapshot on every version, because `employees_salary_history.csv` has no historical values for those columns — there's nothing to version them against. This is a documented scope limit, not an oversight.
2. Where the history file's last row disagrees with `employees_clean.csv`'s current values (`EMP0356`, `EMP0900` — large salary gaps, source data discrepancies), `employees_clean.csv` wins for the `is_current = TRUE` row — it's documented as the authoritative "current state" export. (`EMP0084` looks like a third conflict if its two same-day history rows aren't collapsed first — with the same-day dedup in place its current row agrees with its history; see `VALIDATION_EVIDENCE.md`.)
3. Employees with no salary history get a single row: `valid_from = hire_date`, `valid_to = 9999-12-31`, `is_current = TRUE`.
4. 8 employees have **both** no salary history **and** a `hire_date` that Task 1.3 nulled out as unrecoverable — these get an explicit `1900-01-01` sentinel `valid_from` rather than `NULL`, since `NULL` would violate the `NOT NULL` constraint and break the overlap/gap-check validation queries' interval arithmetic.
5. `employee_key` is a `ROW_NUMBER()`-assigned surrogate ordered by `(employee_id, valid_from)` — stable and reproducible across re-runs against the same source data.

## General

1. All detection/derivation logic is vectorised (boolean masks, `np.select`, window functions) — no `.iterrows()` anywhere, regardless of data volume, to keep the pattern consistent as later pillars scale to 50K–100K rows.
2. Evidence for Task 1.3 is both the cleaned CSV **and** a machine-readable `employees_quality_summary.json` (row counts per issue) — not just log lines, so the counts can be checked programmatically later.
