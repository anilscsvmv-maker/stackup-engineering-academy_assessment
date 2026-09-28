# Data Quality Report — projects

**Checks run:** 9 | **Passed:** 6 | **Failed:** 3

| Check | Status | Details |
|---|---|---|
| completeness | **FAIL** | project_id=1.0; project_name=1.0; department=1.0; status=1.0; start_date=0.87; end_date=0.428; budget=0.936; actual_cost=0.88 ... |
| uniqueness | PASS | project_id: 500 unique / 500 total |
| validity_numeric | PASS | budget: 0 values below minimum (0), 0 values above maximum (10000000); actual_cost: 0 values below minimum (0), 0 values above maximum (10000000) |
| validity_date | PASS | start_date: 0 unparseable, 0 future-dated; end_date: 0 unparseable, 0 future-dated |
| consistency | PASS | start_date <= end_date: 0 violations; actual_cost >= 0: 0 violations |
| referential_integrity | PASS | project_manager_id -> employees.employee_id: 0 orphaned values |
| distribution | **FAIL** | department: top value = 9.8% of rows; status: top value = 42.8% of rows; region: top value = 44.0% of rows |
| freshness | PASS | no freshness_column configured |
| outliers | **FAIL** | budget: 0 values beyond 3 std dev; actual_cost: 2 values beyond 3 std dev |