# Data Quality Report — employees

**Checks run:** 9 | **Passed:** 3 | **Failed:** 6

| Check | Status | Details |
|---|---|---|
| completeness | PASS | employee_id=1.0; full_name=1.0; email=0.99; department=1.0; role=1.0; level=1.0; hire_date=1.0; salary=1.0 ... |
| uniqueness | PASS | employee_id: 1000 unique / 1000 total |
| validity_numeric | **FAIL** | salary: 0 values below minimum (10000), 0 values above maximum (100000); years_experience: 5 values below minimum (0), 0 values above maximum (50) |
| validity_date | **FAIL** | hire_date: 8 unparseable, 0 future-dated |
| consistency | **FAIL** | salary >= 0: 0 violations; salary matches level band: 3 violations |
| referential_integrity | **FAIL** | manager_id -> employees.employee_id: 5 orphaned values |
| distribution | **FAIL** | department: top value = 9.8% of rows; level: top value = 36.6% of rows; region: top value = 43.4% of rows |
| freshness | PASS | no freshness_column configured |
| outliers | **FAIL** | salary: 17 values beyond 3 std dev |