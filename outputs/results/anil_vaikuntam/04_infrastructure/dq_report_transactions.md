# Data Quality Report — transactions

**Checks run:** 9 | **Passed:** 5 | **Failed:** 4

| Check | Status | Details |
|---|---|---|
| completeness | **FAIL** | transaction_id=1.0; project_id=1.0; vendor_id=1.0; vendor_name=1.0; category=1.0; amount=0.9852; currency=1.0; transaction_date=1.0 ... |
| uniqueness | PASS | transaction_id: 50000 unique / 50000 total |
| validity_numeric | PASS | amount: 0 values below minimum (0), 0 values above maximum (1000000) |
| validity_date | PASS | transaction_date: 0 unparseable, 0 future-dated |
| consistency | PASS | amount >= 0: 0 violations |
| referential_integrity | PASS | project_id -> projects.project_id: 0 orphaned values; approved_by -> employees.employee_id: 0 orphaned values |
| distribution | **FAIL** | category: top value = 20.3% of rows; payment_status: top value = 75.1% of rows |
| freshness | **FAIL** | transaction_date: most recent value is 51 days old (threshold 30) |
| outliers | **FAIL** | amount: 1593 values beyond 3 std dev |