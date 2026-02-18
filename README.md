# Junior Data Engineer Assignment — README

**Platform:** Databricks (PySpark + Unity Catalog)
**Author:** Mohak Tandon
**Date:** 18 February 2026

---

## Project Structure

```
submission/
├── pipeline.py   ← Main PySpark script (run this on Databricks)
├── setup.sql     ← Catalog, schema, and volume setup (run first)
├── queries.sql   ← SQL schema definitions + all 5 analytical queries
└── README.md     ← This file
```

---

## How to Run on Databricks

**Step 1 — Run `setup.sql`** in the Databricks SQL Editor.
This creates the catalog `junior_de_assignment`, schema `ecommerce`, and two volumes: `raw_data` (for CSV input) and `output_data` (for pipeline output and quarantine).

**Step 2 — Upload CSVs** to the Unity Catalog Volume:
`/Volumes/junior_de_assignment/ecommerce/raw_data/`
(Go to Catalog → junior_de_assignment → ecommerce → raw_data → Upload)

**Step 3 — Run `pipeline.py`** in a Databricks notebook or as a Workflows Job.
The script reads from the volume, cleans and validates the data, writes Delta tables to Unity Catalog, runs all 5 analytical queries, and saves quarantined rows.

**Step 4 — Verify** using `queries.sql` in the Databricks SQL Editor for manual query execution.

---

## Part 1: Data Cleaning — What Was Fixed

### customers.csv (22 → 20 rows)

| Issue | Action | PySpark Technique |
|---|---|---|
| Duplicate `customer_id = 5` | Kept first occurrence | `row_number()` window function |
| `name = NULL` (customer_id 21) | Dropped — cannot identify customer | `filter(col("name").isNotNull())` |
| Invalid date: `'not_a_date'` | Set to NULL with warning logged | `coalesce(try_to_date(...))` |
| Mixed date formats | Normalised all to DateType | Helper trying 4 common formats |
| Inconsistent country casing | Standardised to Title Case | `initcap(trim(col("country")))` |

### orders.csv (51 → 45 rows)

| Issue | Action | PySpark Technique |
|---|---|---|
| Duplicate `order_id = 11` | Kept first occurrence | `row_number()` window |
| Dates in `DD-MM-YYYY` format | Parsed and normalised to DateType | `coalesce(try_to_date(...))` |
| 5 orders with `customer_id` 21, 22 (not in customers) | **Quarantined** to `output_data/quarantine/orphaned_orders/` | `left_anti` join |

**Why quarantine?** These rows violate foreign key integrity. Rather than silently dropping them, they are saved for upstream investigation.

### payments.csv (40 → 35 rows)

| Issue | Action | PySpark Technique |
|---|---|---|
| Dates in `YYYY/MM/DD` format | Parsed and normalised | `coalesce(try_to_date(...))` |
| 5 payments with invalid `order_id` (55, and orders tied to quarantined orders) | Quarantined to `output_data/quarantine/orphaned_payments/` | `left_anti` join |

---

## Part 2: Database Design — Schema Decisions

```sql
customers  (customer_id INT PK, name STRING NOT NULL, signup_date DATE, country STRING)
orders     (order_id INT PK, customer_id INT FK, order_date DATE, amount DOUBLE NOT NULL, status STRING NOT NULL)
payments   (payment_id INT PK, order_id INT FK, payment_date DATE, payment_status STRING NOT NULL)
```

| Decision | Reasoning |
|---|---|
| **Delta format** (`USING DELTA`) | Databricks standard. Provides ACID transactions, schema enforcement, time travel, and efficient MERGE for upserts. |
| **DATE type** (not STRING) | Enables date arithmetic (`DATE_SUB`) and correct sorting. |
| **DOUBLE for amount** | Handles decimal precision for monetary values. |
| **NOT NULL on `name` and `amount`** | Cannot process nameless customers or orders without amounts. |
| **FK validation in Python** | Spark SQL does not enforce FK constraints — enforced programmatically via `left_anti` joins during cleaning. |
| **Unity Catalog managed tables** | Full data governance: lineage tracking, access controls, and audit logs out of the box. |

---

## Part 3: SQL Queries — Design Notes

All 5 queries are in `queries.sql` and also embedded in the pipeline.

| # | Query | Key Technique |
|---|---|---|
| 1 | Total revenue per customer | `SUM` + `GROUP BY` on `status = 'completed'` orders only |
| 2 | Monthly revenue trend | `DATE_FORMAT(date, 'yyyy-MM')` groups by month |
| 3 | Customers with orders but no successful payment | `NOT IN` subquery against the set of customer_ids with at least one `payment_status = 'success'` |
| 4 | Top 5 customers by revenue | Same as Q1 + `LIMIT 5` |
| 5 | Customers inactive 90 days | `LEFT JOIN` ensures customers with zero orders appear; `DATE_SUB(CURRENT_DATE(), 90)` sets the cutoff |

**Assumption for Query 3:** A customer is considered "unpaid" only if **none** of their orders has ever had a successful payment. This targets customers who genuinely need payment follow-up, rather than customers who simply have some pending/failed orders alongside successful ones.

---

## Part 4: Short Questions

### Q1 — How would you automate this pipeline?

Use **Databricks Workflows (Jobs)** with a scheduled cron trigger (e.g., daily at 2 AM UTC):

1. **Parameterise file paths** — pass input/output paths as job parameters so they can be changed without editing code.
2. **Add retry logic** — configure 2–3 automatic retries per task on failure.
3. **Set up alerts** — email/Slack notifications on failure via Databricks job settings.
4. **Monitor with logs** — all `print()` statements go to Spark logs, viewable in job run details.

For **file-arrival triggers**, use **Auto Loader** (`cloudFiles` format) to automatically process new files as they land in cloud storage, without needing cron schedules:

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load("/Volumes/.../raw_data/") \
    .writeStream \
    .trigger(availableNow=True) \
    .start()
```

For a fully managed, production-grade solution, migrate the pipeline to **Delta Live Tables (DLT)**, which adds declarative quality constraints, automatic lineage, and built-in observability.

---

### Q2 — How would you handle large datasets?

| Technique | How It Helps |
|---|---|
| **Partitioning** | `PARTITION BY (YEAR(order_date), MONTH(order_date))` — queries scan only relevant partitions |
| **Z-Ordering** | `OPTIMIZE table ZORDER BY (customer_id)` — co-locates related data for faster joins |
| **Auto Loader** | Incrementally ingests only new files — does not reprocess old data on every run |
| **Broadcast joins** | `customers.hint("broadcast")` — for small dimension tables, avoids expensive shuffle joins |
| **Caching** | `df.cache()` — if a DataFrame is reused multiple times (e.g., `valid_customer_ids`) |
| **Adaptive Query Execution (AQE)** | Enabled by default in Databricks — automatically optimises joins and shuffles at runtime |

---

### Q3 — What happens if one file fails?

**Current behaviour:** The script raises an exception and stops — downstream tables are not written.

**Production-grade handling:**

1. **Try-except blocks** — wrap each ingestion step; log the error and decide whether to skip or abort:
   ```python
   try:
       raw_customers = spark.read.csv(CUSTOMERS_CSV, schema=customers_schema)
   except Exception as e:
       print(f"ERROR: Failed to load customers.csv — {e}")
       raise  # Re-raise so Databricks Job marks the run as failed
   ```
2. **Quarantine bad rows** — already implemented. Orphaned rows are saved to a separate folder for manual review rather than being silently dropped.
3. **Delta atomicity** — if a Delta write fails mid-way, the Delta log ensures the table stays in its previous valid state (no partial writes or corrupted data).
4. **Databricks Workflows retries** — configure automatic retries at the task level (e.g., 2 retries with a 5-minute delay).
5. **Alerting** — set up email/Slack notifications on job failure in the Databricks Workflows UI.

---

## Assumptions

1. **Multiple payments per order are valid** — they represent retry attempts.
2. **A customer with no name is unusable** — dropped rather than kept with a placeholder.
3. **Orphaned FK rows are data quality issues** — quarantined for investigation rather than silently dropped.
4. **Dates in multiple formats are legitimate** — the pipeline tries 4 common formats before giving up.
5. **Only `completed` orders count as revenue** — Queries 1, 2, and 4 exclude pending and cancelled orders.
6. **"No successful payment" means zero across all orders** — Query 3 targets customers with no payment success on any of their orders, not customers with a mix of successful and failed payments.
