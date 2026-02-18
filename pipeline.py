"""
================================================================================
Junior Data Engineer Assignment — Data Pipeline
Author: Mohak Tandon
Platform: Databricks (PySpark) with Unity Catalog
================================================================================

This pipeline demonstrates:
- CSV ingestion with schema enforcement
- Data cleaning (nulls, duplicates, date standardization, FK validation)
- Delta Lake table creation in Unity Catalog
- Analytical SQL queries

HOW TO RUN ON DATABRICKS:
1. Create catalog and schema:
   CREATE CATALOG IF NOT EXISTS junior_de_assignment;
   CREATE SCHEMA IF NOT EXISTS junior_de_assignment.ecommerce;
   CREATE VOLUME IF NOT EXISTS junior_de_assignment.ecommerce.raw_data;

2. Upload customers.csv, orders.csv, payments.csv to:
   /Volumes/junior_de_assignment/ecommerce/raw_data/

3. Run this script in a Databricks

The pipeline outputs:
- Cleaned Delta tables: junior_de_assignment.ecommerce.{customers, orders, payments}
- Query results displayed in the console
- Orphaned rows (FK violations) quarantined to volumes
================================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

# ================================================================================
# CONFIGURATION
# ================================================================================

# Unity Catalog Configuration
CATALOG = "junior_de_assignment"
SCHEMA  = "ecommerce"
VOLUME  = "raw_data"

# File paths
INPUT_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/"
OUTPUT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/output_data/"

CUSTOMERS_CSV = INPUT_PATH + "customers.csv"
ORDERS_CSV    = INPUT_PATH + "orders.csv"
PAYMENTS_CSV  = INPUT_PATH + "payments.csv"

# Spark session (already available in Databricks; included for completeness)
spark = SparkSession.builder.appName("JuniorDE_Pipeline").getOrCreate()


print("="*80)
print(" JUNIOR DATA ENGINEER ASSIGNMENT — PIPELINE EXECUTION")
print(f" Unity Catalog: {CATALOG}.{SCHEMA}")
print("="*80)


# ================================================================================
# SECTION 1: DEFINE SCHEMAS
# ================================================================================
# Explicit schemas prevent Spark from inferring wrong types (e.g., treating
# amount as string if it encounters a malformed row during sampling)

customers_schema = StructType([
    StructField("customer_id",  IntegerType(), True),
    StructField("name",         StringType(),  True),
    StructField("signup_date",  StringType(),  True),   
    StructField("country",      StringType(),  True),
])

orders_schema = StructType([
    StructField("order_id",    IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date",  StringType(),  True),  
    StructField("amount",      DoubleType(),  True),
    StructField("status",      StringType(),  True),
])

payments_schema = StructType([
    StructField("payment_id",     IntegerType(), True),
    StructField("order_id",       IntegerType(), True),
    StructField("payment_date",   StringType(),  True), 
    StructField("payment_status", StringType(),  True),
])


# ================================================================================
# SECTION 2: INGEST RAW CSV FILES
# ================================================================================

print("\n" + "="*80)
print(" SECTION 2: INGESTING RAW DATA")
print("="*80)

raw_customers = spark.read.csv(CUSTOMERS_CSV, header=True, schema=customers_schema)
raw_orders    = spark.read.csv(ORDERS_CSV,    header=True, schema=orders_schema)
raw_payments  = spark.read.csv(PAYMENTS_CSV,  header=True, schema=payments_schema)

print(f"✓ Loaded customers.csv: {raw_customers.count()} rows")
print(f"✓ Loaded orders.csv:    {raw_orders.count()} rows")
print(f"✓ Loaded payments.csv:  {raw_payments.count()} rows")


# ================================================================================
# SECTION 3: DATA CLEANING & VALIDATION
# ================================================================================

print("\n" + "="*80)
print(" SECTION 3: DATA CLEANING")
print("="*80)


# ────────────────────────────────────────────────────────────────────────────────
# Helper Function: Parse Multiple Date Formats
# ────────────────────────────────────────────────────────────────────────────────
def parse_date_column(col_name: str) -> F.Column:
    """
    Attempts to parse a date string using multiple common formats.
    Returns a DateType column (NULL if no format matches).
    
    Order matters! Try most specific formats first.
    
    Formats tried in order:
    1. dd-MM-yyyy    (e.g., 22-02-2024) - orders.csv uses this
    2. yyyy-MM-dd    (ISO 8601, e.g., 2024-01-15) - customers.csv uses this
    3. yyyy/MM/dd    (e.g., 2024/02/01) - payments.csv uses this
    4. MM/dd/yyyy    (US format, e.g., 02/01/2024)
    """
    formats = ["dd-MM-yyyy", "yyyy-MM-dd", "yyyy/MM/dd", "MM/dd/yyyy"]
    parsed_dates = [F.expr(f"try_to_date({col_name}, '{fmt}')") for fmt in formats]
    return F.coalesce(*parsed_dates)


# ────────────────────────────────────────────────────────────────────────────────
# 3A. CLEAN CUSTOMERS
# ────────────────────────────────────────────────────────────────────────────────
print("\n--- Cleaning: customers.csv ---")

# Step 1: Drop rows where name is NULL
#         A customer without a name cannot be identified in reports
customers = raw_customers.filter(F.col("name").isNotNull())

# Step 2: Remove duplicate customer_id rows — keep first occurrence
#         Use row_number() window function partitioned by customer_id
customers = customers.withColumn(
    "_row_num",
    F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.monotonically_increasing_id())
    )
).filter(F.col("_row_num") == 1).drop("_row_num")

# Step 3: Standardize signup_date → DateType
customers = customers.withColumn("signup_date", parse_date_column("signup_date"))

# Print original values that could not be parsed
unparsed_customers = customers.filter(F.col("signup_date").isNull())
if unparsed_customers.count() > 0:
    print("[customers] Unparseable signup_date values:")
    for row in unparsed_customers.select("customer_id", "name", "signup_date", "signup_date").collect():
        print(f"  customer_id={row['customer_id']}, name={row['name']}, original_signup_date={raw_customers.filter(F.col('customer_id') == row['customer_id']).select('signup_date').head()[0]}")

# Count how many dates could not be parsed (will show as NULL)
unparsed_dates = customers.filter(F.col("signup_date").isNull()).count()
if unparsed_dates > 0:
    print(f"  ⚠ WARNING: {unparsed_dates} row(s) have unparseable signup_date → set to NULL")

# Step 4: Standardize country → Title Case, trim whitespace
customers = customers.withColumn("country", F.initcap(F.trim(F.col("country"))))

print(f"  ✓ Clean rows: {customers.count()}")


# ────────────────────────────────────────────────────────────────────────────────
# 3B. CLEAN ORDERS
# ────────────────────────────────────────────────────────────────────────────────
print("\n--- Cleaning: orders.csv ---")

# Step 1: Remove duplicate order_id rows
orders = raw_orders.withColumn(
    "_row_num",
    F.row_number().over(
        Window.partitionBy("order_id").orderBy(F.monotonically_increasing_id())
    )
).filter(F.col("_row_num") == 1).drop("_row_num")

# Step 2: Drop rows with NULL or negative amount
orders = orders.filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))

# Step 3: Standardize order_date → DateType
orders = orders.withColumn("order_date", parse_date_column("order_date"))

# Check if any dates failed to parse
unparsed_dates = orders.filter(F.col("order_date").isNull()).count()
if unparsed_dates > 0:
    print(f"  ⚠ WARNING: {unparsed_dates} row(s) have unparseable order_date → set to NULL")

# Step 4: Standardize status → lowercase, trim whitespace
orders = orders.withColumn("status", F.lower(F.trim(F.col("status"))))

# Step 5: Foreign Key Validation — find orders with invalid customer_id
#         LEFT ANTI JOIN returns rows in orders that have NO match in customers
valid_customer_ids = customers.select("customer_id")
orphaned_orders = orders.join(valid_customer_ids, on="customer_id", how="left_anti")
orphan_count = orphaned_orders.count()

print(f"  ⚠ Orphaned orders (invalid customer_id): {orphan_count}")

if orphan_count > 0:
    # Quarantine orphaned rows for investigation
    orphaned_orders.write.mode("overwrite").option("header", True) \
        .csv(OUTPUT_PATH + "quarantine/orphaned_orders/")
    print(f"    → Saved to: {OUTPUT_PATH}quarantine/orphaned_orders/")

# Keep only valid orders (those with matching customer_id)
orders = orders.join(valid_customer_ids, on="customer_id", how="inner")
print(f"  ✓ Clean rows: {orders.count()}")


# ────────────────────────────────────────────────────────────────────────────────
# 3C. CLEAN PAYMENTS
# ────────────────────────────────────────────────────────────────────────────────
print("\n--- Cleaning: payments.csv ---")

# Step 1: Remove duplicate payment_id rows (defensive — no duplicates expected)
payments = raw_payments.withColumn(
    "_row_num",
    F.row_number().over(
        Window.partitionBy("payment_id").orderBy(F.monotonically_increasing_id())
    )
).filter(F.col("_row_num") == 1).drop("_row_num")

# Step 2: Standardize payment_date → DateType
payments = payments.withColumn("payment_date", parse_date_column("payment_date"))

# Check if any dates failed to parse
unparsed_dates = payments.filter(F.col("payment_date").isNull()).count()
if unparsed_dates > 0:
    print(f"  ⚠ WARNING: {unparsed_dates} row(s) have unparseable payment_date → set to NULL")

# Step 3: Standardize payment_status → lowercase, trim
payments = payments.withColumn("payment_status", F.lower(F.trim(F.col("payment_status"))))

# Step 4: Foreign Key Validation — find payments with invalid order_id
valid_order_ids = orders.select("order_id")
orphaned_payments = payments.join(valid_order_ids, on="order_id", how="left_anti")
orphan_count = orphaned_payments.count()

print(f"  ⚠ Orphaned payments (invalid order_id): {orphan_count}")

if orphan_count > 0:
    orphaned_payments.write.mode("overwrite").option("header", True) \
        .csv(OUTPUT_PATH + "quarantine/orphaned_payments/")
    print(f"    → Saved to: {OUTPUT_PATH}quarantine/orphaned_payments/")

# Keep only valid payments
payments = payments.join(valid_order_ids, on="order_id", how="inner")
print(f"  ✓ Clean rows: {payments.count()}")


# ================================================================================
# SECTION 4: LOAD INTO UNITY CATALOG DELTA TABLES
# ================================================================================

print("\n" + "="*80)
print(" SECTION 4: WRITING DELTA TABLES TO UNITY CATALOG")
print("="*80)

# Ensure schema exists (catalog must already exist)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Write as managed Unity Catalog tables
# These are stored and governed by Unity Catalog with full lineage tracking
customers.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.customers")

orders.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.orders")

payments.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.payments")

print(f"✓ Delta tables created in Unity Catalog:")
print(f"  - {CATALOG}.{SCHEMA}.customers")
print(f"  - {CATALOG}.{SCHEMA}.orders")
print(f"  - {CATALOG}.{SCHEMA}.payments")


# ================================================================================
# SECTION 5: ANALYTICAL QUERIES
# ================================================================================

print("\n" + "="*80)
print(" SECTION 5: ANALYTICAL QUERIES")
print("="*80)


# ────────────────────────────────────────────────────────────────────────────────
# Query 1: Total Revenue per Customer (completed orders only)
# ────────────────────────────────────────────────────────────────────────────────
print("\n[QUERY 1] Total Revenue per Customer")
print("-" * 80)

query1 = spark.sql(f"""
    SELECT
        c.customer_id,
        c.name,
        ROUND(SUM(o.amount), 2) AS total_revenue
    FROM {CATALOG}.{SCHEMA}.customers c
    JOIN {CATALOG}.{SCHEMA}.orders    o ON c.customer_id = o.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.customer_id, c.name
    ORDER BY total_revenue DESC
""")
query1.show(truncate=False)


# ────────────────────────────────────────────────────────────────────────────────
# Query 2: Monthly Revenue Trend (completed orders only)
# ────────────────────────────────────────────────────────────────────────────────
print("\n[QUERY 2] Monthly Revenue Trend")
print("-" * 80)

query2 = spark.sql(f"""
    SELECT
        DATE_FORMAT(order_date, 'yyyy-MM') AS month,
        ROUND(SUM(amount), 2)              AS monthly_revenue
    FROM {CATALOG}.{SCHEMA}.orders
    WHERE status = 'completed'
    GROUP BY month
    ORDER BY month
""")
query2.show(truncate=False)


# ────────────────────────────────────────────────────────────────────────────────
# Query 3: Customers with Orders but No Successful Payment
# ────────────────────────────────────────────────────────────────────────────────
print("\n[QUERY 3] Customers with Orders but No Successful Payment")
print("-" * 80)

query3 = spark.sql(f"""
    SELECT
        c.customer_id,
        c.name
    FROM {CATALOG}.{SCHEMA}.customers c
    WHERE EXISTS (
        SELECT 1
        FROM {CATALOG}.{SCHEMA}.orders o
        WHERE o.customer_id = c.customer_id
    )
    AND c.customer_id NOT IN (
        SELECT DISTINCT o2.customer_id
        FROM {CATALOG}.{SCHEMA}.orders   o2
        JOIN {CATALOG}.{SCHEMA}.payments p ON o2.order_id = p.order_id
        WHERE p.payment_status = 'success'
    )
    ORDER BY c.customer_id
""")
query3.show(truncate=False)


# ────────────────────────────────────────────────────────────────────────────────
# Query 4: Top 5 Customers by Revenue
# ────────────────────────────────────────────────────────────────────────────────
print("\n[QUERY 4] Top 5 Customers by Revenue")
print("-" * 80)

query4 = spark.sql(f"""
    SELECT
        c.customer_id,
        c.name,
        ROUND(SUM(o.amount), 2) AS total_revenue
    FROM {CATALOG}.{SCHEMA}.customers c
    JOIN {CATALOG}.{SCHEMA}.orders    o ON c.customer_id = o.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.customer_id, c.name
    ORDER BY total_revenue DESC
    LIMIT 5
""")
query4.show(truncate=False)


# ────────────────────────────────────────────────────────────────────────────────
# Query 5: Customers Inactive in Last 90 Days
# ────────────────────────────────────────────────────────────────────────────────
print("\n[QUERY 5] Customers Inactive in Last 90 Days")
print("-" * 80)

query5 = spark.sql(f"""
    SELECT
        c.customer_id,
        c.name,
        MAX(o.order_date) AS last_order_date
    FROM {CATALOG}.{SCHEMA}.customers c
    LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.name
    HAVING last_order_date IS NULL
        OR last_order_date < DATE_SUB(CURRENT_DATE(), 90)
    ORDER BY last_order_date
""")
query5.show(truncate=False)


# ================================================================================
# PIPELINE COMPLETE
# ================================================================================

print("\n" + "="*80)
print(" PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
print("="*80)
print(f"\n✓ Delta tables available in Unity Catalog: {CATALOG}.{SCHEMA}")
print(f"✓ Quarantined data (if any) at: {OUTPUT_PATH}quarantine/")
print("\nNext steps:")
print(f"  - Query tables: SELECT * FROM {CATALOG}.{SCHEMA}.customers;")
print("  - Schedule this pipeline as a Databricks Job for automation")
print("  - Set up email/Slack alerts on pipeline failure")
print(f"  - Grant permissions: GRANT SELECT ON SCHEMA {SCHEMA} TO the mail;")
print("="*80)
