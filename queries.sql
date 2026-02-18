-- ============================================================
--  Junior Data Engineer Assignment — SQL File
--  Database: Databricks SQL / Spark SQL
--  Purpose: Table schemas + All analytical queries
-- ============================================================

-- NOTE: The pipeline.py script automatically creates Delta tables.
-- This file documents the schema design and contains all queries
-- for manual execution in Databricks SQL Editor if needed.

-- ────────────────────────────────────────────────────────────
-- SET CONTEXT (run once before any other statement)
-- ────────────────────────────────────────────────────────────
USE CATALOG junior_de_assignment;
USE SCHEMA ecommerce;
-- ────────────────────────────────────────────────────────────
-- PART 2: DATABASE / TABLE DESIGN
-- ────────────────────────────────────────────────────────────

-- Drop existing tables (for clean re-runs)
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS payments;


-- ────── CUSTOMERS TABLE ──────────────────────────────────────
-- Primary Key: customer_id
-- Business rule: name is NOT NULL (cannot identify nameless customer)
CREATE TABLE customers (
    customer_id  INT      NOT NULL,
    name         STRING   NOT NULL,
    signup_date  DATE,
    country      STRING
)
USING DELTA;

-- Note: USING DELTA provides:
-- - ACID transactions (atomic, consistent, isolated, durable writes)
-- - Schema enforcement and evolution
-- - Time travel (query previous versions of the table)
-- - Efficient MERGE operations for upserts


-- ────── ORDERS TABLE ─────────────────────────────────────────
-- Primary Key: order_id
-- Foreign Key: customer_id → customers.customer_id
--   (enforced programmatically in pipeline via FK validation)
CREATE TABLE orders (
    order_id     INT     NOT NULL,
    customer_id  INT     NOT NULL,
    order_date   DATE,
    amount       DOUBLE  NOT NULL,
    status       STRING  NOT NULL
)
USING DELTA;

-- Status values: 'completed', 'pending', 'cancelled'
-- Amount constraint: Must be >= 0 (enforced in pipeline cleaning step)


-- ────── PAYMENTS TABLE ───────────────────────────────────────
-- Primary Key: payment_id
-- Foreign Key: order_id → orders.order_id
-- Note: One order can have MULTIPLE payment rows (retry attempts)
CREATE TABLE payments (
    payment_id     INT    NOT NULL,
    order_id       INT    NOT NULL,
    payment_date   DATE,
    payment_status STRING NOT NULL
)
USING DELTA;

-- Payment status values: 'success', 'failed'


-- ────────────────────────────────────────────────────────────
-- PART 3: ANALYTICAL QUERIES
-- ────────────────────────────────────────────────────────────


-- ────── QUERY 1: Total Revenue per Customer ─────────────────
-- Aggregates revenue from completed orders only
-- Customers with zero completed orders are excluded (INNER JOIN)
SELECT
    c.customer_id,
    c.name,
    ROUND(SUM(o.amount), 2)  AS total_revenue
FROM customers c
JOIN orders    o ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.customer_id, c.name
ORDER BY total_revenue DESC;


-- ────── QUERY 2: Monthly Revenue Trend ──────────────────────
-- Shows monthly revenue over time from completed orders
-- DATE_FORMAT extracts YYYY-MM from a date column (Spark SQL syntax)
SELECT
    DATE_FORMAT(o.order_date, 'yyyy-MM')  AS month,
    ROUND(SUM(o.amount), 2)               AS monthly_revenue
FROM orders o
WHERE o.status = 'completed'
GROUP BY month
ORDER BY month;


-- ────── QUERY 3: Customers with Orders but No Successful Payment ───
-- Uses NOT EXISTS to efficiently check if ANY payment succeeded
-- This pattern is more efficient than LEFT JOIN ... IS NULL when
-- there are multiple payment rows per order
SELECT
    c.customer_id,
    c.name
FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
)
AND c.customer_id NOT IN (
    SELECT DISTINCT o2.customer_id
    FROM orders   o2
    JOIN payments p ON o2.order_id = p.order_id
    WHERE p.payment_status = 'success'
)
ORDER BY c.customer_id;


-- ────── QUERY 4: Top 5 Customers by Revenue ─────────────────
-- Same aggregation as Query 1, limited to top 5
SELECT
    c.customer_id,
    c.name,
    ROUND(SUM(o.amount), 2)  AS total_revenue
FROM customers c
JOIN orders    o ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.customer_id, c.name
ORDER BY total_revenue DESC
LIMIT 5;


-- ────── QUERY 5: Customers Inactive in Last 90 Days ────────
-- "Inactive" = no orders placed in past 90 days OR never placed an order
-- LEFT JOIN ensures customers with zero orders also appear
-- DATE_SUB(CURRENT_DATE(), 90) → Databricks/Spark SQL syntax
SELECT
    c.customer_id,
    c.name,
    MAX(o.order_date)  AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
HAVING last_order_date IS NULL
    OR last_order_date < DATE_SUB(CURRENT_DATE(), 90)
ORDER BY last_order_date;
