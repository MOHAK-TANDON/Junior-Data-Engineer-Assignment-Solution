-- Step 1: Create Catalog
CREATE CATALOG IF NOT EXISTS junior_de_assignment;

-- Step 2: Use the catalog
USE CATALOG junior_de_assignment;

-- Step 3: Create Schema
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Step 4: Use the schema
USE SCHEMA ecommerce;

-- Step 5: Create Volume
CREATE VOLUME IF NOT EXISTS raw_data;
CREATE VOLUME IF NOT EXISTS output_data;

-- Verify it was created
SHOW VOLUMES;