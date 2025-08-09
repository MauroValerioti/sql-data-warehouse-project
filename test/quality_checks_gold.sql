-- ================================================
-- Quality Checks for Gold Layer Views
-- This script validates data integrity, consistency, and completeness
-- in the gold layer of the Data Warehouse.
-- ================================================

-- 1. Check for duplicates in gold.fact_sales
-- Ensures there are no duplicate sales transactions
SELECT sale_id, COUNT(*) AS occurrences
FROM gold.fact_sales
GROUP BY sale_id
HAVING COUNT(*) > 1;

-- 2. Check for null values in mandatory fields in gold.fact_sales
-- Critical fields like sale_id, customer_id, and product_id should not be null
SELECT COUNT(*) AS null_count
FROM gold.fact_sales
WHERE sale_id IS NULL 
   OR customer_id IS NULL 
   OR product_id IS NULL;

-- 3. Check date consistency in gold.fact_sales
-- Ensures sale_date is not in the future
SELECT COUNT(*) AS invalid_dates
FROM gold.fact_sales
WHERE sale_date > GETDATE();

-- 4. Validate referential integrity between gold.fact_sales and gold.dim_customers
-- All customer_id in fact_sales must exist in dim_customers
SELECT COUNT(*) AS missing_customers
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc 
    ON fs.customer_id = dc.customer_id
WHERE dc.customer_id IS NULL;

-- 5. Validate referential integrity between gold.fact_sales and gold.dim_products
-- All product_id in fact_sales must exist in dim_products
SELECT COUNT(*) AS missing_products
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp 
    ON fs.product_id = dp.product_id
WHERE dp.product_id IS NULL;

-- 6. Check for orphan records in gold.dim_customers
-- Customers with no sales in fact_sales
SELECT dc.customer_id
FROM gold.dim_customers dc
LEFT JOIN gold.fact_sales fs 
    ON dc.customer_id = fs.customer_id
WHERE fs.customer_id IS NULL;

-- 7. Check for orphan records in gold.dim_products
-- Products with no sales in fact_sales
SELECT dp.product_id
FROM gold.dim_products dp
LEFT JOIN gold.fact_sales fs 
    ON dp.product_id = fs.product_id
WHERE fs.product_id IS NULL;

-- 8. Check data completeness in gold.dim_dates
-- Ensures no missing days in the date dimension
SELECT MIN(full_date) AS start_date, MAX(full_date) AS end_date, COUNT(*) AS total_days
FROM gold.dim_dates;

-- 9. Validate that gold.fact_sales.amount is positive
-- Ensures no negative or zero sales amounts
SELECT COUNT(*) AS invalid_amounts
FROM gold.fact_sales
WHERE amount <= 0;

-- 10. Validate currency consistency in gold.fact_sales
-- Ensures only valid currency codes are used
SELECT DISTINCT currency
FROM gold.fact_sales
WHERE currency NOT IN ('USD', 'EUR', 'ARS');
