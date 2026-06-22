-- =====================================================================
-- SNOWFLAKE PIPELINE — 04_gold_business_marts.sql
-- GOLD: business-ready marts for the two objectives.
--   1. Sales Performance
--   2. Customer Churn
-- Schema matches the Databricks Gold tables exactly so the dashboard
-- can read from either platform's output interchangeably.
-- =====================================================================

USE DATABASE NORTHWIND_DB;
USE SCHEMA GOLD;

SET CHURN_THRESHOLD_DAYS = 180;
SET DATASET_REF_DATE = '2025-01-01';  -- day after the dataset's order window

-- ---------------------------------------------------------------------
-- gold_sales_fact — line-item grain
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.SALES_FACT AS
SELECT
    od.ORDER_ID, o.ORDER_DATE, o.SHIPPED_DATE, o.REGION,
    o.CUSTOMER_ID, c.COMPANY_NAME, c.COUNTRY,
    o.EMPLOYEE_ID, e.FIRST_NAME, e.LAST_NAME,
    od.PRODUCT_ID, p.PRODUCT_NAME, p.CATEGORY_NAME,
    od.QUANTITY, od.UNIT_PRICE, od.DISCOUNT, od.LINE_REVENUE
FROM SILVER.ORDER_DETAILS od
JOIN SILVER.ORDERS o     ON od.ORDER_ID = o.ORDER_ID
LEFT JOIN SILVER.PRODUCTS p  ON od.PRODUCT_ID = p.PRODUCT_ID
LEFT JOIN SILVER.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN SILVER.EMPLOYEES e ON o.EMPLOYEE_ID = e.EMPLOYEE_ID;

-- ---------------------------------------------------------------------
-- gold_sales_monthly — revenue trend
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.SALES_MONTHLY AS
SELECT
    TO_CHAR(ORDER_DATE, 'YYYY-MM')        AS YEAR_MONTH,
    ROUND(SUM(LINE_REVENUE), 2)            AS TOTAL_REVENUE,
    COUNT(DISTINCT ORDER_ID)               AS ORDER_COUNT,
    COUNT(DISTINCT CUSTOMER_ID)            AS ACTIVE_CUSTOMERS
FROM GOLD.SALES_FACT
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------
-- gold_sales_by_category / gold_sales_by_region / gold_top_products
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.SALES_BY_CATEGORY AS
SELECT CATEGORY_NAME,
       ROUND(SUM(LINE_REVENUE), 2) AS TOTAL_REVENUE,
       SUM(QUANTITY)               AS UNITS_SOLD
FROM GOLD.SALES_FACT
GROUP BY 1
ORDER BY TOTAL_REVENUE DESC;

CREATE OR REPLACE TABLE GOLD.SALES_BY_REGION AS
SELECT REGION,
       ROUND(SUM(LINE_REVENUE), 2)   AS TOTAL_REVENUE,
       COUNT(DISTINCT ORDER_ID)      AS ORDER_COUNT
FROM GOLD.SALES_FACT
GROUP BY 1
ORDER BY TOTAL_REVENUE DESC;

CREATE OR REPLACE TABLE GOLD.TOP_PRODUCTS AS
SELECT PRODUCT_ID, PRODUCT_NAME, CATEGORY_NAME,
       ROUND(SUM(LINE_REVENUE), 2) AS TOTAL_REVENUE,
       SUM(QUANTITY)               AS UNITS_SOLD
FROM GOLD.SALES_FACT
GROUP BY 1, 2, 3
ORDER BY TOTAL_REVENUE DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- gold_customer_churn — RFM-style features + churn flag/segment
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.CUSTOMER_CHURN AS
WITH cust_orders AS (
    SELECT
        CUSTOMER_ID, ANY_VALUE(COMPANY_NAME) AS COMPANY_NAME,
        ANY_VALUE(COUNTRY) AS COUNTRY, ANY_VALUE(REGION) AS REGION,
        COUNT(DISTINCT ORDER_ID)      AS FREQUENCY_ORDERS,
        ROUND(SUM(LINE_REVENUE), 2)   AS MONETARY_TOTAL,
        MAX(ORDER_DATE)               AS LAST_ORDER_DATE,
        MIN(ORDER_DATE)               AS FIRST_ORDER_DATE
    FROM GOLD.SALES_FACT
    GROUP BY CUSTOMER_ID
)
SELECT
    co.CUSTOMER_ID, co.COMPANY_NAME, co.COUNTRY, co.REGION,
    co.FREQUENCY_ORDERS, co.MONETARY_TOTAL,
    co.FIRST_ORDER_DATE, co.LAST_ORDER_DATE,
    ROUND(co.MONETARY_TOTAL / co.FREQUENCY_ORDERS, 2) AS AVG_ORDER_VALUE,
    COALESCE(
        ca.DAYS_SINCE_LAST_ORDER,
        DATEDIFF('day', co.LAST_ORDER_DATE, TO_DATE($DATASET_REF_DATE))
    ) AS RECENCY_DAYS,
    CASE WHEN COALESCE(ca.DAYS_SINCE_LAST_ORDER,
                DATEDIFF('day', co.LAST_ORDER_DATE, TO_DATE($DATASET_REF_DATE)))
              > $CHURN_THRESHOLD_DAYS
         THEN TRUE ELSE FALSE END AS IS_CHURNED,
    CASE
        WHEN COALESCE(ca.DAYS_SINCE_LAST_ORDER,
                DATEDIFF('day', co.LAST_ORDER_DATE, TO_DATE($DATASET_REF_DATE)))
             > $CHURN_THRESHOLD_DAYS THEN 'Churned'
        WHEN COALESCE(ca.DAYS_SINCE_LAST_ORDER,
                DATEDIFF('day', co.LAST_ORDER_DATE, TO_DATE($DATASET_REF_DATE)))
             <= 60 THEN 'Active'
        ELSE 'At Risk'
    END AS CUSTOMER_SEGMENT
FROM cust_orders co
LEFT JOIN SILVER.CUSTOMER_ACTIVITY ca ON co.CUSTOMER_ID = ca.CUSTOMER_ID;

-- ---------------------------------------------------------------------
-- gold_churn_summary — churn rate by region (dashboard KPI source)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.CHURN_SUMMARY AS
SELECT
    REGION,
    COUNT(*)                                        AS TOTAL_CUSTOMERS,
    SUM(CASE WHEN IS_CHURNED THEN 1 ELSE 0 END)      AS CHURNED_CUSTOMERS,
    ROUND(SUM(CASE WHEN IS_CHURNED THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS CHURN_RATE_PCT
FROM GOLD.CUSTOMER_CHURN
GROUP BY REGION
ORDER BY CHURN_RATE_PCT DESC;

-- ---------------------------------------------------------------------
-- Quick validation
-- ---------------------------------------------------------------------
SELECT 'SALES_FACT' AS TBL, COUNT(*) AS ROWS FROM GOLD.SALES_FACT
UNION ALL SELECT 'SALES_MONTHLY', COUNT(*) FROM GOLD.SALES_MONTHLY
UNION ALL SELECT 'SALES_BY_CATEGORY', COUNT(*) FROM GOLD.SALES_BY_CATEGORY
UNION ALL SELECT 'SALES_BY_REGION', COUNT(*) FROM GOLD.SALES_BY_REGION
UNION ALL SELECT 'TOP_PRODUCTS', COUNT(*) FROM GOLD.TOP_PRODUCTS
UNION ALL SELECT 'CUSTOMER_CHURN', COUNT(*) FROM GOLD.CUSTOMER_CHURN
UNION ALL SELECT 'CHURN_SUMMARY', COUNT(*) FROM GOLD.CHURN_SUMMARY;
