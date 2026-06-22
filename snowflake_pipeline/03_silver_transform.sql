-- =====================================================================
-- SNOWFLAKE PIPELINE — 03_silver_transform.sql
-- SILVER: clean, conform, and join the DB source + Object Store source.
-- =====================================================================

USE DATABASE NORTHWIND_DB;
USE SCHEMA SILVER;

-- ---------------------------------------------------------------------
-- silver_customers (DB source)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMERS AS
SELECT DISTINCT
    UPPER(TRIM(CUSTOMER_ID))   AS CUSTOMER_ID,
    TRIM(COMPANY_NAME)         AS COMPANY_NAME,
    TRIM(CONTACT_NAME)         AS CONTACT_NAME,
    TRIM(COUNTRY)              AS COUNTRY
FROM BRONZE.CUSTOMERS;

-- ---------------------------------------------------------------------
-- silver_employees (DB source)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.EMPLOYEES AS
SELECT DISTINCT
    EMPLOYEE_ID::NUMBER  AS EMPLOYEE_ID,
    TRIM(FIRST_NAME)     AS FIRST_NAME,
    TRIM(LAST_NAME)      AS LAST_NAME,
    TRIM(TITLE)          AS TITLE
FROM BRONZE.EMPLOYEES;

-- ---------------------------------------------------------------------
-- silver_orders (DB source) enriched with Region (Object Store source)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDERS AS
SELECT
    o.ORDER_ID::NUMBER          AS ORDER_ID,
    UPPER(TRIM(o.CUSTOMER_ID))  AS CUSTOMER_ID,
    o.EMPLOYEE_ID::NUMBER       AS EMPLOYEE_ID,
    TO_DATE(o.ORDER_DATE)       AS ORDER_DATE,
    TO_DATE(o.SHIPPED_DATE)     AS SHIPPED_DATE,
    o.SHIP_VIA::NUMBER          AS SHIPPER_ID,
    TRIM(o.SHIP_COUNTRY)        AS SHIP_COUNTRY,
    r.REGION                    AS REGION
FROM BRONZE.ORDERS o
LEFT JOIN BRONZE.REGION_LOOKUP r
    ON UPPER(TRIM(o.SHIP_COUNTRY)) = UPPER(TRIM(r.COUNTRY));

-- ---------------------------------------------------------------------
-- silver_products (Object Store source) enriched with Category & Supplier
-- (also Object Store source)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.PRODUCTS AS
SELECT
    p.PRODUCT_ID::NUMBER      AS PRODUCT_ID,
    TRIM(p.PRODUCT_NAME)      AS PRODUCT_NAME,
    p.CATEGORY_ID::NUMBER     AS CATEGORY_ID,
    c.CATEGORY_NAME           AS CATEGORY_NAME,
    p.SUPPLIER_ID::NUMBER     AS SUPPLIER_ID,
    s.COMPANY_NAME            AS SUPPLIER_NAME,
    s.COUNTRY                 AS SUPPLIER_COUNTRY,
    ROUND(p.UNIT_PRICE::FLOAT, 2) AS UNIT_PRICE
FROM BRONZE.PRODUCTS p
LEFT JOIN BRONZE.CATEGORIES c ON p.CATEGORY_ID = c.CATEGORY_ID
LEFT JOIN BRONZE.SUPPLIERS s ON p.SUPPLIER_ID = s.SUPPLIER_ID;

-- ---------------------------------------------------------------------
-- silver_order_details (DB source) with computed line revenue
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDER_DETAILS AS
SELECT
    ORDER_ID::NUMBER     AS ORDER_ID,
    PRODUCT_ID::NUMBER   AS PRODUCT_ID,
    ROUND(UNIT_PRICE::FLOAT, 2) AS UNIT_PRICE,
    QUANTITY::NUMBER     AS QUANTITY,
    ROUND(DISCOUNT::FLOAT, 2)   AS DISCOUNT,
    ROUND(UNIT_PRICE::FLOAT * QUANTITY::NUMBER * (1 - DISCOUNT::FLOAT), 2) AS LINE_REVENUE
FROM BRONZE.ORDER_DETAILS;

-- ---------------------------------------------------------------------
-- silver_customer_activity (Object Store source, pre-computed export)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMER_ACTIVITY AS
SELECT
    UPPER(TRIM(CUSTOMER_ID))        AS CUSTOMER_ID,
    TOTAL_ORDERS::NUMBER             AS TOTAL_ORDERS_EXPORT,
    TO_DATE(FIRST_ORDER_DATE)        AS FIRST_ORDER_DATE,
    TO_DATE(LAST_ORDER_DATE)         AS LAST_ORDER_DATE,
    DAYS_SINCE_LAST_ORDER::NUMBER    AS DAYS_SINCE_LAST_ORDER
FROM BRONZE.CUSTOMER_ACTIVITY_EXPORT;
