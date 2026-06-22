-- =====================================================================
-- SNOWFLAKE PIPELINE — 01_load_db_source.sql
-- BRONZE: Load the "DB source" tables (Customers, Employees, Shippers,
-- Orders, OrderDetails) — originating from the relational database
-- (northwind.db / SQLite in this project; a Postgres/SQL Server/MySQL
-- production DB in a real deployment).
--
-- Loading method: these are loaded via the Snowflake connector / SQL
-- client running the project's `load_db_source_to_snowflake.py` script,
-- which streams rows from the SQLite DB source straight into these
-- tables with the Snowflake Python connector (executemany / write_pandas).
-- This file defines the BRONZE table DDL that script loads into.
-- =====================================================================

USE DATABASE NORTHWIND_DB;
USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE BRONZE.CUSTOMERS (
    CUSTOMER_ID     STRING,
    COMPANY_NAME    STRING,
    CONTACT_NAME    STRING,
    COUNTRY         STRING,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'db_source_sqlite'
);

CREATE OR REPLACE TABLE BRONZE.EMPLOYEES (
    EMPLOYEE_ID     NUMBER,
    FIRST_NAME      STRING,
    LAST_NAME       STRING,
    TITLE           STRING,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'db_source_sqlite'
);

CREATE OR REPLACE TABLE BRONZE.SHIPPERS (
    SHIPPER_ID      NUMBER,
    COMPANY_NAME    STRING,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'db_source_sqlite'
);

CREATE OR REPLACE TABLE BRONZE.ORDERS (
    ORDER_ID        NUMBER,
    CUSTOMER_ID     STRING,
    EMPLOYEE_ID     NUMBER,
    ORDER_DATE      STRING,   -- cast to DATE in Silver
    SHIPPED_DATE    STRING,
    SHIP_VIA        NUMBER,
    SHIP_COUNTRY    STRING,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'db_source_sqlite'
);

CREATE OR REPLACE TABLE BRONZE.ORDER_DETAILS (
    ORDER_ID        NUMBER,
    PRODUCT_ID      NUMBER,
    UNIT_PRICE      FLOAT,
    QUANTITY        NUMBER,
    DISCOUNT        FLOAT,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'db_source_sqlite'
);

-- Rows are populated by: snowflake_pipeline/load_db_source_to_snowflake.py
-- (uses snowflake-connector-python's write_pandas against the tables above)
