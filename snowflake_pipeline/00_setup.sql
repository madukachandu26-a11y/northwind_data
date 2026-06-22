-- =====================================================================
-- SNOWFLAKE PIPELINE — 00_setup.sql
-- Sales Performance & Customer Churn Project
-- =====================================================================
-- Sets up the database, schemas (Bronze/Raw, Silver, Gold), warehouse,
-- file formats, and stages for both source systems:
--   1. DB source      -> loaded via local SQL client / Snowflake Connector
--                        (simulating an external DB -> Snowflake load, e.g.
--                        via Snowpipe/Fivetran/JDBC in production)
--   2. Object Store source -> loaded via an external/internal STAGE,
--                        simulating S3 / Azure Blob / GCS ingestion
--
-- Data citation: Microsoft Northwind sample database
-- https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs
-- =====================================================================

CREATE WAREHOUSE IF NOT EXISTS NORTHWIND_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE NORTHWIND_WH;

CREATE DATABASE IF NOT EXISTS NORTHWIND_DB;
USE DATABASE NORTHWIND_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE;   -- raw landing zone (both sources)
CREATE SCHEMA IF NOT EXISTS SILVER;   -- cleaned & conformed
CREATE SCHEMA IF NOT EXISTS GOLD;     -- business marts (dashboard reads from here)

-- ---------------------------------------------------------------------
-- File format for Object Store CSV files
-- ---------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT BRONZE.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE;

-- File format for Object Store JSON files
CREATE OR REPLACE FILE FORMAT BRONZE.JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- ---------------------------------------------------------------------
-- Internal stage representing the OBJECT STORE source.
-- In production this would be an external stage on S3/Azure/GCS, e.g.:
--
--   CREATE STAGE BRONZE.OBJECT_STORE_STAGE
--     URL = 's3://my-bucket/northwind/object_store/'
--     STORAGE_INTEGRATION = my_s3_integration
--     FILE_FORMAT = BRONZE.CSV_FORMAT;
--
-- For this small-dataset project we use an internal stage and PUT the
-- local object_store/ files to it (see 01_load_object_store.sql).
-- ---------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS BRONZE.OBJECT_STORE_STAGE
    FILE_FORMAT = BRONZE.CSV_FORMAT
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Simulated object store (S3/ADLS/GCS) landing stage for Products/Categories/Suppliers/CustomerActivity/RegionLookup';
