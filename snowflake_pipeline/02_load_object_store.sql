-- =====================================================================
-- SNOWFLAKE PIPELINE — 02_load_object_store.sql
-- BRONZE: Load the "Object Store source" files (Categories, Suppliers,
-- Products, CustomerActivityExport, RegionLookup) via STAGE + COPY INTO,
-- simulating ingestion from S3 / ADLS / GCS.
--
-- Local upload step (run from SnowSQL or Snowflake CLI):
--   PUT file://object_store/categories.csv               @BRONZE.OBJECT_STORE_STAGE AUTO_COMPRESS=TRUE;
--   PUT file://object_store/suppliers.csv                @BRONZE.OBJECT_STORE_STAGE AUTO_COMPRESS=TRUE;
--   PUT file://object_store/customer_activity_export.csv @BRONZE.OBJECT_STORE_STAGE AUTO_COMPRESS=TRUE;
--   PUT file://object_store/products.json                @BRONZE.OBJECT_STORE_STAGE AUTO_COMPRESS=TRUE;
--   PUT file://object_store/order_region_lookup.json      @BRONZE.OBJECT_STORE_STAGE AUTO_COMPRESS=TRUE;
--
-- In a real deployment, BRONZE.OBJECT_STORE_STAGE would instead be an
-- EXTERNAL STAGE pointing directly at the S3/ADLS/GCS bucket path, and
-- COPY INTO would run on a schedule or via Snowpipe auto-ingest.
-- =====================================================================

USE DATABASE NORTHWIND_DB;
USE SCHEMA BRONZE;

-- ---------------------------------------------------------------------
-- CATEGORIES (csv)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CATEGORIES (
    CATEGORY_ID     NUMBER,
    CATEGORY_NAME   STRING,
    DESCRIPTION     STRING,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'object_store'
);

COPY INTO BRONZE.CATEGORIES (CATEGORY_ID, CATEGORY_NAME, DESCRIPTION)
FROM @BRONZE.OBJECT_STORE_STAGE/categories.csv.gz
FILE_FORMAT = BRONZE.CSV_FORMAT
ON_ERROR = 'ABORT_STATEMENT';

-- ---------------------------------------------------------------------
-- SUPPLIERS (csv)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.SUPPLIERS (
    SUPPLIER_ID     NUMBER,
    COMPANY_NAME    STRING,
    CONTACT_NAME    STRING,
    CITY            STRING,
    COUNTRY         STRING,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'object_store'
);

COPY INTO BRONZE.SUPPLIERS (SUPPLIER_ID, COMPANY_NAME, CONTACT_NAME, CITY, COUNTRY)
FROM @BRONZE.OBJECT_STORE_STAGE/suppliers.csv.gz
FILE_FORMAT = BRONZE.CSV_FORMAT
ON_ERROR = 'ABORT_STATEMENT';

-- ---------------------------------------------------------------------
-- CUSTOMER_ACTIVITY_EXPORT (csv) — derived recency/frequency export
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_ACTIVITY_EXPORT (
    CUSTOMER_ID             STRING,
    TOTAL_ORDERS            NUMBER,
    FIRST_ORDER_DATE        STRING,
    LAST_ORDER_DATE         STRING,
    DAYS_SINCE_LAST_ORDER   NUMBER,
    _INGEST_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM          STRING DEFAULT 'object_store'
);

COPY INTO BRONZE.CUSTOMER_ACTIVITY_EXPORT
    (CUSTOMER_ID, TOTAL_ORDERS, FIRST_ORDER_DATE, LAST_ORDER_DATE, DAYS_SINCE_LAST_ORDER)
FROM @BRONZE.OBJECT_STORE_STAGE/customer_activity_export.csv.gz
FILE_FORMAT = BRONZE.CSV_FORMAT
ON_ERROR = 'ABORT_STATEMENT';

-- ---------------------------------------------------------------------
-- PRODUCTS (json)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.PRODUCTS_RAW (
    RAW_JSON        VARIANT,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'object_store'
);

COPY INTO BRONZE.PRODUCTS_RAW (RAW_JSON)
FROM @BRONZE.OBJECT_STORE_STAGE/products.json.gz
FILE_FORMAT = BRONZE.JSON_FORMAT
ON_ERROR = 'ABORT_STATEMENT';

CREATE OR REPLACE VIEW BRONZE.PRODUCTS AS
SELECT
    RAW_JSON:ProductID::NUMBER   AS PRODUCT_ID,
    RAW_JSON:ProductName::STRING AS PRODUCT_NAME,
    RAW_JSON:CategoryID::NUMBER  AS CATEGORY_ID,
    RAW_JSON:SupplierID::NUMBER  AS SUPPLIER_ID,
    RAW_JSON:UnitPrice::FLOAT    AS UNIT_PRICE,
    _INGEST_TS,
    _SOURCE_SYSTEM
FROM BRONZE.PRODUCTS_RAW;

-- ---------------------------------------------------------------------
-- REGION LOOKUP (json, single object -> key/value pairs)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.REGION_LOOKUP_RAW (
    RAW_JSON        VARIANT,
    _INGEST_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM  STRING DEFAULT 'object_store'
);

COPY INTO BRONZE.REGION_LOOKUP_RAW (RAW_JSON)
FROM @BRONZE.OBJECT_STORE_STAGE/order_region_lookup.json.gz
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = 'ABORT_STATEMENT';

CREATE OR REPLACE VIEW BRONZE.REGION_LOOKUP AS
SELECT
    KEY::STRING   AS COUNTRY,
    VALUE::STRING AS REGION
FROM BRONZE.REGION_LOOKUP_RAW,
     LATERAL FLATTEN(INPUT => RAW_JSON);
