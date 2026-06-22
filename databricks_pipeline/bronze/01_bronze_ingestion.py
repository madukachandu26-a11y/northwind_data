# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE LAYER — Raw Ingestion
# MAGIC **Project:** Sales Performance & Customer Churn Dashboard
# MAGIC **Platform:** Databricks (Medallion Architecture)
# MAGIC **Sources:**
# MAGIC 1. **DB source** — `northwind.db` (SQLite, simulating an OLTP database) → Customers, Employees, Shippers, Orders, OrderDetails
# MAGIC 2. **Object Store source** — CSV/JSON files (simulating S3 / ADLS / GCS) → Categories, Suppliers, Products, CustomerActivityExport, RegionLookup
# MAGIC
# MAGIC Bronze = raw, untransformed, schema-on-read. We just land the data into Delta tables exactly as it arrived, adding ingestion metadata.
# MAGIC
# MAGIC **Data citation:** Microsoft Northwind sample database —
# MAGIC https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs

# COMMAND ----------

# MAGIC %md ### Widgets / config

# COMMAND ----------

dbutils.widgets.text("db_source_path", "/Volumes/workspace/default/northwind_raw/northwind.db")
dbutils.widgets.text("object_store_path", "/Volumes/workspace/default/northwind_raw")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "northwind_bronze")

db_source_path = dbutils.widgets.get("db_source_path")
object_store_path = dbutils.widgets.get("object_store_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## 1. Ingest the DB source (SQLite via JDBC)
# MAGIC In production this would be a JDBC/ODBC connection to Postgres/SQL Server/etc.
# MAGIC Here we read the SQLite file directly via the sqlite3 driver into pandas, then
# MAGIC promote to Spark — demonstrating the DB ingestion path required by the brief.

# COMMAND ----------

import sqlite3
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit

def load_sqlite_table(sqlite_path: str, table_name: str):
    conn = sqlite3.connect(sqlite_path)
    pdf = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    sdf = spark.createDataFrame(pdf)
    return (
        sdf.withColumn("_ingest_ts", current_timestamp())
           .withColumn("_source_system", lit("db_source_sqlite"))
    )

db_tables = ["Customers", "Employees", "Shippers", "Orders", "OrderDetails"]

for t in db_tables:
    df = load_sqlite_table(db_source_path, t)
    target = f"bronze_{t.lower()}"
    df.write.mode("overwrite").format("delta").saveAsTable(target)
    print(f"Loaded {target}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md ## 2. Ingest the Object Store source (CSV/JSON)
# MAGIC Simulates reading files staged in cloud object storage (S3/ADLS/GCS),
# MAGIC mounted here as a Unity Catalog Volume / DBFS path.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# CSV files
csv_files = {
    "categories": "categories.csv",
    "suppliers": "suppliers.csv",
    "customer_activity_export": "customer_activity_export.csv",
}

for target_suffix, filename in csv_files.items():
    path = f"{object_store_path}/{filename}"
    df = (
        spark.read.option("header", True).option("inferSchema", True).csv(path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_system", lit("object_store"))
    )
    target = f"bronze_{target_suffix}"
    df.write.mode("overwrite").format("delta").saveAsTable(target)
    print(f"Loaded {target}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md ### JSON files (Products, Region lookup)

# COMMAND ----------

json_files = {
    "products": "products.json",
    "region_lookup": "order_region_lookup.json",
}

# products.json is a list of records -> multiLine read works directly
df_products = (
    spark.read.option("multiLine", True).json(f"{object_store_path}/products.json")
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_system", lit("object_store"))
)
df_products.write.mode("overwrite").format("delta").saveAsTable("bronze_products")
print(f"Loaded bronze_products: {df_products.count()} rows")

# region_lookup.json is a single dict {country: region} -> read and explode manually
import json as _json

with open(f"{object_store_path}/order_region_lookup.json") as f:
    region_dict = _json.load(f)

region_pdf = pd.DataFrame(list(region_dict.items()), columns=["Country", "Region"])
df_region = (
    spark.createDataFrame(region_pdf)
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_system", lit("object_store"))
)
df_region.write.mode("overwrite").format("delta").saveAsTable("bronze_region_lookup")
print(f"Loaded bronze_region_lookup: {df_region.count()} rows")

# COMMAND ----------

# MAGIC %md ## Bronze layer summary

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
for t in tables:
    cnt = spark.table(f"{catalog}.{schema}.{t.tableName}").count()
    print(f"{t.tableName:35s} {cnt:>6} rows")
