# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LAYER — Cleaned & Conformed
# MAGIC Reads Bronze tables, applies cleaning, type casting, deduplication, and
# MAGIC joins the **two source systems** (DB source + Object Store source) into
# MAGIC unified, business-ready entities.
# MAGIC
# MAGIC This is the layer where DB-origin data (Orders, Customers, Employees) and
# MAGIC Object-Store-origin data (Products, Categories, Suppliers, Region lookup)
# MAGIC are combined for the first time.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("bronze_schema", "northwind_bronze")
dbutils.widgets.text("silver_schema", "northwind_silver")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, trim, upper, round as sround, current_timestamp
)

bronze = lambda name: spark.table(f"{catalog}.{bronze_schema}.bronze_{name}")

# COMMAND ----------

# MAGIC %md ## silver_customers — from DB source

# COMMAND ----------

silver_customers = (
    bronze("customers")
    .select(
        upper(trim(col("CustomerID"))).alias("customer_id"),
        trim(col("CompanyName")).alias("company_name"),
        trim(col("ContactName")).alias("contact_name"),
        trim(col("Country")).alias("country"),
    )
    .dropDuplicates(["customer_id"])
)
silver_customers.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{silver_schema}.silver_customers")
print("silver_customers:", silver_customers.count())

# COMMAND ----------

# MAGIC %md ## silver_employees — from DB source

# COMMAND ----------

silver_employees = (
    bronze("employees")
    .select(
        col("EmployeeID").cast("int").alias("employee_id"),
        trim(col("FirstName")).alias("first_name"),
        trim(col("LastName")).alias("last_name"),
        trim(col("Title")).alias("title"),
    )
    .dropDuplicates(["employee_id"])
)
silver_employees.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{silver_schema}.silver_employees")
print("silver_employees:", silver_employees.count())

# COMMAND ----------

# MAGIC %md ## silver_orders — from DB source, with Region enrichment from Object Store

# COMMAND ----------

region_lookup = bronze("region_lookup").select(
    upper(trim(col("Country"))).alias("country_key"),
    col("Region").alias("region"),
)

silver_orders = (
    bronze("orders")
    .select(
        col("OrderID").cast("int").alias("order_id"),
        upper(trim(col("CustomerID"))).alias("customer_id"),
        col("EmployeeID").cast("int").alias("employee_id"),
        to_date(col("OrderDate")).alias("order_date"),
        to_date(col("ShippedDate")).alias("shipped_date"),
        col("ShipVia").cast("int").alias("shipper_id"),
        trim(col("ShipCountry")).alias("ship_country"),
    )
    .dropDuplicates(["order_id"])
    .join(
        region_lookup,
        upper(trim(col("ship_country"))) == col("country_key"),
        "left",
    )
    .drop("country_key")
)
silver_orders.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{silver_schema}.silver_orders")
print("silver_orders:", silver_orders.count())

# COMMAND ----------

# MAGIC %md ## silver_products — from Object Store, enriched with Category & Supplier (also Object Store)

# COMMAND ----------

categories = bronze("categories").select(
    col("CategoryID").cast("int").alias("category_id"),
    trim(col("CategoryName")).alias("category_name"),
)
suppliers = bronze("suppliers").select(
    col("SupplierID").cast("int").alias("supplier_id"),
    trim(col("CompanyName")).alias("supplier_name"),
    trim(col("Country")).alias("supplier_country"),
)

silver_products = (
    bronze("products")
    .select(
        col("ProductID").cast("int").alias("product_id"),
        trim(col("ProductName")).alias("product_name"),
        col("CategoryID").cast("int").alias("category_id"),
        col("SupplierID").cast("int").alias("supplier_id"),
        sround(col("UnitPrice").cast("double"), 2).alias("unit_price"),
    )
    .dropDuplicates(["product_id"])
    .join(categories, "category_id", "left")
    .join(suppliers, "supplier_id", "left")
)
silver_products.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{silver_schema}.silver_products")
print("silver_products:", silver_products.count())

# COMMAND ----------

# MAGIC %md ## silver_order_details — from DB source, with line totals

# COMMAND ----------

silver_order_details = (
    bronze("orderdetails")
    .select(
        col("OrderID").cast("int").alias("order_id"),
        col("ProductID").cast("int").alias("product_id"),
        sround(col("UnitPrice").cast("double"), 2).alias("unit_price"),
        col("Quantity").cast("int").alias("quantity"),
        sround(col("Discount").cast("double"), 2).alias("discount"),
    )
    .withColumn(
        "line_revenue",
        sround(col("unit_price") * col("quantity") * (1 - col("discount")), 2),
    )
)
silver_order_details.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{silver_schema}.silver_order_details")
print("silver_order_details:", silver_order_details.count())

# COMMAND ----------

# MAGIC %md ## silver_customer_activity — from Object Store (pre-computed recency export)

# COMMAND ----------

silver_customer_activity = (
    bronze("customer_activity_export")
    .select(
        upper(trim(col("CustomerID"))).alias("customer_id"),
        col("TotalOrders").cast("int").alias("total_orders_export"),
        to_date(col("FirstOrderDate")).alias("first_order_date"),
        to_date(col("LastOrderDate")).alias("last_order_date"),
        col("DaysSinceLastOrder").cast("int").alias("days_since_last_order"),
    )
)
silver_customer_activity.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{silver_schema}.silver_customer_activity")
print("silver_customer_activity:", silver_customer_activity.count())

# COMMAND ----------

# MAGIC %md ## Silver layer summary

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {catalog}.{silver_schema}").collect()
for t in tables:
    cnt = spark.table(f"{catalog}.{silver_schema}.{t.tableName}").count()
    print(f"{t.tableName:35s} {cnt:>6} rows")
