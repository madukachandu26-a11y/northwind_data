# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD LAYER — Business Aggregates
# MAGIC Builds the two business-objective marts that feed the dashboard:
# MAGIC 1. **Sales Performance** — revenue by period/product/category/employee/region
# MAGIC 2. **Customer Churn** — RFM-style features, churn flag, churn rate by segment
# MAGIC
# MAGIC Output tables are written in a platform-neutral schema so the same
# MAGIC dashboard can read Gold tables produced by either Databricks or Snowflake.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("silver_schema", "northwind_silver")
dbutils.widgets.text("gold_schema", "northwind_gold")
dbutils.widgets.text("churn_threshold_days", "180")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
CHURN_THRESHOLD_DAYS = int(dbutils.widgets.get("churn_threshold_days"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = lambda name: spark.table(f"{catalog}.{silver_schema}.silver_{name}")

orders = silver("orders")
order_details = silver("order_details")
customers = silver("customers")
products = silver("products")
employees = silver("employees")
activity = silver("customer_activity")

# COMMAND ----------

# MAGIC %md ## OBJECTIVE 1: Sales Performance
# MAGIC ### gold_sales_fact — line-item grain fact table

# COMMAND ----------

gold_sales_fact = (
    order_details
    .join(orders, "order_id", "left")
    .join(products.select("product_id", "product_name", "category_name", "unit_price"), "product_id", "left")
    .join(customers.select("customer_id", "company_name", "country"), "customer_id", "left")
    .join(employees.select("employee_id", "first_name", "last_name"), "employee_id", "left")
    .select(
        "order_id", "order_date", "shipped_date", "region",
        "customer_id", "company_name", "country",
        "employee_id", "first_name", "last_name",
        "product_id", "product_name", "category_name",
        "quantity", "unit_price", "discount", "line_revenue",
    )
)
gold_sales_fact.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{gold_schema}.gold_sales_fact")
print("gold_sales_fact:", gold_sales_fact.count())

# COMMAND ----------

# MAGIC %md ### gold_sales_monthly — revenue trend over time

# COMMAND ----------

gold_sales_monthly = (
    gold_sales_fact
    .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("active_customers"),
    )
    .orderBy("year_month")
)
gold_sales_monthly.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{gold_schema}.gold_sales_monthly")
print("gold_sales_monthly:", gold_sales_monthly.count())

# COMMAND ----------

# MAGIC %md ### gold_sales_by_category, gold_sales_by_region, gold_top_products

# COMMAND ----------

gold_sales_by_category = (
    gold_sales_fact.groupBy("category_name")
    .agg(F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
         F.sum("quantity").alias("units_sold"))
    .orderBy(F.desc("total_revenue"))
)
gold_sales_by_category.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{gold_schema}.gold_sales_by_category")

gold_sales_by_region = (
    gold_sales_fact.groupBy("region")
    .agg(F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
         F.countDistinct("order_id").alias("order_count"))
    .orderBy(F.desc("total_revenue"))
)
gold_sales_by_region.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{gold_schema}.gold_sales_by_region")

gold_top_products = (
    gold_sales_fact.groupBy("product_id", "product_name", "category_name")
    .agg(F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
         F.sum("quantity").alias("units_sold"))
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)
gold_top_products.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{gold_schema}.gold_top_products")

print("gold_sales_by_category:", gold_sales_by_category.count())
print("gold_sales_by_region:", gold_sales_by_region.count())
print("gold_top_products:", gold_top_products.count())

# COMMAND ----------

# MAGIC %md ## OBJECTIVE 2: Customer Churn
# MAGIC ### gold_customer_churn — RFM features + churn flag per customer

# COMMAND ----------

DATASET_REF_DATE = F.lit("2025-01-01")  # day after the dataset's last order window

customer_orders_agg = (
    gold_sales_fact.groupBy("customer_id", "company_name", "country", "region")
    .agg(
        F.countDistinct("order_id").alias("frequency_orders"),
        F.round(F.sum("line_revenue"), 2).alias("monetary_total"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date"),
    )
)

gold_customer_churn = (
    customer_orders_agg
    .join(activity.select("customer_id", "days_since_last_order"), "customer_id", "left")
    .withColumn(
        "recency_days",
        F.coalesce(
            F.col("days_since_last_order"),
            F.datediff(F.to_date(DATASET_REF_DATE), F.col("last_order_date")),
        ),
    )
    .withColumn(
        "avg_order_value",
        F.round(F.col("monetary_total") / F.col("frequency_orders"), 2),
    )
    .withColumn(
        "is_churned",
        F.when(F.col("recency_days") > CHURN_THRESHOLD_DAYS, F.lit(True)).otherwise(F.lit(False)),
    )
    .withColumn(
        "customer_segment",
        F.when(F.col("recency_days") > CHURN_THRESHOLD_DAYS, "Churned")
         .when(F.col("recency_days") <= 60, "Active")
         .otherwise("At Risk"),
    )
    .drop("days_since_last_order")
)

gold_customer_churn.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{gold_schema}.gold_customer_churn")
print("gold_customer_churn:", gold_customer_churn.count())

# COMMAND ----------

# MAGIC %md ### gold_churn_summary — churn rate by region/segment (dashboard KPI source)

# COMMAND ----------

gold_churn_summary = (
    gold_customer_churn.groupBy("region")
    .agg(
        F.count("*").alias("total_customers"),
        F.sum(F.when(F.col("is_churned"), 1).otherwise(0)).alias("churned_customers"),
        F.round(
            F.sum(F.when(F.col("is_churned"), 1).otherwise(0)) / F.count("*") * 100, 1
        ).alias("churn_rate_pct"),
    )
    .orderBy(F.desc("churn_rate_pct"))
)
gold_churn_summary.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{catalog}.{gold_schema}.gold_churn_summary")
print("gold_churn_summary:", gold_churn_summary.count())

# COMMAND ----------

# MAGIC %md ## Gold layer summary

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {catalog}.{gold_schema}").collect()
for t in tables:
    cnt = spark.table(f"{catalog}.{gold_schema}.{t.tableName}").count()
    print(f"{t.tableName:30s} {cnt:>6} rows")

# COMMAND ----------

# MAGIC %md ## Export Gold tables to CSV (for the dashboard / handoff)

# COMMAND ----------

export_path = "/Volumes/main/northwind/exports"
for t in ["gold_sales_fact", "gold_sales_monthly", "gold_sales_by_category",
          "gold_sales_by_region", "gold_top_products", "gold_customer_churn",
          "gold_churn_summary"]:
    spark.table(f"{catalog}.{gold_schema}.{t}").toPandas().to_csv(
        f"{export_path}/{t}.csv", index=False
    )
print("Exports complete.")
