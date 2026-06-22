"""
Executes the SAME bronze -> silver -> gold transformation logic defined in
the Databricks (PySpark) and Snowflake (SQL) pipelines, using pandas, so we
can produce real Gold-layer output files to power the dashboard and verify
the pipeline logic is correct end-to-end.

This script is a local "reference execution" -- the canonical pipeline code
for actual deployment lives in:
  - databricks_pipeline/{bronze,silver,gold}/*.py  (PySpark)
  - snowflake_pipeline/*.sql                        (Snowflake SQL)
"""

import sqlite3
import json
import os
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "db_source", "northwind.db")
OBJ_DIR = os.path.join(BASE, "object_store")
GOLD_DIR = os.path.join(BASE, "gold_output")
os.makedirs(GOLD_DIR, exist_ok=True)

CHURN_THRESHOLD_DAYS = 180
DATASET_REF_DATE = pd.Timestamp("2025-01-01")

# ======================= BRONZE: raw load =======================
conn = sqlite3.connect(DB_PATH)
customers = pd.read_sql_query("SELECT * FROM Customers", conn)
employees = pd.read_sql_query("SELECT * FROM Employees", conn)
shippers = pd.read_sql_query("SELECT * FROM Shippers", conn)
orders = pd.read_sql_query("SELECT * FROM Orders", conn)
order_details = pd.read_sql_query("SELECT * FROM OrderDetails", conn)
conn.close()

categories = pd.read_csv(os.path.join(OBJ_DIR, "categories.csv"))
suppliers = pd.read_csv(os.path.join(OBJ_DIR, "suppliers.csv"))
activity = pd.read_csv(os.path.join(OBJ_DIR, "customer_activity_export.csv"))
with open(os.path.join(OBJ_DIR, "products.json")) as f:
    products = pd.DataFrame(json.load(f))
with open(os.path.join(OBJ_DIR, "order_region_lookup.json")) as f:
    region_dict = json.load(f)
region_lookup = pd.DataFrame(list(region_dict.items()), columns=["Country", "Region"])

print("BRONZE counts:")
for name, df in [("customers", customers), ("employees", employees), ("orders", orders),
                  ("order_details", order_details), ("categories", categories),
                  ("suppliers", suppliers), ("products", products), ("activity", activity)]:
    print(f"  {name}: {len(df)}")

# ======================= SILVER: clean + conform =======================
s_customers = customers.rename(columns={
    "CustomerID": "customer_id", "CompanyName": "company_name",
    "ContactName": "contact_name", "Country": "country"
}).drop_duplicates("customer_id")
s_customers["customer_id"] = s_customers["customer_id"].str.upper().str.strip()

s_employees = employees.rename(columns={
    "EmployeeID": "employee_id", "FirstName": "first_name",
    "LastName": "last_name", "Title": "title"
}).drop_duplicates("employee_id")

region_lookup["country_key"] = region_lookup["Country"].str.upper().str.strip()

s_orders = orders.rename(columns={
    "OrderID": "order_id", "CustomerID": "customer_id", "EmployeeID": "employee_id",
    "OrderDate": "order_date", "ShippedDate": "shipped_date",
    "ShipVia": "shipper_id", "ShipCountry": "ship_country"
}).drop_duplicates("order_id")
s_orders["customer_id"] = s_orders["customer_id"].str.upper().str.strip()
s_orders["order_date"] = pd.to_datetime(s_orders["order_date"])
s_orders["shipped_date"] = pd.to_datetime(s_orders["shipped_date"])
s_orders["_country_key"] = s_orders["ship_country"].str.upper().str.strip()
s_orders = s_orders.merge(
    region_lookup[["country_key", "Region"]], left_on="_country_key", right_on="country_key", how="left"
).rename(columns={"Region": "region"}).drop(columns=["_country_key", "country_key"])

s_categories = categories.rename(columns={"CategoryID": "category_id", "CategoryName": "category_name"})
s_suppliers = suppliers.rename(columns={
    "SupplierID": "supplier_id", "CompanyName": "supplier_name", "Country": "supplier_country"
})

s_products = products.rename(columns={
    "ProductID": "product_id", "ProductName": "product_name",
    "CategoryID": "category_id", "SupplierID": "supplier_id", "UnitPrice": "unit_price"
}).drop_duplicates("product_id")
s_products = s_products.merge(s_categories[["category_id", "category_name"]], on="category_id", how="left")
s_products = s_products.merge(s_suppliers[["supplier_id", "supplier_name", "supplier_country"]], on="supplier_id", how="left")
s_products["unit_price"] = s_products["unit_price"].round(2)

s_order_details = order_details.rename(columns={
    "OrderID": "order_id", "ProductID": "product_id",
    "UnitPrice": "unit_price", "Quantity": "quantity", "Discount": "discount"
})
s_order_details["unit_price"] = s_order_details["unit_price"].round(2)
s_order_details["discount"] = s_order_details["discount"].round(2)
s_order_details["line_revenue"] = (
    s_order_details["unit_price"] * s_order_details["quantity"] * (1 - s_order_details["discount"])
).round(2)

s_activity = activity.rename(columns={
    "CustomerID": "customer_id", "TotalOrders": "total_orders_export",
    "FirstOrderDate": "first_order_date", "LastOrderDate": "last_order_date",
    "DaysSinceLastOrder": "days_since_last_order"
})
s_activity["customer_id"] = s_activity["customer_id"].str.upper().str.strip()

print("\nSILVER counts:")
for name, df in [("s_customers", s_customers), ("s_employees", s_employees), ("s_orders", s_orders),
                  ("s_products", s_products), ("s_order_details", s_order_details), ("s_activity", s_activity)]:
    print(f"  {name}: {len(df)}")

# ======================= GOLD: business marts =======================

# --- OBJECTIVE 1: Sales Performance ---
gold_sales_fact = (
    s_order_details
    .merge(s_orders[["order_id", "order_date", "shipped_date", "region", "customer_id", "employee_id"]], on="order_id", how="left")
    .merge(s_products[["product_id", "product_name", "category_name", "unit_price"]], on="product_id", how="left", suffixes=("", "_prod"))
    .merge(s_customers[["customer_id", "company_name", "country"]], on="customer_id", how="left")
    .merge(s_employees[["employee_id", "first_name", "last_name"]], on="employee_id", how="left")
)
gold_sales_fact = gold_sales_fact[[
    "order_id", "order_date", "shipped_date", "region", "customer_id", "company_name", "country",
    "employee_id", "first_name", "last_name", "product_id", "product_name", "category_name",
    "quantity", "unit_price", "discount", "line_revenue"
]]
gold_sales_fact.to_csv(os.path.join(GOLD_DIR, "gold_sales_fact.csv"), index=False)

gold_sales_fact["year_month"] = gold_sales_fact["order_date"].dt.strftime("%Y-%m")
gold_sales_monthly = (
    gold_sales_fact.groupby("year_month")
    .agg(total_revenue=("line_revenue", "sum"), order_count=("order_id", "nunique"),
         active_customers=("customer_id", "nunique"))
    .reset_index().sort_values("year_month")
)
gold_sales_monthly["total_revenue"] = gold_sales_monthly["total_revenue"].round(2)
gold_sales_monthly.to_csv(os.path.join(GOLD_DIR, "gold_sales_monthly.csv"), index=False)

gold_sales_by_category = (
    gold_sales_fact.groupby("category_name")
    .agg(total_revenue=("line_revenue", "sum"), units_sold=("quantity", "sum"))
    .reset_index().sort_values("total_revenue", ascending=False)
)
gold_sales_by_category["total_revenue"] = gold_sales_by_category["total_revenue"].round(2)
gold_sales_by_category.to_csv(os.path.join(GOLD_DIR, "gold_sales_by_category.csv"), index=False)

gold_sales_by_region = (
    gold_sales_fact.groupby("region")
    .agg(total_revenue=("line_revenue", "sum"), order_count=("order_id", "nunique"))
    .reset_index().sort_values("total_revenue", ascending=False)
)
gold_sales_by_region["total_revenue"] = gold_sales_by_region["total_revenue"].round(2)
gold_sales_by_region.to_csv(os.path.join(GOLD_DIR, "gold_sales_by_region.csv"), index=False)

gold_top_products = (
    gold_sales_fact.groupby(["product_id", "product_name", "category_name"])
    .agg(total_revenue=("line_revenue", "sum"), units_sold=("quantity", "sum"))
    .reset_index().sort_values("total_revenue", ascending=False).head(10)
)
gold_top_products["total_revenue"] = gold_top_products["total_revenue"].round(2)
gold_top_products.to_csv(os.path.join(GOLD_DIR, "gold_top_products.csv"), index=False)

# --- OBJECTIVE 2: Customer Churn ---
cust_orders_agg = (
    gold_sales_fact.groupby("customer_id")
    .agg(
        company_name=("company_name", "first"),
        country=("country", "first"),
        region=("region", "first"),
        frequency_orders=("order_id", "nunique"),
        monetary_total=("line_revenue", "sum"),
        last_order_date=("order_date", "max"),
        first_order_date=("order_date", "min"),
    ).reset_index()
)
cust_orders_agg["monetary_total"] = cust_orders_agg["monetary_total"].round(2)

gold_customer_churn = cust_orders_agg.merge(
    s_activity[["customer_id", "days_since_last_order"]], on="customer_id", how="left"
)
gold_customer_churn["recency_days"] = gold_customer_churn.apply(
    lambda r: r["days_since_last_order"] if pd.notna(r["days_since_last_order"])
    else (DATASET_REF_DATE - r["last_order_date"]).days,
    axis=1
)
gold_customer_churn["avg_order_value"] = (
    gold_customer_churn["monetary_total"] / gold_customer_churn["frequency_orders"]
).round(2)
gold_customer_churn["is_churned"] = gold_customer_churn["recency_days"] > CHURN_THRESHOLD_DAYS
gold_customer_churn["customer_segment"] = gold_customer_churn["recency_days"].apply(
    lambda d: "Churned" if d > CHURN_THRESHOLD_DAYS else ("Active" if d <= 60 else "At Risk")
)
gold_customer_churn = gold_customer_churn.drop(columns=["days_since_last_order"])
gold_customer_churn.to_csv(os.path.join(GOLD_DIR, "gold_customer_churn.csv"), index=False)

gold_churn_summary = (
    gold_customer_churn.groupby("region")
    .agg(total_customers=("customer_id", "count"), churned_customers=("is_churned", "sum"))
    .reset_index()
)
gold_churn_summary["churn_rate_pct"] = (
    gold_churn_summary["churned_customers"] / gold_churn_summary["total_customers"] * 100
).round(1)
gold_churn_summary = gold_churn_summary.sort_values("churn_rate_pct", ascending=False)
gold_churn_summary.to_csv(os.path.join(GOLD_DIR, "gold_churn_summary.csv"), index=False)

print("\nGOLD layer written to gold_output/:")
for f in sorted(os.listdir(GOLD_DIR)):
    print(" -", f)

print("\n--- Sales Monthly ---")
print(gold_sales_monthly.to_string(index=False))
print("\n--- Churn Summary by Region ---")
print(gold_churn_summary.to_string(index=False))
print("\n--- Top Products ---")
print(gold_top_products.to_string(index=False))
