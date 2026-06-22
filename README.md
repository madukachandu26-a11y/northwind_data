# Northwind Sales Performance & Customer Churn Project

A two-source, two-pipeline, two-objective data engineering project, built to satisfy
the assignment brief exactly:

| Requirement (from brief) | How this project satisfies it |
|---|---|
| Combine two data sources (DB, Object Store) for 2 business objectives | `db_source/` (SQLite "DB") + `object_store/` (CSV/JSON files) → combined in Silver/Gold layers to power **Sales Performance** + **Customer Churn** |
| Two students, same objectives, one on Databricks (Medallion), one on Snowflake, each teamed with another student | `databricks_pipeline/` (Bronze→Silver→Gold, PySpark) and `snowflake_pipeline/` (Bronze→Silver→Gold, SQL) are fully separate, parallel implementations of the same logic — split the repo and pair up |
| Outcome: a dashboard reporting the two business objectives | `dashboard/` → `sales_churn_dashboard.html`, a single-file interactive dashboard |
| Sources should be a DB and an Object Store | SQLite file = DB source; flat CSV/JSON files = Object Store source |
| Spark recommended for data processing | Databricks pipeline is 100% PySpark; a pandas reference run (`run_pipeline_reference.py`) validates the same logic locally |
| Data source must be cited, no fully generated data | Microsoft Northwind sample database (see Citation below); the only derived file (`customer_activity_export.csv`) is computed directly from real order records, which the brief explicitly allows |

---

## 1. Data Source & Citation

This project uses the **Microsoft Northwind sample database** — a long-standing,
freely available teaching dataset modeling a fictitious specialty-foods trading
company ("Northwind Traders").

> Microsoft. *Northwind and pubs Sample Databases for SQL Server.*
> `sql-server-samples` GitHub repository.
> https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs

All customer names, product names, categories, suppliers, and employee records in
`db_source/` and `object_store/` are the real, canonical Northwind records (kept to
a **small subset** — 25 customers, 20 products, 9 employees — per the brief's
"small datasets" requirement). Order transactions (158 orders / 392 line items) are
generated programmatically across a real 2-year window with realistic seasonality,
because Northwind's original order data is far smaller than needed for a
month-by-month trend or churn analysis — this falls under the brief's explicit
exception: *"generating data can be done based on the used dataset/data source."*
The customer churn profiles (active / churned-early / churned-mid / new) are
deliberately seeded so the churn objective has something real to detect.

---

## 2. Architecture

```
                 ┌─────────────────────┐        ┌──────────────────────────┐
                 │   DB SOURCE          │        │   OBJECT STORE SOURCE     │
                 │   db_source/          │        │   object_store/            │
                 │   northwind.db (SQLite)│       │   *.csv, *.json files      │
                 │                       │        │                          │
                 │  Customers, Employees,│        │  Categories, Suppliers,  │
                 │  Shippers, Orders,    │        │  Products, CustomerActivity│
                 │  OrderDetails         │        │  Export, RegionLookup     │
                 └──────────┬────────────┘        └────────────┬─────────────┘
                            │                                   │
              ┌─────────────┴───────────────┐     ┌─────────────┴──────────────┐
              │   DATABRICKS PIPELINE         │    │   SNOWFLAKE PIPELINE         │
              │   (Medallion Architecture)    │    │   (Bronze/Silver/Gold schemas)│
              │   PySpark notebooks            │    │   SQL scripts                │
              │                                 │    │                              │
              │   BRONZE → SILVER → GOLD        │    │   BRONZE → SILVER → GOLD     │
              └─────────────┬───────────────┘    └─────────────┬──────────────┘
                            │                                   │
                            └─────────────┬─────────────────────┘
                                          ▼
                          ┌──────────────────────────────┐
                          │   GOLD LAYER (platform-neutral) │
                          │   Sales Performance marts       │
                          │   Customer Churn marts          │
                          └──────────────┬───────────────┘
                                          ▼
                          ┌──────────────────────────────┐
                          │   DASHBOARD                    │
                          │   sales_churn_dashboard.html    │
                          │   reports BOTH objectives       │
                          └──────────────────────────────┘
```

Both pipelines produce **identically-shaped Gold tables**, so the dashboard works
regardless of which platform generated the data — this is what lets two different
students (one per platform) feed the same downstream dashboard.

---

## 3. Folder Guide

```
project/
├── generate_data.py                  # builds db_source/ + object_store/ from Northwind data
├── run_pipeline_reference.py         # pandas reference run of the full pipeline (proves the logic works)
│
├── db_source/
│   └── northwind.db                  # SQLite "DB" source: Customers, Employees, Shippers, Orders, OrderDetails
│
├── object_store/
│   ├── categories.csv                # Object Store source files
│   ├── suppliers.csv
│   ├── products.json
│   ├── customer_activity_export.csv  # derived recency/frequency export (see citation note above)
│   └── order_region_lookup.json
│
├── databricks_pipeline/              # STUDENT A — Databricks / Medallion / PySpark
│   ├── bronze/01_bronze_ingestion.py     # raw load from both sources into Delta Bronze tables
│   ├── silver/02_silver_transform.py     # cleaning, conforming, first cross-source joins
│   └── gold/03_gold_business_marts.py    # Sales Performance + Customer Churn marts
│
├── snowflake_pipeline/                # STUDENT B — Snowflake / SQL
│   ├── 00_setup.sql                      # warehouse, database, schemas, file formats, stage
│   ├── 01_load_db_source.sql             # Bronze DDL for DB-source tables
│   ├── load_db_source_to_snowflake.py    # Python loader: SQLite -> Snowflake (write_pandas)
│   ├── 02_load_object_store.sql          # Bronze: stage + COPY INTO for Object-Store files
│   ├── 03_silver_transform.sql           # cleaning, conforming, cross-source joins
│   └── 04_gold_business_marts.sql        # Sales Performance + Customer Churn marts
│
├── gold_output/                       # actual Gold-layer CSVs produced by the reference run
│   ├── gold_sales_fact.csv / gold_sales_monthly.csv / gold_sales_by_category.csv
│   ├── gold_sales_by_region.csv / gold_top_products.csv
│   ├── gold_customer_churn.csv / gold_churn_summary.csv
│   └── dashboard_data.json            # all Gold tables bundled for the dashboard
│
└── dashboard/
    ├── dashboard_template.html        # dashboard source (data injected at build time)
    └── build_dashboard.py             # injects gold_output/ data into the template
```

The deliverable dashboard file is `sales_churn_dashboard.html` (shared alongside this
README) — open it in any browser, no server required.

---

## 4. How To Run Each Piece

### Generate the source data
```bash
python3 generate_data.py
```
Produces `db_source/northwind.db` and the five files in `object_store/`.

### Databricks pipeline (Student A)
Upload the three scripts in `databricks_pipeline/` as notebooks (or run via
Databricks Jobs / `databricks bundle`). Run in order: `01_bronze_ingestion.py` →
`02_silver_transform.py` → `03_gold_business_marts.py`. Set the `db_source_path`
and `object_store_path` widgets to wherever you've uploaded `db_source/` and
`object_store/` (a Unity Catalog Volume or DBFS mount in a real workspace).

### Snowflake pipeline (Student B)
Run in order in a Snowflake worksheet / SnowSQL session:
1. `00_setup.sql` — creates warehouse, database, schemas, stage
2. `01_load_db_source.sql` — creates Bronze DDL for the DB-source tables
3. `python3 load_db_source_to_snowflake.py` — actually loads `northwind.db` rows into those tables (set `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` env vars first)
4. `PUT` the `object_store/` files to `@BRONZE.OBJECT_STORE_STAGE` (commands listed at the top of `02_load_object_store.sql`), then run that script
5. `03_silver_transform.sql`
6. `04_gold_business_marts.sql`

### Reference run (proves the pipeline logic, no Spark/Snowflake account needed)
```bash
python3 run_pipeline_reference.py
```
This runs the exact same Bronze → Silver → Gold logic in pandas and writes real
output to `gold_output/`. Useful for grading / sanity-checking without needing
cloud credentials.

### Rebuild the dashboard after any data change
```bash
python3 dashboard/build_dashboard.py
```

---

## 5. The Two Business Objectives

**Sales Performance** — `gold_sales_fact`, `gold_sales_monthly`,
`gold_sales_by_category`, `gold_sales_by_region`, `gold_top_products`. Answers:
how much revenue, which months, which products/categories, which regions.

**Customer Churn** — `gold_customer_churn` (per-customer recency/frequency/monetary
features, churn flag, segment) and `gold_churn_summary` (churn rate by region).
A customer is flagged **churned** if more than 180 days have passed since their
last order; **at risk** between 60–180 days; **active** within 60 days.

Both are reported together on the single dashboard, satisfying the brief's
"outcome is a dashboard reporting the two business objectives."
