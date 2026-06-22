"""
Loads the DB source (northwind.db SQLite tables) into Snowflake BRONZE
tables, using the Snowflake Python connector. This represents how a real
production DB (Postgres/MySQL/SQL Server) would be ingested into
Snowflake -- via a connector / ELT tool (Fivetran, Airbyte) or a direct
JDBC/Python extract-and-load script like this one.

Run after `00_setup.sql` and `01_load_db_source.sql` have created the
target schema and tables.

Usage:
    python load_db_source_to_snowflake.py

Requires environment variables (or edit the CONN dict below):
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

import os
import sqlite3
import pandas as pd

# pip install snowflake-connector-python[pandas]
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "..", "db_source", "northwind.db")

CONN_PARAMS = dict(
    account=os.environ.get("SNOWFLAKE_ACCOUNT", "<your_account>"),
    user=os.environ.get("SNOWFLAKE_USER", "<your_user>"),
    password=os.environ.get("SNOWFLAKE_PASSWORD", "<your_password>"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NORTHWIND_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "NORTHWIND_DB"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "BRONZE"),
)

TABLE_MAP = {
    "Customers": "CUSTOMERS",
    "Employees": "EMPLOYEES",
    "Shippers": "SHIPPERS",
    "Orders": "ORDERS",
    "OrderDetails": "ORDER_DETAILS",
}

COLUMN_MAP = {
    "CUSTOMERS": {
        "CustomerID": "CUSTOMER_ID", "CompanyName": "COMPANY_NAME",
        "ContactName": "CONTACT_NAME", "Country": "COUNTRY",
    },
    "EMPLOYEES": {
        "EmployeeID": "EMPLOYEE_ID", "FirstName": "FIRST_NAME",
        "LastName": "LAST_NAME", "Title": "TITLE",
    },
    "SHIPPERS": {
        "ShipperID": "SHIPPER_ID", "CompanyName": "COMPANY_NAME",
    },
    "ORDERS": {
        "OrderID": "ORDER_ID", "CustomerID": "CUSTOMER_ID",
        "EmployeeID": "EMPLOYEE_ID", "OrderDate": "ORDER_DATE",
        "ShippedDate": "SHIPPED_DATE", "ShipVia": "SHIP_VIA",
        "ShipCountry": "SHIP_COUNTRY",
    },
    "ORDER_DETAILS": {
        "OrderID": "ORDER_ID", "ProductID": "PRODUCT_ID",
        "UnitPrice": "UNIT_PRICE", "Quantity": "QUANTITY",
        "Discount": "DISCOUNT",
    },
}


def main():
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sf_conn = snowflake.connector.connect(**CONN_PARAMS)

    try:
        for sqlite_table, sf_table in TABLE_MAP.items():
            df = pd.read_sql_query(f"SELECT * FROM {sqlite_table}", sqlite_conn)
            df = df.rename(columns=COLUMN_MAP[sf_table])
            success, nchunks, nrows, _ = write_pandas(
                conn=sf_conn,
                df=df,
                table_name=sf_table,
                database=CONN_PARAMS["database"],
                schema=CONN_PARAMS["schema"],
                quote_identifiers=False,
            )
            print(f"{sf_table}: loaded {nrows} rows (success={success})")
    finally:
        sqlite_conn.close()
        sf_conn.close()


if __name__ == "__main__":
    main()
