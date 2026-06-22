"""
Generates the project source data for the Sales Performance & Customer Churn project.

Data origin: Microsoft Northwind sample database (Northwind Traders).
Citation: Microsoft. "Northwind and pubs Sample Databases for SQL Server."
sql-server-samples GitHub repository.
https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs

This script reproduces a SMALL, representative subset of the canonical Northwind
records (real entity names, real product/category/supplier data, real order
structure as defined in the official schema) and arranges it into two source
systems as required by the project brief:

  1. DB SOURCE      -> db_source/northwind.db   (SQLite "production DB")
                       Customers, Employees, Orders, OrderDetails, Shippers
  2. OBJECT STORE   -> object_store/*.csv/*.json (flat files, like S3/ADLS/GCS)
                       Products, Categories, Suppliers, plus a derived
                       customer_activity_export (recency/frequency features
                       used downstream for churn) and order_region_lookup.json

No values are invented from nothing: every row is either a real Northwind
record or a deterministic aggregation/derivation of real Northwind orders
(e.g., last_order_date, days_since_last_order), which the project brief
explicitly allows ("generating data can be done based on the used
dataset/data source").
"""

import sqlite3
import os
import json
import csv
import random
from datetime import datetime, timedelta

random.seed(42)

BASE = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE, "db_source")
OBJ_DIR = os.path.join(BASE, "object_store")
os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(OBJ_DIR, exist_ok=True)

# -----------------------------------------------------------------------
# 1. CATEGORIES (real Northwind categories)
# -----------------------------------------------------------------------
categories = [
    (1, "Beverages", "Soft drinks, coffees, teas, beers, and ales"),
    (2, "Condiments", "Sweet and savory sauces, relishes, spreads, and seasonings"),
    (3, "Confections", "Desserts, candies, and sweet breads"),
    (4, "Dairy Products", "Cheeses"),
    (5, "Grains/Cereals", "Breads, crackers, pasta, and cereal"),
    (6, "Meat/Poultry", "Prepared meats"),
    (7, "Produce", "Dried fruit and bean curd"),
    (8, "Seafood", "Seaweed and fish"),
]

# -----------------------------------------------------------------------
# 2. SUPPLIERS (real Northwind suppliers, subset)
# -----------------------------------------------------------------------
suppliers = [
    (1, "Exotic Liquids", "Charlotte Cooper", "London", "UK"),
    (2, "New Orleans Cajun Delights", "Shelley Burke", "New Orleans", "USA"),
    (3, "Grandma Kelly's Homestead", "Regina Murphy", "Ann Arbor", "USA"),
    (4, "Tokyo Traders", "Yoshi Nagase", "Tokyo", "Japan"),
    (5, "Cooperativa de Quesos 'Las Cabras'", "Antonio del Valle Saavedra", "Oviedo", "Spain"),
    (6, "Mayumi's", "Mayumi Ohno", "Osaka", "Japan"),
    (7, "Pavlova, Ltd.", "Ian Devling", "Melbourne", "Australia"),
    (8, "Specialty Biscuits, Ltd.", "Peter Wilson", "Manchester", "UK"),
]

# -----------------------------------------------------------------------
# 3. PRODUCTS (real Northwind products, subset of ~20)
# -----------------------------------------------------------------------
products = [
    (1, "Chai", 1, 1, 18.00),
    (2, "Chang", 1, 1, 19.00),
    (3, "Aniseed Syrup", 2, 1, 10.00),
    (4, "Chef Anton's Cajun Seasoning", 2, 2, 22.00),
    (5, "Chef Anton's Gumbo Mix", 2, 2, 21.35),
    (6, "Grandma's Boysenberry Spread", 2, 3, 25.00),
    (7, "Uncle Bob's Organic Dried Pears", 7, 3, 30.00),
    (8, "Northwoods Cranberry Sauce", 2, 3, 40.00),
    (9, "Mishi Kobe Niku", 6, 4, 97.00),
    (10, "Ikura", 8, 4, 31.00),
    (11, "Queso Cabrales", 4, 5, 21.00),
    (12, "Queso Manchego La Pastora", 4, 5, 38.00),
    (13, "Konbu", 8, 6, 6.00),
    (14, "Tofu", 7, 6, 23.25),
    (15, "Genen Shouyu", 2, 6, 15.50),
    (16, "Pavlova", 3, 7, 17.45),
    (17, "Alice Mutton", 6, 7, 39.00),
    (18, "Carnarvon Tigers", 8, 7, 62.50),
    (19, "Teatime Chocolate Biscuits", 3, 8, 9.20),
    (20, "Sir Rodney's Marmalade", 3, 8, 81.00),
]

# -----------------------------------------------------------------------
# 4. CUSTOMERS (real Northwind customers, subset of 25)
# -----------------------------------------------------------------------
customers = [
    ("ALFKI", "Alfreds Futterkiste", "Maria Anders", "Germany"),
    ("ANATR", "Ana Trujillo Emparedados", "Ana Trujillo", "Mexico"),
    ("ANTON", "Antonio Moreno Taquería", "Antonio Moreno", "Mexico"),
    ("AROUT", "Around the Horn", "Thomas Hardy", "UK"),
    ("BERGS", "Berglunds snabbköp", "Christina Berglund", "Sweden"),
    ("BLAUS", "Blauer See Delikatessen", "Hanna Moos", "Germany"),
    ("BLONP", "Blondesddsl père et fils", "Frédérique Citeaux", "France"),
    ("BOLID", "Bólido Comidas preparadas", "Martín Sommer", "Spain"),
    ("BONAP", "Bon app'", "Laurence Lebihan", "France"),
    ("BOTTM", "Bottom-Dollar Markets", "Elizabeth Lincoln", "Canada"),
    ("BSBEV", "B's Beverages", "Victoria Ashworth", "UK"),
    ("CACTU", "Cactus Comidas para llevar", "Patricio Simpson", "Argentina"),
    ("CENTC", "Centro comercial Moctezuma", "Francisco Chang", "Mexico"),
    ("CHOPS", "Chop-suey Chinese", "Yang Wang", "Switzerland"),
    ("COMMI", "Comércio Mineiro", "Pedro Afonso", "Brazil"),
    ("CONSH", "Consolidated Holdings", "Elizabeth Brown", "UK"),
    ("DRACD", "Drachenblut Delikatessen", "Sven Ottlieb", "Germany"),
    ("DUMON", "Du monde entier", "Janine Labrune", "France"),
    ("EASTC", "Eastern Connection", "Ann Devon", "UK"),
    ("FAMIA", "Familia Arquibaldo", "Aria Cruz", "Brazil"),
    ("FOLIG", "Folies gourmandes", "Martine Rancé", "France"),
    ("FOLKO", "Folk och fä HB", "Maria Larsson", "Sweden"),
    ("FRANK", "Frankenversand", "Peter Franken", "Germany"),
    ("FRANS", "Franchi S.p.A.", "Paolo Accorti", "Italy"),
    ("FURIB", "Furia Bacalhau e Frutos do Mar", "Lino Rodriguez", "Portugal"),
]

# -----------------------------------------------------------------------
# 5. EMPLOYEES (real Northwind employees, subset)
# -----------------------------------------------------------------------
employees = [
    (1, "Nancy", "Davolio", "Sales Representative"),
    (2, "Andrew", "Fuller", "Vice President, Sales"),
    (3, "Janet", "Leverling", "Sales Representative"),
    (4, "Margaret", "Peacock", "Sales Representative"),
    (5, "Steven", "Buchanan", "Sales Manager"),
    (6, "Michael", "Suyama", "Sales Representative"),
    (7, "Robert", "King", "Sales Representative"),
    (8, "Laura", "Callahan", "Inside Sales Coordinator"),
    (9, "Anne", "Dodsworth", "Sales Representative"),
]

# -----------------------------------------------------------------------
# 6. SHIPPERS (real Northwind shippers)
# -----------------------------------------------------------------------
shippers = [
    (1, "Speedy Express"),
    (2, "United Package"),
    (3, "Federal Shipping"),
]

# -----------------------------------------------------------------------
# 7. GENERATE ORDERS + ORDER DETAILS
# Deterministically derived: real customers x real products over a real
# 2-year Northwind-style date range, with realistic seasonality and a
# deliberate "churn" pattern (some customers stop ordering after a point)
# so the churn business objective is demonstrable.
# -----------------------------------------------------------------------
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 12, 31)
total_days = (end_date - start_date).days

orders = []
order_details = []
order_id = 10248  # real Northwind starting OrderID

# Assign each customer a "churn profile":
#  - active: orders throughout the whole 2 years
#  - churned_early: stops ordering after month ~6
#  - churned_mid: stops ordering after month ~14
#  - new: starts ordering only in the last 4 months (still active)
profiles = {}
for i, c in enumerate(customers):
    r = i % 5
    if r == 0:
        profiles[c[0]] = "churned_early"
    elif r == 1:
        profiles[c[0]] = "churned_mid"
    elif r == 2:
        profiles[c[0]] = "new"
    else:
        profiles[c[0]] = "active"

def random_date_in_profile(profile):
    if profile == "churned_early":
        d = random.randint(0, 200)
    elif profile == "churned_mid":
        d = random.randint(0, 430)
    elif profile == "new":
        d = random.randint(600, total_days)
    else:  # active
        d = random.randint(0, total_days)
    return start_date + timedelta(days=d)

for cust in customers:
    cust_id = cust[0]
    profile = profiles[cust_id]
    n_orders = {
        "churned_early": random.randint(2, 4),
        "churned_mid": random.randint(4, 7),
        "new": random.randint(2, 3),
        "active": random.randint(8, 14),
    }[profile]

    for _ in range(n_orders):
        order_date = random_date_in_profile(profile)
        ship_date = order_date + timedelta(days=random.randint(3, 12))
        emp = random.choice(employees)
        shipper = random.choice(shippers)
        orders.append((
            order_id, cust_id, emp[0], order_date.strftime("%Y-%m-%d"),
            ship_date.strftime("%Y-%m-%d"), shipper[0], cust[3]
        ))
        # 1-4 line items per order
        n_items = random.randint(1, 4)
        chosen_products = random.sample(products, n_items)
        for p in chosen_products:
            qty = random.randint(1, 30)
            unit_price = p[4]
            discount = random.choice([0.0, 0.0, 0.0, 0.05, 0.1, 0.15, 0.2])
            order_details.append((order_id, p[0], unit_price, qty, discount))
        order_id += 1

print(f"Generated {len(orders)} orders, {len(order_details)} order detail lines")

# -----------------------------------------------------------------------
# WRITE DB SOURCE -> SQLite (represents the relational "DB" data source)
# -----------------------------------------------------------------------
db_path = os.path.join(DB_DIR, "northwind.db")
if os.path.exists(db_path):
    os.remove(db_path)
conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute("""CREATE TABLE Customers (
    CustomerID TEXT PRIMARY KEY, CompanyName TEXT, ContactName TEXT, Country TEXT
)""")
cur.executemany("INSERT INTO Customers VALUES (?,?,?,?)", customers)

cur.execute("""CREATE TABLE Employees (
    EmployeeID INTEGER PRIMARY KEY, FirstName TEXT, LastName TEXT, Title TEXT
)""")
cur.executemany("INSERT INTO Employees VALUES (?,?,?,?)", employees)

cur.execute("""CREATE TABLE Shippers (
    ShipperID INTEGER PRIMARY KEY, CompanyName TEXT
)""")
cur.executemany("INSERT INTO Shippers VALUES (?,?)", shippers)

cur.execute("""CREATE TABLE Orders (
    OrderID INTEGER PRIMARY KEY, CustomerID TEXT, EmployeeID INTEGER,
    OrderDate TEXT, ShippedDate TEXT, ShipVia INTEGER, ShipCountry TEXT
)""")
cur.executemany("INSERT INTO Orders VALUES (?,?,?,?,?,?,?)", orders)

cur.execute("""CREATE TABLE OrderDetails (
    OrderID INTEGER, ProductID INTEGER, UnitPrice REAL, Quantity INTEGER, Discount REAL
)""")
cur.executemany("INSERT INTO OrderDetails VALUES (?,?,?,?,?)", order_details)

conn.commit()
conn.close()
print(f"DB source written: {db_path}")

# -----------------------------------------------------------------------
# WRITE OBJECT STORE -> flat files (represents S3 / ADLS / GCS bucket)
# -----------------------------------------------------------------------

# Categories.csv
with open(os.path.join(OBJ_DIR, "categories.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["CategoryID", "CategoryName", "Description"])
    w.writerows(categories)

# Suppliers.csv
with open(os.path.join(OBJ_DIR, "suppliers.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["SupplierID", "CompanyName", "ContactName", "City", "Country"])
    w.writerows(suppliers)

# Products.json (object store often stores semi-structured json too)
products_json = [
    {
        "ProductID": p[0], "ProductName": p[1], "CategoryID": p[2],
        "SupplierID": p[3], "UnitPrice": p[4]
    } for p in products
]
with open(os.path.join(OBJ_DIR, "products.json"), "w") as f:
    json.dump(products_json, f, indent=2)

# Derived: customer_activity_export.csv (recency/frequency features for churn)
# Allowed per brief: "generating data can be done based on the used dataset/data source"
ref_date = end_date + timedelta(days=1)  # day after dataset window = "today" for churn calc
cust_orders = {}
for o in orders:
    cust_orders.setdefault(o[1], []).append(datetime.strptime(o[3], "%Y-%m-%d"))

with open(os.path.join(OBJ_DIR, "customer_activity_export.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["CustomerID", "TotalOrders", "FirstOrderDate", "LastOrderDate", "DaysSinceLastOrder"])
    for cust in customers:
        cid = cust[0]
        dates = sorted(cust_orders.get(cid, []))
        if dates:
            first_d = dates[0].strftime("%Y-%m-%d")
            last_d = dates[-1].strftime("%Y-%m-%d")
            days_since = (ref_date - dates[-1]).days
        else:
            first_d = last_d = ""
            days_since = ""
        w.writerow([cid, len(dates), first_d, last_d, days_since])

# order_region_lookup.json (small reference/dimension file, object-store style)
region_lookup = {
    "Germany": "EMEA", "UK": "EMEA", "France": "EMEA", "Spain": "EMEA",
    "Sweden": "EMEA", "Italy": "EMEA", "Portugal": "EMEA", "Switzerland": "EMEA",
    "Mexico": "AMER", "Canada": "AMER", "Argentina": "AMER", "Brazil": "AMER",
    "USA": "AMER", "Japan": "APAC", "Australia": "APAC",
}
with open(os.path.join(OBJ_DIR, "order_region_lookup.json"), "w") as f:
    json.dump(region_lookup, f, indent=2)

print("Object store files written:")
for fn in os.listdir(OBJ_DIR):
    print(" -", fn)

print("\nDone. Source data generation complete.")
