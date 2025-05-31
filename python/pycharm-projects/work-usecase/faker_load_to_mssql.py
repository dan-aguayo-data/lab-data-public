import random
from datetime import datetime, timedelta
from faker import Faker
import pyodbc

fake = Faker()
Faker.seed(0)
random.seed(0)

# SQL Server connection setup
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=XXXXXX;"
    "DATABASE=XXXX;"
    "UID=XXXXXX;"
    "PWD=XXXXX"
)
cursor = conn.cursor()

# Parameters
rows_to_insert = 100_000
sales_order_id_start = 100000
start_date = datetime.now() - timedelta(days=730)  # 2 years ago
end_date = datetime.now()

def random_customer_id():
    if random.random() < 0.5:
        return random.randint(1, 701)
    else:
        return random.randint(29485, 30118)

def random_product_id():
    return random.randint(680, 999)

def random_order_date():
    return fake.date_time_between(start_date=start_date, end_date=end_date)

# Insert rows
for i in range(rows_to_insert):
    sales_order_id = sales_order_id_start + i
    order_date = random_order_date()
    customer_id = random_customer_id()
    sales_qty = random.randint(1, 1000)
    product_id = random_product_id()
    nsv = round(random.uniform(20, 20000), 2)
    modified_date = order_date  # Optional: use same as order

    cursor.execute("""
        INSERT INTO SalesLT.FactSalesTable (
            SalesOrderID, OrderDate, CustomerID, SalesQty,
            ProductID, NSV, ModifiedDate
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, sales_order_id, order_date, customer_id, sales_qty, product_id, nsv, modified_date)

    if i % 1000 == 0:
        conn.commit()

conn.commit()
cursor.close()
conn.close()
print(f"{rows_to_insert} rows inserted.")
