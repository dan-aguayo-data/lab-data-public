import random
from datetime import datetime, timedelta
from faker import Faker
import pyodbc

fake = Faker()
Faker.seed(0)
random.seed(0)

# DB Connection
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=XXXXXX;"
    "DATABASE=XXXX;"
    "UID=XXXXXX;"
    "PWD=XXXXX"
)
cursor = conn.cursor()

# Parameters
sales_order_id = 100000
start_date = datetime.now() - timedelta(days=730)
end_date = datetime.now()

def random_customer_id():
    return random.randint(1, 701) if random.random() < 0.5 else random.randint(29485, 30118)

def random_product_id():
    return random.randint(680, 999)

def insert_sales_record(order_date):
    global sales_order_id
    customer_id = random_customer_id()
    product_id = random_product_id()
    sales_qty = random.randint(1, 1000)
    nsv = round(random.uniform(20, 20000), 2)
    modified_date = order_date

    cursor.execute("""
        INSERT INTO SalesLT.FactSalesTable (
            SalesOrderID, OrderDate, CustomerID, SalesQty,
            ProductID, NSV, ModifiedDate
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, sales_order_id, order_date, customer_id, sales_qty, product_id, nsv, modified_date)

    sales_order_id += 1

# Loop per day
current_date = start_date
while current_date <= end_date:
    # Ensure at least 1 record
    insert_sales_record(current_date)

    # Optional: randomly insert 0–5 more for this day
    for _ in range(random.randint(0, 5)):
        random_time = fake.date_time_between(start_date=current_date, end_date=current_date + timedelta(days=1))
        insert_sales_record(random_time)

    if sales_order_id % 1000 == 0:
        conn.commit()

    current_date += timedelta(days=1)

# Final commit
conn.commit()
cursor.close()
conn.close()
print(f"Finished inserting records up to {sales_order_id - 1}")
