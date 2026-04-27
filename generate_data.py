import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 3000

# ------------------ CUSTOMERS ------------------
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    customers.append({
        "customer_id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "country": fake.country(),
        "signup_date": fake.date_between(start_date="-3y", end_date="today"),
        "is_active": random.choice([0, 1])
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv("customers.csv", index=False)

# ------------------ PRODUCTS ------------------
categories = ["Electronics", "Clothing", "Home", "Sports"]

products = []
for i in range(1, NUM_PRODUCTS + 1):
    products.append({
        "product_id": i,
        "product_name": fake.word().capitalize(),
        "category": random.choice(categories),
        "sub_category": fake.word(),
        "price": round(random.uniform(10, 500), 2),
        "cost": round(random.uniform(5, 300), 2),
        "stock_quantity": random.randint(10, 500),
        "supplier_id": random.randint(1, 50),
        "created_at": fake.date_between(start_date="-2y", end_date="today")
    })

products_df = pd.DataFrame(products)
products_df.to_csv("products.csv", index=False)

# ------------------ ORDERS ------------------
orders = []
for i in range(1, NUM_ORDERS + 1):
    customer_id = random.randint(1, NUM_CUSTOMERS)
    product_id = random.randint(1, NUM_PRODUCTS)
    quantity = random.randint(1, 5)
    price = round(random.uniform(10, 500), 2)

    orders.append({
        "order_id": i,
        "customer_id": customer_id,
        "product_id": product_id,
        "order_date": fake.date_between(start_date="-1y", end_date="today"),
        "ship_date": fake.date_between(start_date="-1y", end_date="today"),
        "quantity": quantity,
        "unit_price": price,
        "discount": round(random.uniform(0, 0.3), 2),
        "total_amount": round(quantity * price, 2),
        "status": random.choice(["Shipped", "Pending", "Cancelled"]),
        "payment_method": random.choice(["Card", "UPI", "NetBanking"]),
        "shipping_country": fake.country()
    })

orders_df = pd.DataFrame(orders)
orders_df.to_csv("orders.csv", index=False)

print("✅ Data generated successfully")
