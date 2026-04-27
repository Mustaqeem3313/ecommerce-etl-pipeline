import csv
import random
from datetime import datetime, timedelta

# -------------------------------
# CONFIG
# -------------------------------
NUM_ROWS = 200

categories = {
    "Electronics": ["Mobile", "Laptop", "Accessories", "Audio", "TV"],
    "Clothing": ["Men", "Women", "Kids"],
    "Home": ["Furniture", "Kitchen", "Decor"],
    "Sports": ["Fitness", "Outdoor", "Indoor"],
    "Beauty": ["Skincare", "Haircare", "Makeup"]
}

product_names = {
    "Mobile": ["iPhone 15", "Samsung Galaxy S23", "OnePlus 11", "Pixel 8"],
    "Laptop": ["MacBook Air M2", "Dell XPS 13", "HP Pavilion", "Lenovo ThinkPad"],
    "Accessories": ["USB Cable", "Power Bank", "Charger", "Wireless Mouse"],
    "Audio": ["Sony Headphones", "JBL Speaker", "AirPods Pro", "Boat Earbuds"],
    "TV": ["Samsung 55 TV", "LG OLED TV", "Sony Bravia", "Mi Smart TV"],
    "Men": ["T-Shirt", "Jeans", "Jacket", "Shirt"],
    "Women": ["Dress", "Top", "Kurti", "Jeans"],
    "Kids": ["Kids T-Shirt", "Kids Shorts", "Kids Jacket"],
    "Furniture": ["Office Chair", "Sofa", "Table", "Bed"],
    "Kitchen": ["Mixer", "Cookware Set", "Knife Set"],
    "Decor": ["Wall Art", "Lamp", "Clock"],
    "Fitness": ["Dumbbells", "Yoga Mat", "Treadmill"],
    "Outdoor": ["Tent", "Backpack", "Sleeping Bag"],
    "Indoor": ["Carrom Board", "Chess Set"],
    "Skincare": ["Face Wash", "Moisturizer", "Sunscreen"],
    "Haircare": ["Shampoo", "Hair Oil"],
    "Makeup": ["Lipstick", "Foundation"]
}

def random_date():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2026, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

# -------------------------------
# GENERATE CSV
# -------------------------------
with open("products.csv", mode="w", newline="") as file:
    writer = csv.writer(file)

    # Header
    writer.writerow([
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "price",
        "cost",
        "stock_quantity",
        "supplier_id",
        "created_at"
    ])

    for i in range(1, NUM_ROWS + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        product = random.choice(product_names[sub_category])

        price = round(random.uniform(100, 2000), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)
        stock = random.randint(10, 500)
        supplier_id = random.randint(1, 20)
        created_at = random_date()

        writer.writerow([
            i,
            product,
            category,
            sub_category,
            price,
            cost,
            stock,
            supplier_id,
            created_at
        ])

print("✅ products.csv generated successfully with 200 rows")