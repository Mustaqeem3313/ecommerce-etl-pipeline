import pandas as pd
from datetime import datetime, timedelta

# ==============================
# CONFIG
# ==============================
start_date = datetime(2020, 1, 1)
end_date = datetime(2026, 12, 31)

# ==============================
# GENERATION
# ==============================
rows = []
current = start_date
date_id = 1

while current <= end_date:
    year = current.year
    month = current.month
    day = current.day

    rows.append({
        "date_id": date_id,
        "full_date": current.strftime("%Y-%m-%d"),

        # Required core columns
        "day_of_week": current.weekday() + 1,   # Monday=1, Sunday=7
        "day_name": current.strftime("%A"),
        "week_of_year": int(current.strftime("%U")),
        "month_name": current.strftime("%B"),
        "is_holiday": 0,  # keep simple (can enhance later)

        # Missing columns from your error (NOW INCLUDED)
        "year": year,
        "month": month,
        "day": day,
        "quarter": (month - 1) // 3 + 1,
        "is_weekend": 1 if current.weekday() >= 5 else 0
    })

    current += timedelta(days=1)
    date_id += 1

# ==============================
# CREATE DATAFRAME
# ==============================
df = pd.DataFrame(rows)

# ==============================
# COLUMN ORDER (IMPORTANT)
# ==============================
df = df[
    [
        "date_id",
        "full_date",
        "day_of_week",
        "day_name",
        "week_of_year",
        "month_name",
        "is_holiday",
        "year",
        "month",
        "day",
        "quarter",
        "is_weekend"
    ]
]

# ==============================
# SAVE FILE
# ==============================
df.to_csv("date_2024_10_v2.csv", index=False)

print(f"✅ date.csv generated successfully with {len(df)} rows")