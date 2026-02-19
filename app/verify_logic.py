
import pandas as pd
import numpy as np
import os
from math import radians, cos, sin, asin, sqrt

# Define haversine function to match main.py
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 6371 * 2 * asin(sqrt(a))

def run_logic():
    print("Starting logic verification...")
    # Use relative path suitable for execution from project root
    csv_path = os.path.join("app", "Redfin", "Output", "redfin_data.csv")
    
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        # Try adjusting path if running from 'app' dir? No, cwd is project root.
        return

    df = pd.read_csv(csv_path)
    print(f"Initial columns: {df.columns.tolist()}")
    
    # Rename
    df = df.rename(
        columns={
            "Sold Price": "price",
            "Sold Date": "sold_date",
            "Number Beds": "beds",
            "Number Baths": "baths",
            "MLS": "mls",
            "Address": "address",
        }
    )
    print(f"Post-rename columns: {df.columns.tolist()}")
    
    if "Days On Market" in df.columns:
        print("Days On Market is still present.")
    else:
        print("Days On Market is GONE!")

    # Numeric conversion
    numeric_cols = ["price", "beds", "baths", "latitude", "longitude"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Dropna
    df = df.dropna(subset=["latitude", "longitude"])
    
    # Distance filtering (mock center for Ottawa)
    center_lat, center_lng = 45.4215, -75.6972
    radius_km = 100 
    
    df["distance_km"] = df.apply(
        lambda row: haversine(
            center_lng, center_lat, row["longitude"], row["latitude"]
        ),
        axis=1,
    )
    df = df[df["distance_km"] <= radius_km]
    print(f"Rows after filter: {len(df)}")

    # DOM handling
    if "Days On Market" in df.columns:
        df["dom"] = pd.to_numeric(df["Days On Market"], errors="coerce")
        print(f"DOM head: {df['dom'].head().tolist()}")
        print(f"DOM NaNs: {df['dom'].isna().sum()} out of {len(df)}")
    else:
        print("Days On Market column MISSING during logic")
        df["dom"] = np.nan

    dom_sum = df["dom"].sum()
    count = len(df)
    avg = dom_sum/count if count else 0
    print(f"Sum: {dom_sum}, Count: {count}, Avg: {avg}")

if __name__ == "__main__":
    run_logic()
