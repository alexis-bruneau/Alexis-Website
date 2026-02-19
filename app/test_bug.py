
import pandas as pd
import os
import numpy as np

csv_path = os.path.join("app", "Redfin", "Output", "redfin_data.csv")
print(f"Reading CSV from: {csv_path}")

try:
    df = pd.read_csv(csv_path)
    print("Columns:", df.columns.tolist())
    
    if "Days On Market" in df.columns:
        print("Days On Market FOUND")
        # Check first few values
        print("Raw DOM head:", df["Days On Market"].head().tolist())
        
        # Try conversion
        df["dom"] = pd.to_numeric(df["Days On Market"], errors="coerce")
        print("Converted DOM head:", df["dom"].head().tolist())
        print("Mean DOM:", df["dom"].mean())
    else:
        print("Days On Market MISSING")
        
    if "Sold Price Difference" in df.columns:
        print("Sold Price Difference FOUND")
        print("Raw Diff head:", df["Sold Price Difference"].head().tolist())
        df["price_diff"] = pd.to_numeric(df["Sold Price Difference"], errors="coerce")
        print("Converted Diff head:", df["price_diff"].head().tolist())
        
        # Calculate pct
        df["price"] = pd.to_numeric(df["Sold Price"], errors="coerce")
        df["list_price"] = df["price"] - df["price_diff"]
        df["price_diff_pct"] = (df["price_diff"] / df["list_price"]) * 100
        print("Diff Pct head:", df["price_diff_pct"].head().tolist())
        print("Mean Diff Pct:", df["price_diff_pct"].mean())

    else:
        print("Sold Price Difference MISSING")

except Exception as e:
    print("Error:", e)
