#!/usr/bin/env python3
import os
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path

# Load environment
load_dotenv()

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"

if not CONN_STR:
    print("❌ Error: Missing AZURE_STORAGE_CONNECTION_STRING in .env")
    exit(1)

def get_latest_silver():
    blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    blobs = list(container_client.list_blobs(name_starts_with="silver/"))
    if not blobs:
        print("❌ No silver data found in Azure.")
        return None
        
    # Sort by name (which includes YYYY-MM) and then last modified
    latest_blob = sorted(blobs, key=lambda b: b.name, reverse=True)[0]
    print(f"📂 Loading latest data: {latest_blob.name}")
    
    data = container_client.get_blob_client(latest_blob.name).download_blob().readall()
    return pd.read_parquet(io.BytesIO(data))

def analyze(df):
    # 1. Convert Sold Date to Datetime for sorting
    if "Sold Date" in df.columns:
        # Redfin format is typically "Feb 03, 2026"
        df["_sold_dt"] = pd.to_datetime(df["Sold Date"], format="%b %d, %Y", errors="coerce")
        df.sort_values("_sold_dt", ascending=True, inplace=True)

    print("\n--- 📊 Data Overview ---")
    print(f"Total Rows: {len(df)}")
    print(f"Columns: {', '.join([c for c in df.columns if not c.startswith('_')])}")
    
    print("\n--- 💰 Quick Summary (Sold Properties) ---")
    if "Sold Price" in df.columns:
        valid_prices = df["Sold Price"].dropna()
        print(f"Scraped count with price: {len(valid_prices)}")
        print(f"Average Sold Price: ${valid_prices.mean():,.2f}")
        print(f"Median Sold Price:  ${valid_prices.median():,.2f}")
    
    print("\n--- 🕒 Oldest 5 Sold Entries ---")
    cols_to_show = ["MLS", "Sold Price", "Sold Date", "Address"]
    cols_to_show = [c for c in cols_to_show if c in df.columns]
    print(df[cols_to_show].head(5))

    print("\n💡 TIP: You can now explore the 'df' variable directly.")

if __name__ == "__main__":
    df = get_latest_silver()
    if df is not None:
        analyze(df)
