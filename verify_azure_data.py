import duckdb
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"

if not AZURE_CONN_STR:
    print("❌ Error: AZURE_STORAGE_CONNECTION_STRING not found in .env")
    exit(1)

print("Connecting to DuckDB/Azure...")
con = duckdb.connect(database=":memory:")
con.execute("INSTALL azure;")
con.execute("LOAD azure;")
con.execute(f"SET azure_storage_connection_string = '{AZURE_CONN_STR}';")

print("Querying Silver Layer (Parquet)...")
try:
    # Count rows
    count = con.execute(f"SELECT COUNT(*) FROM 'azure://{CONTAINER_NAME}/silver/*/*.parquet'").fetchone()[0]
    print(f"✅ Total Rows in Silver Layer: {count}")

    # Show sample
    print("\nSample Data (Top 5):")
    df = con.execute(f"SELECT * FROM 'azure://{CONTAINER_NAME}/silver/*/*.parquet' LIMIT 5").df()
    print(df.to_string())

except Exception as e:
    print(f"❌ Error querying Azure: {e}")
