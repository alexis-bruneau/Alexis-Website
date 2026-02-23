import os
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"

def debug_load():
    if not AZURE_CONN_STR:
        print("❌ AZURE_STORAGE_CONNECTION_STRING not found.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    print("🔍 Searching for ALL parquet files in 'silver/'...")
    blobs = container_client.list_blobs(name_starts_with="silver/")
    
    dfs = []
    found_files = 0
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            found_files += 1
            print(f"   -> Found: {blob.name} (last modified: {blob.last_modified})")
            blob_client = container_client.get_blob_client(blob.name)
            data = blob_client.download_blob().readall()
            
            df_chunk = pd.read_parquet(io.BytesIO(data))
            print(f"      Rows in this file: {len(df_chunk)}")
            dfs.append(df_chunk)
                
    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
        print(f"\nTotal rows before deduplication: {len(full_df)}")
        
        # Check for MLS column
        if "MLS" in full_df.columns:
            duplicates = full_df.duplicated(subset=["MLS"]).sum()
            print(f"Duplicate MLS count: {duplicates}")
            
            full_df_dedup = full_df.drop_duplicates(subset=["MLS"], keep="last")
            print(f"Total rows after deduplication: {len(full_df_dedup)}")
        else:
            print("⚠️ 'MLS' column not found in data!")
            
    else:
        print("⚠️ No parquet files found.")

if __name__ == "__main__":
    debug_load()
