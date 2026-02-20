from flask import Flask, render_template, request, jsonify, send_from_directory
import json
import os
import duckdb
import pandas as pd
import numpy as np
import sys
import traceback
from dotenv import load_dotenv

# ---------------- Config --------------------------------------------------
load_dotenv()

# Azure Storage Helper Config
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"
ACCOUNT_NAME = "stredfinprod" 
BASE_IMG_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/"

# DuckDB Setup
con = duckdb.connect(database=":memory:")

def load_data():
    global con
    try:
        from azure.storage.blob import BlobServiceClient
        import io
        
        # 1. Connect to Azure using Python SDK (Reliable on Heroku)
        if not AZURE_CONN_STR:
             print("⚠️ AZURE_STORAGE_CONNECTION_STRING not found. Running in offline/empty mode.")
             return False, "Missing Connection String"

        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        
        # 2. Iterate to find the latest 'listed_properties.parquet'
        latest_blob = None
        latest_time = None
        
        # Optimization: Only list blobs in 'silver/'
        blobs = container_client.list_blobs(name_starts_with="silver/")
        for blob in blobs:
            if blob.name.endswith("listed_properties.parquet"):
                if latest_time is None or blob.last_modified > latest_time:
                    latest_time = blob.last_modified
                    latest_blob = blob.name
                    
        if latest_blob:
            print(f"✅ Found latest data: {latest_blob}")
            blob_client = container_client.get_blob_client(latest_blob)
            data = blob_client.download_blob().readall()
            
            # 3. Load into Pandas
            # Requires pyarrow/fastparquet engine
            df_parquet = pd.read_parquet(io.BytesIO(data))
            
            # 4. Register as DuckDB View
            # Use CREATE OR REPLACE so we can update it later
            con.register('df_parquet_view', df_parquet)
            con.execute("CREATE OR REPLACE VIEW properties AS SELECT * FROM df_parquet_view")
            print(f"✅ Registered 'properties' view with {len(df_parquet)} rows.")
            return True, f"Loaded {len(df_parquet)} rows from {latest_blob}"
        else:
            print("⚠️ No parquet files found in Azure 'silver/' folder.")
            con.execute("CREATE OR REPLACE VIEW properties AS SELECT CAST(NULL AS DOUBLE) as latitude, CAST(NULL AS DOUBLE) as longitude, CAST(NULL AS DOUBLE) as \"Sold Price\", CAST(NULL AS VARCHAR) as \"Sold Date\", CAST(NULL AS VARCHAR) as \"Address\", CAST(NULL AS VARCHAR) as MLS, CAST(NULL AS DOUBLE) as \"Number Beds\", CAST(NULL AS DOUBLE) as \"Number Baths\", CAST(NULL AS VARCHAR) as url, CAST(NULL AS VARCHAR) as photo_blob, CAST(NULL AS DOUBLE) as \"Days On Market\", CAST(NULL AS DOUBLE) as \"Sold Price Difference\" WHERE 1=0")
            return False, "No parquet files found"

    except Exception as e:
        sys.stderr.write(f"CRITICAL ERROR loading data from Azure: {e}\n{traceback.format_exc()}\n")
        # Create empty table as fallback
        try:
             con.execute("CREATE OR REPLACE VIEW properties AS SELECT CAST(NULL AS DOUBLE) as latitude, CAST(NULL AS DOUBLE) as longitude, CAST(NULL AS DOUBLE) as \"Sold Price\", CAST(NULL AS VARCHAR) as \"Sold Date\", CAST(NULL AS VARCHAR) as \"Address\", CAST(NULL AS VARCHAR) as MLS, CAST(NULL AS DOUBLE) as \"Number Beds\", CAST(NULL AS DOUBLE) as \"Number Baths\", CAST(NULL AS VARCHAR) as url, CAST(NULL AS VARCHAR) as photo_blob, CAST(NULL AS DOUBLE) as \"Days On Market\", CAST(NULL AS DOUBLE) as \"Sold Price Difference\" WHERE 1=0")
        except:
            pass
        return False, str(e)

# Initial Load
load_data()

# ---------------- App -----------------------------------------------------

app = Flask(__name__, template_folder="../templates", static_folder="../static")

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/refresh-data", methods=["POST"])
def refresh_data():
    """Trigger a data reload from Azure without restarting"""
    success, msg = load_data()
    return jsonify({"success": success, "message": msg})

@app.route("/points.json")
def points():
    try:
        # 1. Query DuckDB for ALL points (with valid coords)
        query = """
            SELECT 
                latitude, longitude, "Sold Price" as price, 
                "Sold Date" as sold_date, 
                "Address" as address, 
                MLS as mls, 
                "Number Beds" as beds, 
                "Number Baths" as baths, 
                url, 
                photo_blob, 
                "Days On Market" as dom,
                "Sold Price Difference" as price_diff
            FROM properties
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """
        # Return as Pandas DataFrame for easy manipulation
        df = con.execute(query).fetchdf()
        
        # 2. Add calculated fields
        # List Price = Sold - Diff
        df["list_price"] = df["price"] - df["price_diff"]
        
        # Avoid division by zero
        df["price_diff_pct"] = np.where(
            df["list_price"] != 0, 
            (df["price_diff"] / df["list_price"]) * 100, 
            0
        )

        # Construct Image URL
        if "photo_blob" in df.columns:
            df["photo"] = df["photo_blob"].apply(
                lambda x: f"{BASE_IMG_URL}{x}" if pd.notna(x) and x else None
            )
        else:
            df["photo"] = None

        # Clean NaNs for JSON - Convert to object type first to handle Nones
        df = df.astype(object).where(pd.notnull(df), None)
        points_data = df.to_dict(orient="records")
        
        # Sanitize output
        def sanitize(obj):
            if isinstance(obj, (np.integer, int)):
                return int(obj)
            if isinstance(obj, (np.floating, float)):
                if np.isnan(obj) or np.isinf(obj):
                    return None
                return float(obj)
            if isinstance(obj, dict):
                return {k: sanitize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [sanitize(v) for v in obj]
            return obj
            
        points_data = sanitize(points_data)
        return jsonify(points_data)

    except Exception as e:
        sys.stderr.write(f"ERROR in points: {str(e)}\n{traceback.format_exc()}\n")
        return jsonify({"error": str(e)}), 500

@app.route("/ottawa_map")
def ottawa_map():
    return render_template("ottawa_map.html")

# Restoring: Serve other HTML files (menu_bar, Contact, etc.)
@app.route("/<path:filename>", methods=["GET"])
def serve_static_html(filename):
    return send_from_directory("../templates", filename)

@app.route("/update-coordinates", methods=["POST"])
def update_coordinates():
    # Placeholder for coordinate updates
    data = request.get_json()
    print("Received coordinates:", data)
    return jsonify(success=True, received=data)

@app.route("/filtered-points", methods=["POST"])
def filtered_points():
    try:
        data = request.get_json()
        center_lat, center_lng = data.get("center", [45.4215, -75.6972]) # Default Ottawa
        radius_km = data.get("radius_km", 5)
        filters = data.get("filters", {})

        # ---------------- Query Construction ------------------------------
        where_clauses = ["latitude IS NOT NULL", "longitude IS NOT NULL"]
        
        if "min_price" in filters:
            price = int(filters["min_price"])
            where_clauses.append(f"try_cast(\"Sold Price\" as INTEGER) >= {price}")
            
        if "max_price" in filters:
            price = int(filters["max_price"])
            where_clauses.append(f"try_cast(\"Sold Price\" as INTEGER) <= {price}")

        where_str = " AND ".join(where_clauses)
        
        # 2. Execute Query
        query = f"""
            SELECT 
                latitude, longitude, "Sold Price" as price, 
                "Sold Date" as sold_date, 
                "Address" as address, 
                MLS as mls, 
                "Number Beds" as beds, 
                "Number Baths" as baths, 
                url, 
                photo_blob, 
                "Days On Market" as dom,
                "Sold Price Difference" as price_diff
            FROM properties
            WHERE {where_str}
        """
        
        # return as Pandas DataFrame
        df = con.execute(query).fetchdf()

        # ---------------- Python Post-Processing --------------------------
        
        # numeric conversion
        numeric_cols = ["latitude", "longitude", "price", "beds", "baths", "dom", "price_diff"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Distance Filter
        # Simple Haversine
        def haversine_np(lon1, lat1, lon2, lat2):
            lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
            c = 2 * np.arcsin(np.sqrt(a))
            km = 6371 * c
            return km

        if not df.empty:
            df["distance_km"] = haversine_np(center_lng, center_lat, df["longitude"], df["latitude"])
            df = df[df["distance_km"] <= radius_km]
        else:
            df["distance_km"] = []

        # Additional Filters (Date, etc)
        df["sold_date"] = pd.to_datetime(df["sold_date"], errors="coerce")
        
        if "sold_start" in filters:
            df = df[df["sold_date"] >= pd.to_datetime(filters["sold_start"])]
        if "sold_end" in filters:
            df = df[df["sold_date"] <= pd.to_datetime(filters["sold_end"])]
        if "beds" in filters and filters["beds"]:
             df = df[df["beds"].isin(filters["beds"])]

        # Calculations
        # List Price = Sold - Diff
        df["list_price"] = df["price"] - df["price_diff"]
        df["price_diff_pct"] = (df["price_diff"] / df["list_price"]) * 100

        # Construct Image URL
        if "photo_blob" in df.columns:
            df["photo"] = df["photo_blob"].apply(
                lambda x: f"{BASE_IMG_URL}{x}" if pd.notna(x) and x else None
            )
        else:
            df["photo"] = None

        # ---------------- Response Preparation ----------------------------
        
        # Select Output Columns
        out_cols = [
            "latitude", "longitude", "price", "sold_date", "address", "mls",
            "beds", "baths", "url", "photo", "dom", "price_diff_pct"
        ]
        # Valid columns only
        out_cols = [c for c in out_cols if c in df.columns]
        
        points_df = df[out_cols].dropna(subset=["latitude", "longitude"])
        
        # Clean NaNs for JSON (Must cast to object to hold None)
        points_df = points_df.astype(object).where(pd.notnull(points_df), None)
        points = points_df.to_dict(orient="records")

        # Summary Stats (By Month)
        if not df["sold_date"].isna().all():
            df["month"] = df["sold_date"].dt.to_period("M").astype(str)
            grouped = df.groupby("month")
            by_month = (
                grouped.size().to_frame("count")
                .join(grouped["price"].mean().to_frame("avg_price"))
                .join(grouped["dom"].mean().to_frame("avg_dom"))
                .join(grouped["price_diff_pct"].mean().to_frame("avg_diff_pct"))
                .reset_index()
                .fillna(0)
                .to_dict(orient="records")
            )
        else:
            by_month = []

        summary = {
            "count": int(len(df)), # ensure int
            "average_price": round(df["price"].mean(), 2) if not df.empty else None,
            "avg_dom": round(df["dom"].mean(), 1) if not df.empty else None,
            "avg_diff_pct": round(df["price_diff_pct"].mean(), 2) if not df.empty else None,
            "min_price": df["price"].min() if not df.empty else None,
            "max_price": df["price"].max() if not df.empty else None,
            "by_month": by_month,
        }
        
        # Sanitize summary for NaNs as well
        # Simple helper to sanitize a dict
        def sanitize(obj):
            if isinstance(obj, (np.integer, int)):
                return int(obj)
            if isinstance(obj, (np.floating, float)):
                if np.isnan(obj) or np.isinf(obj):
                    return None
                return float(obj)
            if isinstance(obj, dict):
                return {k: sanitize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [sanitize(v) for v in obj]
            return obj
            
        summary = sanitize(summary)
        points = sanitize(points)

        return jsonify({"points": points, "summary": summary})

    except Exception as e:
        import sys
        sys.stderr.write(f"ERROR in filtered_points: {str(e)}\n{traceback.format_exc()}\n")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
