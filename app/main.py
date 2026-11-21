from flask import Flask, render_template, request, jsonify, send_from_directory
import os
import duckdb
import pandas as pd
import numpy as np
import sys
import traceback
from dotenv import load_dotenv

try:  # served as app.main (gunicorn) or run as a script from app/
    from app.property_types import property_group
except ImportError:
    from property_types import property_group

# Windows consoles default to cp1252 and crash on the emoji in our log lines
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ---------------- Config --------------------------------------------------
load_dotenv()

# Azure Storage Helper Config
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"
ACCOUNT_NAME = "stredfinprod" 
BASE_IMG_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/"
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver")  # e.g. SILVER_PREFIX=silver_v2 to preview a rebuilt silver

# What the sold price is compared with: the final asking price when it sold, or the first asking price.
# Each basis maps to a silver column (sold_vs_<basis>_list, in dollars).
DIFF_BASES = ("last", "original")
# A sold price more than 50% away from the asking price is almost always bad data (an asking price of $1 for
# "contact agent", or a wrong sold price). Those listings get no difference rather than skewing the averages.
MAX_PLAUSIBLE_DIFF_PCT = 50

# DuckDB Setup
con = duckdb.connect(database=":memory:")

def photo_url(blob):
    """Public image URL for a photo_blob path, or None when the scraper stored no real path
    (null, empty, or the literal strings 'None' / 'nan')."""
    if blob is None or pd.isna(blob):
        return None
    blob = str(blob).strip()
    if blob in ("", "None", "nan"):
        return None
    return f"{BASE_IMG_URL}{blob}"

def add_asking_diffs(df):
    """Adds diff_<basis> ($) and diff_pct_<basis> (% over (+) / under (-) the asking price) for each basis.

    The asking price is sold price minus the difference; the percentage is relative to it. Implausible
    differences (see MAX_PLAUSIBLE_DIFF_PCT) are left empty."""
    for basis in DIFF_BASES:
        diff = pd.to_numeric(df[f"diff_{basis}"], errors="coerce").astype(float)
        asking = df["price"] - diff
        pct = pd.Series(np.where(asking > 0, diff / asking * 100, np.nan), index=df.index)
        plausible = pct.abs() <= MAX_PLAUSIBLE_DIFF_PCT
        df[f"diff_{basis}"] = diff.where(plausible)
        df[f"diff_pct_{basis}"] = pct.where(plausible)


def create_empty_view():
    """Empty 'properties' view so the API endpoints still work without data."""
    con.execute("CREATE OR REPLACE VIEW properties AS SELECT CAST(NULL AS DOUBLE) as latitude, CAST(NULL AS DOUBLE) as longitude, CAST(NULL AS DOUBLE) as \"Sold Price\", CAST(NULL AS VARCHAR) as \"Sold Date\", CAST(NULL AS VARCHAR) as \"Address\", CAST(NULL AS VARCHAR) as MLS, CAST(NULL AS DOUBLE) as \"Number Beds\", CAST(NULL AS DOUBLE) as \"Number Baths\", CAST(NULL AS VARCHAR) as url, CAST(NULL AS VARCHAR) as photo_blob, CAST(NULL AS DOUBLE) as \"Days On Market\", CAST(NULL AS DOUBLE) as sold_vs_last_list, CAST(NULL AS DOUBLE) as sold_vs_original_list, CAST(NULL AS VARCHAR) as \"Property Type\", CAST(NULL AS VARCHAR) as photo_type WHERE 1=0")

def load_data():
    global con
    try:
        from azure.storage.blob import BlobServiceClient
        import io
        
        # 1. Connect to Azure using Python SDK (Reliable on Heroku)
        if not AZURE_CONN_STR:
             print("⚠️ AZURE_STORAGE_CONNECTION_STRING not found. Running in offline/empty mode.")
             create_empty_view()
             return False, "Missing Connection String"

        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        
        # 2. Iterate to find ALL 'listed_properties.parquet' files in silver/
        print(f"🔍 Searching for parquet files in '{SILVER_PREFIX}/'...")
        blobs = container_client.list_blobs(name_starts_with=f"{SILVER_PREFIX}/")
        
        dfs = []
        for blob in blobs:
            if blob.name.endswith("listed_properties.parquet"):
                print(f"   -> Found: {blob.name}")
                blob_client = container_client.get_blob_client(blob.name)
                data = blob_client.download_blob().readall()
                
                # Load into Pandas (BytesIO)
                df_chunk = pd.read_parquet(io.BytesIO(data))
                dfs.append(df_chunk)
                    
        if dfs:
            # 3. Combine all dataframes
            full_df = pd.concat(dfs, ignore_index=True)
            
            # Deduplicate on the listing key (property id from the URL + MLS). MLS alone is not enough: it can be
            # empty, and older silver files have no property_id column.
            property_ids = full_df["url"].str.extract(r"/home/(\d+)", expand=False)
            full_df = full_df[~pd.concat([property_ids, full_df["MLS"]], axis=1).duplicated(keep="last")]
            # Older silver files lack columns added later
            for col in ("photo_type", "sold_vs_last_list", "sold_vs_original_list"):
                if col not in full_df.columns:
                    full_df[col] = None
            
            # 4. Register as DuckDB View
            con.register('df_parquet_view', full_df)
            con.execute("CREATE OR REPLACE VIEW properties AS SELECT * FROM df_parquet_view")
            print(f"✅ Registered 'properties' view with {len(full_df)} rows (merged from {len(dfs)} files).")
            return True, f"Loaded {len(full_df)} rows from {len(dfs)} files"
        else:
            print("⚠️ No parquet files found in Azure 'silver/' folder.")
            create_empty_view()
            return False, "No parquet files found"

    except Exception as e:
        sys.stderr.write(f"CRITICAL ERROR loading data from Azure: {e}\n{traceback.format_exc()}\n")
        # Create empty table as fallback
        try:
             create_empty_view()
        except:
            pass
        return False, str(e)

# Initial Load
load_data()

# ---------------- Active listings ------------------------------------------
# One row per active listing we hold a detail page for (app/Redfin/transform_active.py). Kept apart from the sold
# 'properties' view: those rows are sales, these are asking prices.
ACTIVE_BLOB = os.getenv("ACTIVE_BLOB", "silver/active/listings.parquet")
ACTIVE_COLUMNS = ["mls", "address", "list_price", "beds", "baths", "latitude", "longitude", "url", "photo_blob",
                  "property_type_name", "days_on_market", "cumulative_dom", "original_price", "price_changes",
                  "last_change_amount", "last_change_pct", "last_change_date", "last_change_direction",
                  "price_vs_original", "as_of"]
active_df = pd.DataFrame(columns=ACTIVE_COLUMNS)


def load_active_data():
    """Reloads the active-listing snapshot from Azure into memory. An empty frame when it is missing."""
    global active_df
    try:
        from azure.storage.blob import BlobServiceClient
        import io
        if not AZURE_CONN_STR:
            active_df = pd.DataFrame(columns=ACTIVE_COLUMNS)
            return False, "Missing Connection String"
        container_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR).get_container_client(CONTAINER_NAME)
        blob = container_client.get_blob_client(ACTIVE_BLOB)
        if not blob.exists():
            active_df = pd.DataFrame(columns=ACTIVE_COLUMNS)
            return False, f"{ACTIVE_BLOB} not found"
        df = pd.read_parquet(io.BytesIO(blob.download_blob().readall()))
        for col in ACTIVE_COLUMNS:
            if col not in df.columns:
                df[col] = None
        active_df = df
        print(f"✅ Loaded {len(df)} active listings from {ACTIVE_BLOB}")
        return True, f"Loaded {len(df)} active listings"
    except Exception as e:
        sys.stderr.write(f"ERROR loading active listings: {e}\n{traceback.format_exc()}\n")
        active_df = pd.DataFrame(columns=ACTIVE_COLUMNS)
        return False, str(e)


load_active_data()


def sanitize_json(obj):
    """numpy / NaN / dates -> plain JSON values."""
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_json(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        return None if np.isnan(obj) or np.isinf(obj) else float(obj)
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if obj is pd.NA or obj is pd.NaT:
        return None
    return obj


def haversine_km(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    a = np.sin((lat2 - lat1) / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2.0) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(a))

# ---------------- App -----------------------------------------------------

app = Flask(__name__, template_folder="../templates", static_folder="../static")

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/refresh-data", methods=["POST"])
def refresh_data():
    """Trigger a data reload from Azure without restarting"""
    success, msg = load_data()
    active_ok, active_msg = load_active_data()
    return jsonify({"success": success, "message": msg, "active": {"success": active_ok, "message": active_msg}})

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
                sold_vs_last_list as diff_last,
                sold_vs_original_list as diff_original,
                "Property Type" as ptype,
                photo_type
            FROM properties
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """
        # Return as Pandas DataFrame for easy manipulation
        df = con.execute(query).fetchdf()
        
        # 2. Add calculated fields
        add_asking_diffs(df)

        # Construct Image URL
        if "photo_blob" in df.columns:
            df["photo"] = df["photo_blob"].apply(
                photo_url
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

        # Get the actual date range from the data
        date_range = {"min_date": None, "max_date": None}
        try:
            dates_df = con.execute("""
                SELECT "Sold Date" FROM properties
                WHERE "Sold Date" IS NOT NULL AND "Sold Date" != ''
            """).fetchdf()
            if not dates_df.empty:
                parsed = pd.to_datetime(dates_df["Sold Date"], format="mixed", dayfirst=False, errors="coerce")
                parsed = parsed.dropna()
                if not parsed.empty:
                    date_range["min_date"] = parsed.min().strftime("%Y-%m-%d")
                    date_range["max_date"] = parsed.max().strftime("%Y-%m-%d")
        except Exception as e:
            sys.stderr.write(f"Date range error: {e}\n")

        return jsonify({"points": points_data, "date_range": date_range})

    except Exception as e:
        sys.stderr.write(f"ERROR in points: {str(e)}\n{traceback.format_exc()}\n")
        return jsonify({"error": str(e)}), 500


@app.route("/ottawa_map")
def ottawa_map():
    return render_template("ottawa_map.html")

@app.route("/ottawa_active")
def ottawa_active():
    return render_template("ottawa_active.html")

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
                sold_vs_last_list as diff_last,
                sold_vs_original_list as diff_original,
                "Property Type" as ptype,
                photo_type
            FROM properties
            WHERE {where_str}
        """
        
        # return as Pandas DataFrame
        df = con.execute(query).fetchdf()

        # ---------------- Python Post-Processing --------------------------
        
        # numeric conversion
        numeric_cols = ["latitude", "longitude", "price", "beds", "baths", "dom"]
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
        if "ptypes" in filters and filters["ptypes"]:
             # silver holds the raw Redfin value; the filter buttons send grouped names
             df = df[df["ptype"].map(property_group).isin(filters["ptypes"])]

        add_asking_diffs(df)

        # Construct Image URL
        if "photo_blob" in df.columns:
            df["photo"] = df["photo_blob"].apply(
                photo_url
            )
        else:
            df["photo"] = None

        # ---------------- Response Preparation ----------------------------
        
        # Select Output Columns
        out_cols = [
            "latitude", "longitude", "price", "sold_date", "address", "mls",
            "beds", "baths", "url", "photo", "photo_type", "dom", "ptype",
            "diff_last", "diff_original", "diff_pct_last", "diff_pct_original"
        ]
        # Valid columns only
        out_cols = [c for c in out_cols if c in df.columns]
        
        points_df = df[out_cols].dropna(subset=["latitude", "longitude"])
        
        # Clean NaNs for JSON (Must cast to object to hold None)
        points_df = points_df.astype(object).where(pd.notnull(points_df), None)
        points = points_df.to_dict(orient="records")

        # Summary Stats (By Month)
        if not df["sold_date"].isna().all():
            df["month"] = df["sold_date"].dt.to_period("M").astype(str).where(df["sold_date"].notna())
            grouped = df.groupby("month")
            by_month = (
                grouped.size().to_frame("count")
                .join(grouped["price"].mean().to_frame("avg_price"))
                .join(grouped["dom"].mean().to_frame("avg_dom"))
                .join(grouped["diff_pct_last"].mean().to_frame("avg_diff_pct_last"))
                .join(grouped["diff_pct_original"].mean().to_frame("avg_diff_pct_original"))
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
            "avg_diff_pct_last": round(df["diff_pct_last"].mean(), 2) if not df.empty else None,
            "avg_diff_pct_original": round(df["diff_pct_original"].mean(), 2) if not df.empty else None,
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

@app.route("/active-points", methods=["POST"])
def active_points():
    """Active listings inside the search circle (+ filters) and their insights: the counterpart of /filtered-points."""
    try:
        data = request.get_json() or {}
        center_lat, center_lng = data.get("center", [45.4215, -75.6972])
        radius_km = data.get("radius_km", 5)
        filters = data.get("filters", {})

        df = active_df.copy()
        for col in ("latitude", "longitude", "list_price", "beds", "baths", "days_on_market", "cumulative_dom",
                    "original_price", "price_changes", "last_change_amount", "last_change_pct", "price_vs_original"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"])
        if not df.empty:
            df = df[haversine_km(center_lng, center_lat, df["longitude"], df["latitude"]) <= radius_km]
        if "min_price" in filters:
            df = df[df["list_price"] >= int(filters["min_price"])]
        if "max_price" in filters:
            df = df[df["list_price"] <= int(filters["max_price"])]
        if filters.get("beds"):  # the "5+" button sends 5 and means 5 or more
            df = df[df["beds"].isin(filters["beds"]) | (df["beds"] >= 5 if 5 in filters["beds"] else False)]
        if filters.get("ptypes"):
            df = df[df["property_type_name"].map(property_group).isin(filters["ptypes"])]

        df["price"] = df["list_price"]
        df["photo"] = df["photo_blob"].apply(photo_url)
        df["ptype"] = df["property_type_name"]
        df["dom"] = df["days_on_market"]
        out_cols = ["latitude", "longitude", "price", "address", "mls", "beds", "baths", "url", "photo", "ptype", "dom",
                    "cumulative_dom", "original_price", "price_changes", "last_change_amount", "last_change_pct",
                    "last_change_date", "last_change_direction", "price_vs_original"]
        points = df[out_cols].astype(object).where(pd.notnull(df[out_cols]), None).to_dict(orient="records")

        changed = df[df["price_changes"].fillna(0) > 0]
        has_dom = df["dom"].notna().any()
        summary = {
            "count": int(len(df)),
            "average_price": round(df["price"].mean(), 2) if len(df) else None,
            "median_price": round(df["price"].median(), 2) if len(df) else None,
            "min_price": df["price"].min() if len(df) else None,
            "max_price": df["price"].max() if len(df) else None,
            "avg_dom": round(df["dom"].mean(), 1) if has_dom else None,
            "median_dom": round(df["dom"].median(), 1) if has_dom else None,
            "with_price_cut": int((df["last_change_direction"] == "down").sum()),
            "with_price_increase": int((df["last_change_direction"] == "up").sum()),
            "avg_cut_pct": round(float(changed["price_vs_original"].div(changed["original_price"]).mean() * 100), 2)
                           if len(changed) and changed["original_price"].notna().any() else None,
            "as_of": str(df["as_of"].max()) if len(df) and df["as_of"].notna().any() else None,
        }
        return jsonify({"points": sanitize_json(points), "summary": sanitize_json(summary)})
    except Exception as e:
        sys.stderr.write(f"ERROR in active_points: {e}\n{traceback.format_exc()}\n")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
