from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
import os
import duckdb
import pandas as pd
import numpy as np
import sys
import traceback
from dotenv import load_dotenv

# The Redfin pipeline package (pip install -e redfin). These routes only read what it writes to Azure.
from redfin import config as redfin_config
from redfin.analytics import neighbourhoods as nbh
from redfin.analytics.active_filters import apply_active_filters, beds_mask, prepare_active_frame
from redfin.analytics.property_types import property_group
from redfin.analytics.sale_curve import sale_curve
from redfin.analytics.sale_filters import apply_sale_filters

# Windows consoles default to cp1252 and crash on the emoji in our log lines
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ---------------- Config --------------------------------------------------
load_dotenv()

# Azure Storage Helper Config
AZURE_CONN_STR = redfin_config.AZURE_CONN_STR
CONTAINER_NAME = redfin_config.CONTAINER_NAME
BASE_IMG_URL = redfin_config.PUBLIC_BLOB_URL
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
# One row per active listing we hold a detail page for (redfin.transform.active). Kept apart from the sold
# 'properties' view: those rows are sales, these are asking prices.
ACTIVE_BLOB = os.getenv("ACTIVE_BLOB", "silver/active/listings.parquet")
ACTIVE_COLUMNS = ["mls", "status", "address", "list_price", "beds", "baths", "latitude", "longitude", "url", "photo_blob",
                  "property_type_name", "days_on_market", "cumulative_dom", "original_price", "price_changes",
                  "last_change_amount", "last_change_pct", "last_change_date", "last_change_direction",
                  "price_vs_original", "as_of"]
active_df = pd.DataFrame(columns=ACTIVE_COLUMNS)
_active_frame = None   # the snapshot made ready to filter, with neighbourhood ids; rebuilt after every reload
_active_unplaced = 0   # active listings with no coordinates (on no map, so not in the frame)


def load_active_data():
    """Reloads the active-listing snapshot from Azure into memory. An empty frame when it is missing."""
    global active_df, _active_frame
    _active_frame = None
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
    global _sold_frame
    success, msg = load_data()
    _sold_frame = None  # the sold frame (map, curve, neighbourhoods) is rebuilt from the reloaded data
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

def sanitize_numbers(obj):
    """numpy numbers -> plain ints / floats and NaN / inf -> None, in nested dicts and lists. Leaves everything else
    (e.g. timestamps, which Flask serializes as HTTP dates that the page parses) as it is."""
    if isinstance(obj, (np.integer, int)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    if isinstance(obj, dict):
        return {k: sanitize_numbers(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_numbers(v) for v in obj]
    return obj


# ---------------- Sold sales: one frame, one filter path, two ways to pick an area ---------------------------
# The sold map picks the sales to show in one of two ways: a circle the user drags (center + radius) or a fixed Ottawa
# neighbourhood (an id from redfin.analytics.neighbourhoods). Both go through the same steps in /filtered-points:
# area -> filters (redfin.analytics.sale_filters) -> points + summary, and the per-neighbourhood summary applies the same
# filters, so the numbers always agree.
DEFAULT_CENTER = [45.4215, -75.6972]  # Ottawa
DEFAULT_RADIUS_KM = 5

SOLD_SELECT = """
    SELECT latitude, longitude, "Sold Price" AS price, "Sold Date" AS sold_date, "Address" AS address, MLS AS mls,
           "Number Beds" AS beds, "Number Baths" AS baths, url, photo_blob, "Days On Market" AS dom,
           sold_vs_last_list AS diff_last, sold_vs_original_list AS diff_original, "Property Type" AS ptype, photo_type
    FROM properties
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
"""

_sold_frame = None   # built once per data load; refresh_data() clears it
_sold_unplaced = 0   # sales with no coordinates: on no map, so not in the frame (shown as a note, never silently lost)


def get_neighbourhoods():
    """The neighbourhood boundaries (parsed once). Raises when the file is missing or broken."""
    return nbh.load_neighbourhoods()


def sold_frame():
    """Every sale that has coordinates, ready to filter: numeric columns, parsed dates, the asking-price differences
    and the neighbourhood id of each sale. Built once per data load and cached (refresh_data() clears it), so the
    point-in-polygon test never runs per request. Callers must not change it: filtering returns copies."""
    global _sold_frame, _sold_unplaced
    if _sold_frame is None:
        df = con.execute(SOLD_SELECT).fetchdf()
        for col in ("latitude", "longitude", "price", "beds", "baths", "dom"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["sold_date"] = pd.to_datetime(df["sold_date"], format="mixed", errors="coerce")
        add_asking_diffs(df)
        try:
            df = nbh.with_neighbourhood(df, get_neighbourhoods())
        except Exception as e:  # circle mode must keep working without the boundary file
            sys.stderr.write(f"ERROR assigning neighbourhoods: {e}\n{traceback.format_exc()}\n")
            df["neighbourhood_id"] = nbh.UNASSIGNED_ID
        _sold_unplaced = int(con.execute(
            "SELECT count(*) FROM properties WHERE latitude IS NULL OR longitude IS NULL").fetchone()[0])
        _sold_frame = df
    return _sold_frame


def active_frame():
    """Every active listing that has coordinates, ready to filter: numeric columns and the neighbourhood id of each
    listing. Built once per snapshot load and cached (load_active_data() clears it). Callers must not change it:
    selecting and filtering return copies."""
    global _active_frame, _active_unplaced
    if _active_frame is None:
        df, _active_unplaced = prepare_active_frame(active_df)
        try:
            df = nbh.with_neighbourhood(df, get_neighbourhoods())
        except Exception as e:  # circle mode must keep working without the boundary file
            sys.stderr.write(f"ERROR assigning neighbourhoods to active listings: {e}\n{traceback.format_exc()}\n")
            df["neighbourhood_id"] = nbh.UNASSIGNED_ID
        _active_frame = df
    return _active_frame


def parse_area(data):
    """The area a request asks for, as a dict: {"type": "circle", "center": (lat, lng), "radius_km": r} or
    {"type": "neighbourhood", "id": n}. Accepts the new `area` object and the old top-level `center` / `radius_km`
    (which the page sent before neighbourhoods existed). Raises ValueError for anything else."""
    area = data.get("area")
    if area is None:
        area = {"type": "circle", "center": data.get("center", DEFAULT_CENTER),
                "radius_km": data.get("radius_km", DEFAULT_RADIUS_KM)}
    if not isinstance(area, dict):
        raise ValueError("area must be an object")
    kind = area.get("type")
    if kind == "circle":
        try:
            lat, lng = area.get("center", DEFAULT_CENTER)
            return {"type": "circle", "center": (float(lat), float(lng)),
                    "radius_km": float(area.get("radius_km", DEFAULT_RADIUS_KM))}
        except (TypeError, ValueError):
            raise ValueError("a circle needs center [lat, lng] and radius_km")
    if kind == "neighbourhood":
        try:
            nid = int(area["id"])
        except (KeyError, TypeError, ValueError):
            raise ValueError("a neighbourhood area needs an integer id")
        if nid not in get_neighbourhoods():
            raise ValueError(f"unknown neighbourhood id {nid}")
        return {"type": "neighbourhood", "id": nid}
    raise ValueError(f"unknown area type {kind!r} (use 'circle' or 'neighbourhood')")


def select_area(df, area):
    """The sales of `df` inside the parsed area."""
    if area["type"] == "neighbourhood":
        return df[df["neighbourhood_id"] == area["id"]]
    if df.empty:
        return df
    lat, lng = area["center"]
    return df[haversine_km(lng, lat, df["longitude"], df["latitude"]) <= area["radius_km"]]


@app.route("/neighbourhood-boundaries.geojson")
def neighbourhood_boundaries():
    """The neighbourhood polygons (id + name), the same file the server uses to place the sales."""
    return send_file(redfin_config.NEIGHBOURHOODS_FILE, mimetype="application/geo+json", max_age=86400)


@app.route("/neighbourhood-summary", methods=["POST"])
def neighbourhood_summary():
    """One row per neighbourhood (plus the unassigned bucket) for the sales that pass `filters`: the table and the
    choropleth. Body: {"filters": {...}} (the same filters as /filtered-points). Every row has `n`; rows below
    `min_sample` carry low_sample = true so the page can grey them out."""
    try:
        data = request.get_json(silent=True) or {}
        df = apply_sale_filters(sold_frame(), data.get("filters") or {})
        rows = nbh.aggregate(df, get_neighbourhoods())
        return jsonify(sanitize_json({"rows": rows, "min_sample": nbh.MIN_SAMPLE, "total": int(len(df)),
                                      "no_coordinates": _sold_unplaced}))
    except Exception as e:
        sys.stderr.write(f"ERROR in neighbourhood_summary: {e}\n{traceback.format_exc()}\n")
        return jsonify({"error": str(e)}), 500


@app.route("/active-neighbourhood-summary", methods=["POST"])
def active_neighbourhood_summary():
    """The active map's twin of /neighbourhood-summary: one row per neighbourhood (plus the unassigned bucket) for the
    active listings that pass `filters` (the /active-points filters, plus `changes`). Rows below `min_sample` carry
    low_sample = true; the listing count `n` is always exact, so the page can colour by it whatever the sample."""
    try:
        data = request.get_json(silent=True) or {}
        frame = active_frame()
        df = apply_active_filters(frame, data.get("filters") or {})
        rows = nbh.aggregate_active(df, get_neighbourhoods())
        return jsonify(sanitize_json({"rows": rows, "min_sample": nbh.ACTIVE_MIN_SAMPLE, "total": int(len(df)),
                                      "no_coordinates": _active_unplaced}))
    except Exception as e:
        sys.stderr.write(f"ERROR in active_neighbourhood_summary: {e}\n{traceback.format_exc()}\n")
        return jsonify({"error": str(e)}), 500


@app.route("/filtered-points", methods=["POST"])
def filtered_points():
    """The sales in an area that pass the filters, and their summary. The area is a circle or a neighbourhood:
    {"area": {"type": "circle", "center": [lat, lng], "radius_km": r}} or {"area": {"type": "neighbourhood", "id": n}}.
    The old body (top-level `center` and `radius_km`) still means a circle."""
    try:
        data = request.get_json(silent=True) or {}
        try:
            area = parse_area(data)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        df = apply_sale_filters(select_area(sold_frame(), area), data.get("filters", {}))

        # Construct Image URL
        df["photo"] = df["photo_blob"].apply(photo_url)

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

        response = {"points": sanitize_numbers(points), "summary": sanitize_numbers(summary)}
        if area["type"] == "neighbourhood":
            response["area"] = {"type": "neighbourhood", "id": area["id"], "name": get_neighbourhoods().name_of(area["id"])}
        return jsonify(response)

    except Exception as e:
        sys.stderr.write(f"ERROR in filtered_points: {str(e)}\n{traceback.format_exc()}\n")
        return jsonify({"error": str(e)}), 500

# The "how long do homes like these take to sell" curve is built from the sold data the sold map reads (the
# same sold_frame() as above), over the last SOLD_WINDOW_DAYS days of sales.
SOLD_WINDOW_DAYS = 180


def comparable_sales(area, filters):
    """Days on market of the recent sales inside the area (a circle or a neighbourhood, see parse_area) that pass the
    price, beds and property-type filters.

    The window is the SOLD_WINDOW_DAYS days ending on the latest sale in the data (not today's date, so it is the
    same for everyone and it still has sales when the sold scrape is a few days behind). Returns
    (days_on_market list, window_start, window_end); the dates are None when there is no sold data.

    Only price, beds and property type are applied: they describe what a comparable home is. The active-only
    filters (days on market, price changes) are not, because filtering sales by how long they took would bias the
    curve. Rows with no days on market are left out."""
    df = sold_frame()
    window_end = df["sold_date"].max()
    if pd.isna(window_end):
        return [], None, None
    window_start = window_end - pd.Timedelta(days=SOLD_WINDOW_DAYS - 1)
    df = select_area(df[(df["sold_date"] >= window_start) & (df["sold_date"] <= window_end)], area)
    if "min_price" in filters:
        df = df[df["price"] >= int(filters["min_price"])]
    if "max_price" in filters:
        df = df[df["price"] <= int(filters["max_price"])]
    if filters.get("beds"):
        df = df[beds_mask(df["beds"], filters["beds"])]
    if filters.get("ptypes"):
        df = df[df["ptype"].map(property_group).isin(filters["ptypes"])]
    return df["dom"].dropna().tolist(), window_start.date().isoformat(), window_end.date().isoformat()


@app.route("/active-points", methods=["POST"])
def active_points():
    """Active listings inside the area (+ filters) and their insights: the counterpart of /filtered-points. The area is
    a circle or a neighbourhood: {"area": {"type": "circle", "center": [lat, lng], "radius_km": r}} or
    {"area": {"type": "neighbourhood", "id": n}}; the old top-level `center` and `radius_km` still mean a circle."""
    try:
        data = request.get_json() or {}
        try:
            area = parse_area(data)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        filters = data.get("filters", {})

        df = apply_active_filters(select_area(active_frame(), area), filters)

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

        # How long comparable homes took to sell. A failure here must not take the listings down with it.
        sold = {"sold_curve": None, "sold_sample": 0, "sold_window": None}
        try:
            days, window_start, window_end = comparable_sales(area, filters)
            sold = {
                "sold_curve": sale_curve(days),  # None below 20 sales
                "sold_sample": len(days),        # sales in the window, also when too few for a curve
                "sold_window": {"days": SOLD_WINDOW_DAYS, "start": window_start, "end": window_end},
            }
        except Exception as e:
            sys.stderr.write(f"ERROR in active_points sold curve: {e}\n{traceback.format_exc()}\n")
        response = {"points": sanitize_json(points), "summary": sanitize_json(summary), **sanitize_json(sold)}
        if area["type"] == "neighbourhood":
            response["area"] = {"type": "neighbourhood", "id": area["id"], "name": get_neighbourhoods().name_of(area["id"])}
        return jsonify(response)
    except Exception as e:
        sys.stderr.write(f"ERROR in active_points: {e}\n{traceback.format_exc()}\n")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
