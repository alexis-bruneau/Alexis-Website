from flask import Flask, render_template, send_from_directory, request, jsonify
import json
from math import radians, cos, sin, asin, sqrt
import os, numpy as np, pandas as pd

# Email Used alexis17azure@gmail.com

# ──────────────────────────────────────────────────────────────────────────
# 1.  ONE place that defines where the master CSV lives
#     (override with AZURE_CSV_URL env-var when you rotate the SAS token)
# ──────────────────────────────────────────────────────────────────────────
AZURE_CSV_URL = os.getenv("AZURE_CSV_URL")


# Local image URL for development (serve from Flask static route)
BASE_IMG_URL = os.getenv(
    "AZURE_IMG_URL",  # Use Azure in production
    "/redfin-images/",  # Local development: Flask route
)


"""
from scrape import scrape_page
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
"""

app = Flask(__name__, template_folder="../templates", static_folder="../static")

@app.route("/points.json")
def points():
    try:
        # Use local CSV file
        csv_path = os.path.join(os.path.dirname(__file__), "Redfin", "Output", "redfin_data.csv")
        df = pd.read_csv(csv_path, usecols=["latitude", "longitude"])

        # Ensure lat/lng are numeric
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

        # Drop rows with missing coordinates
        df = df.dropna(subset=["latitude", "longitude"])

        pts = df.to_dict(orient="records")
        # Use simplejson/pandas handling via round-trip if needed, but manual clean is safer if done right.
        # Let's use the explicit replacement strategy on the dataframe itself before dict conversion if possible, 
        # but here we already have dicts.
        
        # Round-trip through pandas JSON serialization to handle NaT/NaN
        return jsonify(json.loads(pd.Series(pts).to_json(orient="records")))

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# Commented out to allow local development without Azure access
# df = pd.read_csv(AZURE_CSV_URL, usecols=["latitude", "longitude"])


@app.route("/")
def home():
    return render_template("index.html")


# This optional route if you want to serve other template files by path
@app.route("/<path:filename>", methods=["GET"])
def serve_static_html(filename):
    return send_from_directory("../templates", filename)


@app.route("/ottawa_map")
def ottawa_map():
    return render_template("ottawa_map.html")


@app.route("/redfin-images/<path:filename>")
def serve_redfin_image(filename):
    """Serve local Redfin property images"""
    image_dir = os.path.join(os.path.dirname(__file__), "Redfin", "Output", "images")
    return send_from_directory(image_dir, filename)


@app.route("/update-coordinates", methods=["POST"])
def update_coordinates():
    data = request.get_json()
    # Process the coordinates as needed
    print("Received coordinates:", data)
    # Return a valid JSON response
    return jsonify(success=True, received=data)


def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 6371 * 2 * asin(sqrt(a))  # Radius of Earth in km


@app.route("/filtered-points", methods=["POST"])
def filtered_points():
    try:
        data = request.get_json()
        center_lat, center_lng = data.get("center", [0, 0])
        radius_km = data.get("radius_km", 5)
        filters = data.get("filters", {})

        # Use local CSV file
        csv_path = os.path.join(os.path.dirname(__file__), "Redfin", "Output", "redfin_data.csv")
        df = pd.read_csv(csv_path)

        # Rename for consistency
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

        # Ensure all relevant columns are numeric
        numeric_cols = ["price", "beds", "baths", "latitude", "longitude"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["latitude", "longitude"])
        # print(f"DEBUG: Count after dropna lat/lng: {len(df)}", flush=True)

        df["distance_km"] = df.apply(
            lambda row: haversine(
                center_lng, center_lat, row["longitude"], row["latitude"]
            ),
            axis=1,
        )
        # print(f"DEBUG: center={center_lat},{center_lng} radius={radius_km}", flush=True)
        
        df = df[df["distance_km"] <= radius_km]
        
        print(f"DEBUG: Count after distance filter: {len(df)}", flush=True)

        # DEBUG: Check columns
        # print("Columns:", df.columns.tolist(), flush=True)
        
        if "Days On Market" in df.columns:
            df["dom"] = pd.to_numeric(df["Days On Market"], errors="coerce")
            # print("DOM head:", df["dom"].head(), flush=True)
        else:
            print("Days On Market column MISSING")
            df["dom"] = np.nan

        if "Sold Price Difference" in df.columns:
            df["price_diff"] = pd.to_numeric(df["Sold Price Difference"], errors="coerce")
        else:
            print("Sold Price Difference column MISSING")
            df["price_diff"] = np.nan

        # sold_date parsing
        df["sold_date"] = pd.to_datetime(df["sold_date"], errors="coerce")

        # Calculate % difference
        # price_diff = Sold - List
        # List = Sold - price_diff
        # % diff = (price_diff / List) * 100
        df["list_price"] = df["price"] - df["price_diff"]
        df["price_diff_pct"] = (df["price_diff"] / df["list_price"]) * 100

        # Optional filters
        if "sold_start" in filters:
            df = df[df["sold_date"] >= pd.to_datetime(filters["sold_start"])]
        if "sold_end" in filters:
            df = df[df["sold_date"] <= pd.to_datetime(filters["sold_end"])]
        if "min_price" in filters:
            df = df[df["price"] >= filters["min_price"]]
        if "max_price" in filters:
            df = df[df["price"] <= filters["max_price"]]
        if "beds" in filters:
            df = df[df["beds"].isin(filters["beds"])]

        # Build a public HTTPS link for the first photo of each listing
        if "photo_blob" in df.columns:  # <-- whatever you named it
            df["photo"] = df["photo_blob"].apply(
                lambda b: f"{BASE_IMG_URL}{b}" if pd.notna(b) and b else None
            )
        else:
            df["photo"] = None  # safety if the column is missing

        # Prepare data for response
        # Prepare data for response
        points_df = (
            df[
                [
                    "latitude",
                    "longitude",
                    "price",
                    "sold_date",
                    "address",
                    "mls",
                    "beds",
                    "baths",
                    "url",
                    "photo",
                    "dom",
                    "price_diff_pct",
                ]
            ]
            .dropna(subset=["latitude", "longitude"])
        )
        # Handle NaNs effectively for JSON
        points = points_df.where(pd.notnull(points_df), None).to_dict(orient="records")

        if not df["sold_date"].isna().all():
            df["month"] = df["sold_date"].dt.to_period("M").astype(str)
            grouped = df.groupby("month")
            by_month = (
                grouped.size()
                .to_frame("count")
                .join(grouped["price"].mean().to_frame("avg_price"))
                .join(grouped["dom"].mean().to_frame("avg_dom"))
                .join(grouped["price_diff_pct"].mean().to_frame("avg_diff_pct"))
                .reset_index()
                .fillna(0)  # Ensure no NaNs in JSON
                .to_dict(orient="records")
            )
            # Ensure no NaT/NaNs remain in by_month (e.g. from mean() on all-NaN slice)
            for item in by_month:
                for k, v in item.items():
                    if pd.isna(v):
                        item[k] = None
        else:
            by_month = []

        # Calculate metrics explicitly for debugging
        dom_sum = df["dom"].sum()
        total_count = len(df)
        avg_dom_val = (dom_sum / total_count) if total_count > 0 else 0
        
        # Write to debug file
        try:
            with open("debug_log.txt", "w") as f:
                f.write(f"Columns: {df.columns.tolist()}\n")
                if "Days On Market" in df.columns:
                    f.write(f"Raw DOM head: {df['Days On Market'].head().tolist()}\n")
                f.write(f"DOM column head: {df['dom'].head().tolist()}\n")
                f.write(f"DOM Sum: {dom_sum}, Count: {total_count}, Avg: {avg_dom_val}\n")
        except Exception as e:
            print(f"Failed to write debug log: {e}")

        print(f"DEBUG: DOM Sum: {dom_sum}, Count: {total_count}, Avg: {avg_dom_val}", flush=True)
        
        avg_diff_pct_val = df["price_diff_pct"].mean()
        
        summary = {
            "count": total_count,
            "average_price": round(df["price"].mean(), 2) if not df.empty else None,
            "avg_dom": round(avg_dom_val, 1) if total_count > 0 else None,
            "avg_diff_pct": round(avg_diff_pct_val, 2)
            if not pd.isna(avg_diff_pct_val)
            else None,
            "min_price": df["price"].min() if not df.empty else None,
            "max_price": df["price"].max() if not df.empty else None,
            "by_month": by_month,
        }

        if points:
            print(f"DEBUG: First point: {points[0]}", flush=True)

        # Sanitize using pandas JSON serialization (handles NaT/NaN automatically)
        payload = {"points": points, "summary": summary}
        return jsonify(json.loads(pd.Series([payload]).to_json(orient="records"))[0])

    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        try:
            with open(os.path.join(os.path.dirname(__file__), "server_error.log"), "w") as f:
                f.write(error_msg)
        except:
            pass
        print(error_msg)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
