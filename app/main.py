from flask import Flask, render_template, send_from_directory, request, jsonify
from math import radians, cos, sin, asin, sqrt
import os, numpy as np, pandas as pd

# ──────────────────────────────────────────────────────────────────────────
# 1.  ONE place that defines where the master CSV lives
#     (override with AZURE_CSV_URL env-var when you rotate the SAS token)
# ──────────────────────────────────────────────────────────────────────────
AZURE_CSV_URL = os.getenv(
    "AZURE_CSV_URL",
    "https://redfinstorage.blob.core.windows.net/data/redfin_data.csv?sp=r&st=2025-05-19T18:55:07Z&se=2025-05-20T02:55:07Z&spr=https&sv=2024-11-04&sr=b&sig=UbfjbvUjWftuDedLgu1wnlq0Ol%2B6If16aRS6Dlm1nu8%3D",
)

# put this once near the top of flask_app.py
BASE_IMG_URL = os.getenv(
    "AZURE_IMG_URL",  # handy in prod if you rename the account/container
    "https://redfinstorage.blob.core.windows.net/images/",
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
        # Read directly from Azure Blob Storage
        df = pd.read_csv(AZURE_CSV_URL, usecols=["latitude", "longitude"])

        # Ensure lat/lng are numeric
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

        # Drop rows with missing coordinates
        df = df.dropna(subset=["latitude", "longitude"])

        pts = df.to_dict(orient="records")
        return jsonify(pts)

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


df = pd.read_csv(AZURE_CSV_URL, usecols=["latitude", "longitude"])


@app.route("/")
def home():
    return render_template("index.html")


# This optional route if you want to serve other template files by path
@app.route("/<path:filename>", methods=["GET"])
def serve_static_html(filename):
    return send_from_directory("../templates", filename)


@app.route("/ottawa-map")
def ottawa_map():
    return render_template("ottawa_map.html")


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

        # ⬇️ change this line
        # df = pd.read_csv("app/Redfin/Output/redfin_data.csv")
        df = pd.read_csv(AZURE_CSV_URL)

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

        df["distance_km"] = df.apply(
            lambda row: haversine(
                center_lng, center_lat, row["longitude"], row["latitude"]
            ),
            axis=1,
        )
        df = df[df["distance_km"] <= radius_km]

        # sold_date parsing
        df["sold_date"] = pd.to_datetime(df["sold_date"], errors="coerce")

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
        points = (
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
                ]
            ]
            .dropna(subset=["latitude", "longitude"])
            .to_dict(orient="records")
        )

        if not df["sold_date"].isna().all():
            df["month"] = df["sold_date"].dt.to_period("M").astype(str)
            grouped = df.groupby("month")
            by_month = (
                grouped.size()
                .to_frame("count")
                .join(grouped["price"].mean().to_frame("avg_price"))
                .reset_index()
                .to_dict(orient="records")
            )
        else:
            by_month = []

        summary = {
            "count": len(df),
            "average_price": round(df["price"].mean(), 2) if not df.empty else None,
            "min_price": df["price"].min() if not df.empty else None,
            "max_price": df["price"].max() if not df.empty else None,
            "by_month": by_month,
        }

        # Clean for NaN
        summary_clean = {
            k: (
                None
                if isinstance(v, float) and np.isnan(v)
                else (
                    int(v)
                    if isinstance(v, np.integer)
                    else float(v) if isinstance(v, np.floating) else v
                )
            )
            for k, v in summary.items()
        }
        points_clean = [
            {
                k: (
                    None
                    if isinstance(v, float) and np.isnan(v)
                    else (
                        int(v)
                        if isinstance(v, np.integer)
                        else float(v) if isinstance(v, np.floating) else v
                    )
                )
                for k, v in p.items()
            }
            for p in points
        ]

        return jsonify({"points": points_clean, "summary": summary_clean})

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
