from flask import Flask, render_template, send_from_directory, request, jsonify
from Redfin import scrape_properties, get_properties
from math import radians, cos, sin, asin, sqrt
import os
import pandas as pd


"""
from scrape import scrape_page
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
"""

app = Flask(__name__, template_folder="../templates", static_folder="../static")


@app.route("/points.json")
def points():
    # Always read the latest CSV, specify columns explicitly
    df = pd.read_csv(
        "app/Redfin/Output/redfin_data.csv", usecols=["latitude", "longitude"]
    )

    # Ensure lat/lng are numeric, coercing errors to NaN
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Drop rows with NaNs resulting from coercion
    df = df.dropna(subset=["latitude", "longitude"])

    # Convert to a list of {"latitude":.., "longitude":..}
    pts = df.to_dict(orient="records")
    return jsonify(pts)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/images/<path:filename>")
def serve_images(filename):
    return send_from_directory("Redfin/Output/images", filename)


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
    data = request.get_json()
    center_lat, center_lng = data.get("center", [0, 0])
    radius_km = data.get("radius_km", 5)
    filters = data.get("filters", {})

    # Load data
    df = pd.read_csv("app/Redfin/Output/redfin_data.csv")

    # Rename columns to standardized names
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
    # Ensure numeric lat/lng
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])

    # Apply radius filter
    df["distance_km"] = df.apply(
        lambda row: haversine(
            center_lng, center_lat, row["longitude"], row["latitude"]
        ),
        axis=1,
    )
    df = df[df["distance_km"] <= radius_km]

    # Convert sold_date column to datetime once before filtering
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

    # Extract points for the map
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
            ]
        ]
        .dropna(subset=["latitude", "longitude"])
        .to_dict(orient="records")
    )

    # Group by month for chart (count + average price)
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

    # Summary stats
    summary = {
        "count": len(df),
        "average_price": round(df["price"].mean(), 2) if not df.empty else None,
        "min_price": df["price"].min() if not df.empty else None,
        "max_price": df["price"].max() if not df.empty else None,
        "by_month": by_month,
    }

    return jsonify({"points": points, "summary": summary})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
