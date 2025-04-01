from flask import Flask, render_template, send_from_directory, request, jsonify
from app.redfin import scrape_properties, save_to_csv
import os
import pandas as pd


"""
from scrape import scrape_page
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
"""

app = Flask(__name__, template_folder="../templates", static_folder="../static")


@app.route("/")
def home():
    return render_template("index.html")


# This optional route if you want to serve other template files by path
@app.route("/<path:filename>")
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


"""
@app.route("/scrape", methods=["GET"])
def scrape_route():
    location = "Ottawa-ON--Canada"
    start_date = datetime(2023, 7, 30)
    end_date = datetime(2023, 8, 3)
    adults = 4
    children = 0
    min_bedrooms = 4
    min_beds = 4
    total_pages = 2  # Number of pages to scrape

    data_list = []
    print("Scrape route was called")

    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the executor
        futures = {
            executor.submit(
                scrape_page,
                page,
                location,
                start_date,
                end_date,
                adults,
                children,
                min_bedrooms,
                min_beds,
            )
            for page in range(1, total_pages + 1)
        }

        # Collect the results as they become available
        for future in as_completed(futures):
            data_list.extend(future.result())

    # Convert the list of dictionaries to a DataFrame
    df.head()
    df = pd.DataFrame(data_list)

    return jsonify(df.to_dict(orient="records"))  # return the scraped data as JSON


@app.route("/webscraping", methods=["GET"])
def webscraping():
    return render_template("Webscraping.html")
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
