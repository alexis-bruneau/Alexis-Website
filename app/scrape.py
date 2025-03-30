from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import pandas as pd
from bs4 import BeautifulSoup
import requests


def construct_url(
    location, start_date, end_date, adults, children, min_bedrooms, min_beds
):
    base_url = "https://www.airbnb.ca/s/"
    # Format dates
    start_date = start_date.strftime("%Y-%m-%d")  # assuming date is a datetime object
    end_date = end_date.strftime("%Y-%m-%d")  # assuming date is a datetime object

    url = (
        f"{base_url}{location}/homes?adults={adults}&checkin={start_date}&checkout={end_date}"
        f"&children={children}&min_bedrooms={min_bedrooms}&min_beds={min_beds}"
    )

    return url


def extract_data(parent):
    data = {
        "beds": None,
        "rooms": None,
        "original_price": None,
        "discounted_price": None,
        "star_review": None,
        "total_reviews": None,
        "listing_url": None,
    }

    # Extracting beds and rooms
    beds_and_rooms_elements = parent.find_all(
        "div", class_="fb4nyux s1cjsi4j dir dir-ltr"
    )
    for beds_and_rooms_element in beds_and_rooms_elements:
        beds_and_rooms = [
            span.get_text(strip=True)
            for span in beds_and_rooms_element.find_all("span", class_="dir dir-ltr")
        ]
        beds_and_rooms = [
            info for info in beds_and_rooms if info
        ]  # remove any empty strings
        if beds_and_rooms:
            for info in beds_and_rooms:
                if "bedrooms" in info:
                    matches = re.findall(r"(\d+)\s*bedroom", info)
                    if matches:
                        data["rooms"] = int(matches[0])
                elif "bed" in info:
                    data["beds"] = int("".join(filter(str.isdigit, info)))

    # Extracting prices
    # Check for the existence of both original and discounted price
    price_element = parent.find("span", class_="_1ks8cgb")
    data["original_price"] = price_element.text if price_element else None

    discounted_price_element = parent.find("span", class_="_1y74zjx")
    data["discounted_price"] = (
        discounted_price_element.text if discounted_price_element else None
    )

    # If original price is not found, try the second method
    if data["original_price"] is None:
        price_element = parent.find("div", class_="pquyp1l dir dir-ltr")
        if price_element:
            prices = [
                span.text for span in price_element.find_all("span", class_="_tyxjp1")
            ]
            data["original_price"] = prices[0] if prices else None
            # Only overwrite discounted_price if it is not None and the second method found a potential value
            if data["discounted_price"] is None and len(prices) > 1:
                data["discounted_price"] = prices[1]

    # Extracting review data
    review_element = parent.find("span", class_="r1dxllyb dir dir-ltr")
    if review_element:
        review_data = review_element.text.split(" ")
        data["star_review"] = review_data[0]
        data["total_reviews"] = (
            re.sub(r"\(|\)", "", review_data[1]) if len(review_data) > 1 else None
        )

    # Extracting listing URL
    listing_url_element = parent.find("a", class_="rfexzly dir dir-ltr", href=True)
    if listing_url_element:
        data["listing_url"] = "https://www.airbnb.com" + listing_url_element["href"]

    return data


# Define the function to handle each page
def scrape_page(
    page, location, start_date, end_date, adults, children, min_bedrooms, min_beds
):
    print(f"printing page")
    # Construct the URL for the current page
    page_url = construct_url(
        location, start_date, end_date, adults, children, min_bedrooms, min_beds
    )

    print(page_url)
    # Send a request to the page URL and parse the HTML

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    }

    response = requests.get(page_url, headers=headers)
    print(response.status_code)
    print(response.text)
    soup = BeautifulSoup(response.text, "html.parser")

    # Find all the parent elements on the page
    parent_elements = soup.find_all("div", class_="lxq01kf l1tup9az dir dir-ltr")

    # Extract data from each parent element and append it to the data list
    page_data = []
    for parent in parent_elements:
        data = extract_data(parent)
        page_data.append(data)

    print(page_data)
    return page_data
