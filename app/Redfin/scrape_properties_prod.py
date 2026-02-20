#!/usr/bin/env python3
import os
import io
import json
import time
import zlib
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright, TimeoutError
from azure.storage.blob import BlobServiceClient
import requests
from bs4 import BeautifulSoup

# ---------------- config --------------------------------------------------
from dotenv import load_dotenv

# Explicitly load .env from current directory or parent
env_path = Path(".env")
if not env_path.exists():
    env_path = Path("..") / ".env"
    
load_dotenv(dotenv_path=env_path)

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"

# Test configurations
TEST_MODE = False
MAX_URLS = 250 

URLS_FILE = Path("app/Redfin/Output/property_urls.txt")
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
WAIT_SEC = 3

# ---------------- Azure Client --------------------------------------------
if not CONN_STR:
    raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in .env")

# Initialize Azure Client
try:
    blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    if not container_client.exists():
        container_client.create_container()
except Exception as e:
    print(f"Azure Connection Error: {e}")
    raise

# ---------------- Helpers -------------------------------------------------

def load_redfin_cookies():
    if not COOKIE_FILE.exists():
        return []
    with COOKIE_FILE.open(encoding="utf-8") as fh:
        raw = json.load(fh)
    expire = int(time.time()) + 30 * 24 * 3600
    return [
        {**c, "sameSite": "Lax", "expires": expire}
        for c in raw
        if ".redfin.ca" in c.get("domain", "")
    ]

def upload_bronze_html(html_content, mls):
    """Saves compressed HTML to bronze/YYYY-MM-DD/mls.html.gz"""
    today = datetime.now().strftime("%Y-%m-%d")
    blob_name = f"bronze/{today}/{mls}.html.gz"
    
    # Check if exists (Idempotency)
    try:
        if container_client.get_blob_client(blob_name).exists():
            return False # Skipped
    except:
        pass

    # Compress
    compressed = zlib.compress(html_content.encode("utf-8"))
    
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(compressed, overwrite=True)
    return True # Uploaded

def upload_image(mls, image_url):
    """Uploads ONLY the first image to images/mls_1.jpg"""
    blob_name = f"images/{mls}_1.jpg"
    blob_client = container_client.get_blob_client(blob_name)
    
    # Check if exists to save bandwidth/cost
    if blob_client.exists():
        return 
        
    try:
        resp = requests.get(image_url, timeout=10)
        if resp.status_code == 200:
            blob_client.upload_blob(resp.content, overwrite=True)
            # print(f"   -> Saved Image: {blob_name}")
    except Exception as e:
        print(f"   -> Failed Image {mls}: {e}")

# ---------------- Parsing Logic (Copied from scrape_properties.py) --------

def extract_between(text, start, stop="\\"):
    markers = [start] if '"' not in start else [start, start.replace('"', r"\"")]
    for m in markers:
        idx = text.find(m)
        if idx != -1:
            i = idx + len(m)
            j = text.find(stop, i) if stop else -1
            return text[i:j] if j != -1 else text[i:]
    return "N/A"

def parse_date(s):
    try:
        return datetime.strptime(s, "%b %d, %Y")
    except:
        return None

money_re = re.compile(r"[^\d]")

def money_to_int(s):
    s_clean = money_re.sub("", s)
    return int(s_clean) if s_clean else None

def parse_sale_history(html, url):
    start_marker = r'\"events\":[{'
    
    start_idx = html.find(start_marker)
    if start_idx == -1:
        start_marker = r'"events":[{'
        start_idx = html.find(start_marker)
    
    if start_idx == -1:
        return []

    array_start_idx = start_idx + len(start_marker) - 2
    candidate = html[array_start_idx : array_start_idx + 25000]
    unescaped = candidate.replace(r'\"', '"').replace(r'\\', '\\')
    
    decoder = json.JSONDecoder()
    try:
        events_list, _ = decoder.raw_decode(unescaped)
    except json.JSONDecodeError:
        return []
        
    rows = []
    for e in events_list:
        ts = e.get("eventDate")
        date_str = ""
        if isinstance(ts, int):
            date_str = datetime.fromtimestamp(ts / 1000).strftime("%b %d, %Y")
            
        rows.append({
            "url": url,
            "eventDate": date_str,
            "eventType": e.get("eventDescription", ""),
            "price": str(e.get("price", "")),
            "MLS": e.get("sourceId", "")
        })
    return rows

def find_genmid_values(html):
    return list(dict.fromkeys(re.findall(r"genMid\.([A-Za-z0-9_]+\.jpg)", html)))

def extract_price_from_json(html):
    soup = BeautifulSoup(html, "lxml")
    for script in soup.find_all("script"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
            stack = [data]
            while stack:
                curr = stack.pop()
                if isinstance(curr, dict):
                    if "price" in curr and "priceCurrency" in curr:
                         return int(curr["price"])
                    if "offers" in curr:
                        stack.append(curr["offers"])
                    for v in curr.values():
                        if isinstance(v, (dict, list)):
                            stack.append(v)     
                elif isinstance(curr, list):
                    for item in curr:
                         if isinstance(item, (dict, list)):
                             stack.append(item)
        except:
            continue
    return None

def safe_float(val):
    try:
        if isinstance(val, (float, int)):
            return float(val)
        return float(val) if val and val != "N/A" else None
    except:
        return None

# ---------------- Core Logic ----------------------------------------------

def process_property(url):
    # 1. Scrape HTML
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 ... Chrome/125 Safari/537")
        context.add_cookies(load_redfin_cookies())
        page = context.new_page()
        try:
            page.goto(url, wait_until="load", timeout=60000)
            time.sleep(WAIT_SEC)
            if "/login" in page.url:
                print(f"🔒 Login page encountered at {url}")
                browser.close()
                return None
            html = page.content()
        except TimeoutError:
            print(f"⚠️ Timeout at {url}")
            browser.close()
            return None
        browser.close()

    # 2. Extract MLS
    mls = extract_between(html, "TREB #", "<")
    if not mls or mls == "N/A":
        # Fallback if MLS not found (maybe different region)
        mls = f"UNKNOWN_{int(time.time())}"
    
    # 3. Upload Bronze (HTLM) - Idempotent
    uploaded = upload_bronze_html(html, mls)
    if not uploaded:
        print(f"   [Bronze] Skipped {mls} (Already done today)")
    else:
        print(f"   [Bronze] Uploaded {mls}")

    # 4. Parse Data (Silver Logic)
    # This logic matches your original scrape_properties.py
    history = parse_sale_history(html, url)
    history.sort(key=lambda h: parse_date(h["eventDate"]) or datetime.min)
    
    sold_evts = [h for h in history if "sold" in h["eventType"].lower()]
    sold_evt = sold_evts[-1] if sold_evts else None
    
    listed_evts = [h for h in history if "listed" in h["eventType"].lower() and "delisted" not in h["eventType"].lower()]
    first_listed_evt = listed_evts[0] if listed_evts else None
    
    sold_price = money_to_int(sold_evt["price"]) if sold_evt else None
    if not sold_price:
            json_price = extract_price_from_json(html)
            if json_price:
                sold_price = json_price
    
    first_list_price = money_to_int(first_listed_evt["price"]) if first_listed_evt else None
    
    beds = safe_float(extract_between(html, '"latestListingInfo":{"beds":', ","))
    baths = safe_float(extract_between(html, '"baths":', ","))
    lat = safe_float(extract_between(html, 'latitude":', ","))
    lon = safe_float(extract_between(html, 'longitude":', "}"))
    
    days_on_market = None
    sold_price_diff = None
    
    if sold_evt and first_listed_evt:
        d_sold = parse_date(sold_evt["eventDate"])
        d_listed = parse_date(first_listed_evt["eventDate"])
        if d_sold and d_listed:
            days_on_market = (d_sold - d_listed).days
            
    if sold_price and first_list_price:
        sold_price_diff = sold_price - first_list_price

    # 5. Handle Images (First one only)
    mls_tail = mls[-3:] if len(mls) >= 3 else mls
    image_filenames = find_genmid_values(html)
    if image_filenames:
        # Construct URL for the FIRST image
        first_img_url = f"{BASE_IMAGE_URL}{mls_tail}/genMid.{image_filenames[0]}"
        upload_image(mls, first_img_url)
        first_image_blob = f"images/{mls}_1.jpg" # Reference for DB
    else:
        first_image_blob = None

    # 6. Return Data Record
    record = {
        "url": url,
        "MLS": mls,
        "Sold Price": sold_price,
        "Number Beds": beds,
        "Number Baths": baths,
        "Sold Date": sold_evt["eventDate"] if sold_evt else extract_between(html, '"lastSaleDate":"'),
        "Address": extract_between(html, 'assembledAddress":"', "\\"),
        "Postal Code": extract_between(html, '"postalCode":"', '"'),
        "Property Type": extract_between(html, 'Property Type","content":"', "\\"),
        "latitude": lat,
        "longitude": lon,
        "First Listed Date": first_listed_evt["eventDate"] if first_listed_evt else None,
        "Days On Market": days_on_market,
        "Sold Price Difference": sold_price_diff,
        "photo_blob": first_image_blob, # Azure Path
    }
    return record


def main():
    start_time = time.time()
    
    # Load URLs
    if not URLS_FILE.exists():
        print("URL File not found!")
        return

    with URLS_FILE.open("r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    
    print(f"🚀 Found {len(urls)} total URLs.")
    
    # LIMIT BATCH SIZE (Prevent timeouts)
    MAX_SCRAPE_COUNT = int(os.getenv("MAX_SCRAPE_COUNT", 10))
    if len(urls) > MAX_SCRAPE_COUNT:
        print(f"⚠️ Limiting scrape to last {MAX_SCRAPE_COUNT} URLs (Configured by MAX_SCRAPE_COUNT)")
        urls = urls[-MAX_SCRAPE_COUNT:]
        
    print(f"🚀 Starting scrape for {len(urls)} properties...")

    results = []
    with ThreadPoolExecutor(max_workers=1) as pool:
        futures = {pool.submit(process_property, url): url for url in urls}
        
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)
                print(f"✅ Processed {res.get('MLS', 'Unknown')}")
    
    # 7. Save Silver (Parquet) to Azure
    if results:
        # Resolve FutureWarning: Wrap string in StringIO
        df = pd.read_json(io.StringIO(json.dumps(results))) # Ensure types
        
        # Save locally first
        local_parquet = "temp_silver.parquet"
        df.to_parquet(local_parquet, index=False)
        
        # Upload to Azure Silver
        today_month = datetime.now().strftime("%Y-%m")
        blob_name = f"silver/{today_month}/listed_properties.parquet"
        
        with open(local_parquet, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
            
        print(f"\n🎉 Success! Uploaded {len(results)} rows to {blob_name}")
        os.remove(local_parquet)
    else:
        print("No valid results found.")

    print(f"⏱️ Total time: {time.time() - start_time:.1f}s")

if __name__ == "__main__":
    main()
