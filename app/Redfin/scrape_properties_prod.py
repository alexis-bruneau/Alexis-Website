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
MAX_URLS = 1000

URLS_FILE = Path("app/Redfin/Output/property_urls.txt")
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
WAIT_SEC = 2

# Run limits (all overridable from the environment / --limit):
#   MAX_SCRAPE_COUNT  0 = process every pending URL (default). Set a number to cap a run.
#   CHECKPOINT_EVERY  upload the silver parquet after this many listings, so a crash loses little.
#   MAX_CONSECUTIVE_FAILURES  stop early when Redfin is clearly blocking us (expired cookies, 403s).
MAX_SCRAPE_COUNT = int(os.getenv("MAX_SCRAPE_COUNT", "0"))
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "25"))
MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "8"))
IMAGE_WORKERS = 8

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

# Blob names already in Azure, listed once at startup (replaces one exists() call per listing).
_bronze_today = set()
_existing_images = set()

def upload_bronze_html(html_content, mls):
    """Saves compressed HTML to bronze/YYYY-MM-DD/mls.html.gz. Returns False if already saved today."""
    today = datetime.now().strftime("%Y-%m-%d")
    blob_name = f"bronze/{today}/{mls}.html.gz"
    if blob_name in _bronze_today:
        return False  # Skipped

    compressed = zlib.compress(html_content.encode("utf-8"))
    container_client.get_blob_client(blob_name).upload_blob(compressed, overwrite=True)
    _bronze_today.add(blob_name)
    return True  # Uploaded

def upload_image(mls, image_url, retries=3):
    """Uploads ONLY the first image to images/mls_1.jpg. Returns True only if the blob is in Azure."""
    blob_name = f"images/{mls}_1.jpg"
    if blob_name in _existing_images:
        return True

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(image_url, timeout=15)
            if resp.status_code == 200 and resp.content:
                container_client.get_blob_client(blob_name).upload_blob(resp.content, overwrite=True)
                _existing_images.add(blob_name)
                return True
            print(f"   -> Image {mls}: HTTP {resp.status_code} (attempt {attempt}/{retries})")
        except Exception as e:
            print(f"   -> Image {mls} failed (attempt {attempt}/{retries}): {e}")
        time.sleep(attempt)  # simple backoff
    return False

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

# ---------------- Core Logic ----------------------------------------------

def process_property(url, context):
    # 1. Scrape HTML using existing context
    page = context.new_page()
    
    # Block heavy resources
    def block_heavy(route):
        if route.request.resource_type in ["image", "media", "font"]:
            route.abort()
        else:
            route.continue_()
            
    page.route("**/*", block_heavy)

    try:
        # domcontentloaded is much faster than load (doesn't wait for all assets)
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        
        # Short sleep to let JS hydrate (Redfin needs this)
        time.sleep(1.5) 
        
        if "/login" in page.url:
            print(f"🔒 Login page encountered at {url}")
            page.close()
            return None
        html = page.content()
    except Exception as e:
        print(f"⚠️ Error/Timeout at {url}: {e}")
        page.close()
        return None
    
    page.close()

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
    image_filenames = find_genmid_values(html)
    # Prefer photos that belong to this listing's own MLS number. A re-listed property's page shows
    # photos from the earlier listing (different MLS), so the CDN folder must come from the PHOTO's
    # MLS number, not the current one (stable sort keeps the page order otherwise).
    image_filenames.sort(key=lambda name: not name.startswith(mls))
    photo_mls = image_filenames[0].split("_")[0] if image_filenames else ""
    mls_tail = photo_mls[-3:]
    # The upload happens in main() (in parallel); photo_blob is only set once it succeeds.
    first_img_url = f"{BASE_IMAGE_URL}{mls_tail}/genMid.{image_filenames[0]}" if image_filenames else None

    # Extract Property Type securely
    prop_type_match = re.search(r'"propertyType"\s*:\s*"([^"]+)"', html)
    if prop_type_match:
        property_type = prop_type_match.group(1).title()
    else:
        property_type = extract_between(html, 'Property Type","content":"', "\\")
        if property_type == "N/A":
            property_type = None

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
        "Property Type": property_type,
        "latitude": lat,
        "longitude": lon,
        "First Listed Date": first_listed_evt["eventDate"] if first_listed_evt else None,
        "Days On Market": days_on_market,
        "Sold Price Difference": sold_price_diff,
        "photo_blob": None,        # Azure path, filled in by main() after a successful upload
        "_img_url": first_img_url, # internal: removed before saving
    }
    return record


def load_silver_state(month_blob):
    """Reads all silver parquets once. Returns (known_urls, current_month_df)."""
    known_urls = set()
    current_df = pd.DataFrame()
    for blob in container_client.list_blobs(name_starts_with="silver/"):
        if not blob.name.endswith("listed_properties.parquet"):
            continue
        df = pd.read_parquet(io.BytesIO(container_client.get_blob_client(blob.name).download_blob().readall()))
        if "url" in df.columns:
            known_urls.update(df["url"].dropna())
        if blob.name == month_blob:
            current_df = df
    return known_urls, current_df


def save_silver(df, month_blob):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    container_client.upload_blob(name=month_blob, data=buf, overwrite=True)


def main(limit=None):
    start_time = time.time()
    limit = MAX_SCRAPE_COUNT if limit is None else limit

    if not URLS_FILE.exists():
        print("URL File not found! Run get_properties.py first.")
        return

    month_blob = f"silver/{datetime.now().strftime('%Y-%m')}/listed_properties.parquet"
    today = datetime.now().strftime("%Y-%m-%d")

    # One-time state from Azure: what is already scraped, which blobs already exist.
    print("Reading existing state from Azure...")
    known_urls, current_df = load_silver_state(month_blob)
    _existing_images.update(b.name for b in container_client.list_blobs(name_starts_with="images/"))
    _bronze_today.update(b.name for b in container_client.list_blobs(name_starts_with=f"bronze/{today}/"))
    print(f"   {len(known_urls)} listings already in silver, {len(_existing_images)} images in Azure.")

    with URLS_FILE.open("r", encoding="utf-8") as f:
        all_urls = list(dict.fromkeys(u.strip() for u in f if u.strip()))
    pending = [u for u in all_urls if u not in known_urls]
    print(f"URL file: {len(all_urls)} URLs, {len(pending)} not yet in Azure.")

    if limit and len(pending) > limit:
        print(f"Limiting this run to the first {limit} pending URLs (newest first).")
        pending = pending[:limit]
    if not pending:
        print("Nothing to scrape. Done.")
        return

    new_rows = []          # records not yet written to Azure
    image_jobs = []        # (record, future) for photo uploads
    saved_total = 0
    failures_in_row = 0

    def flush():
        """Wait for image uploads, then merge new rows into this month's parquet and upload it."""
        nonlocal current_df, new_rows, image_jobs, saved_total
        for rec, fut in image_jobs:
            if fut.result():
                rec["photo_blob"] = f"images/{rec['MLS']}_1.jpg"
        image_jobs = []
        if not new_rows:
            return
        batch = pd.DataFrame([{k: v for k, v in r.items() if not k.startswith("_")} for r in new_rows])
        merged = pd.concat([current_df, batch], ignore_index=True) if len(current_df) else batch
        merged = merged.drop_duplicates(subset=["MLS"], keep="last")
        # Never persist the literal string "None" for a missing photo.
        merged["photo_blob"] = merged["photo_blob"].where(~merged["photo_blob"].isin(["None", "nan", ""]), None)
        save_silver(merged, month_blob)
        saved_total += len(new_rows)
        print(f"   [Silver] Saved {len(new_rows)} rows -> {month_blob} (file now {len(merged)} rows)")
        current_df, new_rows = merged, []

    with sync_playwright() as p, ThreadPoolExecutor(max_workers=IMAGE_WORKERS) as pool:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
        context.add_cookies(load_redfin_cookies())

        print(f"Starting scrape for {len(pending)} properties...")
        for i, url in enumerate(pending, 1):
            res = process_property(url, context)
            if res is None:
                failures_in_row += 1
                if failures_in_row >= MAX_CONSECUTIVE_FAILURES:
                    print(f"Stopping: {failures_in_row} failures in a row (blocked or cookies expired?).")
                    break
                continue
            failures_in_row = 0

            img_url = res.get("_img_url")
            if img_url:
                image_jobs.append((res, pool.submit(upload_image, res["MLS"], img_url)))
            new_rows.append(res)
            print(f"[{i}/{len(pending)}] {res.get('MLS', 'Unknown')}")

            if len(new_rows) >= CHECKPOINT_EVERY:
                flush()

        browser.close()
        flush()

    print(f"\nDone: saved {saved_total} new listings in {time.time() - start_time:.0f}s "
          f"({len(pending) - saved_total} not saved).")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape pending Redfin listings into Azure.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max listings this run (default: MAX_SCRAPE_COUNT env, 0 = all pending)")
    main(limit=parser.parse_args().limit)
