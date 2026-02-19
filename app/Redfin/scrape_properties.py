#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime
import json, time, re
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- config --------------------------------------------------
test = True  # False = scrape all URLs in file
MAX_URLS = 250 # Will not scrape all properties if test=True
URLS_FILE = Path("app/Redfin/Output/property_urls.txt")
OUT_CSV = Path("app/Redfin/Output/redfin_data.csv")
OUT_HISTORY = Path("app/Redfin/Output/redfin_sale_history.csv")
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
WAIT_SEC = 3  # Increased to avoid rate limiting
IMAGES_DIR = Path("app/Redfin/Output/images")
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"


# ---------------- helpers -------------------------------------------------
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
    # Try to find the events JSON in the script tag
    # Looking for "events":[{ ... }]
    # Only simple string search + unescape + JSON parse
    
    start_marker = r'\"events\":[{'
    end_marker = r'}]'
    
    start_idx = html.find(start_marker)
    if start_idx == -1:
        start_marker = r'"events":[{'
        start_idx = html.find(start_marker)
    
    if start_idx == -1:
        # Fallback for some pages?
        return []

    # Point to [
    array_start_idx = start_idx + len(start_marker) - 2
    
    # Grab a large chunk
    candidate = html[array_start_idx : array_start_idx + 25000]
    
    # Unescape
    unescaped = candidate.replace(r'\"', '"').replace(r'\\', '\\')
    
    # Parse
    decoder = json.JSONDecoder()
    try:
        events_list, _ = decoder.raw_decode(unescaped)
    except json.JSONDecodeError:
        return []
        
    rows = []
    for e in events_list:
        # Convert timestamp to "%b %d, %Y" for compatibility
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


def download_images(image_urls, mls):
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    for idx, url in enumerate(image_urls, start=1):
        path = IMAGES_DIR / f"{mls}_{idx}.jpg"
        if not path.exists():
            try:
                resp = requests.get(url, timeout=20)
                if resp.status_code == 200:
                    path.write_bytes(resp.content)
            except Exception:
                continue



def extract_price_from_json(html):
    """
    Scans all <script> tags for JSON-LD containing 'offers' -> 'price'.
    Returns the first valid integer price found, or None.
    """
    soup = BeautifulSoup(html, "lxml")
    for script in soup.find_all("script"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
            # Recursive search for "offers" -> "price"
            # or data['offers']['price'] directly
            
            stack = [data]
            while stack:
                curr = stack.pop()
                if isinstance(curr, dict):
                    # Check if this dict matches the Offer schema with price
                    if "price" in curr and "priceCurrency" in curr:
                         return int(curr["price"])
                    
                    # Also check if it has an 'offers' key that is a dict/list
                    if "offers" in curr:
                        stack.append(curr["offers"])
                    
                    # Add all values to stack
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

def scrape_single(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 ... Chrome/125 Safari/537"
        )
        context.add_cookies(load_redfin_cookies())
        page = context.new_page()
        try:
            page.goto(url, wait_until="load", timeout=60000)
            time.sleep(WAIT_SEC)
            if "/login" in page.url:
                print(f"🔒 Login page encountered at {url}")
                return None, []
            html = page.content()
        except TimeoutError:
            print(f"⚠️ Timeout at {url}")
            return None, []
        finally:
            browser.close()

    mls = extract_between(html, "TREB #", "<")
    mls_tail = mls[-3:] if len(mls) >= 3 else mls
    image_filenames = find_genmid_values(html)
    image_urls = [f"{BASE_IMAGE_URL}{mls_tail}/genMid.{fn}" for fn in image_filenames]
    download_images(image_urls, mls)

    history = parse_sale_history(html, url)
    
    # Sort history by date to be safe
    history.sort(key=lambda h: parse_date(h["eventDate"]) or datetime.min)
    
    # 1. Find Sold Event (latest one)
    # Filter for 'sold', take the last one (latest date)
    sold_evts = [h for h in history if "sold" in h["eventType"].lower()]
    sold_evt = sold_evts[-1] if sold_evts else None
    
    # 2. Find First Listed Event
    # Filter for 'listed', take the first one (earliest date)
    listed_evts = [h for h in history if "listed" in h["eventType"].lower() and "delisted" not in h["eventType"].lower()]
    first_listed_evt = listed_evts[0] if listed_evts else None
    
    # Fallback: if no specific listed event, use absolute earliest event?
    # User specifically asked for "First Listed Date".
    earliest_evt = history[0] if history else None

    try:
        # Try to get price from history first
        sold_price = money_to_int(sold_evt["price"]) if sold_evt else None
        
        # Fallback/Primary: JSON-LD extraction
        if not sold_price:
             json_price = extract_price_from_json(html)
             if json_price:
                 sold_price = json_price
        
        first_list_price = money_to_int(first_listed_evt["price"]) if first_listed_evt else None
        
        # Safe float conversions
        beds = safe_float(extract_between(html, '"latestListingInfo":{"beds":', ","))
        baths = safe_float(extract_between(html, '"baths":', ","))
        lat = safe_float(extract_between(html, 'latitude":', ","))
        lon = safe_float(extract_between(html, 'longitude":', "}"))
        
        # Days On Market & Sold Price Difference
        days_on_market = None
        sold_price_diff = None
        
        if sold_evt and first_listed_evt:
            d_sold = parse_date(sold_evt["eventDate"])
            d_listed = parse_date(first_listed_evt["eventDate"])
            if d_sold and d_listed:
                days_on_market = (d_sold - d_listed).days
                
        if sold_price and first_list_price:
            sold_price_diff = sold_price - first_list_price

        # Get first image filename for photo_blob column
        first_image = f"{mls}_1.jpg" if image_filenames else None
        
        return {
            "url": url,
            "MLS": mls,
            "Sold Price": sold_price,
            "Number Beds": beds,
            "Number Baths": baths,
            "Sold Date": (
                sold_evt["eventDate"]
                if sold_evt
                else extract_between(html, '"lastSaleDate":"')
            ),
            "Address": extract_between(html, 'assembledAddress":"', "\\"),
            "Postal Code": extract_between(html, '"postalCode":"', '"'),
            "Property Type": extract_between(html, 'Property Type","content":"', "\\"),
            "Square Foot": extract_between(html, 'Lot Size","content":"', " "),
            "Parking": extract_between(html, 'Parking","content":"', " "),
            "Association Fee": extract_between(html, "Association Fee: <span>$", "<"),
            "latitude": lat,
            "longitude": lon,
            "First Listed Date": first_listed_evt["eventDate"] if first_listed_evt else None,
            "Days On Market": days_on_market,
            "Sold Price Difference": sold_price_diff,
            "photo_blob": first_image,  # First image for local serving
        }, history
    except Exception as e:
        print(f"❌ Error for {mls}: {e}")
        return None, history


# ---------------- main ----------------------------------------------------
def main():
    start_time = time.time()

    # --- load URLs --------------------------------------------------------
    with URLS_FILE.open("r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    if test:
        urls = urls[:MAX_URLS]
    total = len(urls)

    # --- scrape in parallel ----------------------------------------------
    summary, history = [], []
    completed = 0
    with ThreadPoolExecutor(max_workers=1) as pool:  # Reduced to 1 to avoid 403
        futures = {pool.submit(scrape_single, url): url for url in urls}

        for fut in as_completed(futures):
            url = futures[fut]
            result, hist = fut.result()
            completed += 1

            # ❌ Skip entries with missing or invalid sold_price
            if result and isinstance(result.get("Sold Price"), int):
                summary.append(result)
                msg = f"[{completed}/{total}] ✅ scraped MLS {result['MLS']} ({url})"
            else:
                msg = f"[{completed}/{total}] ⚠️ skipped (missing sold price) for {url}"

            print(msg, flush=True)
            history.extend(hist)

    # --- save outputs ----------------------------------------------------
    pd.DataFrame(summary).to_csv(OUT_CSV, index=False)
    pd.DataFrame(history).to_csv(OUT_HISTORY, index=False)
    print(f"\n✅ Saved {len(summary)} listings to {OUT_CSV}")
    print(f"✅ Saved {len(history)} history rows to {OUT_HISTORY}")
    print(f"⏱️ Took {time.time() - start_time:.1f} seconds")


if __name__ == "__main__":
    main()
