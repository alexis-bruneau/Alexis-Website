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
from playwright.sync_api import sync_playwright
from azure.storage.blob import BlobServiceClient
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ---------------- Config --------------------------------------------------
# Explicitly load .env from current directory or parent
env_path = Path(".env")
if not env_path.exists():
    env_path = Path("..") / ".env"
load_dotenv(dotenv_path=env_path)

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"
QUEUE_BLOB_NAME = "queue/pending_urls.txt"
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
START_URL = "https://www.redfin.ca/on/ottawa/filter/sort=hi-sale-date,include=sold-3yr"

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

# ---------------- Extraction Helpers --------------------------------------

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
    try: return datetime.strptime(s, "%b %d, %Y")
    except: return None

money_re = re.compile(r"[^\d]")
def money_to_int(s):
    s_clean = money_re.sub("", s)
    return int(s_clean) if s_clean else None

def safe_float(val):
    try:
        if isinstance(val, (float, int)): return float(val)
        return float(val) if val and val != "N/A" else None
    except: return None

# ---------------- Storage Helpers -----------------------------------------

def get_existing_silver_urls():
    """Fetches already scraped URLs from the latest Silver Parquet."""
    try:
        blobs = container_client.list_blobs(name_starts_with="silver/")
        latest_blob, latest_time = None, None
        for blob in blobs:
            if blob.name.endswith("listed_properties.parquet"):
                if latest_time is None or blob.last_modified > latest_time:
                    latest_time, latest_blob = blob.last_modified, blob.name
        if latest_blob:
            data = container_client.get_blob_client(latest_blob).download_blob().readall()
            df = pd.read_parquet(io.BytesIO(data))
            if "url" in df.columns:
                return set(df["url"].dropna().tolist()), latest_blob
        return set(), None
    except Exception as e:
        print(f"⚠️ Error checking Silver URLs: {e}")
        return set(), None

def get_pending_queue():
    """Fetches the current pending queue from Azure."""
    blob_client = container_client.get_blob_client(QUEUE_BLOB_NAME)
    if not blob_client.exists(): return []
    try:
        data = blob_client.download_blob().readall().decode("utf-8")
        return [line.strip() for line in data.splitlines() if line.strip()]
    except Exception as e:
        print(f"⚠️ Error fetching pending queue: {e}")
        return []

def update_pending_queue(completed_urls=None, new_urls=None):
    """Adds or removes URLs from the Azure pending queue."""
    current_queue = get_pending_queue()
    queue_set = set(current_queue)
    
    if completed_urls:
        queue_set -= set(completed_urls)
    if new_urls:
        queue_set.update(set(new_urls))
        
    updated_list = sorted(list(queue_set))
    container_client.get_blob_client(QUEUE_BLOB_NAME).upload_blob("\n".join(updated_list), overwrite=True)
    
    action = f"Removed {len(completed_urls)}" if completed_urls else f"Added {len(new_urls)}"
    print(f"🧹 Azure queue: {action}. Remaining: {len(updated_list)}")
    return updated_list

def upload_bronze_html(html_content, mls):
    today = datetime.now().strftime("%Y-%m-%d")
    blob_name = f"bronze/{today}/{mls}.html.gz"
    if container_client.get_blob_client(blob_name).exists(): return False
    compressed = zlib.compress(html_content.encode("utf-8"))
    container_client.get_blob_client(blob_name).upload_blob(compressed, overwrite=True)
    return True

def upload_image(mls, image_url):
    blob_name = f"images/{mls}_1.jpg"
    blob_client = container_client.get_blob_client(blob_name)
    if blob_client.exists(): return
    try:
        resp = requests.get(image_url, timeout=10)
        if resp.status_code == 200:
            blob_client.upload_blob(resp.content, overwrite=True)
    except: pass

# ---------------- Discovery Logic -----------------------------------------

def top_up_queue(context, target_count, scraped_urls, current_pending):
    """Finds new properties on Redfin to refill the queue using robust pagination."""
    print(f"🔍 Discovery: Queue ({len(current_pending)}) < Target ({target_count}). Searching for more...")
    page = context.new_page()
    page.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image", "media", "font"] else r.continue_())
    
    known_urls = scraped_urls.union(set(current_pending))
    new_found = []
    current_discovery_url = START_URL
    page_num = 1
    
    while (len(new_found) + len(current_pending)) < target_count:
        print(f"   --- Searching Page {page_num} ---")
        try:
            page.goto(current_discovery_url, wait_until="domcontentloaded", timeout=45000)
            time.sleep(2)
            
            # Scroll to trigger lazy loads
            for _ in range(3): 
                page.mouse.wheel(0, 1000)
                time.sleep(0.5)
            
            # Check for "No Results"
            if page.locator("text=No results found").count() > 0:
                 print("   -> No results found.")
                 break

            links = page.locator("a[href*='/home/']").all()
            count_before = len(new_found)
            for link in links:
                href = link.get_attribute("href")
                if not href: continue
                full_url = f"https://www.redfin.ca{href}" if href.startswith("/") else href
                if full_url not in known_urls and full_url not in new_found:
                    new_found.append(full_url)
                    if (len(new_found) + len(current_pending)) >= target_count: 
                        break
            
            added = len(new_found) - count_before
            print(f"   + Found {added} new properties on this page.")

            if (len(new_found) + len(current_pending)) >= target_count: 
                break
            
            # Pagination Logic
            next_button = page.locator("button[data-rf-test-id='react-data-paginate-next']").first
            if next_button.count() == 0:
                next_button = page.locator(".step-next").first
            
            if next_button.count() > 0 and next_button.is_enabled():
                classes = next_button.get_attribute("class") or ""
                if "disabled" in classes:
                    print("   -> Next button disabled. End of results.")
                    break
                
                next_button.click()
                page_num += 1
                try:
                    page.wait_for_url(lambda u: str(page_num) in u or f"page-{page_num}" in u, timeout=5000)
                    current_discovery_url = page.url
                except:
                    # Fallback to manual URL update if click/wait fails
                    if "/page-" in current_discovery_url:
                        current_discovery_url = re.sub(r'/page-\d+', f'/page-{page_num}', current_discovery_url)
                    else:
                        current_discovery_url = f"{current_discovery_url}/page-{page_num}"
            else:
                # Manual Fallback: Construct next page URL
                print("   -> 'Next' button not found. Attempting manual URL fallback...")
                page_num += 1
                if "/page-" in current_discovery_url:
                    current_discovery_url = re.sub(r'/page-\d+', f'/page-{page_num}', current_discovery_url)
                else:
                    current_discovery_url = f"{current_discovery_url}/page-{page_num}"
                continue

        except Exception as e:
            print(f"❌ Discovery error on page {page_num}: {e}")
            break
            
    page.close()
    if new_found:
        print(f"✨ Found {len(new_found)} new URLs.")
        return update_pending_queue(new_urls=new_found)
    return current_pending

def parse_sale_history(html, url):
    start_marker = r'\"events\":[{'
    start_idx = html.find(start_marker)
    if start_idx == -1:
        start_marker = r'"events":[{'
        start_idx = html.find(start_marker)
    
    if start_idx == -1: return []

    array_start_idx = start_idx + len(start_marker) - 2
    candidate = html[array_start_idx : array_start_idx + 25000]
    unescaped = candidate.replace(r'\"', '"').replace(r'\\', '\\')
    
    decoder = json.JSONDecoder()
    try:
        events_list, _ = decoder.raw_decode(unescaped)
    except: return []
        
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
        if not script.string: continue
        try:
            data = json.loads(script.string)
            stack = [data]
            while stack:
                curr = stack.pop()
                if isinstance(curr, dict):
                    if "price" in curr and "priceCurrency" in curr: return int(curr["price"])
                    if "offers" in curr: stack.append(curr["offers"])
                    for v in curr.values():
                        if isinstance(v, (dict, list)): stack.append(v)     
                elif isinstance(curr, list):
                    for item in curr:
                        if isinstance(item, (dict, list)): stack.append(item)
        except: continue
    return None

# ---------------- Core Processing -----------------------------------------

def process_property(url, context):
    page = context.new_page()
    page.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image", "media", "font"] else r.continue_())
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(1.5)
        if "/login" in page.url: 
            page.close()
            return None
        html = page.content()
    except Exception as e:
        print(f"⚠️ Error at {url}: {e}")
        page.close()
        return None
    page.close()

    # 1. MLS
    mls = extract_between(html, "TREB #", "<")
    if not mls or mls == "N/A": 
        mls = f"UNKNOWN_{int(time.time())}"
    
    # 2. Bronze
    upload_bronze_html(html, mls)
    
    # 3. Parse History
    history = parse_sale_history(html, url)
    history.sort(key=lambda h: parse_date(h["eventDate"]) or datetime.min)
    
    sold_evts = [h for h in history if "sold" in h["eventType"].lower()]
    sold_evt = sold_evts[-1] if sold_evts else None
    
    listed_evts = [h for h in history if "listed" in h["eventType"].lower() and "delisted" not in h["eventType"].lower()]
    first_listed_evt = listed_evts[0] if listed_evts else None
    
    # 4. Price Logic
    sold_price = money_to_int(sold_evt["price"]) if sold_evt else None
    if not sold_price:
        sold_price = extract_price_from_json(html)
    
    first_list_price = money_to_int(first_listed_evt["price"]) if first_listed_evt else None
    
    # 5. Metrics
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

    # 6. Photos
    mls_tail = mls[-3:] if len(mls) >= 3 else mls
    image_filenames = find_genmid_values(html)
    photo_blob = None
    if image_filenames:
        first_img_url = f"{BASE_IMAGE_URL}{mls_tail}/genMid.{image_filenames[0]}"
        upload_image(mls, first_img_url)
        photo_blob = f"images/{mls}_1.jpg"

    # 7. Record
    return {
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
        "photo_blob": photo_blob,
    }

# ---------------- Main ----------------------------------------------------

def main():
    start_time = time.time()
    MAX_SCRAPE = int(os.getenv("MAX_SCRAPE_COUNT", 10))
    
    # 1. Get State
    scraped_urls, _ = get_existing_silver_urls()
    pending_queue = get_pending_queue()
    
    # 2. Check and Top-Up
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
        
        # Load Cookies
        if COOKIE_FILE.exists():
            with COOKIE_FILE.open() as f:
                raw = json.load(f)
                context.add_cookies([{**c, "sameSite": "Lax", "expires": int(time.time())+3600*24*30} for c in raw if ".redfin.ca" in c.get("domain", "")])

        # Top up if queue is dry
        if len(pending_queue) < MAX_SCRAPE:
            pending_queue = top_up_queue(context, MAX_SCRAPE, scraped_urls, pending_queue)
            
        # 3. Execution
        to_process = pending_queue[:MAX_SCRAPE]
        print(f"🚀 Processing {len(to_process)} properties...")
        
        results, processed = [], []
        for url in to_process:
            if url in scraped_urls: # Final guard
                processed.append(url)
                continue
            res = process_property(url, context)
            if res:
                results.append(res)
                processed.append(url)
                print(f"✅ Processed {res.get('MLS')}")
        
        browser.close()

    # 4. Save Results
    if results:
        new_df = pd.DataFrame(results)
        today_month = datetime.now().strftime("%Y-%m")
        blob_name = f"silver/{today_month}/listed_properties.parquet"
        
        try:
            blob_client = container_client.get_blob_client(blob_name)
            if blob_client.exists():
                existing_df = pd.read_parquet(io.BytesIO(blob_client.download_blob().readall()))
                final_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(subset=["MLS"], keep="last")
                print(f"   -> Merged data. Total records: {len(final_df)}")
            else:
                final_df = new_df
            
            local_parquet = "temp_unified.parquet"
            final_df.to_parquet(local_parquet, index=False)
            with open(local_parquet, "rb") as d:
                container_client.upload_blob(name=blob_name, data=d, overwrite=True)
            if os.path.exists(local_parquet): os.remove(local_parquet)
            
            # Cleanup Queue
            update_pending_queue(completed_urls=processed)
            print(f"🎉 Success! Processed {len(results)} properties.")
        except Exception as e:
            print(f"❌ Error saving: {e}")
    else:
        print("No new data processed.")

    print(f"⏱️ Total time: {time.time() - start_time:.1f}s")

if __name__ == "__main__":
    main()
