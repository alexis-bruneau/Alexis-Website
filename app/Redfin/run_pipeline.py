#!/usr/bin/env python3
"""
Unified Redfin Scraping Pipeline
=================================
Combines URL discovery + property scraping into a single run.

Pipeline Steps:
  1. Load Azure state (existing parquet → known URLs + scraped MLS set)
  2. Scrape any UNSCRAPED URLs (in the URL list but missing from parquet)
  3. Discover NEW URLs from Redfin search pages
  4. Scrape the newly discovered URLs
  5. Merge all results and upload to Azure

Environment Variables (set in .env):
  AZURE_STORAGE_CONNECTION_STRING  — required
  MAX_SCRAPE_COUNT                 — max properties to scrape per run (default: 50)
  MAX_NEW_URLS                     — max new URLs to discover per run (default: 100)
"""

import os
import io
import json
import time
import zlib
import re
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

# Load .env (search current dir then parent)
env_path = Path(".env")
if not env_path.exists():
    env_path = Path("..") / ".env"
    if not env_path.exists():
        env_path = Path("../..") / ".env"
load_dotenv(dotenv_path=env_path)

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"

# Batch sizes from env
MAX_SCRAPE_COUNT = int(os.getenv("MAX_SCRAPE_COUNT", 5000))
MAX_NEW_URLS = int(os.getenv("MAX_NEW_URLS", 5000))

# Redfin search URL (sold in last 3 years, sorted by sale date)
SEARCH_URL = "https://www.redfin.ca/on/ottawa/filter/sort=hi-sale-date,include=sold-3yr"

# File paths
URLS_FILE = Path("app/Redfin/Output/property_urls.txt")
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"
WAIT_SEC = 2

# ──────────────────────────────────────────────────────────────────────────────
# AZURE CLIENT
# ──────────────────────────────────────────────────────────────────────────────

if not CONN_STR:
    raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in .env")

blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
if not container_client.exists():
    container_client.create_container()

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

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


def load_azure_state():
    """
    Downloads ALL parquet files from Azure and returns:
      - scraped_urls: set of URLs that have been scraped (in parquet)
      - scraped_mls: set of MLS numbers already in parquet
      - existing_df: combined DataFrame of all existing data (for merging)
    """
    scraped_urls = set()
    scraped_mls = set()
    dfs = []

    try:
        blobs = list(container_client.list_blobs(name_starts_with="silver/"))
        parquet_blobs = [b for b in blobs if b.name.endswith("listed_properties.parquet")]

        if not parquet_blobs:
            print("ℹ️  No existing parquet files in Azure.")
            return scraped_urls, scraped_mls, None

        for blob in parquet_blobs:
            blob_client = container_client.get_blob_client(blob.name)
            data = blob_client.download_blob().readall()
            df = pd.read_parquet(io.BytesIO(data))
            dfs.append(df)

        if dfs:
            existing_df = pd.concat(dfs, ignore_index=True)
            existing_df = existing_df.drop_duplicates(subset=["MLS"], keep="last")

            if "url" in existing_df.columns:
                scraped_urls = set(existing_df["url"].dropna().tolist())
            if "MLS" in existing_df.columns:
                scraped_mls = set(existing_df["MLS"].dropna().tolist())

            print(f"✅ Azure state: {len(scraped_urls)} URLs, {len(scraped_mls)} MLS numbers, {len(existing_df)} total rows")
            return scraped_urls, scraped_mls, existing_df

    except Exception as e:
        print(f"❌ Error loading Azure state: {e}")

    return scraped_urls, scraped_mls, None


def load_local_urls():
    """Load URLs from the local file."""
    if not URLS_FILE.exists():
        return []
    with URLS_FILE.open("r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def save_urls_to_file(urls):
    """Append new URLs to the local file."""
    URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with URLS_FILE.open("a", encoding="utf-8") as f:
        for url in urls:
            f.write(f"{url}\n")
    print(f"💾 Saved {len(urls)} new URLs to {URLS_FILE}")


# ──────────────────────────────────────────────────────────────────────────────
# PARSING HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def extract_between(text, start, stop="\\"):
    markers = [start] if '"' not in start else [start, start.replace('"', r'\"')]
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


# ──────────────────────────────────────────────────────────────────────────────
# IMAGE EXTRACTION (IMPROVED)
# ──────────────────────────────────────────────────────────────────────────────

def is_real_image_url(url):
    """Filter out Redfin branding/logo/placeholder URLs."""
    if not url:
        return False
    bad_patterns = [
        "redfin-logo", "redfin_logo", "rf-logo",
        "/logos/", "/branding/", "/favicon",
        "redfin-default", "no-photo", "placeholder",
        "social-share", "og-image-default",
        "/vLATEST/images/",  # Redfin's static assets path
    ]
    url_lower = url.lower()
    return not any(p in url_lower for p in bad_patterns)


# Minimum image size to accept (bytes) — Redfin logo is ~7KB, real photos are 50KB+
MIN_IMAGE_BYTES = 30_000


def extract_image_url(html, mls):
    """
    Extracts the best image URL for a property using multiple strategies.
    Order is tuned based on real-world testing:
      1. JSON-LD <script type="application/ld+json"> → "image" field
      2. Inline JSON "photos" / "mediaBrowserPhotos" arrays
      3. genMid regex + constructed CDN URL (most reliable for real photos)
      4. Open Graph <meta property="og:image"> tag (LAST — often returns Redfin logo)
    All URLs are filtered to reject known Redfin branding/logo patterns.
    Returns the image URL string, or None.
    """

    # Strategy 1: JSON-LD structured data
    try:
        soup = BeautifulSoup(html, "lxml")
        for script in soup.find_all("script", type="application/ld+json"):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
                # Could be a single object or a list
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    # Direct "image" field
                    img = item.get("image")
                    if img:
                        if isinstance(img, list) and len(img) > 0:
                            img = img[0]
                        if isinstance(img, dict):
                            img = img.get("url") or img.get("contentUrl")
                        if isinstance(img, str) and img.startswith("http") and is_real_image_url(img):
                            return img
                    # "photo" field (Redfin sometimes uses this)
                    photos = item.get("photo")
                    if photos:
                        if isinstance(photos, list) and len(photos) > 0:
                            photo = photos[0]
                            if isinstance(photo, dict):
                                url = photo.get("contentUrl") or photo.get("url")
                                if url and is_real_image_url(url):
                                    return url
                            elif isinstance(photo, str) and photo.startswith("http") and is_real_image_url(photo):
                                return photo
            except (json.JSONDecodeError, TypeError):
                continue
    except Exception:
        pass

    # Strategy 2: Inline JSON photo arrays (Redfin embeds these in script tags)
    try:
        # Look for mediaBrowserPhotos or photoUrls in the inline JSON
        photo_match = re.search(r'"photoUrls":\s*\{[^}]*"fullScreenPhotoUrl":\s*"([^"]+)"', html)
        if photo_match:
            url = photo_match.group(1).replace("\\", "")
            if is_real_image_url(url):
                return url

        # mediaBrowserPhotos array
        photo_match = re.search(r'"mediaBrowserPhotos":\s*\[\s*\{[^}]*"photoUrl":\s*\{[^}]*"fullScreenPhotoUrl":\s*"([^"]+)"', html)
        if photo_match:
            url = photo_match.group(1).replace("\\", "")
            if is_real_image_url(url):
                return url
    except Exception:
        pass

    # Strategy 3: genMid regex (most reliable for actual property photos)
    mls_tail = mls[-3:] if len(mls) >= 3 else mls
    image_filenames = find_genmid_values(html)
    if image_filenames:
        return f"{BASE_IMAGE_URL}{mls_tail}/genMid.{image_filenames[0]}"

    # Strategy 4: Open Graph meta tag (LAST — often returns Redfin logo for sold listings)
    try:
        og_match = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if not og_match:
            og_match = re.search(r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', html, re.IGNORECASE)
        if og_match:
            og_url = og_match.group(1)
            if og_url.startswith("http") and is_real_image_url(og_url):
                return og_url
    except Exception:
        pass

    return None


def upload_image(mls, image_url):
    """Upload image to Azure. Returns True if uploaded, False if skipped/failed.
    Also checks existing images — if they're too small (likely a logo), re-downloads."""
    blob_name = f"images/{mls}_1.jpg"
    blob_client = container_client.get_blob_client(blob_name)

    # Check if exists AND is large enough (not a logo)
    if blob_client.exists():
        props = blob_client.get_blob_properties()
        if props.size and props.size >= MIN_IMAGE_BYTES:
            return True  # Already has a real image
        # Existing image is too small (likely a logo) — re-download
        print(f"   🔄 Replacing small image ({props.size}B)", end="")

    try:
        resp = requests.get(image_url, timeout=15)
        if resp.status_code == 200 and len(resp.content) >= MIN_IMAGE_BYTES:
            blob_client.upload_blob(resp.content, overwrite=True)
            return True
        else:
            if resp.status_code == 200:
                # Downloaded but too small — don't save it
                pass
            return False
    except Exception as e:
        print(f"   ⚠️ Failed image {mls}: {e}")
        return False


def upload_bronze_html(html_content, mls):
    """Save compressed HTML to bronze/YYYY-MM-DD/mls.html.gz"""
    today = datetime.now().strftime("%Y-%m-%d")
    blob_name = f"bronze/{today}/{mls}.html.gz"

    try:
        if container_client.get_blob_client(blob_name).exists():
            return False
    except:
        pass

    compressed = zlib.compress(html_content.encode("utf-8"))
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(compressed, overwrite=True)
    return True


# ──────────────────────────────────────────────────────────────────────────────
# CORE: SCRAPE A SINGLE PROPERTY
# ──────────────────────────────────────────────────────────────────────────────

def scrape_property(url, context):
    """
    Scrapes a single property page and returns a dict of parsed data,
    or None on failure.
    """
    page = context.new_page()

    def block_heavy(route):
        if route.request.resource_type in ["image", "media", "font"]:
            route.abort()
        else:
            route.continue_()

    page.route("**/*", block_heavy)

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(1.5)

        if "/login" in page.url:
            print(f"   🔒 Login page — skipping {url}")
            page.close()
            return None

        html = page.content()
    except Exception as e:
        print(f"   ⚠️ Error at {url}: {e}")
        try:
            page.close()
        except:
            pass
        return None

    page.close()

    # Extract MLS
    mls = extract_between(html, "TREB #", "<")
    if not mls or mls == "N/A":
        mls = f"UNKNOWN_{int(time.time())}"

    # Upload bronze HTML
    bronze_ok = upload_bronze_html(html, mls)

    # Parse sale history
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

    # Image extraction (IMPROVED — multi-strategy)
    image_url = extract_image_url(html, mls)
    first_image_blob = None
    if image_url:
        uploaded = upload_image(mls, image_url)
        if uploaded:
            first_image_blob = f"images/{mls}_1.jpg"

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
        "photo_blob": first_image_blob,
    }
    return record


# ──────────────────────────────────────────────────────────────────────────────
# CORE: DISCOVER NEW URLS
# ──────────────────────────────────────────────────────────────────────────────

def discover_new_urls(known_urls, max_urls):
    """
    Scrape Redfin search pages to find new property URLs.
    Returns a list of new URLs not in known_urls.
    """
    print(f"\n{'='*60}")
    print(f"📡 STEP: Discovering new URLs (max {max_urls})")
    print(f"{'='*60}")

    new_found = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )

        cookies = load_redfin_cookies()
        if cookies:
            context.add_cookies(cookies)

        page = context.new_page()

        def block_heavy(route):
            if route.request.resource_type in ["image", "media", "font"]:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", block_heavy)

        current_url = SEARCH_URL
        page_num = 1

        while len(new_found) < max_urls:
            print(f"\n--- Search Page {page_num} ---")

            try:
                page.goto(current_url, wait_until="domcontentloaded", timeout=45000)
                time.sleep(WAIT_SEC)

                # Scroll to trigger lazy loads
                for _ in range(3):
                    page.mouse.wheel(0, 1000)
                    time.sleep(0.5)

                # Check for "No Results"
                if page.locator("text=No results found").count() > 0:
                    print("   → No results found.")
                    break

                # Scrape property links
                links = page.locator("a[href*='/home/']").all()
                print(f"   Found {len(links)} links on page")

                if len(links) == 0:
                    print(f"   ⚠️ No links found on page {page_num}. Stopping.")
                    break

                count_before = len(new_found)

                for link in links:
                    href = link.get_attribute("href")
                    if not href:
                        continue
                    full_url = f"https://www.redfin.ca{href}" if href.startswith("/") else href

                    if full_url not in known_urls and full_url not in new_found:
                        new_found.append(full_url)
                        known_urls.add(full_url)

                added = len(new_found) - count_before
                print(f"   + {added} new URLs (total: {len(new_found)})")

                if len(new_found) >= max_urls:
                    print("   🛑 Reached URL limit.")
                    break

                # Pagination
                next_button = page.locator("button[data-rf-test-id='react-data-paginate-next']").first
                if next_button.count() == 0:
                    next_button = page.locator(".step-next").first

                if next_button.count() > 0 and next_button.is_enabled():
                    classes = next_button.get_attribute("class") or ""
                    if "disabled" in classes:
                        print("   → End of results.")
                        break

                    next_button.click()
                    page_num += 1

                    try:
                        page.wait_for_url(lambda u: str(page_num) in u or f"page-{page_num}" in u, timeout=5000)
                        current_url = page.url
                    except:
                        if "/page-" in current_url:
                            current_url = re.sub(r'/page-\d+', f'/page-{page_num}', current_url)
                        else:
                            current_url = f"{current_url}/page-{page_num}"
                else:
                    # Manual pagination fallback
                    page_num += 1
                    if "/page-" in current_url:
                        current_url = re.sub(r'/page-\d+', f'/page-{page_num}', current_url)
                    else:
                        current_url = f"{current_url}/page-{page_num}"

            except Exception as e:
                print(f"   ❌ Error on page {page_num}: {e}")
                break

        browser.close()

    print(f"\n✅ Discovered {len(new_found)} new URLs")
    return new_found


# ──────────────────────────────────────────────────────────────────────────────
# CORE: BATCH SCRAPE PROPERTIES
# ──────────────────────────────────────────────────────────────────────────────

def batch_scrape(urls, label=""):
    """
    Scrape a batch of URLs. Returns list of result dicts.
    Uses a single browser instance for efficiency.
    """
    if not urls:
        return []

    print(f"\n{'='*60}")
    print(f"🔍 STEP: Scraping {len(urls)} properties {label}")
    print(f"{'='*60}")

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )
        context.add_cookies(load_redfin_cookies())

        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] {url[:80]}...")
            record = scrape_property(url, context)
            if record:
                results.append(record)
                img_status = "📷" if record.get("photo_blob") else "🚫"
                print(f"   ✅ {record.get('MLS', '?')} — ${record.get('Sold Price', '?')} {img_status}")
            else:
                print(f"   ❌ Failed")

        browser.close()

    print(f"\n✅ Scraped {len(results)}/{len(urls)} properties successfully")
    return results


# ──────────────────────────────────────────────────────────────────────────────
# CORE: UPLOAD RESULTS TO AZURE
# ──────────────────────────────────────────────────────────────────────────────

def upload_results(results, existing_df):
    """Merge new results with existing data and upload to Azure."""
    if not results:
        print("\n⚠️ No results to upload.")
        return

    print(f"\n{'='*60}")
    print(f"☁️  STEP: Uploading {len(results)} results to Azure")
    print(f"{'='*60}")

    new_df = pd.DataFrame(results)

    if existing_df is not None and len(existing_df) > 0:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["MLS"], keep="last")
        print(f"   Merged: {len(new_df)} new + {len(existing_df)} existing = {len(combined_df)} total")
    else:
        combined_df = new_df
        print(f"   New dataset: {len(combined_df)} rows")

    # Determine blob path
    today_month = datetime.now().strftime("%Y-%m")
    blob_name = f"silver/{today_month}/listed_properties.parquet"

    # Save locally then upload
    local_path = "temp_pipeline_silver.parquet"
    combined_df.to_parquet(local_path, index=False)

    with open(local_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)

    print(f"   🎉 Uploaded {len(combined_df)} rows to {blob_name}")

    if os.path.exists(local_path):
        os.remove(local_path)

    return combined_df


# ──────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

def main():
    start_time = time.time()

    print("=" * 60)
    print("🚀 REDFIN UNIFIED PIPELINE")
    print(f"   MAX_SCRAPE_COUNT = {MAX_SCRAPE_COUNT}")
    print(f"   MAX_NEW_URLS     = {MAX_NEW_URLS}")
    print("=" * 60)

    # ── Step 1: Load Azure state ──
    print(f"\n{'='*60}")
    print("📦 STEP 1: Loading Azure state")
    print(f"{'='*60}")

    scraped_urls, scraped_mls, existing_df = load_azure_state()
    local_urls = load_local_urls()
    all_known_urls = scraped_urls.union(set(local_urls))

    print(f"   Local URL file: {len(local_urls)} URLs")
    print(f"   Azure scraped:  {len(scraped_urls)} URLs")
    print(f"   Combined known: {len(all_known_urls)} URLs")

    all_results = []
    scrape_budget = MAX_SCRAPE_COUNT

    # ── Step 2: Scrape unscraped backlog ──
    # Find URLs that are in the local file but NOT yet scraped (not in parquet)
    unscraped = [u for u in local_urls if u not in scraped_urls]
    if unscraped:
        batch = unscraped[:scrape_budget]
        print(f"\n📋 Found {len(unscraped)} unscraped URLs in backlog (scraping {len(batch)})")
        backlog_results = batch_scrape(batch, label="(backlog)")
        all_results.extend(backlog_results)
        scrape_budget -= len(batch)
    else:
        print("\n✅ No backlog — all local URLs are scraped.")

    # ── Step 3: Discover new URLs ──
    if scrape_budget > 0:
        new_urls = discover_new_urls(all_known_urls, MAX_NEW_URLS)

        if new_urls:
            # Save to local file immediately
            save_urls_to_file(new_urls)

            # ── Step 4: Scrape new URLs ──
            batch = new_urls[:scrape_budget]
            print(f"\n📋 Scraping {len(batch)} of {len(new_urls)} newly discovered URLs")
            new_results = batch_scrape(batch, label="(new)")
            all_results.extend(new_results)
        else:
            print("\n⚠️ No new URLs discovered.")
    else:
        print(f"\n⏭️ Scrape budget exhausted ({MAX_SCRAPE_COUNT} used on backlog). Skipping discovery.")

    # ── Step 5: Upload results ──
    if all_results:
        upload_results(all_results, existing_df)

        # Stats
        with_images = sum(1 for r in all_results if r.get("photo_blob"))
        print(f"\n📊 Image coverage: {with_images}/{len(all_results)} ({100*with_images/len(all_results):.0f}%)")
    else:
        print("\n⚠️ No results to upload.")

    elapsed = time.time() - start_time
    print(f"\n⏱️  Pipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
