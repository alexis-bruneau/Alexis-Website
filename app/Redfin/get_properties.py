#!/usr/bin/env python3
import os
import sys
import json
import time
import random
import io
import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from parsing import property_id

# ---------------- Config --------------------------------------------------
load_dotenv()

# Azure Config
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"

# Redfin Config
# "Sold 3yr, Sort by Sale Date (High to Low)"
START_URL = "https://www.redfin.ca/on/ottawa/filter/sort=hi-sale-date,include=sold-3yr"
OUTPUT_FILE = Path("app/Redfin/Output/property_urls.txt")
MAX_NEW_URLS = int(os.getenv("MAX_NEW_URLS", 500)) 
WAIT_SEC = 2

# ---------------- Helpers -------------------------------------------------

def get_latest_azure_urls():
    """Returns the set of listing URLs already stored in ANY silver/<month>/ parquet in Azure."""
    if not AZURE_CONN_STR:
        print("No AZURE_STORAGE_CONNECTION_STRING. Skipping Azure check.")
        return set()

    try:
        container_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR).get_container_client(CONTAINER_NAME)
        urls = set()
        files = 0
        for blob in container_client.list_blobs(name_starts_with="silver/"):
            if not blob.name.endswith("listed_properties.parquet"):
                continue
            data = container_client.get_blob_client(blob.name).download_blob().readall()
            df = pd.read_parquet(io.BytesIO(data), columns=["url"])
            urls.update(df["url"].dropna())
            files += 1
        print(f"Loaded {len(urls)} known URLs from {files} Azure parquet file(s).")
        return urls

    except Exception as e:
        print(f"Error fetching Azure data: {e}")
        return set()

def get_local_urls():
    if not OUTPUT_FILE.exists():
        return set()
    with OUTPUT_FILE.open("r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def save_urls(new_urls):
    # Append to file
    with OUTPUT_FILE.open("a", encoding="utf-8") as f:
        for url in new_urls:
            f.write(f"{url}\n")
    print(f"✅ Saved {len(new_urls)} new URLs to {OUTPUT_FILE}")

COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")

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

# ---------------- Main Logic ----------------------------------------------

def main():
    # 1. Load Ignore List
    azure_urls = get_latest_azure_urls()
    local_urls = get_local_urls()
    
    known_urls = azure_urls.union(local_urls)
    known_ids = {property_id(u) for u in known_urls}
    print(f"ℹ️ Total known URLs to ignore: {len(known_urls)}")
    
    new_found = []
    blocked = False
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Use a realistic user agent
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )
        
        # Load Cookies
        cookies = load_redfin_cookies()
        if cookies:
            context.add_cookies(cookies)
            print(f"ℹ️ Loaded {len(cookies)} cookies.")
            
        page = context.new_page()
        
        # Optimization: Block heavy resources for faster loading
        def block_heavy(route):
            if route.request.resource_type in ["image", "media", "font"]:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", block_heavy)

        current_url = START_URL
        page_num = 1
        empty_pages_in_row = 0   # pages with 0 listing links (403 / challenge / end of results)
        MAX_EMPTY_PAGES = 3
        
        while len(new_found) < MAX_NEW_URLS:
            print(f"\n--- Scraping Page {page_num} ---")
            print(f"URL: {current_url}")
            
            try:
                page.goto(current_url, wait_until="domcontentloaded", timeout=45000)
                time.sleep(WAIT_SEC)
                
                # Scroll to trigger lazy loads
                for _ in range(3):
                    page.mouse.wheel(0, 1000)
                    time.sleep(0.5)
                
                # Check for "No Results"
                if page.locator("text=No results found").count() > 0:
                     print("   -> No results found.")
                     break

                # Scrape Links
                links = page.locator("a[href*='/home/']").all()
                print(f"   [DEBUG] Found {len(links)} raw links on page.")
                
                if len(links) == 0:
                    empty_pages_in_row += 1
                    # One debug screenshot (overwritten) + title, to see whether we are blocked
                    debug_path = Path("app/Redfin/Output/debug_blocked.png")
                    page.screenshot(path=str(debug_path))
                    print(f"   Found 0 links. Saved screenshot to {debug_path}")
                    print(f"   [DEBUG] Page Title: {page.title()}")

                    if empty_pages_in_row >= MAX_EMPTY_PAGES:
                        print(f"STOPPING: {empty_pages_in_row} pages in a row had no listings. "
                              "Redfin is probably blocking us (403 / expired aws-waf-token). "
                              "Export fresh cookies - see app/Redfin/HOW_TO_FIX_403_ERROR.md")
                        blocked = True
                        break
                else:
                    empty_pages_in_row = 0

                count_before = len(new_found)
                
                for link in links:
                    href = link.get_attribute("href")
                    if not href: continue
                    
                    full_url = f"https://www.redfin.ca{href}" if href.startswith("/") else href
                    
                    # Dedupe
                    pid = property_id(full_url)
                    if pid not in known_ids and full_url not in new_found:
                        new_found.append(full_url)
                        known_ids.add(pid)
                
                added = len(new_found) - count_before
                print(f"   + Added {added} new properties (Total New: {len(new_found)})")
                
                if len(new_found) >= MAX_NEW_URLS:
                    print("🛑 Reached MAX_NEW_URLS limit.")
                    break

                # Pagination Logic
                # Try clicking next button
                # Selectors used by Redfin:
                # 1. data-rf-test-id='react-data-paginate-next'
                # 2. button with class containing 'step-next'
                # 3. a or button with aria-label='Next Page'
                next_button = page.locator("button[data-rf-test-id='react-data-paginate-next']").first
                if next_button.count() == 0:
                    next_button = page.locator(".step-next").first
                
                if next_button.count() > 0 and next_button.is_enabled():
                     # Double check if disabled via class
                    classes = next_button.get_attribute("class") or ""
                    if "disabled" in classes:
                        print("   -> Next button disabled (class). End of results.")
                        break
                    
                    next_button.click()
                    print(f"   -> Clicked 'Next'. Waiting...")
                    page_num += 1
                    # Update current_url for logging purposes, though click handles nav
                    # We accept we might drift from 'current_url' variable if we rely solely on click
                    # But better to sync it if possible.
                    # Actually, if we click, we should just let the loop continue.
                    # But we need to update page_num for the log.
                    
                else:
                    print("   -> 'Next' button not found or not detectable. Attempting manual URL navigation...")
                    
                    # Manual Fallback: Construct next page URL
                    page_num += 1
                    
                    # Redfin pagination URL structure:
                    # Base: .../filter/...
                    # Page 2: .../filter/.../page-2
                    if "/page-" in current_url:
                        import re
                        current_url = re.sub(r'/page-\d+', f'/page-{page_num}', current_url)
                    else:
                        current_url = f"{current_url}/page-{page_num}"
                        
                    print(f"   -> Forcing navigation to Page {page_num}: {current_url}")
                    # Force goto in next iteration via loop variable update? 
                    # The loop uses 'current_url' at start, so updating it here works IF we don't click.
                    # Since we didn't click, we just loop around and page.goto(current_url) happens.
                    continue
                    
                # If we clicked, we need to wait for nav or update current_url for the next loop's goto?
                # The loop structure:
                # while...
                #    page.goto(current_url) ...
                # 
                # If we click 'Next', the page navigates. 
                # On next loop iteration, page.goto(current_url) will reload the OLD url unless we update 'current_url' to the new one.
                # Redfin changes URL on click.
                
                # Wait for URL to change if we clicked
                try:
                    page.wait_for_url(lambda u: str(page_num) in u or f"page-{page_num}" in u, timeout=5000)
                    current_url = page.url
                    print(f"   -> Navigation successful. New URL: {current_url}")
                except:
                    # If wait fails, maybe we just assume it worked or fallback to manual
                    print("   -> URL didn't change after click (or timeout). Forcing manual URL update.")
                    if "/page-" in current_url:
                        import re
                        current_url = re.sub(r'/page-\d+', f'/page-{page_num}', current_url)
                    else:
                        current_url = f"{current_url}/page-{page_num}"
                    
            except Exception as e:
                print(f"❌ Error on page {page_num}: {e}")
                # Try to skip to next page anyway if error was temporary? 
                # Or abort? Abort seems safer to avoid infinite loops.
                break
        
        browser.close()

    # Save results
    if new_found:
        save_urls(new_found)
    else:
        print("\n⚠️ No new URLs found in this run.")

    if blocked:
        sys.exit(2)

if __name__ == "__main__":
    main()
