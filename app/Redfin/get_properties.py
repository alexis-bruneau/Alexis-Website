#!/usr/bin/env python3
import os
import json
import time
import random
from pathlib import Path
from playwright.sync_api import sync_playwright

# ---------------- Config --------------------------------------------------
# Updated to Sold 3 Years as requested
OTTAWA_URL = "https://www.redfin.ca/city/15044/ON/Ottawa/filter/include=sold-3yr,sort=lo-days"
OUTPUT_FILE = Path("app/Redfin/Output/property_urls.txt")
MAX_NEW_URLS = int(os.getenv("MAX_NEW_URLS", 10))
WAIT_SEC = 2 # Optimized wait time

# ---------------- Logic ---------------------------------------------------

def get_existing_urls():
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

def main():
    existing = get_existing_urls()
    print(f"ℹ️ Found {len(existing)} existing URLs in cache.")
    
    new_found = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
        page = context.new_page()
        
        print(f"🔍 Searching Redfin Ottawa: {OTTAWA_URL}")
        
        # Optimization: Block images for faster loading
        def block_heavy(route):
            if route.request.resource_type in ["image", "media", "font"]:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", block_heavy)
        
        try:
            page.goto(OTTAWA_URL, wait_until="domcontentloaded", timeout=60000)
            time.sleep(WAIT_SEC) 
            
            # Scroll to load more
            for _ in range(3):
                page.mouse.wheel(0, 1000)
                time.sleep(1)

            # Selector for property cards (Redfin valid classes vary, usually 'a.slider-item' or specific classes)
            # We look for hrefs containing '/ON/Ottawa'
            
            # Universal finding strategy: all links with /home/ in them
            links = page.locator("a[href*='/home/']").all()
            
            print(f"   Found {len(links)} potential home links on page.")
            
            for link in links:
                if len(new_found) >= MAX_NEW_URLS:
                    break
                    
                href = link.get_attribute("href")
                if not href:
                    continue
                    
                full_url = f"https://www.redfin.ca{href}" if href.startswith("/") else href
                
                # Dedupe
                if full_url not in existing and full_url not in new_found:
                    new_found.append(full_url)
                    print(f"   + Found New: {full_url}")
                    
        except Exception as e:
            print(f"❌ Error during search: {e}")
        finally:
            browser.close()

    if new_found:
        save_urls(new_found)
    else:
        print("⚠️ No new URLs found.")

if __name__ == "__main__":
    main()
