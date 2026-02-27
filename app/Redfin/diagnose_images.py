#!/usr/bin/env python3
"""Quick diagnostic: visit a property page and show what each image strategy returns."""

import os, io, json, time, re, requests
from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"

def load_cookies():
    if not COOKIE_FILE.exists(): return []
    raw = json.load(COOKIE_FILE.open(encoding="utf-8"))
    expire = int(time.time()) + 30*24*3600
    return [{**c, "sameSite": "Lax", "expires": expire} for c in raw if ".redfin.ca" in c.get("domain","")]

# Test URLs — ones that showed Redfin logo
test_urls = [
    "https://www.redfin.ca/on/ottawa/41-Civic-Pl-K1Y-2E1/home/149022739",
    "https://www.redfin.ca/on/ottawa/1336-Cahill-Dr-K1V-7K4/home/149403586",
]

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    context.add_cookies(load_cookies())

    for url in test_urls:
        print(f"\n{'='*70}")
        print(f"URL: {url}")
        print(f"{'='*70}")

        page = context.new_page()
        def block_heavy(route):
            if route.request.resource_type in ["image", "media", "font"]:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", block_heavy)

        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(2)
        html = page.content()
        page.close()

        # Strategy 1: JSON-LD
        print("\n--- Strategy 1: JSON-LD ---")
        soup = BeautifulSoup(html, "lxml")
        for script in soup.find_all("script", type="application/ld+json"):
            if not script.string: continue
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict): continue
                    if "image" in item:
                        print(f"  image field: {item['image']}")
                    if "photo" in item:
                        photos = item["photo"]
                        if isinstance(photos, list):
                            print(f"  photo field ({len(photos)} entries): {photos[0] if photos else 'empty'}")
                        else:
                            print(f"  photo field: {photos}")
            except Exception as e:
                print(f"  Error parsing: {e}")

        # Strategy 2: OG meta
        print("\n--- Strategy 2: OG Meta ---")
        og_match = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if not og_match:
            og_match = re.search(r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', html, re.IGNORECASE)
        if og_match:
            og_url = og_match.group(1)
            print(f"  og:image URL: {og_url}")
            # Check size
            try:
                resp = requests.get(og_url, timeout=10)
                print(f"  og:image size: {len(resp.content)} bytes ({len(resp.content)/1000:.1f}KB)")
            except Exception as e:
                print(f"  Failed to fetch: {e}")
        else:
            print("  No og:image found")

        # Strategy 3: Inline JSON
        print("\n--- Strategy 3: Inline JSON ---")
        photo_match = re.search(r'"photoUrls":\s*\{[^}]*"fullScreenPhotoUrl":\s*"([^"]+)"', html)
        if photo_match:
            print(f"  photoUrls.fullScreenPhotoUrl: {photo_match.group(1)[:100]}")
        else:
            print("  No photoUrls found")

        photo_match = re.search(r'"mediaBrowserPhotos":\s*\[\s*\{[^}]*"photoUrl":\s*\{[^}]*"fullScreenPhotoUrl":\s*"([^"]+)"', html)
        if photo_match:
            print(f"  mediaBrowserPhotos: {photo_match.group(1)[:100]}")
        else:
            print("  No mediaBrowserPhotos found")

        # Strategy 4: genMid
        print("\n--- Strategy 4: genMid ---")
        genmids = list(dict.fromkeys(re.findall(r"genMid\.([A-Za-z0-9_]+\.jpg)", html)))
        if genmids:
            print(f"  genMid filenames: {genmids[:5]}")
        else:
            print("  No genMid values found")

        # Additional: Check for any large image URLs in the page
        print("\n--- All image URLs in HTML ---")
        all_img_urls = set()
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if src.startswith("http") and "cdn-redfin" in src:
                all_img_urls.add(src[:120])
        for u in sorted(all_img_urls)[:10]:
            print(f"  {u}")

    browser.close()
print("\nDone!")
