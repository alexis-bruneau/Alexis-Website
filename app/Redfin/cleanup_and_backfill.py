#!/usr/bin/env python3
"""
One-off data cleanup + image backfill script.

1. Downloads the Azure parquet, removes non-Ottawa rows, re-uploads.
2. For properties that have no image in Azure, re-visits the URL 
   and uses the improved multi-strategy image extraction to grab one.
"""

import os
import io
import json
import time
import re
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────
env_path = Path(".env")
if not env_path.exists():
    env_path = Path("..") / ".env"
    if not env_path.exists():
        env_path = Path("../..") / ".env"
load_dotenv(dotenv_path=env_path)

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"

blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# ── Helpers ───────────────────────────────────────────────────────────────────

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


def find_genmid_values(html):
    return list(dict.fromkeys(re.findall(r"genMid\.([A-Za-z0-9_]+\.jpg)", html)))


def extract_image_url(html, mls):
    """Multi-strategy image extraction (same as run_pipeline.py)."""
    # Strategy 1: JSON-LD
    try:
        soup = BeautifulSoup(html, "lxml")
        for script in soup.find_all("script", type="application/ld+json"):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    img = item.get("image")
                    if img:
                        if isinstance(img, list) and len(img) > 0:
                            img = img[0]
                        if isinstance(img, dict):
                            img = img.get("url") or img.get("contentUrl")
                        if isinstance(img, str) and img.startswith("http"):
                            return img
                    photos = item.get("photo")
                    if photos:
                        if isinstance(photos, list) and len(photos) > 0:
                            photo = photos[0]
                            if isinstance(photo, dict):
                                url = photo.get("contentUrl") or photo.get("url")
                                if url:
                                    return url
                            elif isinstance(photo, str) and photo.startswith("http"):
                                return photo
            except:
                continue
    except:
        pass

    # Strategy 2: OG meta tag
    try:
        og_match = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if not og_match:
            og_match = re.search(r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', html, re.IGNORECASE)
        if og_match:
            og_url = og_match.group(1)
            if og_url.startswith("http"):
                return og_url
    except:
        pass

    # Strategy 3: Inline JSON
    try:
        photo_match = re.search(r'"photoUrls":\s*\{[^}]*"fullScreenPhotoUrl":\s*"([^"]+)"', html)
        if photo_match:
            return photo_match.group(1).replace("\\", "")
        photo_match = re.search(r'"mediaBrowserPhotos":\s*\[\s*\{[^}]*"photoUrl":\s*\{[^}]*"fullScreenPhotoUrl":\s*"([^"]+)"', html)
        if photo_match:
            return photo_match.group(1).replace("\\", "")
    except:
        pass

    # Strategy 4: genMid fallback
    mls_tail = mls[-3:] if len(mls) >= 3 else mls
    image_filenames = find_genmid_values(html)
    if image_filenames:
        return f"{BASE_IMAGE_URL}{mls_tail}/genMid.{image_filenames[0]}"

    return None


def upload_image(mls, image_url):
    """Upload image to Azure. Returns True if successful."""
    blob_name = f"images/{mls}_1.jpg"
    blob_client = container_client.get_blob_client(blob_name)
    if blob_client.exists():
        return True  # Already has image
    try:
        resp = requests.get(image_url, timeout=15)
        if resp.status_code == 200 and len(resp.content) > 500:
            blob_client.upload_blob(resp.content, overwrite=True)
            return True
    except Exception as e:
        print(f"   ⚠️ Failed: {e}")
    return False


# ──────────────────────────────────────────────────────────────────────────────
# STEP 1: CLEAN NON-OTTAWA DATA
# ──────────────────────────────────────────────────────────────────────────────

def clean_non_ottawa():
    """Remove rows where the URL is not an Ottawa/Ontario property."""
    print("=" * 60)
    print("🧹 STEP 1: Cleaning non-Ottawa properties")
    print("=" * 60)

    # Load all parquet files
    blobs = list(container_client.list_blobs(name_starts_with="silver/"))
    parquet_blobs = [b for b in blobs if b.name.endswith("listed_properties.parquet")]

    dfs = []
    for blob in parquet_blobs:
        data = container_client.get_blob_client(blob.name).download_blob().readall()
        dfs.append(pd.read_parquet(io.BytesIO(data)))

    if not dfs:
        print("No parquet files found!")
        return None

    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["MLS"], keep="last")
    total_before = len(df)
    print(f"   Total rows before: {total_before}")

    # Filter: keep only Ottawa/Ontario URLs (contain "/on/" in the path)
    # Also keep rows where URL might be missing (we'll keep those and check by lat/lon)
    ottawa_mask = df["url"].str.contains("/on/", na=False)

    # Also check by coordinates — Ottawa is roughly lat 45.2-45.6, lon -75.3 to -76.0
    coord_mask = (
        df["latitude"].between(45.0, 45.7) &
        df["longitude"].between(-76.5, -75.0)
    )

    keep_mask = ottawa_mask | coord_mask
    removed = df[~keep_mask]
    df_clean = df[keep_mask].copy()

    print(f"   Removed {len(removed)} non-Ottawa rows:")
    if len(removed) > 0:
        for _, row in removed.iterrows():
            print(f"      - {row.get('Address', 'N/A')} | {row.get('url', 'N/A')[:60]}")

    print(f"   Rows after cleaning: {len(df_clean)}")

    # Re-upload cleaned data
    today_month = datetime.now().strftime("%Y-%m")
    blob_name = f"silver/{today_month}/listed_properties.parquet"
    local_path = "temp_clean.parquet"
    df_clean.to_parquet(local_path, index=False)

    with open(local_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)

    print(f"   ✅ Uploaded cleaned data to {blob_name}")
    if os.path.exists(local_path):
        os.remove(local_path)

    # Also clean the local URL file
    urls_file = Path("app/Redfin/Output/property_urls.txt")
    if urls_file.exists():
        with urls_file.open("r", encoding="utf-8") as f:
            all_urls = [u.strip() for u in f if u.strip()]
        ottawa_urls = [u for u in all_urls if "/on/" in u]
        removed_urls = len(all_urls) - len(ottawa_urls)
        with urls_file.open("w", encoding="utf-8") as f:
            for u in ottawa_urls:
                f.write(f"{u}\n")
        print(f"   🗂️ Cleaned URL file: removed {removed_urls} non-Ottawa URLs ({len(ottawa_urls)} remaining)")

    return df_clean


# ──────────────────────────────────────────────────────────────────────────────
# STEP 2: BACKFILL MISSING IMAGES
# ──────────────────────────────────────────────────────────────────────────────

def backfill_images(df):
    """Re-visit properties with no photo_blob and try to extract images."""
    print(f"\n{'='*60}")
    print("📷 STEP 2: Backfilling missing images")
    print("=" * 60)

    if df is None or len(df) == 0:
        print("No data to process!")
        return

    # Find rows missing images
    missing = df[df["photo_blob"].isna() | (df["photo_blob"] == "")]
    # Also check Azure — some might have images uploaded but photo_blob not set
    truly_missing = []
    for _, row in missing.iterrows():
        mls = row.get("MLS")
        url = row.get("url")
        if not mls or not url or str(mls).startswith("UNKNOWN"):
            continue
        blob_name = f"images/{mls}_1.jpg"
        if not container_client.get_blob_client(blob_name).exists():
            truly_missing.append({"mls": mls, "url": url})

    print(f"   Properties without photo_blob: {len(missing)}")
    print(f"   Properties truly missing images in Azure: {len(truly_missing)}")

    if not truly_missing:
        print("   ✅ All properties have images!")
        return

    print(f"\n   Visiting {len(truly_missing)} property pages to extract images...\n")

    fixed = 0
    fixed_mls = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )
        context.add_cookies(load_redfin_cookies())

        for i, item in enumerate(truly_missing, 1):
            mls = item["mls"]
            url = item["url"]
            print(f"   [{i}/{len(truly_missing)}] {mls} — {url[:60]}...", end=" ")

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
                    print("🔒 Login")
                    page.close()
                    continue

                html = page.content()
                page.close()

                image_url = extract_image_url(html, mls)
                if image_url:
                    ok = upload_image(mls, image_url)
                    if ok:
                        print("✅ 📷")
                        fixed += 1
                        fixed_mls.append(mls)
                    else:
                        print("❌ download failed")
                else:
                    print("❌ no image found")

            except Exception as e:
                print(f"⚠️ {e}")
                try:
                    page.close()
                except:
                    pass

        browser.close()

    print(f"\n   ✅ Fixed {fixed}/{len(truly_missing)} missing images")

    # Update photo_blob in parquet for fixed ones
    if fixed_mls:
        for mls in fixed_mls:
            df.loc[df["MLS"] == mls, "photo_blob"] = f"images/{mls}_1.jpg"

        today_month = datetime.now().strftime("%Y-%m")
        blob_name = f"silver/{today_month}/listed_properties.parquet"
        local_path = "temp_backfill.parquet"
        df.to_parquet(local_path, index=False)
        with open(local_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        print(f"   ☁️ Updated parquet with {len(fixed_mls)} new photo_blob entries")
        if os.path.exists(local_path):
            os.remove(local_path)


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    start = time.time()

    # Step 1: Clean
    df = clean_non_ottawa()

    # Step 2: Backfill images
    if df is not None:
        backfill_images(df)

    print(f"\n⏱️ Total time: {time.time() - start:.1f}s")
    print("🎉 Done!")


if __name__ == "__main__":
    main()
