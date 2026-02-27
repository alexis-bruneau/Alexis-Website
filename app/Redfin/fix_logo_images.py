#!/usr/bin/env python3
"""
Backfill missing images using saved HTML from Azure bronze/.

For properties where photo_blob is empty (or image blob doesn't exist),
looks up saved HTML in bronze/, extracts genMid image URLs, and downloads them.
"""

import os, io, json, time, re, zlib, requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── Config ──
env_path = Path(".env")
if not env_path.exists():
    env_path = Path("..") / ".env"
    if not env_path.exists():
        env_path = Path("../..") / ".env"
load_dotenv(dotenv_path=env_path)

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"
MIN_IMAGE_BYTES = 30_000  # 30KB

blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


def find_genmid_values(html):
    return list(dict.fromkeys(re.findall(r"genMid\.([A-Za-z0-9_]+\.jpg)", html)))


def is_real_image_url(url):
    if not url: return False
    bad = ["redfin-logo", "redfin_logo", "rf-logo", "/logos/", "/branding/",
           "/favicon", "redfin-default", "no-photo", "placeholder",
           "social-share", "og-image-default", "/vLATEST/images/"]
    return not any(p in url.lower() for p in bad)


def extract_image_url(html, mls):
    """Extract image URL from HTML, filtering out Redfin logos."""
    
    # Strategy 1: JSON-LD
    try:
        soup = BeautifulSoup(html, "lxml")
        for script in soup.find_all("script", type="application/ld+json"):
            if not script.string: continue
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict): continue
                    img = item.get("image")
                    if img:
                        if isinstance(img, list) and len(img) > 0: img = img[0]
                        if isinstance(img, dict): img = img.get("url") or img.get("contentUrl")
                        if isinstance(img, str) and img.startswith("http") and is_real_image_url(img):
                            return img
            except: continue
    except: pass

    # Strategy 2: Inline JSON
    try:
        m = re.search(r'"photoUrls":\s*\{[^}]*"fullScreenPhotoUrl":\s*"([^"]+)"', html)
        if m:
            url = m.group(1).replace("\\", "")
            if is_real_image_url(url): return url
        m = re.search(r'"mediaBrowserPhotos":\s*\[\s*\{[^}]*"photoUrl":\s*\{[^}]*"fullScreenPhotoUrl":\s*"([^"]+)"', html)
        if m:
            url = m.group(1).replace("\\", "")
            if is_real_image_url(url): return url
    except: pass

    # Strategy 3: genMid regex (CDN URLs persist even after listing removed)
    mls_tail = mls[-3:] if len(mls) >= 3 else mls
    genmids = find_genmid_values(html)
    if genmids:
        return f"{BASE_IMAGE_URL}{mls_tail}/genMid.{genmids[0]}"

    return None


def main():
    start = time.time()

    # ── Step 1: Load parquet and find properties missing images ──
    print("=" * 60)
    print("📦 STEP 1: Finding properties with missing images")
    print("=" * 60)

    dfs = []
    for blob in container_client.list_blobs(name_starts_with="silver/"):
        if blob.name.endswith("listed_properties.parquet"):
            data = container_client.get_blob_client(blob.name).download_blob().readall()
            dfs.append(pd.read_parquet(io.BytesIO(data)))
    df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["MLS"], keep="last")
    print(f"   Total properties: {len(df)}")

    # Find those with missing/empty photo_blob
    missing_photo = df[df["photo_blob"].isna() | (df["photo_blob"] == "")].copy()
    print(f"   Properties without photo_blob: {len(missing_photo)}")

    # Also check: photo_blob is set but the blob doesn't actually exist
    has_photo = df[df["photo_blob"].notna() & (df["photo_blob"] != "")].copy()
    orphaned = []
    for _, row in has_photo.iterrows():
        blob_name = row["photo_blob"]
        if not container_client.get_blob_client(blob_name).exists():
            orphaned.append(row["MLS"])
    print(f"   Properties with photo_blob but missing blob: {len(orphaned)}")

    # Combine both sets
    all_missing_mls = set(missing_photo["MLS"].tolist()) | set(orphaned)
    # Exclude UNKNOWN_* entries (no valid MLS)
    all_missing_mls = {m for m in all_missing_mls if not str(m).startswith("UNKNOWN")}
    print(f"   Total to fix (excluding UNKNOWN_*): {len(all_missing_mls)}")

    # ── Step 2: Build bronze/ map ──
    print(f"\n{'='*60}")
    print("📂 STEP 2: Indexing bronze/ saved HTML files")
    print("=" * 60)

    bronze_blobs = list(container_client.list_blobs(name_starts_with="bronze/"))
    bronze_map = {}
    for blob in bronze_blobs:
        if blob.name.endswith(".html.gz"):
            filename = blob.name.split("/")[-1]
            mls = filename.replace(".html.gz", "")
            if mls not in bronze_map or blob.last_modified > bronze_map[mls]["time"]:
                bronze_map[mls] = {"blob": blob.name, "time": blob.last_modified}

    print(f"   Total bronze HTML files: {len(bronze_map)}")

    can_fix = all_missing_mls & set(bronze_map.keys())
    cannot_fix = all_missing_mls - can_fix
    print(f"   Missing images with bronze HTML: {len(can_fix)}")
    print(f"   Missing images without bronze HTML: {len(cannot_fix)}")

    if not can_fix:
        print("   Nothing to fix!")
        return

    # ── Step 3: Extract and download images ──
    print(f"\n{'='*60}")
    print(f"📷 STEP 3: Extracting images from {len(can_fix)} saved HTML files")
    print("=" * 60)

    fixed = 0
    no_image = 0
    download_fail = 0
    fixed_mls = []

    for i, mls in enumerate(sorted(can_fix), 1):
        print(f"   [{i}/{len(can_fix)}] {mls}...", end=" ")

        try:
            # Download and decompress saved HTML
            blob_client = container_client.get_blob_client(bronze_map[mls]["blob"])
            compressed = blob_client.download_blob().readall()
            html = zlib.decompress(compressed).decode("utf-8")

            # Extract image URL
            image_url = extract_image_url(html, mls)
            if not image_url:
                print("❌ no image in HTML")
                no_image += 1
                continue

            # Download the actual image from CDN
            resp = requests.get(image_url, timeout=15)
            if resp.status_code == 200 and len(resp.content) >= MIN_IMAGE_BYTES:
                blob_name = f"images/{mls}_1.jpg"
                container_client.get_blob_client(blob_name).upload_blob(resp.content, overwrite=True)
                print(f"✅ ({len(resp.content)/1000:.0f}KB)")
                fixed += 1
                fixed_mls.append(mls)
            else:
                sz = len(resp.content) if resp.status_code == 200 else 0
                print(f"❌ bad download ({resp.status_code}, {sz/1000:.0f}KB)")
                download_fail += 1

        except Exception as e:
            print(f"⚠️ {e}")
            download_fail += 1

    # ── Step 4: Update parquet ──
    print(f"\n{'='*60}")
    print("☁️ STEP 4: Updating parquet")
    print("=" * 60)

    for mls in fixed_mls:
        df.loc[df["MLS"] == mls, "photo_blob"] = f"images/{mls}_1.jpg"

    # Clear photo_blob for properties we confirmed have no image
    for mls in all_missing_mls - set(fixed_mls):
        df.loc[df["MLS"] == mls, "photo_blob"] = None

    today_month = datetime.now().strftime("%Y-%m")
    blob_path = f"silver/{today_month}/listed_properties.parquet"
    local_path = "temp_fix.parquet"
    df.to_parquet(local_path, index=False)
    with open(local_path, "rb") as f:
        container_client.upload_blob(name=blob_path, data=f, overwrite=True)
    print(f"   ✅ Updated parquet ({len(df)} rows)")
    if os.path.exists(local_path): os.remove(local_path)

    # Summary
    total_with_images = df["photo_blob"].notna().sum()
    total_without = df["photo_blob"].isna().sum()
    print(f"\n{'='*60}")
    print(f"📊 Summary:")
    print(f"   ✅ Fixed: {fixed}")
    print(f"   ❌ No image in HTML: {no_image}")
    print(f"   ❌ Download failed: {download_fail}")
    print(f"   📈 Total with images: {total_with_images}/{len(df)} ({100*total_with_images/len(df):.0f}%)")
    print(f"   📉 Total without images: {total_without}/{len(df)}")
    print(f"   ⏱️ {time.time()-start:.1f}s")
    print("🎉 Done!")


if __name__ == "__main__":
    main()
