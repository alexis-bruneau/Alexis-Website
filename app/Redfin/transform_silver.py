#!/usr/bin/env python3
"""TRANSFORM: rebuild the silver parquet files from the raw HTML in bronze/. Never contacts redfin.ca.

For every MLS number the most recent bronze page wins. Pages are parsed with parsing.py, so fixing a
parser bug and re-running this script corrects every listing, old and new.

Output goes to <out-prefix>/<YYYY-MM>/listed_properties.parquet (month of the bronze page). The default
prefix is `silver_v2`, NOT the live `silver/`, so you can compare before switching:
    python app/Redfin/transform_silver.py --limit 200            # quick trial on 200 listings
    python app/Redfin/transform_silver.py                        # full rebuild into silver_v2/
    python app/Redfin/compare_silver.py                          # check silver_v2 against silver
    python app/Redfin/transform_silver.py --incremental --out-prefix silver   # daily run: only new listings
    python app/Redfin/transform_silver.py --out-prefix silver    # full rewrite of live silver (back it up first)

Images: a listing's photo is attached when it already exists in images/. Missing ones are downloaded
from the CDN (not redfin.ca pages) unless you pass --skip-images.
"""
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import azure_store as store  # noqa: E402
import parsing  # noqa: E402

MISSING = {"", "None", "nan"}


def latest_bronze_per_mls(entries):
    """[(day, mls, blob)] -> one entry per MLS, the newest day."""
    latest = {}
    for day, mls, blob in entries:
        if mls not in latest or day > latest[mls][0]:
            latest[mls] = (day, mls, blob)
    return sorted(latest.values())


def transform_one(entry, streetview):
    """bronze page -> (day, record, None) or (day, None, error message)."""
    day, mls, blob = entry
    try:
        html = store.read_bronze(blob)
        url = parsing.extract_url(html) or store.bronze_url_metadata(blob)
        if not url:
            return day, None, f"{mls}: no url in page or blob metadata"
        record, _events = parsing.parse_listing(html, url, mls, streetview=streetview)
        return day, record, None
    except Exception as e:  # one bad page must not stop a full rebuild
        return day, None, f"{mls}: {type(e).__name__}: {e}"


def load_existing_output(prefix):
    """{month: DataFrame} already written under the output prefix (for --incremental merges)."""
    frames = {}
    for blob in store.list_silver(prefix + "/"):
        frames[blob.split("/")[1]] = store.read_parquet(blob)
    return frames


def attach_images(records, skip_images):
    """Sets photo_blob / photo_type on records whose image is (or becomes) available in Azure."""
    existing = store.load_existing_images()
    jobs = []
    with ThreadPoolExecutor(max_workers=store.IMAGE_WORKERS) as pool:
        for rec in records:
            kind, img_url = rec["_img_kind"], rec["_img_url"]
            if not img_url:
                continue
            listing_blob = store.image_blob_name(rec["MLS"], "listing")
            blob = store.image_blob_name(rec["MLS"], kind)
            if listing_blob in existing:  # a real photo already saved (e.g. by an older parser) always wins
                rec["photo_blob"], rec["photo_type"] = listing_blob, "listing"
            elif blob in existing:
                rec["photo_blob"], rec["photo_type"] = blob, kind
            elif not skip_images:
                jobs.append((rec, pool.submit(store.upload_image, rec["MLS"], img_url, kind)))
        for rec, fut in jobs:
            blob = fut.result()
            if blob:
                rec["photo_blob"], rec["photo_type"] = blob, rec["_img_kind"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out-prefix", default="silver_v2", help="where to write (default silver_v2, the live one is 'silver')")
    ap.add_argument("--limit", type=int, default=0, help="only transform the N newest listings (0 = all)")
    ap.add_argument("--workers", type=int, default=6, help="parallel download+parse threads")
    ap.add_argument("--incremental", action="store_true",
                    help="only transform listings not already in the output, and merge them into its month files")
    ap.add_argument("--skip-images", action="store_true", help="do not download images that are missing in Azure")
    ap.add_argument("--no-streetview", action="store_true", help="never use Street View as the fallback photo")
    args = ap.parse_args()
    t0 = time.time()

    entries = latest_bronze_per_mls(store.list_bronze())
    print(f"Bronze: {len(entries)} distinct listings.")
    existing = load_existing_output(args.out_prefix) if args.incremental else {}
    if args.incremental:
        done = set().union(*(set(df["MLS"]) for df in existing.values())) if existing else set()
        entries = [e for e in entries if e[1] not in done]
        print(f"Incremental: {len(entries)} listings not yet in {args.out_prefix}/.")
        if not entries:
            return
    if args.limit:
        entries = entries[-args.limit:]
        print(f"Limiting to the {len(entries)} newest.")

    results, errors = [], []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for i, out in enumerate(pool.map(lambda e: transform_one(e, not args.no_streetview), entries), 1):
            day, rec, err = out
            if rec:
                results.append((day, rec))
            else:
                errors.append(err)
            if i % 500 == 0:
                print(f"   parsed {i}/{len(entries)}")

    print(f"Parsed {len(results)} listings, {len(errors)} skipped.")
    for err in errors[:10]:
        print(f"   skipped: {err}")

    records = [rec for _, rec in results]
    attach_images(records, args.skip_images)

    by_month = {}
    for day, rec in results:
        by_month.setdefault(day[:7], []).append({k: v for k, v in rec.items() if not k.startswith("_")})
    for month, rows in sorted(by_month.items()):
        df = pd.DataFrame(rows)
        if month in existing:
            df = pd.concat([existing[month], df], ignore_index=True).drop_duplicates(subset=["MLS"], keep="last")
        df["photo_blob"] = df["photo_blob"].where(~df["photo_blob"].isin(MISSING), None)
        blob = f"{args.out_prefix}/{month}/listed_properties.parquet"
        store.write_parquet(df, blob)
        print(f"   wrote {len(df):>5} rows -> {blob}")

    print(f"Done in {time.time() - t0:.0f}s.")


if __name__ == "__main__":
    main()
