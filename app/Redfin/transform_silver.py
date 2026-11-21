#!/usr/bin/env python3
"""TRANSFORM: rebuild the silver parquet files from the raw HTML in bronze/. Never contacts redfin.ca.

A listing is identified by (property_id, MLS): property_id is the number in the URL, MLS is the real MLS
number or empty when the page has none. For each listing the most recent bronze page wins. Pages that are
not Ontario pages on redfin.ca (e.g. US redfin.com listings) are skipped. Pages are parsed with parsing.py, so fixing a
parser bug and re-running this script corrects every listing, old and new.

Output goes to <out-prefix>/<YYYY-MM>/listed_properties.parquet (month of the bronze page), plus
<out-prefix>/sale_history.parquet: one row per sale-history event (listed, price change, sold, ...), linked
to the listings by property_id (and by mls where the event has one). The default
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
import metrics  # noqa: E402
import parsing  # noqa: E402

MISSING = {"", "None", "nan"}
KEY = ["property_id", "MLS"]
EVENT_KEY = ["property_id", "event_date", "event_type", "source_id", "price"]


def latest_bronze_per_name(entries):
    """[(day, name, blob)] -> one entry per bronze file name (the MLS, or P<property_id>), the newest day."""
    latest = {}
    for day, name, blob in entries:
        if name not in latest or day > latest[name][0]:
            latest[name] = (day, name, blob)
    return sorted(latest.values())


def transform_one(entry, streetview):
    """bronze page -> (day, record, events, None) or (day, None, None, (kind, message)); kind is 'scope' or 'error'."""
    day, name, blob = entry
    try:
        html = store.read_bronze(blob)
        url = parsing.extract_url(html) or store.bronze_url_metadata(blob)
        if not url:
            return day, None, None, ("error", f"{name}: no url in page or blob metadata")
        if not parsing.in_scope(url):
            return day, None, None, ("scope", f"{name}: {url}")
        record, events = parsing.parse_listing(html, url, parsing.extract_mls(html), streetview=streetview)
        return day, record, events, None
    except Exception as e:  # one bad page must not stop a full rebuild
        return day, None, None, ("error", f"{name}: {type(e).__name__}: {e}")


def load_existing_output(prefix):
    """{month: DataFrame} already written under the output prefix (for --incremental merges)."""
    frames = {}
    for blob in store.list_silver(prefix + "/"):
        df = store.read_parquet(blob)
        if "property_id" not in df.columns:  # written before the key existed
            df.insert(0, "property_id", df["url"].map(parsing.property_id))
        frames[blob.split("/")[1]] = df
    return frames


def write_sale_history(results, out_prefix, incremental):
    """Events of the parsed pages -> <out_prefix>/sale_history.parquet (merged with the existing file when incremental)."""
    rows = [event for _, _, events in results for event in events]
    df = pd.DataFrame(rows)
    blob = f"{out_prefix}/sale_history.parquet"
    if incremental and store.container_client.get_blob_client(blob).exists():
        df = pd.concat([store.read_parquet(blob), df], ignore_index=True)
    if df.empty:
        return
    df = df.drop_duplicates(subset=EVENT_KEY, keep="last")
    df["price"] = df["price"].astype("Int64")
    df = df.sort_values(["property_id", "event_date", "event_seq"], kind="stable").reset_index(drop=True)
    store.write_parquet(df, blob)
    print(f"   wrote {len(df):>5} events ({df['property_id'].nunique()} properties) -> {blob}")


def attach_images(records, skip_images):
    """Sets photo_blob / photo_type on records whose image is (or becomes) available in Azure."""
    existing = store.load_existing_images()
    jobs = []
    with ThreadPoolExecutor(max_workers=store.IMAGE_WORKERS) as pool:
        for rec in records:
            kind, img_url = rec["_img_kind"], rec["_img_url"]
            if not img_url:
                continue
            listing_blob = store.image_blob_name(rec["_key"], "listing")
            blob = store.image_blob_name(rec["_key"], kind)
            if listing_blob in existing:  # a real photo already saved (e.g. by an older parser) always wins
                rec["photo_blob"], rec["photo_type"] = listing_blob, "listing"
            elif blob in existing:
                rec["photo_blob"], rec["photo_type"] = blob, kind
            elif not skip_images:
                jobs.append((rec, pool.submit(store.upload_image, rec["_key"], img_url, kind)))
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

    entries = latest_bronze_per_name(store.list_bronze())
    print(f"Bronze: {len(entries)} distinct pages.")
    existing = load_existing_output(args.out_prefix) if args.incremental else {}
    done = set()  # (property_id, MLS) already in the output
    if args.incremental:
        for df in existing.values():
            done.update(zip(df["property_id"], df["MLS"].where(df["MLS"].notna(), None)))
        # Cheap pre-filter on the file name; legacy UNKNOWN_<time> pages can only be matched after parsing.
        done_names = {parsing.listing_key(pid, mls) for pid, mls in done}
        entries = [e for e in entries if e[1] not in done_names]
        print(f"Incremental: {len(entries)} listings not yet in {args.out_prefix}/.")
        if not entries:
            return
    if args.limit:
        entries = entries[-args.limit:]
        print(f"Limiting to the {len(entries)} newest.")

    results, errors, out_of_scope = [], [], 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for i, out in enumerate(pool.map(lambda e: transform_one(e, not args.no_streetview), entries), 1):
            day, rec, events, err = out
            if rec:
                results.append((day, rec, events))
            elif err[0] == "scope":
                out_of_scope += 1
            else:
                errors.append(err[1])
            if i % 500 == 0:
                print(f"   parsed {i}/{len(entries)}")

    # One row per (property_id, MLS): the newest page wins. Pages of the same listing under different file
    # names (legacy UNKNOWN_<time> duplicates) collapse here; listings already in the output are dropped.
    newest = {}
    for day, rec, events in sorted(results, key=lambda r: r[0]):
        newest[(rec["property_id"], rec["MLS"])] = (day, rec, events)
    results = [v for k, v in newest.items() if k not in done]

    print(f"Parsed {len(results)} listings; skipped {out_of_scope} out-of-scope pages and {len(errors)} unreadable ones.")
    for err in errors[:10]:
        print(f"   skipped: {err}")

    records = [rec for _, rec, _ in results]
    attach_images(records, args.skip_images)

    by_month = {}
    for day, rec, _ in results:
        by_month.setdefault(day[:7], []).append({k: v for k, v in rec.items() if not k.startswith("_")})
    for month, rows in sorted(by_month.items()):
        df = pd.DataFrame(rows)
        if month in existing:
            df = pd.concat([existing[month], df], ignore_index=True).drop_duplicates(subset=KEY, keep="last")
        for col in metrics.METRICS:  # whole numbers with gaps, not floats with NaN
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        df["photo_blob"] = df["photo_blob"].where(~df["photo_blob"].isin(MISSING), None)
        blob = f"{args.out_prefix}/{month}/listed_properties.parquet"
        store.write_parquet(df, blob)
        print(f"   wrote {len(df):>5} rows -> {blob}")

    write_sale_history(results, args.out_prefix, args.incremental)

    print(f"Done in {time.time() - t0:.0f}s.")


if __name__ == "__main__":
    main()
