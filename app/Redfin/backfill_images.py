#!/usr/bin/env python3
"""Fill in / remove listing images for rows that already exist in Azure. Never contacts redfin.ca.

For every silver row without a working photo it re-reads the raw HTML that was saved in bronze/,
picks the best image (the listing's own photo, else Redfin's Street View image) and uploads it.
DRY RUN by default: nothing is written until you pass --apply.

Run from the project root (needs AZURE_STORAGE_CONNECTION_STRING in .env):
    python app/Redfin/backfill_images.py                       # dry run: what would be recovered
    python app/Redfin/backfill_images.py --apply               # download images, update parquet files
    python app/Redfin/backfill_images.py --no-streetview --apply   # listing photos only
    python app/Redfin/backfill_images.py --drop-streetview --apply # delete ALL Street View images + flags
"""
import argparse
import io
import os
import sys
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import azure_store as store  # noqa: E402  (connects to Azure, shares the image helpers)
import parsing  # noqa: E402

container = store.container_client
# Street View as the fallback photo (SAVE_STREETVIEW=0 or --no-streetview switches it off)
STREETVIEW = os.getenv("SAVE_STREETVIEW", "1") == "1"
MISSING = {"", "None", "nan"}


def is_missing(value):
    return value is None or pd.isna(value) or str(value).strip() in MISSING


def silver_files():
    return [b.name for b in container.list_blobs(name_starts_with="silver/")
            if b.name.endswith("listed_properties.parquet")]


def read_parquet(name):
    return pd.read_parquet(io.BytesIO(container.get_blob_client(name).download_blob().readall()))


def write_parquet(df, name):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    container.upload_blob(name=name, data=buf, overwrite=True)


def latest_bronze_by_mls():
    """MLS -> blob name of its most recent saved HTML (bronze/<date>/<MLS>.html.gz)."""
    latest = {}
    for b in container.list_blobs(name_starts_with="bronze/"):
        parts = b.name.split("/")
        if len(parts) != 3 or not parts[2].endswith(".html.gz"):
            continue
        mls, date = parts[2][: -len(".html.gz")], parts[1]
        if mls not in latest or date > latest[mls][0]:
            latest[mls] = (date, b.name)
    return {mls: name for mls, (_, name) in latest.items()}


def drop_streetview(apply):
    sv_blobs = [b.name for b in container.list_blobs(name_starts_with="images/") if b.name.endswith("_sv.jpg")]
    print(f"Street View images in Azure: {len(sv_blobs)}")
    total_rows = 0
    for name in silver_files():
        df = read_parquet(name)
        if "photo_type" not in df.columns:
            continue
        mask = df["photo_type"] == "streetview"
        total_rows += int(mask.sum())
        print(f"  {name}: {int(mask.sum())} rows flagged streetview")
        if apply and mask.any():
            df.loc[mask, ["photo_blob", "photo_type"]] = None
            write_parquet(df, name)
    if apply:
        for blob in sv_blobs:
            container.delete_blob(blob)
        print(f"Deleted {len(sv_blobs)} images and cleared {total_rows} rows.")
    else:
        print(f"DRY RUN: would delete {len(sv_blobs)} images and clear {total_rows} rows. Re-run with --apply.")


def backfill(apply):
    existing_images = set(store._existing_images) or {b.name for b in container.list_blobs(name_starts_with="images/")}
    store._existing_images.update(existing_images)
    bronze = latest_bronze_by_mls()
    print(f"Saved HTML pages in bronze/: {len(bronze)}")

    plan = []        # (silver_file, row_index, mls, url, kind)
    frames = {}
    stats = {"rows": 0, "ok": 0, "no_bronze": 0, "no_image": 0}
    for name in silver_files():
        df = read_parquet(name)
        if "photo_type" not in df.columns:
            df["photo_type"] = None
        frames[name] = df
        for idx, row in df.iterrows():
            stats["rows"] += 1
            blob = row.get("photo_blob")
            if not is_missing(blob) and blob in existing_images:
                stats["ok"] += 1
                continue
            mls = row["MLS"]
            if mls not in bronze:
                stats["no_bronze"] += 1
                continue
            html = zlib.decompress(container.get_blob_client(bronze[mls]).download_blob().readall()).decode("utf-8", "ignore")
            url, kind = parsing.pick_image(mls, html, STREETVIEW)
            if not url:
                stats["no_image"] += 1
                continue
            plan.append((name, idx, mls, url, kind))

    by_kind = pd.Series([p[4] for p in plan]).value_counts().to_dict() if plan else {}
    print(f"\nRows checked: {stats['rows']}")
    print(f"  already have a working image : {stats['ok']}")
    unique = {}  # one download per listing, even if it appears in several monthly files
    for _, _, mls, url, kind in plan:
        unique.setdefault(mls, (url, kind))
    kinds_unique = pd.Series([k for _, k in unique.values()]).value_counts().to_dict() if unique else {}
    print(f"  recoverable from saved HTML  : {len(plan)} rows = {len(unique)} listings  {kinds_unique}")
    print(f"  no saved HTML to read        : {stats['no_bronze']}")
    print(f"  page has no image at all     : {stats['no_image']}")

    if not apply:
        print("\nDRY RUN: nothing written. Re-run with --apply to download these images and update the parquet files.")
        return

    print(f"\nDownloading {len(unique)} images...")
    mls_list = list(unique)
    with ThreadPoolExecutor(max_workers=store.IMAGE_WORKERS) as pool:
        uploaded = dict(zip(mls_list, pool.map(lambda m: store.upload_image(m, *unique[m]), mls_list)))
    results = [b for b in uploaded.values() if b]

    changed = set()
    for name, idx, mls, url, kind in plan:
        blob = uploaded.get(mls)
        if blob:
            frames[name].at[idx, "photo_blob"] = blob
            frames[name].at[idx, "photo_type"] = kind
            changed.add(name)
    done = len(results)
    # Label the rows that already had a real listing photo, and clear dead references
    for name, df in frames.items():
        has_blob = ~df["photo_blob"].map(is_missing)
        df.loc[has_blob & df["photo_type"].isna(), "photo_type"] = "listing"
        dead = has_blob & ~df["photo_blob"].isin(existing_images | set(results))
        if dead.any():
            df.loc[dead, ["photo_blob", "photo_type"]] = None
        changed.add(name)
    for name in changed:
        write_parquet(frames[name], name)
        print(f"  updated {name}")
    print(f"Done: {done} of {len(unique)} images saved, {len(unique) - done} failed.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="actually write to Azure (default is a dry run)")
    ap.add_argument("--no-streetview", action="store_true", help="only use real listing photos")
    ap.add_argument("--drop-streetview", action="store_true",
                    help="delete every images/*_sv.jpg and clear photo_blob/photo_type on those rows")
    args = ap.parse_args()

    if args.drop_streetview:
        drop_streetview(args.apply)
        return
    if args.no_streetview:
        global STREETVIEW
        STREETVIEW = False
    backfill(args.apply)


if __name__ == "__main__":
    main()
