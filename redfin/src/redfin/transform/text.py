#!/usr/bin/env python3
"""TRANSFORM: listing descriptions from the raw HTML in bronze/ -> <out-prefix>/listing_text.parquet. Never contacts redfin.ca.

One row per listing (property_id, MLS), same key and same page-selection rule as redfin.transform.sold (the newest
bronze page wins, out-of-scope pages are skipped). Columns: property_id, MLS, text, bronze_day. Listings whose page
has no description are left out and counted in the coverage report.

This is a separate, text-only pass on purpose: it downloads and parses pages but writes no images, no sale history
and no listed_properties files, so it can backfill every page already in bronze without touching the other silver
tables. The file name does not end in `listed_properties.parquet`, so the sold map never loads it.

    python -m redfin.transform.text --limit 200                  # quick trial into silver_v2/
    python -m redfin.transform.text                              # full rebuild into silver_v2/
    python -m redfin.transform.text --incremental --out-prefix silver   # daily run: only listings not yet there
"""
import argparse
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from redfin import runstats
from redfin.storage import azure_store as store
from redfin.transform import parsing
from redfin.transform.sold import KEY, latest_bronze_per_name


def text_one(entry):
    """bronze page -> (day, row or None, status); status is 'ok', 'no_text', 'scope' or an error message."""
    day, name, blob = entry
    try:
        html = store.read_bronze(blob)
        url = parsing.extract_url(html) or store.bronze_url_metadata(blob)
        if not url:
            return day, None, f"{name}: no url in page or blob metadata"
        if not parsing.in_scope(url):
            return day, None, "scope"
        text = parsing.extract_remarks(html)
        if not text:
            return day, None, "no_text"
        row = {"property_id": parsing.property_id(url), "MLS": parsing.extract_mls(html), "text": text, "bronze_day": day}
        return day, row, "ok"
    except Exception as e:  # one bad page must not stop a full rebuild
        return day, None, f"{name}: {type(e).__name__}: {e}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out-prefix", default="silver_v2", help="where to write (default silver_v2, the live one is 'silver')")
    ap.add_argument("--limit", type=int, default=0, help="only the N newest pages (0 = all)")
    ap.add_argument("--workers", type=int, default=6, help="parallel download+parse threads")
    ap.add_argument("--incremental", action="store_true", help="skip listings already in the output file and merge into it")
    args = ap.parse_args()
    t0 = time.time()

    blob = f"{args.out_prefix}/listing_text.parquet"
    existing = None
    if args.incremental and store.container().get_blob_client(blob).exists():
        existing = store.read_parquet(blob)

    entries = latest_bronze_per_name(store.list_bronze())
    print(f"Bronze: {len(entries)} distinct pages.")
    if existing is not None:
        # Cheap pre-filter on the file name (the MLS, or P<property_id>); the exact key is re-checked after parsing.
        done_names = {parsing.listing_key(pid, mls) for pid, mls in zip(existing["property_id"], existing["MLS"].where(existing["MLS"].notna(), None))}
        entries = [e for e in entries if e[1] not in done_names]
        print(f"Incremental: {len(entries)} pages not yet in {blob}.")
        if not entries:
            runstats.report("transform.text", rows_added=0)
            return
    if args.limit:
        entries = entries[-args.limit:]
        print(f"Limiting to the {len(entries)} newest.")

    rows, status, errors, missing_by_month = [], Counter(), [], Counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for i, (day, row, st) in enumerate(pool.map(text_one, entries), 1):
            key = st if st in ("ok", "no_text", "scope") else "error"
            status[key] += 1
            if row:
                rows.append(row)
            elif st == "no_text":
                missing_by_month[day[:7]] += 1
            elif key == "error":
                errors.append(st)
            if i % 500 == 0:
                print(f"   parsed {i}/{len(entries)}")

    df = pd.DataFrame(rows, columns=["property_id", "MLS", "text", "bronze_day"])
    if existing is not None:
        df = pd.concat([existing, df], ignore_index=True)
    # Newest page wins when several bronze files describe the same listing.
    df = df.sort_values("bronze_day", kind="stable").drop_duplicates(subset=KEY, keep="last").reset_index(drop=True)

    print(f"Pages: {status['ok']} with a description, {status['no_text']} without, "
          f"{status['scope']} out of scope, {status['error']} unreadable.")
    if missing_by_month:
        print("   pages without a description, by month: " + ", ".join(f"{m}: {n}" for m, n in sorted(missing_by_month.items())))
    for err in errors[:10]:
        print(f"   skipped: {err}")
    if df.empty:
        print("Nothing to write.")
        runstats.report("transform.text", rows_added=0)
        return
    store.write_parquet(df, blob)
    lengths = df["text"].str.len()
    print(f"   wrote {len(df):>5} rows -> {blob}  (text length min/median/max: "
          f"{lengths.min()}/{int(lengths.median())}/{lengths.max()})")
    runstats.report("transform.text", rows_added=int(status["ok"]))
    print(f"Done in {time.time() - t0:.0f}s.")


if __name__ == "__main__":
    main()
