#!/usr/bin/env python3
"""EXTRACT (active listings): fetch the detail pages of a sample of the day's census into bronze/active/<day>/.

The census (census.py) says what is on the market; the detail page adds the listing's price history, which the
search results do not carry. Pages go to bronze/active/, NOT bronze/<day>/: the sold transform reads every page in
bronze/<day>/ and would turn an active listing into a fake sale.

Each page is checked against the census: it must contain the census row's listingId. Redfin sometimes shows a
neighbouring or older record, and the page's own 'TREB #' list can name several MLS numbers, so the census row is
the identity of the listing and the page is only used for its events.

    python app/Redfin/fetch_active.py                 # 50 listings from the latest census in Azure
    python app/Redfin/fetch_active.py --n 10 --local-dir census_out
"""
import argparse
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).parent))
import census  # noqa: E402
import parsing  # noqa: E402

MAX_CONSECUTIVE_FAILURES = 8
OLDER_SHARE = 0.6        # share of the sample that has been listed 30+ days (those are the ones with price history)
OLDER_DAYS = 30
# The map is limited to this window (static/js/map.js ottawaBounds); a listing outside it could never be shown.
MAP_BOUNDS = {"lat": (45.25, 45.75), "lon": (-76.0, -75.4)}


def eligible(row):
    return (row["mls"] and row["list_price"] and row["latitude"] is not None and row["longitude"] is not None
            and parsing.in_scope(row["url"])
            and MAP_BOUNDS["lat"][0] <= row["latitude"] <= MAP_BOUNDS["lat"][1]
            and MAP_BOUNDS["lon"][0] <= row["longitude"] <= MAP_BOUNDS["lon"][1])


def pick_sample(rows, n, seed):
    """n census rows to fetch, reproducible for a given seed: OLDER_SHARE of them already on the market 30+ days.

    A test sample, not a random one: a uniform draw would be mostly brand-new listings with no price history, and
    the point of the detail page is the history."""
    pool = sorted((r for r in rows if eligible(r)), key=lambda r: r["mls"])
    rng = random.Random(seed)
    older = [r for r in pool if (r["dom_redfin"] or 0) >= OLDER_DAYS]
    newer = [r for r in pool if (r["dom_redfin"] or 0) < OLDER_DAYS]
    n_older = min(len(older), round(n * OLDER_SHARE))
    n_newer = min(len(newer), n - n_older)
    return rng.sample(older, n_older) + rng.sample(newer, n_newer)


def page_is_listing(html, listing_id):
    """True when the page carries this listingId (Redfin's id for the listing, unique per MLS entry)."""
    return bool(listing_id) and re.search(rf'listingId\\?"\s*:\s*{re.escape(str(listing_id))}\b', html) is not None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=50, help="how many listings to fetch")
    ap.add_argument("--day", help="census day (default: the latest in Azure)")
    ap.add_argument("--local-dir", help="read the census from this local folder instead of Azure (pages still go to Azure)")
    ap.add_argument("--headed", action="store_true")
    args = ap.parse_args()

    import azure_store as store
    from scrape_properties_prod import USER_AGENT, fetch_html, load_redfin_cookies

    sink = census.LocalSink(args.local_dir) if args.local_dir else census.AzureSink()
    day = args.day or (sink.days() or [None])[-1]
    if not day:
        sys.exit("No census found. Run census.py first.")
    rows = census.read_census(sink, day)
    sample = pick_sample(rows, args.n, seed=day)
    have = store.list_active_pages(day)
    todo = [r for r in sample if r["mls"] not in have]
    print(f"Census {day}: {len(rows)} listings, sample of {len(sample)}, {len(sample) - len(todo)} already in bronze/active/.")

    saved = failures_in_row = mismatches = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headed)
        context = browser.new_context(user_agent=USER_AGENT)
        context.add_cookies(load_redfin_cookies())
        for i, row in enumerate(todo, 1):
            html = fetch_html(row["url"], context)
            if html is None:
                failures_in_row += 1
                if failures_in_row >= MAX_CONSECUTIVE_FAILURES:
                    print(f"Stopping: {failures_in_row} failures in a row (blocked or cookies expired?).")
                    break
                continue
            failures_in_row = 0
            if not page_is_listing(html, row["listing_id"]):
                mismatches += 1
                print(f"[{i}/{len(todo)}] {row['mls']}: page does not contain listingId {row['listing_id']}, not saved")
                continue
            saved += store.put_bronze(html, row["mls"], row["url"], day=day, prefix="bronze/active")
            print(f"[{i}/{len(todo)}] {row['mls']} {row['address']} saved")
            time.sleep(1.5)
        browser.close()

    print(f"\nDone: {saved} pages saved, {mismatches} wrong-listing pages skipped, "
          f"{len(todo) - saved - mismatches} not fetched. Now run transform_active.py.")
    sys.exit(0 if saved + mismatches == len(todo) else 2)


if __name__ == "__main__":
    main()
