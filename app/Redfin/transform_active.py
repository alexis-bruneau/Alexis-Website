#!/usr/bin/env python3
"""TRANSFORM (active listings): census + detail pages in bronze -> silver/active/listings.parquet. Never contacts redfin.ca.

One row per active listing we hold a detail page for. The listing's identity, price, status, coordinates and
bedrooms come from the day's census row; the price history and days on market come from the page's events
(metrics.active_metrics). Nothing reads the clock: `as_of` is the census day, so re-running on the same bronze gives the
same rows.

The output is a snapshot of one day, not a history. It lives under silver/active/ and does not end in
listed_properties.parquet, so the map of sold homes never loads it.

    python app/Redfin/transform_active.py                 # latest census day that has active pages
    python app/Redfin/transform_active.py --day 2026-10-18 --skip-images
"""
import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import census  # noqa: E402
import metrics  # noqa: E402
import parsing  # noqa: E402

OUT_BLOB = "silver/active/listings.parquet"


def build_row(census_row, html, as_of, streetview=False):
    """One census row + its detail page -> (silver row, image url or None). Pure."""
    events = parsing.parse_events(html, census_row["url"])
    m = metrics.active_metrics(events, census_row["mls"], as_of, census_row["list_price"])
    row = {k: v for k, v in census_row.items() if k != "alt_property_ids"}
    row["alt_property_ids"] = ",".join(census_row.get("alt_property_ids") or []) or None
    row.update(m)
    # Days on market: from the event history when it has this MLS's Listed event, else Redfin's own figure.
    row["dom_basis"] = "events" if m["days_on_market"] is not None else "redfin"
    if m["days_on_market"] is None:
        row["days_on_market"] = census_row["dom_redfin"]
    amount = m["last_change_amount"]
    row["last_change_direction"] = None if not amount else ("up" if amount > 0 else "down")
    row["property_type_name"] = parsing.extract_property_type(html)   # the search results carry only a numeric code
    row["as_of"] = as_of
    image_url, image_kind = parsing.pick_image(census_row["mls"], html, streetview)
    return row, (image_url if image_kind == "listing" else None)


def latest_day_with_pages(store):
    days = [d for d in reversed(store.list_search_days()) if store.list_active_pages(d)]
    if not days:
        sys.exit("No census day has active pages yet. Run census.py and fetch_active.py first.")
    return days[0]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", help="census day (default: the latest with active pages)")
    ap.add_argument("--skip-images", action="store_true", help="do not download photos that are missing in Azure")
    ap.add_argument("--out-blob", default=OUT_BLOB)
    args = ap.parse_args()

    import azure_store as store
    day = args.day or latest_day_with_pages(store)
    as_of = date.fromisoformat(day)
    by_mls = {r["mls"]: r for r in census.read_census(census.AzureSink(), day)}
    pages = store.list_active_pages(day)
    print(f"Census {day}: {len(by_mls)} listings; {len(pages)} active detail pages.")

    rows, image_urls, skipped = [], {}, []
    for mls, blob in sorted(pages.items()):
        if mls not in by_mls:  # the listing left the census between the two runs: keep the page, drop the row
            skipped.append(mls)
            continue
        row, image_url = build_row(by_mls[mls], store.read_bronze(blob), as_of)
        rows.append(row)
        image_urls[mls] = image_url

    existing = store.load_existing_images()
    for row in rows:
        mls = row["mls"]
        blob = store.image_blob_name(mls)
        if blob in existing:
            row["photo_blob"] = blob
        elif image_urls[mls] and not args.skip_images:
            row["photo_blob"] = store.upload_image(mls, image_urls[mls])
        else:
            row["photo_blob"] = None

    df = pd.DataFrame(rows)
    for col in ("list_price", "original_price", "last_change_amount", "price_vs_original",
                "days_on_market", "cumulative_dom", "price_changes"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    store.write_parquet(df, args.out_blob)

    changed = df[df["price_changes"].fillna(0) > 0]
    print(f"Wrote {len(df)} listings -> {args.out_blob} ({len(skipped)} pages skipped: not in the census anymore)")
    print(f"   with a price change: {len(changed)} ({(changed['last_change_direction'] == 'down').sum()} down, "
          f"{(changed['last_change_direction'] == 'up').sum()} up)")
    print(f"   days on market from events: {(df['dom_basis'] == 'events').sum()}, from Redfin's figure: {(df['dom_basis'] == 'redfin').sum()}")
    print(f"   photos: {df['photo_blob'].notna().sum()} of {len(df)}")


if __name__ == "__main__":
    main()
