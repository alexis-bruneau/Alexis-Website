#!/usr/bin/env python3
"""Remove sold listings from silver (and what hangs off them) whose own sale was before a date. Never contacts redfin.ca.

Removes, for each listing whose Sold/Closed event (for its own MLS) is before --before: its row in
silver/<month>/listed_properties.parquet, its photo in images/, its row in silver/listing_text.parquet, and the sale-history
events of its property when no other listing of that property stays. Bronze is NOT touched. The listings are added to
redfin/data/queue/excluded_listings.txt (commit it) so discovery, fetch and the transforms keep them out.

DRY RUN by default: nothing is changed until you pass --apply. There is no backup: the deletions are permanent, except that
silver can be rebuilt from the kept bronze pages if you empty the exclusion list.

Run from the repo root (needs AZURE_STORAGE_CONNECTION_STRING in .env):
    python redfin/scripts/purge_sold_before.py --before 2025-06-01             # dry run: counts and a few examples
    python redfin/scripts/purge_sold_before.py --before 2025-06-01 --apply     # do it
Then reload the maps: python -m redfin.run.daily --transform-only
"""
import argparse
from datetime import date

from redfin import config, exclusions
from redfin.storage import azure_store as store
from redfin.transform import purge

SILVER = "silver"


def read_optional(blob):
    return store.read_parquet(blob) if store.container().get_blob_client(blob).exists() else None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--before", required=True, help="YYYY-MM-DD: purge listings whose own sale was strictly before this day")
    ap.add_argument("--apply", action="store_true", help="really delete (default: only report)")
    args = ap.parse_args()
    cutoff = date.fromisoformat(args.before)

    frames = {b: store.read_parquet(b) for b in store.list_silver(f"{SILVER}/")}
    history_blob, text_blob = f"{SILVER}/sale_history.parquet", f"{SILVER}/listing_text.parquet"
    history, text = read_optional(history_blob), read_optional(text_blob)
    p = purge.plan(frames, history, text, cutoff)

    existing_images = {b.name for b in store.container().list_blobs(name_starts_with="images/")}
    images = [name for name in p.images if name in existing_images]
    properties = {pid for pid, _, _ in p.listings}
    removed_per_file = {blob: len(frames[blob]) - (0 if kept is None else len(kept)) for blob, kept in p.silver.items()}

    print(f"Purge listings sold before {cutoff}: {len(p.listings)} of {sum(map(len, frames.values()))} silver rows "
          f"({len(properties)} properties, {len(p.partial_properties)} of which keep another listing)")
    if p.listings:
        print(f"   sold from {min(s for _, _, s in p.listings)} to {max(s for _, _, s in p.listings)}")
    for blob, n in sorted(removed_per_file.items()):
        kept = p.silver[blob]
        print(f"   {blob}: remove {n} rows" + (f", {len(kept)} stay" if kept is not None else ", the file would be empty and is deleted"))
    print(f"   images to delete:        {len(images)}")
    print(f"   listing_text rows:       {p.text_dropped}")
    print(f"   sale_history events:     {p.history_dropped}")
    print(f"   exclusion list entries:  {len(p.listings)} -> {config.EXCLUDED_FILE}")
    print("   bronze pages:            untouched")
    for pid, mls, sold in p.listings[:5]:
        print(f"   e.g. property {pid}  MLS {mls or '-'}  sold {sold}")

    if not args.apply:
        print("\nDRY RUN: nothing changed. Add --apply to delete.")
        return
    if not p.listings:
        print("\nNothing to purge.")
        return

    # Order matters for a re-run after a failure: the exclusion list and the images first, the listed_properties rows
    # last (they are what a re-run uses to find the listings again).
    added = exclusions.append(p.listings)
    for name in images:
        store.delete_blob(name)
    if p.text is not None:
        store.write_parquet(p.text, text_blob)
    if p.history is not None:
        store.write_parquet(p.history, history_blob)
    for blob, frame in p.silver.items():
        if frame is None:
            store.delete_blob(blob)
        else:
            store.write_parquet(frame, blob)
    print(f"\nDone: {len(p.listings)} listings purged, {len(images)} images deleted, {added} exclusion entries added.")
    print("Commit redfin/data/queue/excluded_listings.txt, then reload the maps: python -m redfin.run.daily --transform-only")


if __name__ == "__main__":
    main()
