#!/usr/bin/env python3
"""VERIFY: is silver up to date with bronze and the census, so the maps show today's data? Read-only; never contacts redfin.ca.

The maps read silver, never bronze. A fetch that was not followed by its transform leaves the maps on old data without any
error, so this check compares what the fetch steps saved with what silver holds:

  sold    the newest bronze day (bronze/<day>/) has N pages; how many are not in silver/<month>/listed_properties.parquet
          yet? Stale when more than max(3 pages, 5%) are missing (a few pages are legitimately unreadable or out of scope).
  active  silver/active/listings.parquet must be as of the latest census day (its `as_of`), and must not be empty.

Exit code 1 when anything is stale. redfin.daily runs it as its last step.

    python -m redfin.verify                 # sold and active
    python -m redfin.verify --sold          # only the sold side
"""
import argparse
import math
import sys

from redfin import runstats
from redfin.storage import azure_store as store
from redfin.transform import active, sold

TOLERANCE_PAGES = 3
TOLERANCE_SHARE = 0.05


def sold_report(entries, pending):
    """Pure. entries: the newest bronze page per listing name [(day, name, blob)]; pending: those not in silver.
    -> {newest_day, pages, pending_newest, pending_total, fresh}."""
    newest_day = max((day for day, _, _ in entries), default=None)
    pages = sum(day == newest_day for day, _, _ in entries)
    pending_newest = sum(day == newest_day for day, _, _ in pending)
    allowed = max(TOLERANCE_PAGES, math.ceil(TOLERANCE_SHARE * pages))
    return {"newest_day": newest_day, "pages": pages, "pending_newest": pending_newest,
            "pending_total": len(pending), "fresh": pending_newest <= allowed}


def active_report(census_day, as_of, rows):
    """Pure. as_of / rows describe silver/active/listings.parquet (None / 0 when it does not exist).
    Fresh when the snapshot is as of the latest census day and not empty."""
    return {"census_day": census_day, "as_of": None if as_of is None else str(as_of), "rows": rows,
            "fresh": census_day is not None and as_of is not None and str(as_of) == census_day and rows > 0}


def check_sold(prefix):
    entries = sold.latest_bronze_per_name(store.list_bronze())
    pending = sold.pending_entries(entries, sold.done_keys(sold.load_existing_output(prefix)))
    r = sold_report(entries, pending)
    detail = f"newest bronze day {r['newest_day']}: {r['pages']} pages, {r['pending_newest']} not in {prefix}/ yet"
    if r["fresh"]:
        print(f"sold    OK     {detail}")
    else:
        print(f"sold    STALE  {detail}\n        -> python -m redfin.transform.sold --incremental --out-prefix {prefix}")
    runstats.report("verify", sold_fresh=r["fresh"], sold_pages=r["pages"], sold_pending=r["pending_newest"],
                    sold_day=r["newest_day"])
    return r["fresh"]


def check_active():
    days = store.list_search_days()
    blob = store.container().get_blob_client(active.OUT_BLOB)
    as_of, rows = None, 0
    if blob.exists():
        df = store.read_parquet(active.OUT_BLOB)
        rows, as_of = len(df), (df["as_of"].max() if len(df) and df["as_of"].notna().any() else None)
    r = active_report(days[-1] if days else None, as_of, rows)
    detail = f"snapshot as of {r['as_of']} with {rows} rows, latest census {r['census_day']}"
    if r["fresh"]:
        print(f"active  OK     {detail}")
    else:
        print(f"active  STALE  {detail}\n        -> python -m redfin.transform.active")
    runstats.report("verify", active_fresh=r["fresh"], active_rows=rows, active_as_of=r["as_of"],
                    census_day=r["census_day"])
    return r["fresh"]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sold", action="store_true", help="check only the sold side")
    ap.add_argument("--active", action="store_true", help="check only the active side")
    ap.add_argument("--silver-prefix", default="silver", help="the silver prefix the sold maps read (default silver)")
    args = ap.parse_args(argv)
    both = not args.sold and not args.active

    results = []
    if args.sold or both:
        results.append(check_sold(args.silver_prefix))
    if args.active or both:
        results.append(check_active())
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
