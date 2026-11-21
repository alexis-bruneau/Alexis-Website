#!/usr/bin/env python3
"""TRANSFORM (active listings): census + detail pages in bronze -> silver/active/listings.parquet. Never contacts redfin.ca.

One row per listing that is in the census AND has a detail page. The page used is the newest one for that MLS across
all days (bronze/active/<any day>/<MLS>.html.gz, up to the census day), so a daily batch of ~200 fetches adds to the
list instead of replacing it. The listing's identity, price, status, coordinates and bedrooms always come from the
census row of the census day; the price history and days on market come from the page's events
(metrics.active_metrics). Nothing reads the clock: `as_of` is the census day, so re-running on the same bronze gives the
same rows. `page_day` says how old the page behind a row is.

Reconcile with sold (pure, no Redfin access, nothing is deleted): a row whose MLS has really been sold is left out of
listings.parquet and written to silver/active/reconciled_out.parquet (mls, sold_date, reason) instead.
  * matched on MLS only, never on property_id: a home sold in September and relisted in October has the same
    property_id but a new MLS, and stays active;
  * a silver listed_properties row counts as a sale only when it has proof for its own MLS: a Sold/Closed event for that
    MLS in silver/sale_history.parquet, or a sale metric on the row (days_on_market, sold_vs_*_list, cumulative_dom: they
    exist only when the MLS has its own Sold event). `Sold Date` alone is NOT proof: for a relisted home the parser fills
    it from the previous sale (ACTIVE_LISTINGS.md finding 7), and ~113 rows of listed_properties are not sales at all
    (finding 2).

Both outputs live under silver/active/ and do not end in listed_properties.parquet, so the map of sold homes never loads them.

    python -m redfin.transform.active                 # latest census day
    python -m redfin.transform.active --day 2026-10-18 --skip-images
"""
import argparse
import sys
from datetime import date

import pandas as pd

from redfin import runstats
from redfin.extract import census
from redfin.storage import azure_store as store
from redfin.transform import metrics, parsing

OUT_BLOB = "silver/active/listings.parquet"
RECONCILED_BLOB = "silver/active/reconciled_out.parquet"
RECONCILED_COLUMNS = ["mls", "sold_date", "reason"]
REASON_EVENT = "sale_history"     # a Sold/Closed event for this MLS
REASON_METRICS = "sold_metrics"   # a sale metric on the listed_properties row
NO_MLS = {"", "None", "nan", "N/A"}


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


def build_rows(census_by_mls, pages, read_html, as_of):
    """Rows for every census listing that has a page. pages is {MLS: (page day, blob)}, read_html(blob) -> html.
    Returns ([row], {MLS: image url or None}). Pure given read_html; sorted by MLS."""
    rows, image_urls = [], {}
    for mls in sorted(census_by_mls):
        if mls not in pages:
            continue
        page_day, blob = pages[mls]
        row, image_urls[mls] = build_row(census_by_mls[mls], read_html(blob), as_of)
        row["page_day"] = page_day
        rows.append(row)
    return rows, image_urls


# ---------------- reconcile with sold ----------------------------------------------------------

def is_sale_event(event_type):
    """True for a completed sale ('Sold', 'Closed', 'Sold (MLS)'...). A conditional sale ('Sold-Conditional', 'Pending') is not."""
    t = (event_type or "").strip().lower()
    return t == "closed" or (t.startswith("sold") and "conditional" not in t)


def _clean_mls(value):
    return None if value is None or pd.isna(value) or str(value).strip() in NO_MLS else str(value).strip()


def sold_mls(listed, history):
    """{MLS: (sold date as 'YYYY-MM-DD' or None, reason)} for the listed_properties rows that really are sales. Pure.

    listed is the concatenated silver listed_properties frames (column MLS); history is silver/sale_history.parquet (columns
    mls, event_type, event_date) or None. Only MLS numbers present in `listed` are returned."""
    sale_dates = {}   # MLS -> latest Sold/Closed event date (None when the event has no date)
    if history is not None and not history.empty:
        events = history[history["mls"].notna() & history["event_type"].map(is_sale_event)]
        for mls, when in zip(events["mls"], events["event_date"]):
            when = None if pd.isna(when) else str(when)
            sale_dates[mls] = max(filter(None, [sale_dates.get(mls), when]), default=None)

    metric_cols = [c for c in metrics.METRICS if c in listed.columns]
    has_metric = listed[metric_cols].notna().any(axis=1) if metric_cols else pd.Series(False, index=listed.index)
    sold_dates = listed["Sold Date"] if "Sold Date" in listed.columns else pd.Series(None, index=listed.index)
    sold = {}
    for raw, metric, sold_date in zip(listed["MLS"], has_metric, sold_dates):
        mls = _clean_mls(raw)
        if not mls or mls in sold:
            continue
        if mls in sale_dates:
            sold[mls] = (sale_dates[mls], REASON_EVENT)
        elif metric:
            parsed = parsing.parse_date(sold_date)
            sold[mls] = (parsed.date().isoformat() if parsed else None, REASON_METRICS)
    return sold


def reconcile(df, sold):
    """Splits active rows into (kept, removed). A row is removed when its `mls` is in `sold` (MLS only, never property_id).
    removed has RECONCILED_COLUMNS, sorted by mls. Pure: nothing is deleted anywhere."""
    mask = df["mls"].isin(sold)
    removed = pd.DataFrame([{"mls": m, "sold_date": sold[m][0], "reason": sold[m][1]} for m in sorted(df.loc[mask, "mls"])],
                           columns=RECONCILED_COLUMNS).astype("string")
    return df[~mask].reset_index(drop=True), removed


def load_sold(silver_prefix):
    """The sold side of the reconcile, read from silver: {MLS: (sold date, reason)} (see sold_mls)."""
    blobs = store.list_silver(silver_prefix + "/")
    if not blobs:
        print(f"   no listed_properties files under {silver_prefix}/: nothing to reconcile against")
        return {}
    listed = pd.concat([store.read_parquet(b) for b in blobs], ignore_index=True)
    history_blob = f"{silver_prefix}/sale_history.parquet"
    history = store.read_parquet(history_blob) if store.container().get_blob_client(history_blob).exists() else None
    return sold_mls(listed, history)


# ---------------- main -------------------------------------------------------------------------

def latest_census_day(store):
    days = store.list_search_days()
    if not days:
        sys.exit("No census found. Run python -m redfin.extract.census first.")
    return days[-1]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", help="census day (default: the latest)")
    ap.add_argument("--skip-images", action="store_true", help="do not download photos that are missing in Azure")
    ap.add_argument("--out-blob", default=OUT_BLOB)
    ap.add_argument("--reconciled-blob", default=RECONCILED_BLOB)
    ap.add_argument("--silver-prefix", default="silver", help="where the sold listed_properties / sale_history are read (default silver)")
    args = ap.parse_args()

    day = args.day or latest_census_day(store)
    as_of = date.fromisoformat(day)
    sink = census.AzureSink()
    complete = sink.read(day, "manifest.json").get("complete")
    by_mls = {r["mls"]: r for r in census.read_census(sink, day) if r["mls"]}
    pages = store.newest_page_per_mls(store.list_active_pages_all(), upto_day=day)
    usable = sum(mls in by_mls for mls in pages)
    print(f"Census {day} ({'complete' if complete else 'INCOMPLETE'}): {len(by_mls)} listings; {len(pages)} MLS with a detail page "
          f"(newest page of each), {usable} of them in the census, {len(by_mls) - usable} census listings without a page.")

    rows, image_urls = build_rows(by_mls, pages, store.read_bronze, as_of)
    if not rows:
        sys.exit(f"No census listing has a detail page, so there is nothing to write. {args.out_blob} left unchanged.")
    sold = load_sold(args.silver_prefix)
    df, flagged = reconcile(pd.DataFrame(rows), sold)
    print(f"Reconcile with sold: {len(sold)} sold MLS numbers in {args.silver_prefix}/, {len(flagged)} of them still in the census.")

    existing = store.load_existing_images()
    photos = []
    for mls in df["mls"]:
        blob = store.image_blob_name(mls)
        if blob in existing:
            photos.append(blob)
        elif image_urls[mls] and not args.skip_images:
            photos.append(store.upload_image(mls, image_urls[mls]))
        else:
            photos.append(None)
    df["photo_blob"] = photos

    for col in ("list_price", "original_price", "last_change_amount", "price_vs_original",
                "days_on_market", "cumulative_dom", "price_changes"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    store.write_parquet(df, args.out_blob)
    store.write_parquet(flagged, args.reconciled_blob)   # written every run, so an old list never lingers

    changed = df[df["price_changes"].fillna(0) > 0]
    stale = (pd.to_datetime(df["as_of"]) - pd.to_datetime(df["page_day"])).dt.days
    print(f"Wrote {len(df)} listings -> {args.out_blob}")
    print(f"   removed because sold: {len(flagged)} -> {args.reconciled_blob}")
    print(f"   page age: {(stale == 0).sum()} fetched on the census day, {(stale > 0).sum()} older (oldest {int(stale.max())} days)")
    print(f"   with a price change: {len(changed)} ({(changed['last_change_direction'] == 'down').sum()} down, "
          f"{(changed['last_change_direction'] == 'up').sum()} up)")
    print(f"   days on market from events: {(df['dom_basis'] == 'events').sum()}, from Redfin's figure: {(df['dom_basis'] == 'redfin').sum()}")
    print(f"   photos: {df['photo_blob'].notna().sum()} of {len(df)}")
    runstats.report("transform.active", census_day=day, rows_written=len(df), removed_sold=len(flagged))


if __name__ == "__main__":
    main()
