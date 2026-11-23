"""Prints the sold map's visitor numbers for the last N days (UTC): page views and unique visitors per day, the device
split and the top referrers. Reads the `pageviews` table in the storage account of AZURE_STORAGE_CONNECTION_STRING
(taken from the environment or from .env). Read-only. What the numbers mean: docs/ANALYTICS.md.

    python scripts/analytics_report.py              # last 30 days
    python scripts/analytics_report.py --days 7 --top 5
"""
import argparse
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))  # so `app.analytics` imports

from app import analytics  # noqa: E402  (no Flask / data loading: app/__init__.py does not exist)

FIELDS = ["PartitionKey", "visitor_hash", "device", "referrer_host"]


def summarize(entities, days, today):
    """Turns the page-view rows into the report numbers. `entities` are dicts with the FIELDS; `today` is a date.
    Returns {"days": [(date, views, uniques), ...] oldest first (days without a visit included),
             "devices": Counter, "referrers": Counter}."""
    start = today - timedelta(days=days - 1)
    views = Counter()
    visitors = defaultdict(set)
    devices, referrers = Counter(), Counter()
    for e in entities:
        day = e["PartitionKey"]
        if day < start.isoformat():
            continue
        views[day] += 1
        visitors[day].add(e.get("visitor_hash", ""))
        devices[e.get("device") or "unknown"] += 1
        referrers[e.get("referrer_host") or "(direct / none)"] += 1
    rows = []
    for i in range(days):
        day = (start + timedelta(days=i)).isoformat()
        rows.append((day, views[day], len(visitors[day])))
    return {"days": rows, "devices": devices, "referrers": referrers}


def format_report(summary, top):
    rows = summary["days"]
    total_views = sum(r[1] for r in rows)
    out = [f"Sold map, {rows[0][0]} to {rows[-1][0]} (UTC)  -  paths: {', '.join(sorted(analytics.TRACKED_PATHS))}",
           "", "date          views  unique visitors", "-" * 34]
    out += [f"{day}  {v:7d}  {u:15d}" for day, v, u in rows]
    out += ["-" * 34,
            f"total       {total_views:7d}  {sum(r[2] for r in rows):15d}   (uniques are per day, so a person who came on 3 days counts 3 times)",
            "", "Device split (page views)"]
    for name, n in summary["devices"].most_common():
        out.append(f"  {name:<10} {n:6d}  {n / total_views:6.1%}")
    out += ["", f"Top {top} referrers (host only)"]
    for name, n in summary["referrers"].most_common(top):
        out.append(f"  {name:<30} {n:6d}")
    if total_views == 0:
        out.append("\nNo page views in this period.")
    elif total_views >= 50 and max(r[2] for r in rows) == 1:
        out.append("\nWarning: many views but never more than 1 unique visitor a day. The visitor IP is probably not the "
                   "real one: check PROXY_HOPS in app/main.py (docs/ANALYTICS.md).")
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--days", type=int, default=30, help="how many days back, today included (default 30)")
    parser.add_argument("--top", type=int, default=10, help="how many referrers to list (default 10)")
    args = parser.parse_args()
    if args.days < 1:
        parser.error("--days must be at least 1")

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    if not analytics.is_enabled():
        sys.exit("AZURE_STORAGE_CONNECTION_STRING is not set (environment or .env).")

    from azure.data.tables import TableServiceClient
    table = TableServiceClient.from_connection_string(analytics._conn_str()).get_table_client(analytics.PAGEVIEWS_TABLE)
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=args.days - 1)).isoformat()
    entities = table.query_entities("PartitionKey ge @start", parameters={"start": start}, select=FIELDS)
    print(format_report(summarize(entities, args.days, today), args.top))


if __name__ == "__main__":
    main()
