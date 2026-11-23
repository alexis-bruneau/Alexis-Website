"""Deletes page-view rows older than N days from the `pageviews` table (retention: docs/ANALYTICS.md). Shows what it
would delete and stops, unless --yes is given.

    python scripts/analytics_purge.py --older-than-days 400            # dry run
    python scripts/analytics_purge.py --older-than-days 400 --yes      # delete
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from app import analytics  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--older-than-days", type=int, default=400, help="delete days before this many days ago (default 400, ~13 months)")
    parser.add_argument("--yes", action="store_true", help="really delete (without it, only count)")
    args = parser.parse_args()
    if args.older_than_days < 30:
        parser.error("--older-than-days must be at least 30 (a typo should not wipe the table)")

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    if not analytics.is_enabled():
        sys.exit("AZURE_STORAGE_CONNECTION_STRING is not set (environment or .env).")

    from azure.data.tables import TableServiceClient
    table = TableServiceClient.from_connection_string(analytics._conn_str()).get_table_client(analytics.PAGEVIEWS_TABLE)
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=args.older_than_days)).isoformat()

    by_day = {}
    for row in table.query_entities("PartitionKey lt @cutoff", parameters={"cutoff": cutoff}, select=["PartitionKey", "RowKey"]):
        by_day.setdefault(row["PartitionKey"], []).append(row["RowKey"])
    total = sum(len(v) for v in by_day.values())
    print(f"{total} rows in {len(by_day)} days before {cutoff}.")
    if not args.yes:
        print("Dry run: nothing deleted. Add --yes to delete.")
        return
    for day, row_keys in by_day.items():
        for i in range(0, len(row_keys), 100):       # a transaction: one partition, at most 100 operations
            table.submit_transaction([("delete", {"PartitionKey": day, "RowKey": rk}) for rk in row_keys[i:i + 100]])
    print(f"Deleted {total} rows.")


if __name__ == "__main__":
    main()
