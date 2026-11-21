#!/usr/bin/env python3
"""EXTRACT (sold): fetch listing pages from redfin.ca and save the raw HTML to bronze/. Nothing else.

No parsing happens here. Turning the HTML into rows (and fetching images) is redfin.transform.sold's job,
so a parser fix never needs another visit to redfin.ca. Bronze pages are written once and never changed.

Which URLs are pending: the ones in the queue (config.URLS_FILE) that are not yet in silver. Run
redfin.transform.sold afterwards so silver catches up, otherwise the next extract fetches the same pages again.

    python -m redfin.extract.fetch_sold              # every pending URL (or MAX_SCRAPE_COUNT)
    python -m redfin.extract.fetch_sold --limit 20   # at most 20 pages
"""
import argparse
import time

from playwright.sync_api import sync_playwright

from redfin import config, runstats
from redfin.scraping.browser import USER_AGENT, fetch_html, load_redfin_cookies
from redfin.storage import azure_store as store
from redfin.transform import parsing


def known_property_ids():
    """Property ids already parsed into silver (matched by id: silver holds the page's canonical URL,
    which can be spelled differently from the URL we discovered)."""
    ids = set()
    for blob in store.list_silver():
        ids.update(parsing.property_id(u) for u in store.read_parquet(blob)["url"].dropna())
    return ids


def main(limit=None):
    start_time = time.time()
    limit = config.MAX_SCRAPE_COUNT if limit is None else limit

    if not config.URLS_FILE.exists():
        print(f"URL file {config.URLS_FILE} not found! Run python -m redfin.extract.discover_sold first.")
        return

    print("Reading existing state from Azure...")
    known = known_property_ids()
    with config.URLS_FILE.open("r", encoding="utf-8") as f:
        all_urls = list(dict.fromkeys(u.strip() for u in f if u.strip()))
    in_scope = [u for u in all_urls if parsing.in_scope(u)]
    pending = [u for u in in_scope if parsing.property_id(u) not in known]
    print(f"URL file: {len(all_urls)} URLs ({len(all_urls) - len(in_scope)} out of scope, ignored), "
          f"{len(pending)} not yet in silver.")

    if limit and len(pending) > limit:
        print(f"Limiting this run to the first {limit} pending URLs (newest first).")
        pending = pending[:limit]
    if not pending:
        print("Nothing to fetch. Done.")
        runstats.report("fetch_sold", pending=0, saved=0, stopped_early=False)
        return

    saved = failures_in_row = 0
    stopped_early = False
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        context.add_cookies(load_redfin_cookies())

        print(f"Starting fetch for {len(pending)} pages...")
        for i, url in enumerate(pending, 1):
            html = fetch_html(url, context)
            if html is None:
                failures_in_row += 1
                if failures_in_row >= config.MAX_CONSECUTIVE_FAILURES:
                    print(f"Stopping: {failures_in_row} failures in a row (blocked or cookies expired?).")
                    stopped_early = True
                    break
                continue
            failures_in_row = 0

            # Named by MLS, or P<property_id> when the page has none: the same page always gets the same name
            name = parsing.listing_key(parsing.property_id(url), parsing.extract_mls(html))
            written = store.put_bronze(html, name, url)
            saved += written
            print(f"[{i}/{len(pending)}] {name} {'saved to bronze' if written else 'already in bronze today'}")

        browser.close()

    runstats.report("fetch_sold", pending=len(pending), saved=int(saved), stopped_early=stopped_early)
    print(f"\nDone: {saved} new pages in bronze in {time.time() - start_time:.0f}s "
          f"({len(pending) - saved} not saved). Now run python -m redfin.transform.sold.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=None,
                        help="Max pages this run (default: MAX_SCRAPE_COUNT env, 0 = all pending)")
    main(limit=parser.parse_args().limit)
