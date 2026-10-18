#!/usr/bin/env python3
"""EXTRACT: fetch listing pages from redfin.ca and save the raw HTML to bronze/. Nothing else.

No parsing happens here. Turning the HTML into rows (and fetching images) is transform_silver.py's job,
so a parser fix never needs another visit to redfin.ca. Bronze pages are written once and never changed.

Which URLs are pending: the ones in property_urls.txt that are not yet in silver. (Temporary: the state
queue in step 3 of the refactor replaces this.) Run transform_silver.py afterwards so silver catches up,
otherwise the next extract would fetch the same pages again.
"""
import argparse
import json
import os
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

import parsing
import azure_store as store

URLS_FILE = Path("app/Redfin/Output/property_urls.txt")
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# Run limits (all overridable from the environment / --limit):
#   MAX_SCRAPE_COUNT  0 = fetch every pending URL (default). Set a number to cap a run.
#   MAX_CONSECUTIVE_FAILURES  stop early when Redfin is clearly blocking us (expired cookies, 403s).
MAX_SCRAPE_COUNT = int(os.getenv("MAX_SCRAPE_COUNT", "0"))
MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "8"))


def load_redfin_cookies():
    if not COOKIE_FILE.exists():
        return []
    with COOKIE_FILE.open(encoding="utf-8") as fh:
        raw = json.load(fh)
    expire = int(time.time()) + 30 * 24 * 3600
    return [
        {**c, "sameSite": "Lax", "expires": expire}
        for c in raw
        if ".redfin.ca" in c.get("domain", "")
    ]


def known_property_ids():
    """Property ids already parsed into silver (matched by id: silver holds the page's canonical URL,
    which can be spelled differently from the URL we discovered)."""
    ids = set()
    for blob in store.list_silver():
        ids.update(parsing.property_id(u) for u in store.read_parquet(blob)["url"].dropna())
    return ids


def fetch_html(url, context):
    """Returns the rendered HTML of one listing page, or None on error / login wall."""
    page = context.new_page()
    # Block heavy resources
    page.route("**/*", lambda route: route.abort()
               if route.request.resource_type in ("image", "media", "font") else route.continue_())
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(1.5)  # let JS hydrate (Redfin needs this)
        if "/login" in page.url:
            print(f"🔒 Login page encountered at {url}")
            return None
        return page.content()
    except Exception as e:
        print(f"⚠️ Error/Timeout at {url}: {e}")
        return None
    finally:
        page.close()


def main(limit=None):
    start_time = time.time()
    limit = MAX_SCRAPE_COUNT if limit is None else limit

    if not URLS_FILE.exists():
        print("URL File not found! Run get_properties.py first.")
        return

    print("Reading existing state from Azure...")
    known = known_property_ids()
    with URLS_FILE.open("r", encoding="utf-8") as f:
        all_urls = list(dict.fromkeys(u.strip() for u in f if u.strip()))
    pending = [u for u in all_urls if parsing.property_id(u) not in known]
    print(f"URL file: {len(all_urls)} URLs, {len(pending)} not yet in silver.")

    if limit and len(pending) > limit:
        print(f"Limiting this run to the first {limit} pending URLs (newest first).")
        pending = pending[:limit]
    if not pending:
        print("Nothing to fetch. Done.")
        return

    saved = failures_in_row = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        context.add_cookies(load_redfin_cookies())

        print(f"Starting fetch for {len(pending)} pages...")
        for i, url in enumerate(pending, 1):
            html = fetch_html(url, context)
            if html is None:
                failures_in_row += 1
                if failures_in_row >= MAX_CONSECUTIVE_FAILURES:
                    print(f"Stopping: {failures_in_row} failures in a row (blocked or cookies expired?).")
                    break
                continue
            failures_in_row = 0

            mls = parsing.extract_mls(html) or f"UNKNOWN_{int(time.time())}"
            written = store.put_bronze(html, mls, url)
            saved += written
            print(f"[{i}/{len(pending)}] {mls} {'saved to bronze' if written else 'already in bronze today'}")

        browser.close()

    print(f"\nDone: {saved} new pages in bronze in {time.time() - start_time:.0f}s "
          f"({len(pending) - saved} not saved). Now run transform_silver.py.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch pending Redfin listing pages into bronze/.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max pages this run (default: MAX_SCRAPE_COUNT env, 0 = all pending)")
    main(limit=parser.parse_args().limit)
