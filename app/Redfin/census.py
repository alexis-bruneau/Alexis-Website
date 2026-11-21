#!/usr/bin/env python3
"""CENSUS (extract): snapshot every active listing in the Ottawa area from redfin.ca's for-sale search.

A search page embeds the results as JSON (MLS number, status, asking price, coordinates, Redfin's days on
market, propertyId, listingId) but only the newest SEARCH_RESULT_CAP (350) of them, and page 2 repeats the same
350. So the area is cut into price bands and any band with more than CENSUS_SLICE_MAX results is split in two
(at the median price) until every band is complete. The bands overlap a little at their edges (Redfin rounds
price filters); the duplicates are removed by listingId when the census is read.

Each band is saved as it is fetched, as the raw homes array, to bronze/search/<day>/<min>_<max>.json.gz. A
manifest.json is written last and says whether the day's census is complete. Nothing here parses or interprets
the homes: parsing.census_row() does that later, so a parser fix never needs another visit to redfin.ca.

A band already saved for today is reused, so a run that was blocked partway can be re-run and continues.

    python app/Redfin/census.py                          # fetch today's census into Azure bronze/search/
    python app/Redfin/census.py --local-dir census_out   # same, but write to a local folder (no Azure needed)
    python app/Redfin/census.py --headed                 # show the browser
    python app/Redfin/census.py --max-slices 5           # stop after 5 page loads (smoke test)
"""
import argparse
import json
import os
import random
import sys
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).parent))
import parsing  # noqa: E402

# The map window of "all of Ottawa" in the for-sale search (lat N : lat S : lon E : lon W). It also covers
# neighbouring towns (e.g. Clarence-Rockland); keeping only the City of Ottawa is a later, silver-layer step.
VIEWPORT = "viewport=45.5849:45.04992:-75.12143:-76.14796"
SEARCH_BASE = "https://www.redfin.ca/on/ottawa/filter/"
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

CENSUS_SLICE_MAX = int(os.getenv("CENSUS_SLICE_MAX", "340"))  # a band with more results than this gets split
MAX_RETRIES = 1          # one retry (after a pause) when a page has no result header, then call it blocked
COVERAGE_TOLERANCE = 0.02  # distinct MLS numbers vs the unfiltered header count


def load_redfin_cookies():
    if not COOKIE_FILE.exists():
        return []
    with COOKIE_FILE.open(encoding="utf-8") as fh:
        raw = json.load(fh)
    expire = int(time.time()) + 30 * 24 * 3600
    return [{**c, "sameSite": "Lax", "expires": expire} for c in raw if ".redfin.ca" in c.get("domain", "")]


def band_name(lo, hi):
    return f"{'' if lo is None else lo}_{'' if hi is None else hi}.json.gz"


def band_url(lo, hi):
    filters = ([] if lo is None else [f"min-price={lo}"]) + ([] if hi is None else [f"max-price={hi}"])
    return SEARCH_BASE + ",".join(filters + [VIEWPORT, "no-outline"])


class LocalSink:
    """Census blobs in a local folder: <root>/<day>/<name>. Same interface as AzureSink."""

    def __init__(self, root):
        self.root = Path(root)

    def read(self, day, name):
        path = self.root / day / name
        if not path.exists():
            return None
        data = path.read_bytes()
        return json.loads(data if name == "manifest.json" else zlib.decompress(data))

    def days(self):
        return sorted(d.name for d in self.root.iterdir() if (d / "manifest.json").exists()) if self.root.exists() else []

    def write(self, day, name, payload, overwrite=False):
        path = self.root / day / name
        if path.exists() and not overwrite:
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        if name == "manifest.json":
            path.write_text(json.dumps(payload, indent=1), encoding="utf-8")
        else:  # zlib, like the Azure blobs (and like the bronze HTML pages)
            path.write_bytes(zlib.compress(json.dumps(payload, separators=(",", ":")).encode("utf-8")))
        return True


class AzureSink:
    def __init__(self):
        import azure_store  # imported here so --local-dir works without an Azure connection string
        self.store = azure_store

    def read(self, day, name):
        return self.store.read_search_blob(day, name)

    def days(self):
        return self.store.list_search_days()

    def write(self, day, name, payload, overwrite=False):
        return self.store.put_search_blob(day, name, payload, overwrite=overwrite)


def read_census(sink, day):
    """The day's census as flat rows (parsing.census_row), one per MLS number, sorted by MLS.

    Bands overlap at their edges, and Redfin sometimes holds the same MLS listing under two propertyIds (same
    address). Those collapse to one row, keeping the lowest propertyId; the others go in `alt_property_ids`."""
    manifest = sink.read(day, "manifest.json")
    if manifest is None:
        raise FileNotFoundError(f"no census manifest for {day}")
    seen = {}
    for s in manifest["slices"]:
        for home in sink.read(day, band_name(s["min"], s["max"]))["homes"]:
            row = parsing.census_row(home)
            seen.setdefault(row["mls"] or f"P{row['property_id']}", {})[row["property_id"]] = row
    rows = []
    for key in sorted(seen):
        by_pid = seen[key]
        row = dict(by_pid[min(by_pid, key=int)])
        row["alt_property_ids"] = sorted(set(by_pid) - {row["property_id"]}, key=int)
        rows.append(row)
    return rows


class Census:
    def __init__(self, page, sink, day, max_slices=0, pause=(3.0, 5.0)):
        self.page, self.sink, self.day = page, sink, day
        self.max_slices, self.pause = max_slices, pause
        self.loads = 0
        self.slices = []      # one summary per finished band
        self.problems = []
        self.blocked = False

    def _load(self, lo, hi):
        """(homes, total) of one band, or None when the page has no result header (blocked / login wall)."""
        for attempt in range(MAX_RETRIES + 1):
            self.loads += 1
            self.page.goto(band_url(lo, hi), wait_until="domcontentloaded", timeout=60000)
            try:
                self.page.wait_for_selector('[data-rf-test-id="homes-description"]', state="attached", timeout=20000)
            except Exception:
                pass
            time.sleep(random.uniform(*self.pause))
            homes, total = parsing.parse_search_page(self.page.content())
            if total is not None:
                return homes, total
            print(f"   no result header for {lo}..{hi} (title: {self.page.title()!r}), attempt {attempt + 1}", flush=True)
            time.sleep(20)
        return None

    def band(self, lo, hi):
        """Fetches (or reuses) one band. Returns (homes, total) or None when blocked / out of page-load budget."""
        saved = self.sink.read(self.day, band_name(lo, hi))
        if saved is not None:
            print(f"band {lo}..{hi}: reused (header={saved['total']} homes={len(saved['homes'])})", flush=True)
            return saved["homes"], saved["total"]
        if self.max_slices and self.loads >= self.max_slices:
            self.problems.append("stopped at --max-slices")
            return None
        result = self._load(lo, hi)
        if result is None:
            self.blocked = True
            return None
        homes, total = result
        self.sink.write(self.day, band_name(lo, hi), {
            "min": lo, "max": hi, "total": total, "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "homes": homes})
        print(f"band {lo}..{hi}: header={total} homes={len(homes)}", flush=True)
        return homes, total

    def run(self):
        todo = [(None, None)]
        while todo and not self.blocked:
            lo, hi = todo.pop()
            result = self.band(lo, hi)
            if result is None:
                self.problems.append(f"band {lo}..{hi} not fetched")
                break
            homes, total = result
            self.slices.append({"min": lo, "max": hi, "total": total, "homes": len(homes)})
            if total <= CENSUS_SLICE_MAX:
                if len(homes) != total:  # header and embedded list disagree: do not trust this band silently
                    self.problems.append(f"band {lo}..{hi}: header {total} but {len(homes)} homes")
                continue
            prices = sorted(p for p in (parsing.census_row(h)["list_price"] for h in homes) if p)
            mid = prices[len(prices) // 2] if prices else None
            if hi is not None:
                mid = None if mid is None else min(mid, hi - 1)
            if lo is not None and mid is not None:
                mid = max(mid, lo)
            if mid is None or (lo is not None and hi is not None and hi <= lo):
                self.problems.append(f"band {lo}..{hi} has {total} results and cannot be split by price")
                continue
            todo += [(mid + 1, hi), (lo, mid)]   # (lo, mid) is fetched first, lowest prices first

    def manifest(self):
        listings, mls = {}, set()
        for s in self.slices:
            saved = self.sink.read(self.day, band_name(s["min"], s["max"]))
            for home in (saved or {}).get("homes", []):
                row = parsing.census_row(home)
                listings[row["listing_id"] or row["property_id"]] = row
                mls.add(row["mls"])
        root_total = next((s["total"] for s in self.slices if s["min"] is None and s["max"] is None), None)
        problems = list(self.problems)
        if root_total and abs(len(mls) - root_total) > COVERAGE_TOLERANCE * root_total:
            problems.append(f"{len(mls)} distinct MLS numbers vs header {root_total} (more than "
                            f"{COVERAGE_TOLERANCE:.0%} apart)")
        return {
            "day": self.day, "complete": not problems and not self.blocked,
            "root_total": root_total, "distinct_listings": len(listings), "distinct_mls": len(mls),
            "page_loads": self.loads, "slices": self.slices, "problems": problems,
            "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--local-dir", help="write to this folder instead of Azure")
    ap.add_argument("--day", default=datetime.now().strftime("%Y-%m-%d"), help="census date (default today)")
    ap.add_argument("--headed", action="store_true", help="show the browser window")
    ap.add_argument("--max-slices", type=int, default=0, help="max page loads this run (0 = no limit)")
    args = ap.parse_args()

    sink = LocalSink(args.local_dir) if args.local_dir else AzureSink()
    t0 = time.time()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headed)
        context = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1400, "height": 900})
        context.add_cookies(load_redfin_cookies())
        page = context.new_page()
        census = Census(page, sink, args.day, max_slices=args.max_slices)
        census.run()
        browser.close()

    manifest = census.manifest()
    sink.write(args.day, "manifest.json", manifest, overwrite=True)
    print(f"\nCensus {args.day}: {manifest['distinct_listings']} listings, {manifest['distinct_mls']} MLS numbers "
          f"(header said {manifest['root_total']}), {manifest['page_loads']} page loads, {time.time() - t0:.0f}s, "
          f"complete={manifest['complete']}")
    for problem in manifest["problems"]:
        print("   PROBLEM:", problem)
    sys.exit(0 if manifest["complete"] else 2)


if __name__ == "__main__":
    main()
