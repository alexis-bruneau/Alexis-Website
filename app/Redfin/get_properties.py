#!/usr/bin/env python3
"""
Scrape Redfin sold-last-5-years property URLs for Ottawa.

What this script does
- Discovers property "home" URLs by scanning Redfin search pages.
- De-duplicates globally (thread-safe) so the same home_id is only counted once.
- Runs viewports in parallel with ThreadPoolExecutor.
- Writes the final list of canonical URLs to a text file.

Two discovery modes
1) MODE="postal_test"
   - For testing: specify a small list of Ottawa FSAs (first 3 chars of postal codes),
     e.g. ["K1P", "K1R", "K1N"].
   - The script converts each FSA into a small viewport box around a centroid.
   - This keeps runs small and predictable while you iterate.

2) MODE="grid"
   - For production/backfill: scan a full rectangular region using a fine grid.

Notes
- Viewport scanning does NOT guarantee perfect postal-code isolation (Redfin returns
  listings in a viewport; some may be outside your intended FSA boundaries).
  For strict postal filtering, apply an additional filter during the detail-scrape
  step after extracting the actual Postal Code from the listing page.
"""

from __future__ import annotations

import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import cos, radians
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Set, Tuple

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# 0) RUNTIME SETTINGS
# ─────────────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
    )
}

# Choose discovery strategy:
# - "postal_test": scan small viewports around specific FSAs (best for testing)
# - "grid": scan a full region with a grid (best for production/backfill)
MODE = "postal_test"  # "postal_test" or "grid"

# -----------------------
# Postal-test configuration
# -----------------------
# Downtown Ottawa postal areas - comprehensive coverage
TEST_FSAS: List[str] = ["K1R", "K2C", "K1H", "K1L", "K1M", "K2P", "K1N"]

# Approximate centroids for Ottawa FSAs
# These cover downtown and central Ottawa comprehensively
FSA_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "K1P": (45.4197, -75.7000),  # Centretown / Downtown core
    "K1R": (45.4095, -75.7230),  # West Centretown / Little Italy
    "K1N": (45.4295, -75.6900),  # ByWard Market / Lowertown
    "K1H": (45.4150, -75.6650),  # Sandy Hill / University area
    "K1L": (45.4000, -75.6500),  # Alta Vista / Hurdman area
    "K1M": (45.3850, -75.6700),  # Old Ottawa East / Billings Bridge
    "K2C": (45.3650, -75.7500),  # Nepean / Baseline area
    "K2P": (45.4115, -75.7020),  # South of downtown core
}

# Box size around each centroid (km)
# 1.5 km provides finer-grained coverage to catch more properties
POSTAL_BOX_KM = 1

# -----------------------
# Grid (production) configuration
# -----------------------
# Full Ottawa bounds
LAT_MIN, LAT_MAX = 45.1789, 45.5177  # Ottawa south-north
LON_MIN, LON_MAX = -76.1658, -75.4078  # Ottawa west-east

# Grid resolution (bigger step = fewer cells / fewer requests)
LAT_STEP, LON_STEP = 0.01, 0.01  # ≈ 550 m × 400 m cells

# -----------------------
# Shared scraping controls
# -----------------------
MAX_PAGES = 5 if MODE == "postal_test" else 9
SLEEP_RANGE = (0.35, 0.65) if MODE == "postal_test" else (0.25, 0.45)
MAX_WORKERS = 8 if MODE == "postal_test" else 32
PRINT_EVERY = 1

# Optional hard cap during testing if you still end up with many cells
MAX_CELLS = None  # e.g., 50


# ─────────────────────────────────────────────────────────────────────────────
# 1) GLOBALS FOR THREAD-SAFE DE-DUPLICATION
# ─────────────────────────────────────────────────────────────────────────────
_seen_ids: Dict[str, str] = {}  # home_id -> full_url
_seen_lock = Lock()

URL_RE = re.compile(r"/home/(\d+)")  # extract numeric home-id from URL


# ─────────────────────────────────────────────────────────────────────────────
# 2) HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def frange(start: float, stop: float, step: float) -> Iterable[float]:
    while start < stop:
        yield start
        start += step


def box_around(lat: float, lon: float, km: float) -> Tuple[float, float, float, float]:
    """
    Create a roughly km-sized bounding box around a lat/lon point.
    Returns (lat_lo, lat_hi, lon_lo, lon_hi).
    """
    # ~111 km per 1 degree latitude
    dlat = km / 111.0
    # longitude degrees shrink with latitude
    dlon = km / (111.0 * cos(radians(lat)))
    return (lat - dlat, lat + dlat, lon - dlon, lon + dlon)


def grid_cells() -> List[Tuple[float, float, float, float]]:
    """
    Build a list of viewport boxes (lat_lo, lat_hi, lon_lo, lon_hi) depending on MODE.
    """
    cells: List[Tuple[float, float, float, float]] = []

    if MODE == "postal_test":
        missing = [fsa for fsa in TEST_FSAS if fsa not in FSA_CENTROIDS]
        if missing:
            raise ValueError(
                f"Missing centroid(s) for FSAs: {missing}. Add them to FSA_CENTROIDS."
            )

        for fsa in TEST_FSAS:
            lat, lon = FSA_CENTROIDS[fsa]
            lat_lo, lat_hi, lon_lo, lon_hi = box_around(lat, lon, km=POSTAL_BOX_KM)
            cells.append((lat_lo, lat_hi, lon_lo, lon_hi))

        return cells

    # Default: full region grid
    for lat_lo in frange(LAT_MIN, LAT_MAX, LAT_STEP):
        lat_hi = min(lat_lo + LAT_STEP, LAT_MAX)
        for lon_lo in frange(LON_MIN, LON_MAX, LON_STEP):
            lon_hi = min(lon_lo + LON_STEP, LON_MAX)
            cells.append((lat_lo, lat_hi, lon_lo, lon_hi))

    return cells


# ─────────────────────────────────────────────────────────────────────────────
# 3) SCRAPER CORE
# ─────────────────────────────────────────────────────────────────────────────
def collect_from_viewport(vp: Tuple[float, float, float, float]) -> int:
    """
    Scrape every page for one viewport and add *new* home IDs into _seen_ids.
    Returns: how many NEW urls were inserted by this viewport.
    """
    lat_lo, lat_hi, lon_lo, lon_hi = vp
    newly_added = 0

    for page in range(1, MAX_PAGES + 1):
        vp_str = f"{lat_hi:.5f}:{lat_lo:.5f}:{lon_hi:.5f}:{lon_lo:.5f}"
        url = (
            "https://www.redfin.ca/on/ottawa/"
            f"filter/include=sold-5yr,viewport={vp_str}/page-{page}"
        )

        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
        except requests.RequestException:
            break

        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        found_any = False

        for script in soup.find_all("script"):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
            except Exception:
                continue

            for item in data if isinstance(data, list) else [data]:
                if not isinstance(item, dict):
                    continue
                u = item.get("url")
                if not u or u == "https://www.redfin.ca":
                    continue

                full = f"https://www.redfin.ca{u}" if u.startswith("/") else u
                m = URL_RE.search(full)
                if not m:
                    continue

                home_id = m.group(1)

                with _seen_lock:
                    existing_url = _seen_ids.get(home_id)
                    if existing_url is None:
                        _seen_ids[home_id] = full
                        newly_added += 1
                        print(f"[{home_id}] New URL: {full}")
                    else:
                        # Found again. If this new 'full' is longer than what we have, update it.
                        if len(full) > len(existing_url):
                            _seen_ids[home_id] = full
                            # Note: we don't increment newly_added because the ID was already known.
                            print(f"[{home_id}] Updated to longer URL: {full}")

                found_any = True

        if not found_any:
            break

        time.sleep(random.uniform(*SLEEP_RANGE))

    return newly_added


# ─────────────────────────────────────────────────────────────────────────────
# 4) MAIN DRIVER
# ─────────────────────────────────────────────────────────────────────────────
def main():
    cells = grid_cells()

    if MAX_CELLS:
        cells = cells[:MAX_CELLS]

    total_cells = len(cells)

    if MODE == "postal_test":
        print(
            f"MODE=postal_test  FSAs={TEST_FSAS}  BOX_KM={POSTAL_BOX_KM}  "
            f"MAX_PAGES={MAX_PAGES}  WORKERS={MAX_WORKERS}"
        )
    else:
        print(
            f"MODE=grid  LAT[{LAT_MIN},{LAT_MAX}]  LON[{LON_MIN},{LON_MAX}]  "
            f"STEP[{LAT_STEP},{LON_STEP}]  MAX_PAGES={MAX_PAGES}  WORKERS={MAX_WORKERS}"
        )

    print(f"Will scan {total_cells} viewport(s)…\n")

    done = 0
    total_new = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(collect_from_viewport, vp): vp for vp in cells}

        for fut in as_completed(futures):
            added = fut.result()
            done += 1
            total_new += added

            if done % PRINT_EVERY == 0:
                print(
                    f"📍  {done:4d} / {total_cells} viewports done  –  "
                    f"+{added:3d} new  →  {_safe_len(_seen_ids):,} unique homes"
                )

    # ------- write final file -------
    out_path = Path("app/Redfin/Output/property_urls.txt")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        for hid in sorted(_seen_ids.keys(), key=int):
            #  
            # It redirects to the full canonical URL.
            f.write(f"{_seen_ids[hid]}\n")

    print(
        f"\n✅  Finished: {len(_seen_ids):,} unique properties "
        f"written to {out_path}"
    )


def _safe_len(s: Set[str]) -> int:
    # helper to avoid any weirdness if you ever change the global type
    return len(s)


if __name__ == "__main__":
    main()
