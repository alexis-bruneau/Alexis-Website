"""Every path, environment variable and run cap of the Redfin pipeline, in one place.

Paths resolve from this file's location (the repo checkout), never from the current directory, so every entry point
behaves the same whether it is run from the repo root, from redfin/, or from a GitHub Action.

Environment (read once, at import; `.env` at the repo root is loaded first, real environment variables win):
    AZURE_STORAGE_CONNECTION_STRING  required by anything that touches Azure
    MAX_SCRAPE_COUNT          sold extract: max pages per run, 0 = every pending URL (the daily Action sets 300)
    MAX_CONSECUTIVE_FAILURES  extract: stop after this many failed fetches in a row (blocked / cookies expired)
    MAX_NEW_URLS              sold discovery: max new URLs per run
    CENSUS_SLICE_MAX          census: a price band with more results than this is split
    SAVE_STREETVIEW           "1" (default) = use Google Street View as the fallback photo
    REDFIN_COOKIE_FILE        override the cookie file location
    REDFIN_STATS_FILE         set by redfin.run.daily: a JSON file the steps append their counts to (see runstats.py)
"""
import os
from pathlib import Path

from dotenv import load_dotenv

PACKAGE_DIR = Path(__file__).resolve().parent      # redfin/src/redfin
PROJECT_DIR = PACKAGE_DIR.parents[1]               # redfin/
REPO_ROOT = PROJECT_DIR.parent                     # the git checkout

load_dotenv(REPO_ROOT / ".env")

# ---------------- local files --------------------------------------------------
# Sold-listing URLs found by discovery and waiting to be fetched (committed to git).
URLS_FILE = PROJECT_DIR / "data" / "queue" / "property_urls.txt"
# Ottawa neighbourhood polygons for the sold map (committed, read-only; rebuilt by scripts/build_neighbourhoods.py).
# Source, licence and attribution: data/neighbourhoods/README.md.
NEIGHBOURHOODS_FILE = PROJECT_DIR / "data" / "neighbourhoods" / "ottawa_neighbourhoods.geojson"
# Redfin login cookies exported from Chrome (gitignored; the daily Action writes it from the REDFIN_COOKIES_JSON secret).
COOKIE_FILE = Path(os.getenv("REDFIN_COOKIE_FILE") or PROJECT_DIR / "chrome_cookies.json")
# Local, gitignored output (e.g. the screenshot discovery saves when Redfin blocks it).
OUTPUT_DIR = PROJECT_DIR / "output"
DEBUG_SCREENSHOT = OUTPUT_DIR / "debug_blocked.png"

# ---------------- Azure ----------------------------------------------------------
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"
ACCOUNT_NAME = "stredfinprod"
PUBLIC_BLOB_URL = f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/"

# ---------------- run caps -------------------------------------------------------
MAX_SCRAPE_COUNT = int(os.getenv("MAX_SCRAPE_COUNT", "0"))
MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "8"))
MAX_NEW_URLS = int(os.getenv("MAX_NEW_URLS", "500"))
CENSUS_SLICE_MAX = int(os.getenv("CENSUS_SLICE_MAX", "340"))
SAVE_STREETVIEW = os.getenv("SAVE_STREETVIEW", "1") == "1"
# Silver keeps only bronze days from this month on (YYYY-MM). Bronze itself keeps everything.
SILVER_MIN_MONTH = os.getenv("SILVER_MIN_MONTH", "2025-06")
STATS_FILE = os.getenv("REDFIN_STATS_FILE")  # unset outside redfin.run.daily: steps then report nothing
