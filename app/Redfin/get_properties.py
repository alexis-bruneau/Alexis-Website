#!/usr/bin/env python3
"""
Collect Redfin property-detail URLs for the given Ottawa postal codes
and save them to property_urls.txt (one URL per line).
"""
import time, random, json, requests
from bs4 import BeautifulSoup
from pathlib import Path

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

POSTAL_CODES = ["K1N"]  # add more if you wish

MAX_PAGES = 50  # stop if pagination gets this far w/out results
SLEEP_RANGE = (0.3, 0.6)  # polite delay between page requests


def collect_urls_for(code: str) -> set[str]:
    """Return a set of property URLs for one postal code."""
    urls = set()
    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.redfin.ca/on/{code.lower()}/filter/include=sold-5yr/page-{page}"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        found_this_page = False

        for script in soup.find_all("script"):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
            except Exception:
                continue

            # data can be a dict with "url", or a list of such dicts
            for item in data if isinstance(data, list) else [data]:
                if not isinstance(item, dict):
                    continue

                url_value = item.get("url")  # <-- safe lookup
                if url_value and url_value != "https://www.redfin.ca":
                    urls.add(url_value)
                    found_this_page = True

        if not found_this_page:
            break  # no more listings

        time.sleep(random.uniform(*SLEEP_RANGE))

    return urls


def main():
    all_urls = set()
    for pc in POSTAL_CODES:
        all_urls.update(collect_urls_for(pc))

    with open("app/Redfin/Output/property_urls.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(all_urls)))

    print(f"✅  Saved {len(all_urls)} URLs to property_urls.txt")


if __name__ == "__main__":
    main()
