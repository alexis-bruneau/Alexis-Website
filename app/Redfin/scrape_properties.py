#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime
import json, time, re
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- config --------------------------------------------------
test = True
MAX_URLS = 40
URLS_FILE = Path("app/Redfin/Output/property_urls.txt")
OUT_CSV = Path("app/Redfin/Output/redfin_data.csv")
OUT_HISTORY = Path("app/Redfin/Output/redfin_sale_history.csv")
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
WAIT_SEC = 0.5
IMAGES_DIR = Path("app/Redfin/Output/images")
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"


# ---------------- helpers -------------------------------------------------
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


def extract_between(text, start, stop="\\"):
    markers = [start] if '"' not in start else [start, start.replace('"', r"\"")]
    for m in markers:
        idx = text.find(m)
        if idx != -1:
            i = idx + len(m)
            j = text.find(stop, i) if stop else -1
            return text[i:j] if j != -1 else text[i:]
    return "N/A"


def parse_date(s):
    try:
        return datetime.strptime(s, "%b %d, %Y")
    except:
        return None


money_re = re.compile(r"[^\d]")


def money_to_int(s):
    s_clean = money_re.sub("", s)
    return int(s_clean) if s_clean else None


def parse_sale_history(html, url):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for div in soup.select("div.PropertyHistoryEventRow"):
        date = div.select_one("div.col-4 p")
        desc = div.select_one(".description-col div")
        price = div.select_one(".price-col")
        mls = div.select_one(".description-col p.subtext")

        rows.append(
            {
                "url": url,
                "eventDate": date.get_text(strip=True) if date else "",
                "eventType": desc.get_text(strip=True) if desc else "",
                "price": (
                    price.get_text(strip=True).replace("\xa0", " ") if price else ""
                ),
                "MLS": (
                    re.sub(r"^\s*TREB\s+#", "", mls.get_text(strip=True)) if mls else ""
                ),
            }
        )
    return rows


def find_genmid_values(html):
    return list(dict.fromkeys(re.findall(r"genMid\.([A-Za-z0-9_]+\.jpg)", html)))


def download_images(image_urls, mls):
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    for idx, url in enumerate(image_urls, start=1):
        path = IMAGES_DIR / f"{mls}_{idx}.jpg"
        if not path.exists():
            try:
                resp = requests.get(url, timeout=20)
                if resp.status_code == 200:
                    path.write_bytes(resp.content)
            except Exception:
                continue


def scrape_single(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 ... Chrome/125 Safari/537"
        )
        context.add_cookies(load_redfin_cookies())
        page = context.new_page()
        try:
            page.goto(url, wait_until="load", timeout=60000)
            time.sleep(WAIT_SEC)
            if "/login" in page.url:
                print(f"🔒 Login page encountered at {url}")
                return None, []
            html = page.content()
        except TimeoutError:
            print(f"⚠️ Timeout at {url}")
            return None, []
        finally:
            browser.close()

    mls = extract_between(html, "TREB #", "<")
    mls_tail = mls[-3:] if len(mls) >= 3 else mls
    image_filenames = find_genmid_values(html)
    image_urls = [f"{BASE_IMAGE_URL}{mls_tail}/genMid.{fn}" for fn in image_filenames]
    download_images(image_urls, mls)

    history = parse_sale_history(html, url)
    sold_evt = next((h for h in history if "sold" in h["eventType"].lower()), None)
    earliest_evt = min(
        (h for h in history if h["eventDate"]),
        key=lambda h: parse_date(h["eventDate"]) or datetime.max,
        default=None,
    )

    try:
        sold_price = money_to_int(sold_evt["price"]) if sold_evt else None
        first_price = money_to_int(earliest_evt["price"]) if earliest_evt else None
        days_on_market = (
            (
                parse_date(sold_evt["eventDate"])
                - parse_date(earliest_evt["eventDate"])
            ).days
            if sold_evt and earliest_evt
            else None
        )

        return {
            "url": url,
            "MLS": mls,
            "Sold Price": sold_price,
            "Number Beds": float(
                extract_between(html, '"latestListingInfo":{"beds":', ",")
            ),
            "Number Baths": float(extract_between(html, '"baths":', ",")),
            "Sold Date": (
                sold_evt["eventDate"]
                if sold_evt
                else extract_between(html, '"lastSaleDate":"')
            ),
            "Address": extract_between(html, 'assembledAddress":"', "\\"),
            "Postal Code": extract_between(html, '"postalCode":"', '"'),
            "Property Type": extract_between(html, 'Property Type","content":"', "\\"),
            "Square Foot": extract_between(html, 'Lot Size","content":"', " "),
            "Parking": extract_between(html, 'Parking","content":"', " "),
            "Association Fee": extract_between(html, "Association Fee: <span>$", "<"),
            "latitude": float(extract_between(html, 'latitude":', ",")),
            "longitude": float(extract_between(html, 'longitude":', "}")),
            "First Listed Date": earliest_evt["eventDate"] if earliest_evt else None,
            "Days On Market": days_on_market,
            "Sold Price Difference": (
                sold_price - first_price if sold_price and first_price else None
            ),
        }, history
    except Exception as e:
        print(f"❌ Error for {mls}: {e}")
        return None, history


# ---------------- main ----------------------------------------------------
def main():
    start_time = time.time()

    # --- load URLs --------------------------------------------------------
    with URLS_FILE.open("r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    if test:
        urls = urls[:MAX_URLS]
    total = len(urls)

    # --- scrape in parallel ----------------------------------------------
    summary, history = [], []
    completed = 0
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(scrape_single, url): url for url in urls}

        for fut in as_completed(futures):
            url = futures[fut]
            result, hist = fut.result()
            completed += 1

            # ❌ Skip entries with missing or invalid sold_price
            if result and isinstance(result.get("Sold Price"), int):
                summary.append(result)
                msg = f"[{completed}/{total}] ✅ scraped MLS {result['MLS']} ({url})"
            else:
                msg = f"[{completed}/{total}] ⚠️ skipped (missing sold price) for {url}"

            print(msg, flush=True)
            history.extend(hist)

    # --- save outputs ----------------------------------------------------
    pd.DataFrame(summary).to_csv(OUT_CSV, index=False)
    pd.DataFrame(history).to_csv(OUT_HISTORY, index=False)
    print(f"\n✅ Saved {len(summary)} listings to {OUT_CSV}")
    print(f"✅ Saved {len(history)} history rows to {OUT_HISTORY}")
    print(f"⏱️ Took {time.time() - start_time:.1f} seconds")


if __name__ == "__main__":
    main()
