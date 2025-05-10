#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime
import json, time, re
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError
import requests
import time

start_time = time.time()


# ---------------- config --------------------------------------------------
test = True
MAX_URLS = 20
URLS_FILE = Path("app/Redfin/Output/property_urls.txt")
OUT_CSV = Path("app/Redfin/Output/redfin_data.csv")
OUT_HISTORY = Path("app/Redfin/Output/redfin_sale_history.csv")
COOKIE_FILE = Path("app/Redfin/chrome_cookies.json")
WAIT_SEC = 0.5
IMAGES_DIR = Path("app/Redfin/Output/images")
BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"


# ---------------- helpers -------------------------------------------------
def load_redfin_cookies() -> list[dict]:
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


def extract_between(text: str, start: str, stop: str | None = "\\"):
    markers = [start] if '"' not in start else [start, start.replace('"', r"\"")]
    for m in markers:
        idx = text.find(m)
        if idx != -1:
            i = idx + len(m)
            j = text.find(stop, i) if stop else -1
            return text[i:j] if j != -1 else text[i:]
    return "N/A"


DATE_FMT = "%b %d, %Y"  # e.g. "Mar 5, 2025"


def parse_date(s: str):
    try:
        return datetime.strptime(s, DATE_FMT)
    except Exception:
        return None


money_re = re.compile(r"[^\d]")


def money_to_int(s: str):
    s_clean = money_re.sub("", s)
    return int(s_clean) if s_clean else None


def parse_sale_history(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for div in soup.select("div.PropertyHistoryEventRow"):
        date_text = div.select_one("div.col-4 p")
        date_text = date_text.get_text(strip=True) if date_text else ""
        desc = div.select_one(".description-col div")
        desc_text = desc.get_text(strip=True) if desc else ""
        price = div.select_one(".price-col")
        price_text = price.get_text(strip=True).replace("\xa0", " ") if price else ""
        mls_raw = div.select_one(".description-col p.subtext")
        mls_raw = mls_raw.get_text(strip=True) if mls_raw else ""
        mls_id = re.sub(r"^\s*TREB\s+#", "", mls_raw) if mls_raw else ""
        rows.append(
            {
                "url": page_url,
                "eventDate": date_text,
                "eventType": desc_text,
                "price": price_text,
                "MLS": mls_id,
            }
        )
    return rows


def find_genmid_values(html: str) -> list[str]:
    # grab filenames after genMid. ending in .jpg
    matches = re.findall(r"genMid\.([A-Za-z0-9_]+\.jpg)", html)
    return list(dict.fromkeys(matches))


def download_images(image_urls: list[str], mls: str) -> None:
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    for idx, url in enumerate(image_urls, start=1):
        fname = f"{mls}_{idx}.jpg"
        path = IMAGES_DIR / fname
        if not path.exists():
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            path.write_bytes(resp.content)


# ---------------- main ----------------------------------------------------
def main():
    summary_rows, history_rows = [], []

    with URLS_FILE.open(encoding="utf-8") as fh:
        scraped = 0
        for line in fh:
            url = line.strip()
            if not url:
                continue
            scraped += 1
            print(f"[{scraped}] {url}")

            # ---- fetch page ------------------------------------------------
            with sync_playwright() as p:
                b = p.chromium.launch(headless=True)
                ctx = b.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/125.0.0.0 Safari/537.36"
                    )
                )
                ctx.add_cookies(load_redfin_cookies())
                page = ctx.new_page()
                try:
                    page.goto(url, wait_until="load", timeout=60_000)
                except TimeoutError:
                    print("⚠️  timed out after 60 s")
                if "/login" in page.url:
                    print("❌  redirected to login, skipping")
                    b.close()
                    continue
                time.sleep(WAIT_SEC)
                html = page.content()
                b.close()
            # -----------------------------------------------------------------

            # save images for this page --------------------------------------
            mls_code = extract_between(html, "TREB #", "<")
            mls_digits = mls_code[-3:] if len(mls_code) >= 3 else mls_code
            genmid_list = find_genmid_values(html)
            # build full URLs
            image_urls = [
                f"{BASE_IMAGE_URL}{mls_digits}/genMid.{fn}" for fn in genmid_list
            ]
            download_images(image_urls, mls_code)
            print(f"💾  Saved {len(image_urls)} images for MLS {mls_code}")

            # ---- sale‑history ----------------------------------------------
            hist = parse_sale_history(html, url)
            history_rows.extend(hist)

            # ---- compute earliest/price diff -------------------------------
            earliest_evt = min(
                (h for h in hist if h["eventDate"]),
                key=lambda h: parse_date(h["eventDate"]) or datetime.max,
                default=None,
            )
            sold_evt = next((h for h in hist if "sold" in h["eventType"].lower()), None)

            first_dt = parse_date(earliest_evt["eventDate"]) if earliest_evt else None
            sold_dt = parse_date(sold_evt["eventDate"]) if sold_evt else None
            days_on_mk = (sold_dt - first_dt).days if first_dt and sold_dt else None

            first_price = money_to_int(earliest_evt["price"]) if earliest_evt else None
            sold_price = money_to_int(sold_evt["price"]) if sold_evt else None
            price_diff = (
                sold_price - first_price
                if sold_price is not None and first_price is not None
                else None
            )

            # ---- summary row -----------------------------------------------
            try:
                mls_code = extract_between(html, "TREB #", "<")

                row = {
                    "url": url,
                    "MLS": mls_code,
                    "Sold Price": float(sold_price) if sold_price is not None else None,
                    "Number Beds": float(
                        extract_between(html, '"latestListingInfo":{"beds":', ",")
                    ),
                    "Number Baths": float(extract_between(html, '"baths":', ",")),
                    "Sold Date": (
                        sold_evt["eventDate"]
                        if sold_evt
                        else extract_between(html, '"lastSaleDate":"', "\\")
                    ),
                    "Address": extract_between(html, 'assembledAddress":"', "\\"),
                    "Postal Code": extract_between(html, '"postalCode":"', '"'),
                    "Property Type": extract_between(
                        html, 'Property Type","content":"', "\\"
                    ),
                    "Square Foot": extract_between(html, 'Lot Size","content":"', " "),
                    "Parking": str(extract_between(html, 'Parking","content":"', " ")),
                    "Association Fee": str(
                        extract_between(html, "Association Fee: <span>$", "<")
                    ),
                    "latitude": float(extract_between(html, 'latitude":', ",")),
                    "longitude": float(extract_between(html, 'longitude":', "}")),
                    "First Listed Date": (
                        earliest_evt["eventDate"] if earliest_evt else None
                    ),
                    "Days On Market": (
                        float(days_on_mk) if days_on_mk is not None else None
                    ),
                    "Sold Price Difference": (
                        float(price_diff) if price_diff is not None else None
                    ),
                }

                summary_rows.append(row)

            except Exception as e:
                print(
                    f"❌ Skipped MLS {mls_code if 'mls_code' in locals() else 'N/A'} due to error in field: {e}"
                )

            if test and scraped >= MAX_URLS:
                break

    # ---------------- write CSVs ------------------------------------------
    pd.DataFrame(summary_rows).to_csv(OUT_CSV, index=False)
    pd.DataFrame(history_rows).to_csv(OUT_HISTORY, index=False)
    print(f"\n✅  Saved {len(summary_rows)} rows to {OUT_CSV}")
    print(f"✅  Saved {len(history_rows)} sale‑history rows to {OUT_HISTORY}")

    end_time = time.time()

    print(f"⏱️  Script runtime: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
