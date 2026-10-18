"""Pure HTML -> record parsing for Redfin listing pages. No network, no Azure, no environment.

This is the "transform" half of the pipeline: given the raw HTML saved in bronze/ it returns the
silver record plus the sale-history events. Because it is pure, fixing a parser bug and re-running
transform_silver.py rebuilds every listing from the raw HTML without contacting redfin.ca again.
"""
import html as htmllib
import json
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

BASE_IMAGE_URL = "https://ssl.cdn-redfin.com/photo/248/mbphotov3/"

_money_re = re.compile(r"[^\d]")
_STREETVIEW_RE = re.compile(r'<(?:img|link)[^>]*?(?:src|href)="(https://maps\.googleapis\.com/maps/api/streetview\?[^"]+)"')
_OG_URL_RE = re.compile(r'property="og:url"\s+content="([^"]+)"')


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
    except (TypeError, ValueError):
        return None


def money_to_int(s):
    s_clean = _money_re.sub("", s)
    return int(s_clean) if s_clean else None


def safe_float(val):
    try:
        if isinstance(val, (float, int)):
            return float(val)
        return float(val) if val and val != "N/A" else None
    except (TypeError, ValueError):
        return None


def property_id(url):
    """Redfin's numeric property id, the tail of the URL (.../home/149231176). The same home can have
    several URL spellings (neighbourhood slug vs 'ottawa'), so listings are matched on this, not the string."""
    m = re.search(r"/home/(\d+)", url or "")
    return m.group(1) if m else None


def in_scope(url):
    """True for Ontario pages on redfin.ca. Anything else (e.g. redfin.com US listings that slipped into the
    search results) is out of scope for this project."""
    return bool(re.match(r"https://www\.redfin\.ca/on/", url or "", re.I))


def listing_key(property_id, mls):
    """Name used for a listing's files (bronze page, images): the MLS number, or P<property_id> when the page
    has none. Deterministic, so re-scraping the same page never creates a second key."""
    return mls or f"P{property_id}"


def extract_mls(html):
    """The MLS number shown on the page ('TREB #X123'), or None."""
    mls = extract_between(html, "TREB #", "<")
    return None if not mls or mls == "N/A" else mls


def extract_url(html):
    """The listing's canonical URL (og:url), or None. Lets bronze be rebuilt without a separate URL index."""
    m = _OG_URL_RE.search(html)
    return htmllib.unescape(m.group(1)) if m else None


def parse_sale_history(html, url):
    start_marker = r'\"events\":[{'

    start_idx = html.find(start_marker)
    if start_idx == -1:
        start_marker = r'"events":[{'
        start_idx = html.find(start_marker)

    if start_idx == -1:
        return []

    array_start_idx = start_idx + len(start_marker) - 2
    candidate = html[array_start_idx : array_start_idx + 25000]
    unescaped = candidate.replace(r'\"', '"').replace(r'\\', '\\')

    decoder = json.JSONDecoder()
    try:
        events_list, _ = decoder.raw_decode(unescaped)
    except json.JSONDecodeError:
        return []

    rows = []
    for e in events_list:
        ts = e.get("eventDate")
        date_str = ""
        if isinstance(ts, int):
            # UTC, so a GitHub runner and a laptop agree on the date (Redfin stamps events at local midnight)
            date_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%b %d, %Y")

        rows.append({
            "url": url,
            "eventDate": date_str,
            "eventType": e.get("eventDescription", ""),
            "price": str(e.get("price", "")),
            "MLS": e.get("sourceId", ""),
            "source": e.get("source"),
            "status": e.get("mlsDescription"),
        })
    return rows


def history_rows(history, pid):
    """Sale-history events of one property as rows for the sale_history table.

    The history belongs to the PROPERTY: one page lists the events of every listing the home has had, each
    event carrying the MLS of its own listing (`mls`). Sold (Public Records) and CREA price changes have no
    board MLS number, so `mls` is None for them; `source` / `source_id` keep what Redfin said."""
    rows = []
    for seq, h in enumerate(history):
        d = parse_date(h["eventDate"])
        source_id = h["MLS"] or None
        rows.append({
            "property_id": pid,
            "event_seq": seq,
            "event_date": d.date() if d else None,
            "event_type": h["eventType"] or None,
            "price": money_to_int(h["price"]) if h["price"] not in ("", "None") else None,
            "source": h["source"],
            "source_id": source_id,
            "mls": source_id if h["source"] and h["source"] != "CREA" else None,
            "status": h["status"],
        })
    return rows


def find_genmid_values(html):
    return list(dict.fromkeys(re.findall(r"genMid\.([A-Za-z0-9_]+\.jpg)", html)))


def find_streetview_url(html):
    """The signed Google Street View URL Redfin embeds when a listing has no MLS photos."""
    m = _STREETVIEW_RE.search(html)
    return htmllib.unescape(m.group(1)) if m else None


def pick_image(mls, html, streetview=True):
    """Returns (image_url, kind) with kind 'listing' | 'streetview', or (None, None).

    Listing photos first. A re-listed property's page shows photos of its EARLIER listing (a different
    MLS number), so the CDN folder is taken from the photo's own MLS number, and photos that belong to
    this MLS are preferred. Street View is only a fallback, and only when `streetview` is True."""
    names = find_genmid_values(html)
    if names:
        names.sort(key=lambda n: not n.startswith(mls or ""))  # stable: keeps page order otherwise
        photo_mls = names[0].split("_")[0]
        return f"{BASE_IMAGE_URL}{photo_mls[-3:]}/genMid.{names[0]}", "listing"
    if streetview:
        sv = find_streetview_url(html)
        if sv:
            return sv, "streetview"
    return None, None


def extract_price_from_json(html):
    soup = BeautifulSoup(html, "lxml")
    for script in soup.find_all("script"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
            stack = [data]
            while stack:
                curr = stack.pop()
                if isinstance(curr, dict):
                    if "price" in curr and "priceCurrency" in curr:
                        return int(curr["price"])
                    if "offers" in curr:
                        stack.append(curr["offers"])
                    for v in curr.values():
                        if isinstance(v, (dict, list)):
                            stack.append(v)
                elif isinstance(curr, list):
                    for item in curr:
                        if isinstance(item, (dict, list)):
                            stack.append(item)
        except Exception:
            continue
    return None


def parse_listing(html, url, mls, streetview=True):
    """Turns one listing page into (record, events).

    A listing is identified by (property_id, MLS): property_id comes from the URL and is always set, MLS is
    the real MLS number or None when the page has none (never a made-up value).

    record is the silver row. photo_blob / photo_type stay None here: they are only set once the image
    is really in Azure. The chosen image is returned in the internal _img_url / _img_kind keys."""
    history = parse_sale_history(html, url)
    history.sort(key=lambda h: parse_date(h["eventDate"]) or datetime.min)

    sold_evts = [h for h in history if "sold" in h["eventType"].lower()]
    sold_evt = sold_evts[-1] if sold_evts else None

    listed_evts = [h for h in history if "listed" in h["eventType"].lower() and "delisted" not in h["eventType"].lower()]
    first_listed_evt = listed_evts[0] if listed_evts else None

    sold_price = money_to_int(sold_evt["price"]) if sold_evt else None
    if not sold_price:
        json_price = extract_price_from_json(html)
        if json_price:
            sold_price = json_price

    first_list_price = money_to_int(first_listed_evt["price"]) if first_listed_evt else None

    beds = safe_float(extract_between(html, '"latestListingInfo":{"beds":', ","))
    baths = safe_float(extract_between(html, '"baths":', ","))
    lat = safe_float(extract_between(html, 'latitude":', ","))
    lon = safe_float(extract_between(html, 'longitude":', "}"))

    days_on_market = None
    sold_price_diff = None

    if sold_evt and first_listed_evt:
        d_sold = parse_date(sold_evt["eventDate"])
        d_listed = parse_date(first_listed_evt["eventDate"])
        if d_sold and d_listed:
            days_on_market = (d_sold - d_listed).days

    if sold_price and first_list_price:
        sold_price_diff = sold_price - first_list_price

    pid = property_id(url)
    img_url, img_kind = pick_image(mls, html, streetview)

    prop_type_match = re.search(r'"propertyType"\s*:\s*"([^"]+)"', html)
    if prop_type_match:
        property_type = prop_type_match.group(1).title()
    else:
        property_type = extract_between(html, 'Property Type","content":"', "\\")
        if property_type == "N/A":
            property_type = None

    record = {
        "property_id": pid,
        "url": url,
        "MLS": mls,
        "Sold Price": sold_price,
        "Number Beds": beds,
        "Number Baths": baths,
        "Sold Date": sold_evt["eventDate"] if sold_evt else extract_between(html, '"lastSaleDate":"'),
        "Address": extract_between(html, 'assembledAddress":"', "\\"),
        "Postal Code": extract_between(html, '"postalCode":"', '"'),
        "Property Type": property_type,
        "latitude": lat,
        "longitude": lon,
        "First Listed Date": first_listed_evt["eventDate"] if first_listed_evt else None,
        "Days On Market": days_on_market,
        "Sold Price Difference": sold_price_diff,
        "photo_blob": None,
        "photo_type": None,
        "_key": listing_key(pid, mls),   # internal: file name for the image
        "_img_url": img_url,
        "_img_kind": img_kind,
    }
    return record, history
