"""Per-listing sale metrics, computed from a property's event timeline. Pure: no network, no Azure.

Input is the list of event rows of ONE property (parsing.history_rows). One Redfin page carries the events of
every listing the home has had, each tagged with its own MLS number, so every metric is measured on the listing
that sold (matched by MLS) and never on an earlier listing of the same home.

    sold_vs_last_list       sold price - the last Listed / Price Changed price of that MLS on or before the sale
                            (the asking price when it sold; the industry's sale-to-list measure)
    sold_vs_original_list   sold price - the first Listed price of that MLS
    days_on_market          sold date - the first Listed date of that MLS
    cumulative_dom          days on market of this listing plus the earlier listings of the same home that did not
                            sell (relists), counted back to the previous sale. A relist does not reset it.

A metric is None when the events needed for it are missing.
"""
ASKING_EVENTS = ("Listed", "Price Changed")

METRICS = ("sold_vs_last_list", "sold_vs_original_list", "days_on_market", "cumulative_dom")


def _is_sold(event):
    return (event["event_type"] or "").startswith("Sold")


def _chronological(events):
    """Events that have a date, oldest first. event_seq keeps same-day events in page order."""
    return sorted((e for e in events if e["event_date"]), key=lambda e: (e["event_date"], e["event_seq"]))


def _listing_spans(events):
    """{mls: (first Listed date, last event date)} for every listing of the property that has a Listed event."""
    by_mls = {}
    for e in _chronological(events):
        if e["mls"]:
            by_mls.setdefault(e["mls"], []).append(e)
    spans = {}
    for mls, own in by_mls.items():
        listed = [e["event_date"] for e in own if e["event_type"] == "Listed"]
        if listed:
            spans[mls] = (listed[0], own[-1]["event_date"])
    return spans


def _sold_dates(events):
    """{mls: sold date} for every listing of the property that has a Sold event."""
    return {e["mls"]: e["event_date"] for e in _chronological(events)
            if e["mls"] and _is_sold(e)}


def _cumulative_dom(events, mls, first_listed, sold_date):
    """Days this listing was on the market plus the unsold listings that led up to it (since the previous sale)."""
    spans = _listing_spans(events)
    previous_sales = [d for m, d in _sold_dates(events).items() if m != mls and d <= first_listed]
    cutoff = max(previous_sales, default=None)
    total = (sold_date - first_listed).days
    for other, (start, end) in spans.items():
        if other != mls and start <= first_listed and (cutoff is None or start > cutoff):
            total += (min(end, first_listed) - start).days  # never run past the start of the next listing
    return total


def listing_metrics(events, mls, sold_price):
    """The four metrics for the listing `mls` of one property, as a dict (None where they cannot be computed)."""
    result = dict.fromkeys(METRICS)
    if not mls:
        return result

    own = _chronological(e for e in events if e["mls"] == mls)
    sold = [e for e in own if _is_sold(e)]
    if not sold:
        return result
    sold_date = sold[-1]["event_date"]

    listed = [e for e in own if e["event_type"] == "Listed" and e["event_date"] <= sold_date]
    asking = [e for e in own if e["event_type"] in ASKING_EVENTS and e["event_date"] <= sold_date and e["price"]]

    if sold_price and asking:
        result["sold_vs_last_list"] = int(sold_price - asking[-1]["price"])
    if listed:
        first = listed[0]
        result["days_on_market"] = (sold_date - first["event_date"]).days
        result["cumulative_dom"] = _cumulative_dom(events, mls, first["event_date"], sold_date)
        if sold_price and first["price"]:
            result["sold_vs_original_list"] = int(sold_price - first["price"])
    return result
