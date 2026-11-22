"""The active map's listing frame and filters (asking price, days on market, beds, property type, price changes).

Read-time helpers for app/main.py: the circle and the neighbourhood paths of POST /active-points and the
per-neighbourhood summary all go through these functions, so they cannot drift apart. Pure: no Flask, no Azure, no clock.

`prepare_active_frame` turns the raw silver snapshot into the frame the map filters. `apply_active_filters` takes the
dict the page sends; every key is optional:

    min_price, max_price   dollars on the asking price (`list_price`), inclusive
    min_dom, max_dom       days on market, inclusive; a listing with no value drops out once either is set
    beds                   list of counts; the "5+" button sends 5 and means 5 or more (`beds_mask`)
    ptypes                 grouped names ("Condo", "Townhome", "Single-family", "Multi-family"); silver holds the raw
                           value, so each listing is grouped first (analytics.property_types)
    changes                ["down"], ["up"] or both: the direction of the last asking-price change. The page applies
                           this one itself for the map and list; it is sent for the neighbourhood table so the table
                           and the map count the same listings
"""
import pandas as pd

from redfin.analytics.property_types import property_group

NUMERIC_COLUMNS = ("latitude", "longitude", "list_price", "beds", "baths", "days_on_market", "cumulative_dom",
                   "original_price", "price_changes", "last_change_amount", "last_change_pct", "price_vs_original")


def beds_mask(beds, selected):
    """True where `beds` is one of the selected counts; the "5+" button sends 5 and means 5 or more."""
    mask = beds.isin(selected)
    if 5 in selected:
        mask = mask | (beds >= 5)
    return mask


def prepare_active_frame(raw):
    """(listings that can be mapped, number of active listings with no coordinates).

    The census can return homes that are no longer for sale (e.g. mlsStatus "Off Market"): only "Active" ones stay. A
    row with no status (a snapshot written before the column existed) stays too. Numeric columns are converted, and
    listings without latitude and longitude (on no map) are dropped and counted, never lost silently."""
    df = raw[raw["status"].isna() | (raw["status"] == "Active")].copy()
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    placed = df.dropna(subset=["latitude", "longitude"])
    return placed, int(len(df) - len(placed))


def apply_active_filters(df, filters):
    """Rows of `df` that pass `filters`. Returns a new frame; `df` is never changed."""
    filters = filters or {}
    mask = pd.Series(True, index=df.index)
    if "min_price" in filters:
        mask &= df["list_price"] >= int(filters["min_price"])
    if "max_price" in filters:
        mask &= df["list_price"] <= int(filters["max_price"])
    if "min_dom" in filters:   # listings with no days-on-market value drop out once either bound is set
        mask &= df["days_on_market"] >= int(filters["min_dom"])
    if "max_dom" in filters:
        mask &= df["days_on_market"] <= int(filters["max_dom"])
    if filters.get("beds"):
        mask &= beds_mask(df["beds"], filters["beds"])
    if filters.get("ptypes"):
        mask &= df["property_type_name"].map(property_group).isin(filters["ptypes"])
    if filters.get("changes"):
        mask &= df["last_change_direction"].isin(filters["changes"])
    return df[mask].copy()
