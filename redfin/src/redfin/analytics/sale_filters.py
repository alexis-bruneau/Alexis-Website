"""The sold map's filters (sold date, price, beds, property type), applied to a frame of sales.

Read-time helper for app/main.py: the circle and the neighbourhood paths of POST /filtered-points and the
per-neighbourhood summary all call this one function, so they cannot drift apart. Pure: no Flask, no Azure, no clock.

The frame needs the columns `price`, `sold_date` (datetime64, NaT when unknown), `beds` and `ptype` (the raw
Redfin property type). `filters` is the dict the page sends; every key is optional:

    sold_start, sold_end   "YYYY-MM-DD", inclusive. A sale with no sold date drops out once either is set
    min_price, max_price   dollars, inclusive. A sale with no price drops out once either is set
    beds                   list of counts; a sale matches when its bedrooms are in the list (the "5+" button
                           sends 5, so on this map it matches exactly 5, unlike the active map's beds_mask)
    ptypes                 grouped names ("Condo", "Townhome", "Single-family", "Multi-family"); silver holds the
                           raw value, so each sale is grouped first (analytics.property_types)
"""
import pandas as pd

from redfin.analytics.property_types import property_group


def apply_sale_filters(df, filters):
    """Rows of `df` that pass `filters`. Returns a new frame; `df` is never changed."""
    filters = filters or {}
    mask = pd.Series(True, index=df.index)
    if "min_price" in filters:
        mask &= df["price"] >= int(filters["min_price"])
    if "max_price" in filters:
        mask &= df["price"] <= int(filters["max_price"])
    if "sold_start" in filters:
        mask &= df["sold_date"] >= pd.to_datetime(filters["sold_start"])
    if "sold_end" in filters:
        mask &= df["sold_date"] <= pd.to_datetime(filters["sold_end"])
    if filters.get("beds"):
        mask &= df["beds"].isin(filters["beds"])
    if filters.get("ptypes"):
        mask &= df["ptype"].map(property_group).isin(filters["ptypes"])
    return df[mask].copy()
