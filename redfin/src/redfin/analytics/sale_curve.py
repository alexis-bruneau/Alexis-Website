"""How long do homes like these take to sell: the cumulative share of sales that closed within N days on market.

Read-time helper for the active map (app/main.py, POST /active-points). Pure: it takes the days-on-market of the
sales the caller already selected (circle, filters, sold window) and never reads the clock, so the same input
always gives the same curve.

The curve is survivorship-biased: it is built from listings that SOLD. Homes that were withdrawn, expired or are
still unsold are not in the data, so it makes homes look quicker to sell than they are. The map labels it as such.
"""
import math
from bisect import bisect_right

DEFAULT_MAX_DAYS = 180   # the curve covers N = 0..180 days on market
MIN_SAMPLE = 20          # fewer sales than this and the curve is noise: no curve is returned


def _clean(days_on_market):
    """Sorted list of the usable values: numbers >= 0. None, NaN and negatives (bad data) are dropped."""
    values = []
    for value in days_on_market:
        if value is None or value != value:  # None or NaN
            continue
        if value >= 0:
            values.append(float(value))
    values.sort()
    return values


def _first_day_reaching(counts, needed):
    """Smallest N whose cumulative count is at least `needed`, or None when the curve never gets there."""
    for day, count in enumerate(counts):
        if count >= needed:
            return day
    return None


def sale_curve(days_on_market, max_days=DEFAULT_MAX_DAYS, min_sample=MIN_SAMPLE):
    """Cumulative sale curve, or None when there are fewer than `min_sample` usable sales.

    Returns {"sample": n, "max_days": max_days, "share": [...], "p50_days": ..., "p80_days": ...} where
    share[N] is the fraction of the n sales with days_on_market <= N, for N = 0..max_days (4 decimals), and
    p50_days / p80_days are the first day by which half / 80% of them had sold (None when the curve has not
    got there by max_days: a long tail of slow sales).
    """
    values = _clean(days_on_market)
    n = len(values)
    if n < min_sample:
        return None
    counts = [bisect_right(values, day) for day in range(max_days + 1)]
    return {
        "sample": n,
        "max_days": max_days,
        "share": [round(c / n, 4) for c in counts],
        # ceil(q * n) sales must be in; the 1e-9 keeps float noise (0.8 * 35 = 28.000000000000004) from asking for one more
        "p50_days": _first_day_reaching(counts, math.ceil(0.5 * n - 1e-9)),
        "p80_days": _first_day_reaching(counts, math.ceil(0.8 * n - 1e-9)),
    }
