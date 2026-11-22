"""Which Ottawa neighbourhood is a sale in, and how does each neighbourhood look: the sold map's second way to pick an area.

Read-time helper for app/main.py (POST /filtered-points and /active-points with an `area`, POST /neighbourhood-summary and
/active-neighbourhood-summary). Pure: no Flask, no
Azure, no clock. Nothing is stored in silver: the app assigns every sale once when it loads the data and keeps the
result in memory, so a new boundary file never needs a re-scrape or a rebuild of silver.

Boundaries: the Ottawa Neighbourhood Study (ONS) Gen 3 polygons from the City of Ottawa's open data, simplified and
reduced to `id` and `name` (data/neighbourhoods/README.md has the source, licence and attribution; DECISIONS D17 has
the comparison with the alternatives). A sale is placed by its latitude/longitude with a point-in-polygon test. Redfin's
own `neighbourhood` text is deliberately not used: its spelling varies and it does not match any official area.

Assignment rules:
  - a point inside a polygon gets that polygon's id;
  - a point exactly on a border (or a vertex) counts as inside, and when that border is shared the LOWEST id wins,
    so the answer never depends on the order of the file;
  - a point in no polygon (outside Ottawa, in the Ottawa River, ...) or with a missing coordinate gets
    UNASSIGNED_ID. Those sales are never dropped: aggregate() returns them as their own row.
"""
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import shapely
from shapely.geometry import shape

UNASSIGNED_ID = 0
UNASSIGNED_NAME = "Outside Ottawa / unassigned"

# A neighbourhood with fewer sales than this is flagged `low_sample` and the map greys it out.
# Why 10: this data has a coefficient of variation of about 0.5 for the sold price (sd 339k on a mean of 697k), which puts the
# standard error of a median price near 19% at 10 sales and 11% at 30; at fewer than 10 a single sale moves an average by
# 10% or more. 20 (the sell-time curve's floor, which needs a whole distribution) would grey out far more areas as soon as
# the price, beds or type filters are used. Rows below it are still returned, with their n.
MIN_SAMPLE = 10

# The active map's floor. There are few listings for sale at any moment (326 in the 2026-11-21 snapshot, about 3 per
# neighbourhood, and only 5 of the 116 neighbourhoods had 10 or more), so MIN_SAMPLE would grey out nearly every area.
# 5 keeps the 27 busiest areas; below that the median asking price of a handful of homes says little. The listing COUNT
# is a plain fact, not an estimate, so the map can always colour by it; only the median and the averages use this floor.
ACTIVE_MIN_SAMPLE = 5


@dataclass(frozen=True)
class Neighbourhoods:
    """The loaded boundaries: parallel arrays sorted by id, plus the spatial index over the polygons."""
    ids: np.ndarray        # int64, ascending; the lowest id wins on a shared border
    names: tuple           # display names, same order as ids
    geoms: np.ndarray      # shapely polygons, same order
    tree: shapely.STRtree  # spatial index over `geoms`

    def name_of(self, nid):
        """Display name of a neighbourhood id (UNASSIGNED_NAME for UNASSIGNED_ID), or None for an unknown id."""
        if nid == UNASSIGNED_ID:
            return UNASSIGNED_NAME
        hit = np.flatnonzero(self.ids == nid)
        return self.names[hit[0]] if len(hit) else None

    def __contains__(self, nid):
        return nid == UNASSIGNED_ID or bool((self.ids == nid).any())


@lru_cache(maxsize=4)
def _load(path):
    with open(path, encoding="utf-8") as handle:
        features = json.load(handle)["features"]
    rows = sorted((int(f["properties"]["id"]), str(f["properties"]["name"]), shape(f["geometry"])) for f in features)
    ids = [r[0] for r in rows]
    if len(set(ids)) != len(ids) or UNASSIGNED_ID in ids:
        raise ValueError(f"{path}: neighbourhood ids must be unique and not {UNASSIGNED_ID}")
    geoms = np.empty(len(rows), dtype=object)
    geoms[:] = [r[2] for r in rows]
    return Neighbourhoods(np.array(ids, dtype=np.int64), tuple(r[1] for r in rows), geoms, shapely.STRtree(geoms))


def load_neighbourhoods(path=None):
    """The boundaries from a GeoJSON file (features with properties `id` and `name`), parsed once per path."""
    if path is None:
        from redfin import config
        path = config.NEIGHBOURHOODS_FILE
    return _load(str(Path(path)))


def assign(latitude, longitude, neighbourhoods):
    """Neighbourhood id for each (latitude, longitude): an int64 array, UNASSIGNED_ID where there is none.

    Vectorized: one spatial-index query for all points, so assigning the whole sold frame takes milliseconds. Call
    it once when the data loads, not per request."""
    lat = np.asarray(latitude, dtype=float)
    lon = np.asarray(longitude, dtype=float)
    out = np.full(lat.shape, UNASSIGNED_ID, dtype=np.int64)
    usable = np.isfinite(lat) & np.isfinite(lon)
    if not usable.any():
        return out
    positions = np.flatnonzero(usable)
    points = shapely.points(lon[positions], lat[positions])
    point_idx, geom_idx = neighbourhoods.tree.query(points, predicate="intersects")
    if len(point_idx):
        # ids are ascending, so the smallest geometry index is the lowest id: keep one hit per point
        order = np.lexsort((geom_idx, point_idx))
        point_idx, geom_idx = point_idx[order], geom_idx[order]
        first = np.concatenate(([True], point_idx[1:] != point_idx[:-1]))
        out[positions[point_idx[first]]] = neighbourhoods.ids[geom_idx[first]]
    return out


def _round(value, digits):
    return None if value is None or value != value else round(float(value), digits)


def _rows(frame, neighbourhoods, min_sample, stats):
    """One row per neighbourhood (including those with no rows) plus the unassigned bucket, in id order with unassigned
    last. `stats(g)` gets the neighbourhood's rows (never an empty frame) and returns that row's statistics."""
    groups = {nid: g for nid, g in frame.groupby("neighbourhood_id")} if len(frame) else {}
    rows = []
    for nid, name in [(int(i), n) for i, n in zip(neighbourhoods.ids, neighbourhoods.names)] + [(UNASSIGNED_ID, UNASSIGNED_NAME)]:
        g = groups.get(nid)
        n = 0 if g is None else int(len(g))
        row = {"id": nid, "name": name, "n": n, "low_sample": n < min_sample}
        row.update(stats(g) if n else {})
        rows.append(row)
    return rows


def aggregate(sales, neighbourhoods, min_sample=MIN_SAMPLE):
    """One row per neighbourhood (including those with no sales) plus the unassigned bucket, from a frame of sales.

    `sales` needs `neighbourhood_id` (from assign) and the numeric columns `price`, `dom`, `diff_pct_last`,
    `diff_pct_original`; the caller has already applied the filters. Every row carries `n`, and `low_sample` is True when
    n < min_sample, so the page can grey it out. Statistics:

        median_price          the median: prices are right-skewed, a few big sales would drag a mean
        avg_dom, median_dom   both: days on market has a long tail, the average is what the page shows, the median is
                              the typical sale
        avg_diff_pct_last / _original
                              mean % over (+) / under (-) the final / first asking price (already limited to
                              plausible values by the caller)

    A statistic is None when no sale in the row has the value. Rows are ordered by id, unassigned last."""
    keys = ("median_price", "avg_dom", "median_dom", "avg_diff_pct_last", "avg_diff_pct_original")

    def stats(g):
        return {
            "median_price": _round(g["price"].median(), 0),
            "avg_dom": _round(g["dom"].mean(), 1),
            "median_dom": _round(g["dom"].median(), 1),
            "avg_diff_pct_last": _round(g["diff_pct_last"].mean(), 2),
            "avg_diff_pct_original": _round(g["diff_pct_original"].mean(), 2),
        }

    return [{**dict.fromkeys(keys), **row} for row in _rows(sales, neighbourhoods, min_sample, stats)]


def aggregate_active(listings, neighbourhoods, min_sample=ACTIVE_MIN_SAMPLE):
    """The active map's twin of aggregate(): one row per neighbourhood plus the unassigned bucket.

    `listings` needs `neighbourhood_id` and the numeric columns `list_price` and `days_on_market` plus
    `last_change_direction` ("down" / "up" / empty); the caller has already applied the filters. Statistics:

        n, low_sample         listings for sale; low_sample when n < min_sample (ACTIVE_MIN_SAMPLE)
        median_price          median asking price
        avg_dom, median_dom   days on market so far
        with_cut, cut_pct     listings whose last asking-price change was a cut, and their share of n in %: the same
                              definition as the page's "With a Price Cut" card

    A statistic is None when no listing in the row has the value. Rows are ordered by id, unassigned last."""
    keys = ("median_price", "avg_dom", "median_dom", "with_cut", "cut_pct")

    def stats(g):
        cuts = int((g["last_change_direction"] == "down").sum())
        return {
            "median_price": _round(g["list_price"].median(), 0),
            "avg_dom": _round(g["days_on_market"].mean(), 1),
            "median_dom": _round(g["days_on_market"].median(), 1),
            "with_cut": cuts,
            "cut_pct": _round(100 * cuts / len(g), 1),
        }

    rows = [{**dict.fromkeys(keys), **row} for row in _rows(listings, neighbourhoods, min_sample, stats)]
    for row in rows:
        if row["n"] == 0:
            row["with_cut"] = 0   # no listings: zero cuts, but no share
    return rows


def with_neighbourhood(sales, neighbourhoods):
    """A copy of a frame of sales (columns `latitude`, `longitude`) with a `neighbourhood_id` column."""
    out = sales.copy()
    out["neighbourhood_id"] = assign(out["latitude"], out["longitude"], neighbourhoods)
    return out
