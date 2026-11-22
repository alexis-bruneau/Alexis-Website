"""The active map's side of the neighbourhoods: analytics.active_filters and neighbourhoods.aggregate_active.

Uses the same tiny fixture as test_neighbourhoods.py: Alpha (3001), Bravo (3002) and Charlie (3003), three 1x1-degree
squares. Listings are built here, as the raw silver snapshot would hold them (string-ish numbers, a status column)."""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from redfin.analytics.active_filters import apply_active_filters, beds_mask, prepare_active_frame
from redfin.analytics.neighbourhoods import (ACTIVE_MIN_SAMPLE, MIN_SAMPLE, UNASSIGNED_ID, aggregate_active,
                                             load_neighbourhoods, with_neighbourhood)

FIXTURE = Path(__file__).parent / "fixtures" / "neighbourhoods_tiny.geojson"
ALPHA, BRAVO, CHARLIE = 3001, 3002, 3003


@pytest.fixture(scope="module")
def nb():
    return load_neighbourhoods(FIXTURE)


def listing(mls, lat, lon, price, dom, beds=3, ptype="Single Family Residential", direction=None, status="Active"):
    return {"mls": mls, "status": status, "latitude": lat, "longitude": lon, "list_price": price,
            "days_on_market": dom, "beds": beds, "baths": 2, "property_type_name": ptype,
            "cumulative_dom": dom, "original_price": price, "price_changes": 1 if direction else 0,
            "last_change_amount": None, "last_change_pct": None, "price_vs_original": None,
            "last_change_direction": direction}


@pytest.fixture
def raw():
    return pd.DataFrame([
        listing("A1", 0.5, 0.5, 400_000, 10, beds=2, ptype="Condominium", direction="down"),
        listing("A2", 0.4, 0.4, 600_000, 30, beds=3, direction="up"),
        listing("A3", 0.6, 0.6, 800_000, 50, beds=5, direction="down"),
        listing("B1", 0.5, 1.5, 500_000, 20, beds=4),
        listing("C1", 5.5, 5.5, 900_000, None, beds=3),
        listing("X1", 40.0, 40.0, 300_000, 5),                       # outside every neighbourhood
        listing("N1", None, None, 700_000, 7),                       # no coordinates: on no map
        listing("S1", 0.5, 0.5, 450_000, 3, status="Off Market"),    # no longer for sale
    ])


@pytest.fixture
def frame(raw, nb):
    placed, _ = prepare_active_frame(raw)
    return with_neighbourhood(placed, nb)


def test_prepare_drops_off_market_and_counts_listings_without_coordinates(raw):
    placed, unplaced = prepare_active_frame(raw)
    assert sorted(placed["mls"]) == ["A1", "A2", "A3", "B1", "C1", "X1"]
    assert unplaced == 1                                              # N1; S1 is off market, not "unplaced"


def test_prepare_keeps_a_listing_with_no_status():
    df = pd.DataFrame([listing("M1", 0.5, 0.5, 1, 1, status=None)])
    placed, _ = prepare_active_frame(df)
    assert list(placed["mls"]) == ["M1"]


def test_prepare_does_not_change_the_snapshot(raw):
    before = raw.copy()
    prepare_active_frame(raw)
    pd.testing.assert_frame_equal(raw, before)


def test_beds_mask_five_means_five_or_more():
    beds = pd.Series([1, 4, 5, 6, np.nan])
    assert beds_mask(beds, [5]).tolist() == [False, False, True, True, False]
    assert beds_mask(beds, [4]).tolist() == [False, True, False, False, False]


def mls(df):
    return sorted(df["mls"])


def test_no_filters_returns_everything_as_a_copy(frame):
    out = apply_active_filters(frame, {})
    assert mls(out) == mls(frame)
    out["list_price"] = 0
    assert frame["list_price"].min() > 0                              # the cached frame is never changed


def test_price_filters_are_inclusive(frame):
    assert mls(apply_active_filters(frame, {"min_price": 500_000, "max_price": 800_000})) == ["A2", "A3", "B1"]


def test_days_on_market_filters_drop_listings_without_a_value(frame):
    assert mls(apply_active_filters(frame, {"max_dom": 100})) == ["A1", "A2", "A3", "B1", "X1"]     # C1 has none
    assert mls(apply_active_filters(frame, {"min_dom": 20, "max_dom": 50})) == ["A2", "A3", "B1"]


def test_beds_and_property_type_filters(frame):
    assert mls(apply_active_filters(frame, {"beds": [5]})) == ["A3"]
    assert mls(apply_active_filters(frame, {"beds": [4, 5]})) == ["A3", "B1"]
    assert mls(apply_active_filters(frame, {"ptypes": ["Condo"]})) == ["A1"]


def test_price_change_filter(frame):
    assert mls(apply_active_filters(frame, {"changes": ["down"]})) == ["A1", "A3"]
    assert mls(apply_active_filters(frame, {"changes": ["down", "up"]})) == ["A1", "A2", "A3"]


def test_filters_combine(frame):
    assert mls(apply_active_filters(frame, {"changes": ["down"], "min_price": 500_000})) == ["A3"]


def test_aggregate_rows_cover_every_neighbourhood_and_the_unassigned_bucket(frame, nb):
    rows = aggregate_active(frame, nb)
    assert [r["id"] for r in rows] == [ALPHA, BRAVO, CHARLIE, UNASSIGNED_ID]
    assert sum(r["n"] for r in rows) == len(frame) == 6               # nothing is dropped
    by = {r["id"]: r for r in rows}
    assert by[ALPHA]["n"] == 3 and by[BRAVO]["n"] == 1 and by[CHARLIE]["n"] == 1 and by[UNASSIGNED_ID]["n"] == 1


def test_aggregate_statistics(frame, nb):
    alpha = {r["id"]: r for r in aggregate_active(frame, nb)}[ALPHA]
    assert alpha["median_price"] == 600_000
    assert alpha["avg_dom"] == 30.0 and alpha["median_dom"] == 30.0
    assert alpha["with_cut"] == 2 and alpha["cut_pct"] == 66.7        # A1 and A3 were cut, A2 went up


def test_aggregate_leaves_out_missing_days_on_market(frame, nb):
    charlie = {r["id"]: r for r in aggregate_active(frame, nb)}[CHARLIE]
    assert charlie["n"] == 1 and charlie["median_price"] == 900_000
    assert charlie["avg_dom"] is None and charlie["median_dom"] is None  # unknown, not 0


def test_aggregate_empty_row_has_no_statistics_and_zero_cuts(frame, nb):
    empty = aggregate_active(frame.iloc[0:0], nb)
    assert [r["n"] for r in empty] == [0, 0, 0, 0] and all(r["low_sample"] for r in empty)
    assert empty[0]["median_price"] is None and empty[0]["cut_pct"] is None and empty[0]["with_cut"] == 0


def test_low_sample_flag_uses_the_active_floor(frame, nb):
    assert ACTIVE_MIN_SAMPLE < MIN_SAMPLE
    by = {r["id"]: r for r in aggregate_active(frame, nb)}
    assert by[ALPHA]["low_sample"] and by[BRAVO]["low_sample"]        # 3 and 1 listings, floor is 5
    many = pd.concat([frame[frame["neighbourhood_id"] == ALPHA]] * 2)   # 6 listings
    assert not {r["id"]: r for r in aggregate_active(many, nb)}[ALPHA]["low_sample"]
    assert {r["id"]: r for r in aggregate_active(many, nb, min_sample=7)}[ALPHA]["low_sample"]


def test_aggregate_after_filters_adds_up(frame, nb):
    filtered = apply_active_filters(frame, {"min_price": 450_000})
    rows = aggregate_active(filtered, nb)
    assert sum(r["n"] for r in rows) == len(filtered) == 4
