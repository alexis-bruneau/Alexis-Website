"""analytics.neighbourhoods and analytics.sale_filters on a tiny fixture: three 1x1-degree squares.

Alpha (3001) is lon 0..1 x lat 0..1, Bravo (3002) is lon 1..2 x lat 0..1 and shares the x = 1 edge with Alpha, Charlie
(3003) is far away. The file lists them as Bravo, Charlie, Alpha on purpose: the answer must not depend on file order."""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from redfin.analytics.neighbourhoods import (MIN_SAMPLE, UNASSIGNED_ID, UNASSIGNED_NAME, aggregate, assign,
                                             load_neighbourhoods, with_neighbourhood)
from redfin.analytics.sale_filters import apply_sale_filters

FIXTURE = Path(__file__).parent / "fixtures" / "neighbourhoods_tiny.geojson"
ALPHA, BRAVO, CHARLIE = 3001, 3002, 3003


@pytest.fixture(scope="module")
def nb():
    return load_neighbourhoods(FIXTURE)


def ids(nb, points):
    """assign() for a list of (latitude, longitude)."""
    lat, lon = zip(*points)
    return assign(lat, lon, nb).tolist()


def test_loads_sorted_by_id_whatever_the_file_order(nb):
    assert nb.ids.tolist() == [ALPHA, BRAVO, CHARLIE]
    assert nb.names == ("Alpha", "Bravo", "Charlie")
    assert nb.name_of(BRAVO) == "Bravo"
    assert nb.name_of(UNASSIGNED_ID) == UNASSIGNED_NAME
    assert nb.name_of(1234) is None
    assert BRAVO in nb and UNASSIGNED_ID in nb and 1234 not in nb


def test_point_inside(nb):
    assert ids(nb, [(0.5, 0.5), (0.5, 1.5), (5.5, 5.5)]) == [ALPHA, BRAVO, CHARLIE]


def test_point_outside_is_unassigned(nb):
    assert ids(nb, [(10, 10), (-1, 0.5), (0.5, 3)]) == [UNASSIGNED_ID] * 3


def test_point_on_the_edge_counts_as_inside(nb):
    # (latitude, longitude): the left edge of Alpha, its bottom edge, the far corner of Charlie
    assert ids(nb, [(0.5, 0.0), (0.0, 0.5), (6.0, 6.0)]) == [ALPHA, ALPHA, CHARLIE]


def test_point_on_a_shared_edge_goes_to_the_lowest_id(nb):
    # lon 1.0 is Alpha's right edge and Bravo's left edge; (1, 1) is a corner of both
    assert ids(nb, [(0.5, 1.0), (1.0, 1.0), (0.0, 1.0)]) == [ALPHA, ALPHA, ALPHA]


def test_missing_or_non_finite_coordinates_are_unassigned(nb):
    lat = [0.5, np.nan, 0.5, np.inf, None]
    lon = [0.5, 0.5, np.nan, 0.5, 0.5]
    assert assign(lat, lon, nb).tolist() == [ALPHA, UNASSIGNED_ID, UNASSIGNED_ID, UNASSIGNED_ID, UNASSIGNED_ID]


def test_assign_handles_empty_input(nb):
    assert assign([], [], nb).tolist() == []
    assert assign([np.nan], [np.nan], nb).tolist() == [UNASSIGNED_ID]


def test_assign_is_vectorized_over_many_points(nb):
    rng = np.random.default_rng(0)
    lat, lon = rng.uniform(-1, 7, 20000), rng.uniform(-1, 7, 20000)
    got = assign(lat, lon, nb)
    expected = np.where((lat > 0) & (lat < 1) & (lon > 0) & (lon < 1), ALPHA,
               np.where((lat > 0) & (lat < 1) & (lon > 1) & (lon < 2), BRAVO,
               np.where((lat > 5) & (lat < 6) & (lon > 5) & (lon < 6), CHARLIE, UNASSIGNED_ID)))
    assert (got == expected).all()


# ---- aggregation with filters ----------------------------------------------------------------------------------------

def sales_frame():
    """Nine sales: 4 in Alpha, 2 in Bravo, none in Charlie, 3 outside every polygon (one with no coordinates)."""
    rows = [
        # lat, lon, price, sold_date, beds, ptype, dom, diff_pct_last, diff_pct_original
        (0.5, 0.5, 400000, "2026-01-10", 2, "Condo/Co-Op", 10, 2.0, 4.0),
        (0.2, 0.2, 500000, "2026-02-10", 3, "Townhouse", 20, -1.0, 0.0),
        (0.3, 0.7, 600000, "2026-03-10", 3, "Single Family Residential", 30, 0.0, -2.0),
        (0.9, 0.1, 900000, "2026-04-10", 4, "Single Family Residential", None, None, None),
        (0.5, 1.5, 450000, "2026-02-20", 2, "Condo/Co-Op", 40, 1.0, 1.0),
        (0.6, 1.6, 650000, "2026-05-20", 3, "Townhouse", 60, 3.0, 3.0),
        (10.0, 10.0, 300000, "2026-03-01", 2, "Condo/Co-Op", 5, 0.0, 0.0),
        (-3.0, 0.5, 700000, "2026-06-01", 3, "Single Family Residential", 15, 1.0, 1.0),
        (np.nan, np.nan, 800000, "2026-07-01", 4, "Single Family Residential", 25, 2.0, 2.0),
    ]
    df = pd.DataFrame(rows, columns=["latitude", "longitude", "price", "sold_date", "beds", "ptype", "dom",
                                     "diff_pct_last", "diff_pct_original"])
    df["sold_date"] = pd.to_datetime(df["sold_date"])
    return df


def by_id(rows):
    return {r["id"]: r for r in rows}


def test_aggregate_without_filters(nb):
    sales = with_neighbourhood(sales_frame(), nb)
    rows = by_id(aggregate(sales, nb, min_sample=2))
    assert list(rows) == [ALPHA, BRAVO, CHARLIE, UNASSIGNED_ID]            # every neighbourhood, unassigned last
    assert rows[ALPHA]["n"] == 4 and rows[BRAVO]["n"] == 2 and rows[CHARLIE]["n"] == 0
    assert rows[UNASSIGNED_ID]["n"] == 3 and rows[UNASSIGNED_ID]["name"] == UNASSIGNED_NAME
    assert sum(r["n"] for r in rows.values()) == len(sales)                # nothing is dropped
    assert rows[ALPHA]["median_price"] == 550000                           # median of 400, 500, 600, 900 thousand
    assert rows[ALPHA]["avg_dom"] == 20.0 and rows[ALPHA]["median_dom"] == 20.0   # the missing DOM is skipped, not a 0
    assert rows[ALPHA]["avg_diff_pct_last"] == 0.33 and rows[ALPHA]["avg_diff_pct_original"] == 0.67
    assert rows[BRAVO]["median_price"] == 550000 and rows[BRAVO]["avg_dom"] == 50.0
    assert rows[CHARLIE]["median_price"] is None and rows[CHARLIE]["avg_dom"] is None   # no sales: no statistic


def test_low_sample_flag_uses_the_minimum(nb):
    sales = with_neighbourhood(sales_frame(), nb)
    rows = by_id(aggregate(sales, nb, min_sample=3))
    assert [rows[i]["low_sample"] for i in (ALPHA, BRAVO, CHARLIE, UNASSIGNED_ID)] == [False, True, True, False]
    assert MIN_SAMPLE == 10
    assert by_id(aggregate(sales, nb))[ALPHA]["low_sample"] is True        # 4 sales < the default minimum


def test_aggregate_with_filters(nb):
    sales = with_neighbourhood(sales_frame(), nb)
    # beds 2 or 3, condos and townhomes, sold 2026-02-01 to 2026-05-31, up to $650,000
    filtered = apply_sale_filters(sales, {"sold_start": "2026-02-01", "sold_end": "2026-05-31", "max_price": 650000,
                                          "beds": [2, 3], "ptypes": ["Condo", "Townhome"]})
    # Alpha 500k, Bravo 450k and 650k, and the $300k condo outside every polygon (it passes the filters too)
    assert filtered["price"].tolist() == [500000, 450000, 650000, 300000]
    rows = by_id(aggregate(filtered, nb, min_sample=2))
    assert (rows[ALPHA]["n"], rows[BRAVO]["n"], rows[CHARLIE]["n"], rows[UNASSIGNED_ID]["n"]) == (1, 2, 0, 1)
    assert rows[ALPHA]["median_price"] == 500000 and rows[ALPHA]["low_sample"] is True
    assert rows[BRAVO]["median_price"] == 550000 and rows[BRAVO]["low_sample"] is False
    assert rows[UNASSIGNED_ID]["median_price"] == 300000 and rows[UNASSIGNED_ID]["low_sample"] is True
    assert sum(r["n"] for r in rows.values()) == len(filtered)             # counts always add up to the filtered sales


def test_aggregate_of_no_sales_still_lists_every_neighbourhood(nb):
    empty = with_neighbourhood(sales_frame().iloc[0:0], nb)
    rows = aggregate(empty, nb)
    assert [r["id"] for r in rows] == [ALPHA, BRAVO, CHARLIE, UNASSIGNED_ID]
    assert all(r["n"] == 0 and r["low_sample"] for r in rows)


# ---- the filter module on its own -------------------------------------------------------------------------------------

def test_filters_leave_the_input_alone_and_empty_filters_keep_everything():
    sales = sales_frame()
    before = sales.copy()
    assert len(apply_sale_filters(sales, {})) == len(sales) == len(apply_sale_filters(sales, None))
    apply_sale_filters(sales, {"min_price": 500000})
    pd.testing.assert_frame_equal(sales, before)


def test_each_filter():
    sales = sales_frame()
    assert apply_sale_filters(sales, {"min_price": 650000})["price"].tolist() == [900000, 650000, 700000, 800000]
    assert apply_sale_filters(sales, {"max_price": 400000})["price"].tolist() == [400000, 300000]
    assert len(apply_sale_filters(sales, {"sold_start": "2026-06-01"})) == 2
    assert len(apply_sale_filters(sales, {"sold_end": "2026-01-10"})) == 1             # inclusive
    assert len(apply_sale_filters(sales, {"beds": [4]})) == 2
    assert len(apply_sale_filters(sales, {"beds": []})) == len(sales)                  # an empty list means no filter
    assert len(apply_sale_filters(sales, {"ptypes": ["Single-family"]})) == 4
    assert len(apply_sale_filters(sales, {"ptypes": ["Multi-family"]})) == 0


def test_a_sale_with_no_date_drops_out_once_a_date_is_set():
    sales = sales_frame()
    sales.loc[0, "sold_date"] = pd.NaT
    assert len(apply_sale_filters(sales, {})) == len(sales)
    assert len(apply_sale_filters(sales, {"sold_start": "2000-01-01"})) == len(sales) - 1


# ---- the committed Ottawa file ----------------------------------------------------------------------------------------

def test_the_committed_ottawa_boundaries_load_and_place_known_spots():
    real = load_neighbourhoods()                      # redfin/data/neighbourhoods/ottawa_neighbourhoods.geojson
    assert len(real.ids) == 116 and len(set(real.ids.tolist())) == 116 and UNASSIGNED_ID not in real.ids
    lansdowne, stittsville_main, toronto = (45.4005, -75.6836), (45.2570, -75.9230), (43.6532, -79.3832)
    placed = assign(*zip(lansdowne, stittsville_main, toronto), real).tolist()
    assert real.name_of(placed[0]) == "Glebe-Dows Lake"
    assert real.name_of(placed[1]).startswith("Stittsville")
    assert placed[2] == UNASSIGNED_ID
