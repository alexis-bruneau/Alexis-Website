"""Tests for redfin.transform.active: the snapshot built from the newest page per MLS, and the reconcile with sold.
Offline: frames and strings only, no Azure. Run:  python -m pytest redfin"""
import unittest
from datetime import date

import pandas as pd

from redfin.transform import active

AS_OF = date(2026, 11, 21)


def listed(*rows):
    """silver listed_properties rows: (property_id, MLS, extra columns)."""
    return pd.DataFrame([{"property_id": pid, "MLS": mls, "Sold Date": None, "days_on_market": None,
                          "cumulative_dom": None, "sold_vs_last_list": None, "sold_vs_original_list": None, **extra}
                         for pid, mls, extra in rows])


def history(*events):
    """sale_history rows: (mls, event_type, event_date)."""
    return pd.DataFrame([{"property_id": "1", "mls": m, "event_type": t, "event_date": d} for m, t, d in events])


def active_rows(*rows):
    return pd.DataFrame([{"mls": mls, "property_id": pid, "list_price": 500_000} for mls, pid in rows])


class ReconcileTest(unittest.TestCase):
    def test_sold_mls_that_is_in_the_census_is_removed(self):
        sold = active.sold_mls(listed(("1", "X1", {})), history(("X1", "Sold", date(2026, 11, 18))))
        kept, removed = active.reconcile(active_rows(("X1", "1"), ("X2", "2")), sold)
        self.assertEqual(list(kept["mls"]), ["X2"])
        self.assertEqual(removed.to_dict("records"),
                         [{"mls": "X1", "sold_date": "2026-11-18", "reason": active.REASON_EVENT}])
        self.assertEqual(list(removed.columns), ["mls", "sold_date", "reason"])

    def test_same_property_id_with_a_new_mls_stays_active(self):
        # Home 7 sold in September as X_OLD and was relisted in October as X_NEW.
        sold = active.sold_mls(listed(("7", "X_OLD", {})), history(("X_OLD", "Sold", date(2026, 9, 4))))
        kept, removed = active.reconcile(active_rows(("X_NEW", "7")), sold)
        self.assertEqual(list(kept["mls"]), ["X_NEW"])
        self.assertTrue(removed.empty)

    def test_sold_silver_row_without_a_sale_event_is_not_a_sale(self):
        # One of the ~113 listed_properties rows that are not sales. Even with a Sold Date copied from the previous
        # sale (ACTIVE_LISTINGS.md finding 7) it must not take an active listing off the map.
        rows = listed(("3", "X3", {"Sold Date": "Sep 04, 2026"}), ("4", "X4", {}))
        sold = active.sold_mls(rows, history(("X3", "Listed", date(2026, 10, 1)), ("X3", "Delisted", date(2026, 10, 9))))
        self.assertEqual(sold, {})
        kept, removed = active.reconcile(active_rows(("X3", "3"), ("X4", "4")), sold)
        self.assertEqual(list(kept["mls"]), ["X3", "X4"])
        self.assertTrue(removed.empty)

    def test_conditional_sale_and_pending_are_not_sales(self):
        events = history(("A", "Sold-Conditional", date(2026, 11, 1)), ("B", "Pending", date(2026, 11, 1)),
                         ("C", "Closed", date(2026, 11, 2)), ("D", "Sold (MLS)", date(2026, 11, 3)))
        sold = active.sold_mls(listed(*[(str(i), m, {}) for i, m in enumerate("ABCD")]), events)
        self.assertEqual(sorted(sold), ["C", "D"])

    def test_a_sale_metric_on_the_row_proves_the_sale_when_there_is_no_history(self):
        # transform.sold only fills these when the MLS has its own Sold event.
        rows = listed(("5", "X5", {"days_on_market": 41, "Sold Date": "Nov 03, 2026"}))
        self.assertEqual(active.sold_mls(rows, None), {"X5": ("2026-11-03", active.REASON_METRICS)})

    def test_history_with_a_later_sale_uses_the_latest_date(self):
        sold = active.sold_mls(listed(("1", "X1", {})), history(("X1", "Sold", date(2026, 5, 1)), ("X1", "Sold", date(2026, 6, 1))))
        self.assertEqual(sold["X1"][0], "2026-06-01")

    def test_only_mls_in_listed_properties_are_candidates(self):
        sold = active.sold_mls(listed(("1", "X1", {})), history(("X1", "Sold", date(2026, 1, 1)), ("OTHER", "Sold", date(2026, 1, 1))))
        self.assertEqual(list(sold), ["X1"])

    def test_missing_mls_values_are_ignored(self):
        rows = listed(("1", None, {"days_on_market": 3}), ("2", "None", {"days_on_market": 3}), ("3", "", {"days_on_market": 3}))
        self.assertEqual(active.sold_mls(rows, None), {})

    def test_nothing_sold_keeps_every_row_and_writes_an_empty_frame(self):
        df = active_rows(("A", "1"), ("B", "2"))
        kept, removed = active.reconcile(df, {})
        self.assertEqual(list(kept["mls"]), ["A", "B"])
        self.assertEqual(list(removed.columns), active.RECONCILED_COLUMNS)
        self.assertTrue(removed.empty)

    def test_reconcile_does_not_modify_its_input(self):
        df = active_rows(("A", "1"), ("B", "2"))
        before = df.copy()
        active.reconcile(df, {"A": ("2026-11-01", active.REASON_EVENT)})
        pd.testing.assert_frame_equal(df, before)

    def test_output_file_name_is_not_picked_up_as_sold_data(self):
        self.assertFalse(active.RECONCILED_BLOB.endswith("listed_properties.parquet"))
        self.assertFalse(active.OUT_BLOB.endswith("listed_properties.parquet"))


class BuildRowsTest(unittest.TestCase):
    CENSUS = {m: {"mls": m, "property_id": pid, "listing_id": "L" + m, "status": "Active", "list_price": price,
                  "latitude": 45.4, "longitude": -75.7, "dom_redfin": 12, "alt_property_ids": [],
                  "url": f"https://www.redfin.ca/on/ottawa/x/home/{pid}", "address": "1 Main St"}
              for m, pid, price in (("A", "1", 400_000), ("B", "2", 500_000), ("C", "3", 600_000))}
    HTML = "<html></html>"   # no events: the row falls back to Redfin's own days on market

    def build(self, pages):
        return active.build_rows(self.CENSUS, pages, lambda blob: self.HTML, AS_OF)

    def test_every_census_listing_with_a_page_gets_a_row_whatever_the_page_day(self):
        rows, _ = self.build({"A": ("2026-11-21", "a"), "B": ("2026-10-02", "b")})
        self.assertEqual([(r["mls"], r["page_day"]) for r in rows], [("A", "2026-11-21"), ("B", "2026-10-02")])

    def test_identity_price_and_status_come_from_the_census_and_as_of_is_the_census_day(self):
        rows, _ = self.build({"B": ("2026-10-02", "b")})
        r = rows[0]
        self.assertEqual((r["property_id"], r["listing_id"], r["status"], r["list_price"]), ("2", "LB", "Active", 500_000))
        self.assertEqual((r["latitude"], r["longitude"]), (45.4, -75.7))
        self.assertEqual(r["as_of"], AS_OF)
        self.assertEqual((r["days_on_market"], r["dom_basis"]), (12, "redfin"))

    def test_page_of_a_listing_that_left_the_census_is_ignored(self):
        rows, _ = self.build({"A": ("2026-11-21", "a"), "GONE": ("2026-11-01", "g")})
        self.assertEqual([r["mls"] for r in rows], ["A"])

    def test_census_listing_without_a_page_gets_no_row(self):
        rows, _ = self.build({})
        self.assertEqual(rows, [])

    def test_rebuilding_gives_the_same_rows(self):
        pages = {"A": ("2026-11-21", "a"), "C": ("2026-11-05", "c")}
        self.assertEqual(self.build(pages)[0], self.build(pages)[0])


if __name__ == "__main__":
    unittest.main()
