"""Tests for redfin.run.verify (is silver as new as bronze and the census?) and the pending-page helper it shares with
transform.sold. Pure functions only: no Azure. Run:  python -m pytest redfin"""
import unittest

import pandas as pd

from redfin.run import verify
from redfin.transform import sold


def page(day, name):
    return (day, name, f"bronze/{day}/{name}.html.gz")


class PendingEntriesTest(unittest.TestCase):
    def test_done_keys_and_pending_entries_match_on_the_listing_name(self):
        existing = {"2026-10": pd.DataFrame({"property_id": ["1", "2"], "MLS": ["X1", None]})}
        done = sold.done_keys(existing)
        self.assertEqual(done, {("1", "X1"), ("2", None)})
        entries = [page("2026-11-21", "X1"), page("2026-11-21", "P2"), page("2026-11-21", "X3")]
        self.assertEqual([e[1] for e in sold.pending_entries(entries, done)], ["X3"])

    def test_nothing_done_everything_pending(self):
        entries = [page("2026-11-21", "X1")]
        self.assertEqual(sold.pending_entries(entries, set()), entries)


class SoldReportTest(unittest.TestCase):
    def test_fresh_when_the_newest_day_is_in_silver(self):
        entries = [page("2026-11-20", "A"), page("2026-11-21", "B"), page("2026-11-21", "C")]
        r = verify.sold_report(entries, pending=[])
        self.assertEqual((r["newest_day"], r["pages"], r["pending_newest"], r["fresh"]), ("2026-11-21", 2, 0, True))

    def test_stale_when_a_fetch_was_not_transformed(self):
        entries = [page("2026-11-21", f"X{i}") for i in range(150)]
        r = verify.sold_report(entries, pending=entries)
        self.assertEqual((r["pending_newest"], r["fresh"]), (150, False))

    def test_a_few_unreadable_pages_are_tolerated(self):
        entries = [page("2026-11-21", f"X{i}") for i in range(150)]
        self.assertTrue(verify.sold_report(entries, pending=entries[:7])["fresh"])    # 5% of 150 = 8
        self.assertFalse(verify.sold_report(entries, pending=entries[:9])["fresh"])
        small = entries[:20]
        self.assertTrue(verify.sold_report(small, pending=small[:3])["fresh"])        # at least 3 pages
        self.assertFalse(verify.sold_report(small, pending=small[:4])["fresh"])

    def test_old_leftovers_do_not_make_it_stale(self):
        entries = [page("2026-03-01", "OLD"), page("2026-11-21", "NEW")]
        r = verify.sold_report(entries, pending=[entries[0]])
        self.assertEqual((r["pending_total"], r["pending_newest"], r["fresh"]), (1, 0, True))

    def test_empty_bronze(self):
        self.assertTrue(verify.sold_report([], [])["fresh"])


class ActiveReportTest(unittest.TestCase):
    def test_fresh_when_the_snapshot_is_as_of_the_latest_census(self):
        self.assertTrue(verify.active_report("2026-11-21", "2026-11-21", 200)["fresh"])

    def test_stale_when_the_snapshot_is_older_than_the_census(self):
        r = verify.active_report("2026-11-21", "2026-10-18", 50)
        self.assertFalse(r["fresh"])
        self.assertEqual(r["as_of"], "2026-10-18")

    def test_stale_when_there_is_no_snapshot_or_no_census_or_no_rows(self):
        self.assertFalse(verify.active_report("2026-11-21", None, 0)["fresh"])
        self.assertFalse(verify.active_report(None, "2026-11-21", 10)["fresh"])
        self.assertFalse(verify.active_report("2026-11-21", "2026-11-21", 0)["fresh"])


if __name__ == "__main__":
    unittest.main()
