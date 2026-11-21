"""Tests for the daily selection in redfin.extract.fetch_active (pick_daily) and the page helper it relies on.
Offline: no browser, no Azure. Run:  python -m pytest redfin"""
import unittest

from redfin.extract import fetch_active
from redfin.storage import azure_store

DAY = "2026-11-21"


def row(mls, dom=5, price=500_000, lat=45.4, lon=-75.7, url=None):
    return {"mls": mls, "list_price": price, "latitude": lat, "longitude": lon, "dom_redfin": dom,
            "url": url or f"https://www.redfin.ca/on/ottawa/{mls}/home/{abs(hash(mls)) % 10**6}"}


def picked(rows, n, newest_page_day=None, day=DAY):
    return [r["mls"] for r in fetch_active.pick_daily(rows, n, newest_page_day or {}, day)]


class PickDailyTest(unittest.TestCase):
    def test_listings_without_any_page_come_first_newest_listing_first(self):
        rows = [row("OLD", dom=90), row("NEW", dom=1), row("MID", dom=20)]
        self.assertEqual(picked(rows, 10), ["NEW", "MID", "OLD"])

    def test_then_the_stalest_page_first(self):
        rows = [row("A"), row("B"), row("C"), row("NEVER", dom=300)]
        pages = {"A": "2026-11-18", "B": "2026-11-01", "C": "2026-11-10"}
        self.assertEqual(picked(rows, 10, pages), ["NEVER", "B", "C", "A"])

    def test_cuts_at_n_after_ordering(self):
        rows = [row("A"), row("B"), row("NEW1", dom=1), row("NEW2", dom=2)]
        pages = {"A": "2026-11-18", "B": "2026-11-01"}
        self.assertEqual(picked(rows, 3, pages), ["NEW1", "NEW2", "B"])
        self.assertEqual(picked(rows, 0, pages), [])

    def test_a_listing_already_fetched_on_the_census_day_is_left_out(self):
        rows = [row("TODAY"), row("EARLIER"), row("NEVER")]
        pages = {"TODAY": DAY, "EARLIER": "2026-11-20"}
        self.assertEqual(picked(rows, 10, pages), ["NEVER", "EARLIER"])

    def test_ineligible_rows_are_never_picked(self):
        rows = [row("OK"),
                row("NOPRICE", price=None),
                row("NOMLS") | {"mls": None},
                row("FAR", lat=46.5),                                    # outside the map window
                row("WEST", lon=-77.0),
                row("US", url="https://www.redfin.com/WA/Seattle/home/1")]  # out of scope
        self.assertEqual(picked(rows, 10), ["OK"])

    def test_unknown_days_on_market_sorts_after_known(self):
        rows = [row("UNKNOWN", dom=None), row("KNOWN", dom=400)]
        self.assertEqual(picked(rows, 10), ["KNOWN", "UNKNOWN"])

    def test_same_input_same_order(self):
        rows = [row(f"X{i}", dom=i % 3) for i in range(30)]
        pages = {f"X{i}": f"2026-11-{10 + i % 5:02d}" for i in range(0, 30, 2)}
        self.assertEqual(picked(rows, 12, pages), picked(list(reversed(rows)), 12, pages))


class OldSampleStillWorksTest(unittest.TestCase):
    def test_pick_sample_is_unchanged_reproducible_and_eligible_only(self):
        rows = [row(f"S{i}", dom=i) for i in range(100)] + [row("BAD", price=None)]
        a = fetch_active.pick_sample(rows, 10, seed=DAY)
        self.assertEqual([r["mls"] for r in a], [r["mls"] for r in fetch_active.pick_sample(rows, 10, seed=DAY)])
        self.assertEqual(len(a), 10)
        self.assertNotIn("BAD", [r["mls"] for r in a])
        self.assertEqual(sum((r["dom_redfin"] or 0) >= fetch_active.OLDER_DAYS for r in a), 6)


class NewestPagePerMlsTest(unittest.TestCase):
    ENTRIES = [("2026-11-01", "A", "bronze/active/2026-11-01/A.html.gz"),
               ("2026-11-20", "A", "bronze/active/2026-11-20/A.html.gz"),
               ("2026-11-10", "B", "bronze/active/2026-11-10/B.html.gz"),
               ("2026-11-25", "B", "bronze/active/2026-11-25/B.html.gz")]

    def test_newest_day_wins_per_mls(self):
        newest = azure_store.newest_page_per_mls(self.ENTRIES)
        self.assertEqual(newest["A"], ("2026-11-20", "bronze/active/2026-11-20/A.html.gz"))
        self.assertEqual(newest["B"][0], "2026-11-25")

    def test_pages_after_the_census_day_are_ignored(self):
        newest = azure_store.newest_page_per_mls(self.ENTRIES, upto_day="2026-11-21")
        self.assertEqual(newest["B"][0], "2026-11-10")
        self.assertEqual(newest["A"][0], "2026-11-20")

    def test_empty(self):
        self.assertEqual(azure_store.newest_page_per_mls([]), {})


if __name__ == "__main__":
    unittest.main()
