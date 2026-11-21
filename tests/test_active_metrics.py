"""Tests for metrics.active_metrics in app/Redfin/metrics.py. Run from the project root:  python -m unittest discover -s tests -v"""
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app" / "Redfin"))
import metrics  # noqa: E402

TODAY = date(2026, 10, 18)


def event(seq, day, kind, price, mls):
    return {"property_id": "1", "event_seq": seq, "event_date": day, "event_type": kind,
            "price": price, "mls": mls, "source": "TREB", "source_id": mls, "status": None}


class ActiveMetricsTest(unittest.TestCase):
    def test_price_cut(self):
        events = [event(0, date(2026, 9, 1), "Listed", 800_000, "A"),
                  event(1, date(2026, 10, 1), "Price Changed", 760_000, "A")]
        m = metrics.active_metrics(events, "A", TODAY, 760_000)
        self.assertEqual(m["days_on_market"], 47)
        self.assertEqual(m["original_price"], 800_000)
        self.assertEqual(m["price_changes"], 1)
        self.assertEqual((m["last_change_amount"], m["last_change_pct"]), (-40_000, -5.0))
        self.assertEqual(m["last_change_date"], date(2026, 10, 1))
        self.assertEqual(m["price_vs_original"], -40_000)

    def test_price_increase(self):
        events = [event(0, date(2026, 10, 1), "Listed", 500_000, "A"),
                  event(1, date(2026, 10, 10), "Price Changed", 525_000, "A")]
        m = metrics.active_metrics(events, "A", TODAY, 525_000)
        self.assertEqual(m["last_change_amount"], 25_000)

    def test_only_the_latest_change_is_reported(self):
        events = [event(0, date(2026, 8, 1), "Listed", 900_000, "A"),
                  event(1, date(2026, 8, 20), "Price Changed", 850_000, "A"),
                  event(2, date(2026, 9, 20), "Price Changed", 820_000, "A")]
        m = metrics.active_metrics(events, "A", TODAY, 820_000)
        self.assertEqual((m["price_changes"], m["last_change_amount"]), (2, -30_000))
        self.assertEqual(m["price_vs_original"], -80_000)

    def test_no_change(self):
        m = metrics.active_metrics([event(0, date(2026, 10, 17), "Listed", 600_000, "A")], "A", TODAY, 600_000)
        self.assertEqual((m["days_on_market"], m["price_changes"]), (1, 0))
        self.assertIsNone(m["last_change_amount"])

    def test_search_price_newer_than_the_page_history(self):
        events = [event(0, date(2026, 10, 1), "Listed", 600_000, "A")]
        m = metrics.active_metrics(events, "A", TODAY, 575_000)
        self.assertEqual((m["last_change_amount"], m["last_change_date"]), (-25_000, TODAY))

    def test_a_repeated_price_is_not_a_change(self):
        events = [event(0, date(2026, 9, 1), "Listed", 600_000, "A"),
                  event(1, date(2026, 10, 1), "Price Changed", 600_000, "A")]
        self.assertEqual(metrics.active_metrics(events, "A", TODAY, 600_000)["price_changes"], 0)

    def test_other_listings_of_the_home_do_not_leak_in(self):
        events = [event(0, date(2026, 8, 1), "Listed", 700_000, "OLD"),
                  event(1, date(2026, 8, 20), "Price Changed", 650_000, "OLD"),
                  event(2, date(2026, 10, 10), "Listed", 640_000, "A")]
        m = metrics.active_metrics(events, "A", TODAY, 640_000)
        self.assertEqual((m["original_price"], m["price_changes"], m["days_on_market"]), (640_000, 0, 8))

    def test_relist_does_not_reset_cumulative_dom(self):
        events = [event(0, date(2026, 6, 1), "Listed", 700_000, "OLD"),
                  event(1, date(2026, 7, 1), "Delisted", None, "OLD"),
                  event(2, date(2026, 9, 1), "Listed", 690_000, "A")]
        m = metrics.active_metrics(events, "A", TODAY, 690_000)
        self.assertEqual(m["days_on_market"], 47)
        self.assertEqual(m["cumulative_dom"], 47 + 30)

    def test_events_after_as_of_are_ignored(self):
        events = [event(0, date(2026, 9, 1), "Listed", 800_000, "A"),
                  event(1, date(2026, 10, 25), "Price Changed", 700_000, "A")]
        m = metrics.active_metrics(events, "A", TODAY, 800_000)
        self.assertEqual(m["price_changes"], 0)

    def test_no_history_still_gives_the_price(self):
        m = metrics.active_metrics([], "A", TODAY, 500_000)
        self.assertEqual((m["original_price"], m["price_changes"]), (500_000, 0))
        self.assertIsNone(m["days_on_market"])


if __name__ == "__main__":
    unittest.main()
