"""Tests for app/Redfin/metrics.py. Run from the project root:  python -m unittest discover -s tests -v"""
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app" / "Redfin"))
import metrics  # noqa: E402


def event(seq, day, kind, price, mls):
    return {"property_id": "1", "event_seq": seq, "event_date": day, "event_type": kind,
            "price": price, "mls": mls, "source": "TREB", "source_id": mls, "status": None}


class ListingMetricsTest(unittest.TestCase):
    def test_price_cut_then_sold(self):
        events = [
            event(0, date(2024, 1, 1), "Listed", 800_000, "A"),
            event(1, date(2024, 1, 20), "Price Changed", 750_000, "A"),
            event(2, date(2024, 2, 1), "Sold (MLS)", 740_000, "A"),
        ]
        m = metrics.listing_metrics(events, "A", 740_000)
        self.assertEqual(m["sold_vs_last_list"], -10_000)      # against the 750k asking when it sold
        self.assertEqual(m["sold_vs_original_list"], -60_000)  # against the 800k it first listed at
        self.assertEqual(m["days_on_market"], 31)
        self.assertEqual(m["cumulative_dom"], 31)

    def test_sold_over_asking(self):
        events = [event(0, date(2024, 5, 1), "Listed", 500_000, "A"), event(1, date(2024, 5, 8), "Sold (MLS)", 560_000, "A")]
        m = metrics.listing_metrics(events, "A", 560_000)
        self.assertEqual((m["sold_vs_last_list"], m["sold_vs_original_list"]), (60_000, 60_000))

    def test_earlier_listing_of_the_same_home_is_not_used(self):
        events = [
            event(0, date(2005, 2, 2), "Listed", 100_000, "OLD"),
            event(1, date(2005, 4, 1), "Delisted", None, "OLD"),
            event(2, date(2023, 9, 1), "Listed", 400_000, "NEW"),
            event(3, date(2023, 10, 1), "Sold (MLS)", 390_000, "NEW"),
        ]
        m = metrics.listing_metrics(events, "NEW", 390_000)
        self.assertEqual(m["sold_vs_original_list"], -10_000)  # not +290k against the 2005 listing
        self.assertEqual(m["days_on_market"], 30)

    def test_cumulative_dom_adds_unsold_relists(self):
        events = [
            event(0, date(2020, 1, 1), "Listed", 600_000, "A"),
            event(1, date(2020, 3, 1), "Delisted", None, "A"),       # 60 days, did not sell
            event(2, date(2020, 6, 1), "Listed", 590_000, "B"),
            event(3, date(2020, 7, 1), "Sold (MLS)", 580_000, "B"),  # 30 days
        ]
        m = metrics.listing_metrics(events, "B", 580_000)
        self.assertEqual(m["days_on_market"], 30)
        self.assertEqual(m["cumulative_dom"], 90)

    def test_cumulative_dom_stops_at_the_previous_sale(self):
        events = [
            event(0, date(2015, 1, 1), "Listed", 300_000, "A"),
            event(1, date(2015, 3, 1), "Sold (MLS)", 295_000, "A"),
            event(2, date(2020, 1, 1), "Listed", 500_000, "B"),
            event(3, date(2020, 2, 1), "Sold (MLS)", 510_000, "B"),
        ]
        m = metrics.listing_metrics(events, "B", 510_000)
        self.assertEqual(m["cumulative_dom"], m["days_on_market"])  # the 2015 sale starts a new cycle

    def test_later_listing_does_not_change_an_earlier_sale(self):
        events = [
            event(0, date(2020, 1, 1), "Listed", 500_000, "A"),
            event(1, date(2020, 2, 1), "Sold (MLS)", 505_000, "A"),
            event(2, date(2024, 1, 1), "Listed", 900_000, "B"),
            event(3, date(2024, 1, 10), "Price Changed", 850_000, "B"),
        ]
        m = metrics.listing_metrics(events, "A", 505_000)
        self.assertEqual(m["sold_vs_last_list"], 5_000)
        self.assertEqual(m["cumulative_dom"], 31)

    def test_same_day_events_use_page_order(self):
        events = [
            event(0, date(2024, 3, 1), "Listed", 700_000, "A"),
            event(1, date(2024, 3, 1), "Price Changed", 680_000, "A"),
            event(2, date(2024, 3, 9), "Sold (MLS)", 690_000, "A"),
        ]
        m = metrics.listing_metrics(events, "A", 690_000)
        self.assertEqual(m["sold_vs_last_list"], 10_000)
        self.assertEqual(m["sold_vs_original_list"], -10_000)

    def test_missing_data_gives_none(self):
        unsold = [event(0, date(2024, 1, 1), "Listed", 500_000, "A")]
        self.assertEqual(metrics.listing_metrics(unsold, "A", 480_000), dict.fromkeys(metrics.METRICS))
        sold = [event(0, date(2024, 1, 1), "Listed", 500_000, "A"), event(1, date(2024, 1, 5), "Sold (MLS)", 1, "A")]
        self.assertEqual(metrics.listing_metrics(sold, None, 480_000), dict.fromkeys(metrics.METRICS))
        no_listed = [event(0, date(2024, 1, 5), "Sold (MLS)", 480_000, "A")]
        m = metrics.listing_metrics(no_listed, "A", 480_000)
        self.assertIsNone(m["days_on_market"])
        self.assertIsNone(m["sold_vs_last_list"])


if __name__ == "__main__":
    unittest.main()
