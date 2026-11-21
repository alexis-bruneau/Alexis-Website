"""Tests for redfin.transform.purge (which sold listings a purge removes) and redfin.exclusions (keeping them out).
Pure: frames and a temp file only; no Azure. Run:  python -m pytest redfin"""
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from redfin import exclusions
from redfin.transform import purge

CUTOFF = date(2025, 6, 1)


def listed(*rows):
    """rows: (property_id, MLS, photo_blob or None)"""
    return pd.DataFrame([{"property_id": p, "MLS": m, "url": f"https://www.redfin.ca/on/ottawa/x/home/{p}",
                          "Sold Date": "Jan 01, 2020", "photo_blob": photo} for p, m, photo in rows])


def history(*events):
    """events: (property_id, mls, event_type, event_date)"""
    return pd.DataFrame([{"property_id": p, "mls": m, "event_type": t, "event_date": d} for p, m, t, d in events])


def text(*rows):
    return pd.DataFrame([{"property_id": p, "MLS": m, "text": "x", "bronze_day": "2026-02-20"} for p, m in rows])


class PurgePlanTest(unittest.TestCase):
    def test_a_listing_whose_own_sale_is_before_the_cutoff_is_purged(self):
        frames = {"silver/2026-02/listed_properties.parquet": listed(("1", "OLD", None), ("2", "NEW", None))}
        h = history(("1", "OLD", "Sold", date(2024, 3, 1)), ("2", "NEW", "Sold", date(2026, 1, 5)))
        p = purge.plan(frames, h, None, CUTOFF)
        self.assertEqual(p.listings, [("1", "OLD", "2024-03-01")])
        self.assertEqual(list(p.silver["silver/2026-02/listed_properties.parquet"]["MLS"]), ["NEW"])

    def test_the_cutoff_day_itself_is_kept(self):
        frames = {"f": listed(("1", "A", None), ("2", "B", None))}
        h = history(("1", "A", "Sold", date(2025, 5, 31)), ("2", "B", "Sold", date(2025, 6, 1)))
        self.assertEqual([m for _, m, _ in purge.plan(frames, h, None, CUTOFF).listings], ["A"])

    def test_a_listing_that_never_sold_is_kept_even_with_an_old_sold_date_column(self):
        frames = {"f": listed(("1", "UNSOLD", None))}   # Sold Date column says 2020, inherited from an earlier sale
        h = history(("1", "UNSOLD", "Listed", date(2025, 1, 1)), ("1", "OTHER", "Sold", date(2020, 1, 1)))
        p = purge.plan(frames, h, None, CUTOFF)
        self.assertEqual(p.listings, [])
        self.assertEqual(p.silver, {})

    def test_a_conditional_sale_is_not_a_sale(self):
        frames = {"f": listed(("1", "A", None))}
        h = history(("1", "A", "Sold-Conditional", date(2024, 1, 1)))
        self.assertEqual(purge.plan(frames, h, None, CUTOFF).listings, [])

    def test_no_history_means_nothing_is_purged(self):
        self.assertEqual(purge.plan({"f": listed(("1", "A", None))}, None, None, CUTOFF).listings, [])

    def test_a_month_file_left_empty_is_deleted_and_the_other_files_are_rewritten_only_if_touched(self):
        frames = {"empty": listed(("1", "A", None)), "mixed": listed(("2", "B", None), ("3", "C", None)),
                  "untouched": listed(("4", "D", None))}
        h = history(("1", "A", "Sold", date(2024, 1, 1)), ("2", "B", "Sold", date(2024, 1, 1)),
                    ("3", "C", "Sold", date(2026, 1, 1)), ("4", "D", "Sold", date(2026, 1, 1)))
        p = purge.plan(frames, h, None, CUTOFF)
        self.assertIsNone(p.silver["empty"])
        self.assertEqual(list(p.silver["mixed"]["MLS"]), ["C"])
        self.assertNotIn("untouched", p.silver)

    def test_sale_history_goes_only_with_properties_that_have_no_listing_left(self):
        frames = {"f": listed(("1", "A", None), ("2", "B", None), ("2", "B2", None))}
        h = history(("1", "A", "Sold", date(2024, 1, 1)), ("1", "A", "Listed", date(2023, 12, 1)),
                    ("2", "B", "Sold", date(2024, 1, 1)), ("2", "B2", "Sold", date(2026, 1, 1)))
        p = purge.plan(frames, h, None, CUTOFF)
        self.assertEqual(p.partial_properties, {"2"})            # B purged, B2 stays
        self.assertEqual(p.history_dropped, 2)                   # both events of property 1
        self.assertEqual(set(p.history["property_id"]), {"2"})   # property 2 keeps all its events

    def test_images_text_and_exclusions_follow_the_listing(self):
        frames = {"f": listed(("1", "A", "images/A_1.jpg"), ("2", "B", "images/B_1.jpg"))}
        h = history(("1", "A", "Sold", date(2024, 1, 1)), ("2", "B", "Sold", date(2026, 1, 1)))
        p = purge.plan(frames, h, text(("1", "A"), ("2", "B")), CUTOFF)
        self.assertEqual(p.images, ["images/A_1.jpg", "images/A_sv.jpg"])
        self.assertEqual(p.text_dropped, 1)
        self.assertEqual(list(p.text["MLS"]), ["B"])
        self.assertEqual(p.listings, [("1", "A", "2024-01-01")])

    def test_old_silver_files_without_a_property_id_column_still_work(self):
        old = listed(("1", "A", None)).drop(columns=["property_id"])
        p = purge.plan({"f": old}, history(("1", "A", "Sold", date(2024, 1, 1))), None, CUTOFF)
        self.assertEqual(p.listings, [("1", "A", "2024-01-01")])

    def test_timestamp_dates_work_too(self):
        h = history(("1", "A", "Sold", pd.Timestamp("2024-01-01")))
        self.assertEqual(len(purge.plan({"f": listed(("1", "A", None))}, h, None, CUTOFF).listings), 1)

    def test_the_latest_sale_decides(self):
        # Sold in 2024, relisted under the same MLS and sold again in 2026: the listing is not old.
        h = history(("1", "A", "Sold", date(2024, 1, 1)), ("1", "A", "Sold", date(2026, 1, 1)))
        self.assertEqual(purge.plan({"f": listed(("1", "A", None))}, h, None, CUTOFF).listings, [])


class ExclusionsTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "excluded_listings.txt"

    def test_missing_file_is_an_empty_list(self):
        self.assertEqual(exclusions.load(self.path), [])

    def test_append_then_load_round_trips_and_is_idempotent(self):
        entries = [("2", "B", "2024-02-02"), ("1", None, "2024-01-01")]
        self.assertEqual(exclusions.append(entries, self.path), 2)
        self.assertEqual(exclusions.append(entries, self.path), 0)
        self.assertEqual(sorted(exclusions.load(self.path)), [("1", None, "2024-01-01"), ("2", "B", "2024-02-02")])
        self.assertTrue(self.path.read_text(encoding="utf-8").startswith("#"))

    def test_comments_and_blank_lines_are_ignored(self):
        self.path.write_text("# header\n\n10 X10 2024-01-01  # why\n11 - -\n", encoding="utf-8")
        self.assertEqual(exclusions.load(self.path), [("10", "X10", "2024-01-01"), ("11", None, None)])

    def test_keys_names_and_property_ids(self):
        entries = [("10", "X10", None), ("11", None, None)]
        self.assertEqual(exclusions.keys(entries), {("10", "X10"), ("11", None)})
        self.assertEqual(exclusions.names(entries), {"X10", "P11"})       # the bronze file names
        self.assertEqual(exclusions.property_ids(entries), {"10", "11"})

    def test_bronze_pages_of_purged_listings_are_skipped_before_any_download(self):
        bronze = [("2026-02-20", "X10", "b1"), ("2026-02-20", "P11", "b2"), ("2026-02-20", "X12", "b3")]
        kept = exclusions.without_excluded_pages(bronze, [("10", "X10", None), ("11", None, None)])
        self.assertEqual([name for _, name, _ in kept], ["X12"])
        self.assertEqual(exclusions.without_excluded_pages(bronze, []), bronze)


if __name__ == "__main__":
    unittest.main()
