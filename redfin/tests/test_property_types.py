"""Tests for redfin.analytics.property_types. Run:  python -m pytest redfin"""
import unittest

from redfin.analytics.property_types import property_group
from redfin.config import REPO_ROOT


class PropertyGroupTest(unittest.TestCase):
    def test_every_spelling_seen_in_silver(self):
        expected = {
            "Single-family": "Single-family", "Single Family Residential": "Single-family", "Ranch": "Single-family",
            "Townhome": "Townhome", "Townhouse": "Townhome",
            "Condo": "Condo", "Condo/Co-Op": "Condo", "Condo (co-op)": "Condo",
            "Multi-family": "Multi-family", "Multi-Family (2-4 Unit)": "Multi-family",
            "Vacant land": "Other", "Vacant Land": "Other", "Manufactured": "Other", "Mobile/Manufactured Home": "Other",
        }
        for raw, group in expected.items():
            self.assertEqual(property_group(raw), group, raw)

    def test_missing_values_are_other(self):
        for raw in (None, float("nan"), "", "None"):
            self.assertEqual(property_group(raw), "Other")

    def test_groups_match_the_filter_buttons(self):
        html = (REPO_ROOT / "templates" / "ottawa_map.html").read_text(encoding="utf-8")
        for group in ("Condo", "Townhome", "Single-family", "Multi-family"):
            self.assertIn(f'data-ptype="{group}"', html)


if __name__ == "__main__":
    unittest.main()
