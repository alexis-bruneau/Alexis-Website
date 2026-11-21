"""Tests for the search-results parsing in app/Redfin/parsing.py. Run from the project root:
    python -m unittest discover -s tests -v

The fixture is four real homes from a redfin.ca search (tests/fixtures/search_homes.json); the page around them
is rebuilt in the escaped form Redfin uses for its page state."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app" / "Redfin"))
import parsing  # noqa: E402

HOMES = json.loads((Path(__file__).parent / "fixtures" / "search_homes.json").read_text(encoding="utf-8"))


def search_page(homes, total):
    """A minimal search page: the header count plus the results inside an escaped page-state string."""
    state = json.dumps(homes, separators=(",", ":")).replace('"', '\\"').replace("/", "\u002F")
    return (f'<div class="homes summary" data-rf-test-id="homes-description">{total:,} homes</div>'
            f'<script>window.__state = "...\\"dom\\":{{}},\\"homes\\":{state},\\"showHigher\\":true";</script>')


class ParseSearchPageTest(unittest.TestCase):
    def test_returns_the_embedded_homes_and_the_header_total(self):
        homes, total = parsing.parse_search_page(search_page(HOMES, 4951))
        self.assertEqual(total, 4951)
        self.assertEqual(homes, HOMES)

    def test_page_without_results_or_header(self):
        self.assertEqual(parsing.parse_search_page("<html>Access denied</html>"), ([], None))

    def test_header_without_results_array(self):
        page = '<div data-rf-test-id="homes-description">0 homes</div>'
        self.assertEqual(parsing.parse_search_page(page), ([], 0))

    def test_single_home_header(self):
        page = search_page(HOMES[:1], 1).replace("1 homes", "1 home")
        self.assertEqual(parsing.parse_search_page(page)[1], 1)


class CensusRowTest(unittest.TestCase):
    def test_new_listing(self):
        row = parsing.census_row(HOMES[0])
        self.assertEqual(row["mls"], "X14091584")
        self.assertEqual((row["property_id"], row["listing_id"]), ("149579893", "224493705"))
        self.assertEqual((row["status"], row["list_price"], row["dom_redfin"]), ("Active", 783000, 1))
        self.assertEqual((row["latitude"], row["longitude"]), (45.3666411, -75.6859979))
        self.assertEqual(row["url"], "https://www.redfin.ca/on/ottawa/2713-Dupont-St-K1V-8N2/home/149579893")

    def test_hidden_fields_become_none_not_errors(self):
        row = parsing.census_row(HOMES[0])  # sqFt / yearBuilt / hoa carry only a "level", no value
        self.assertIsNone(row["sqft"])
        self.assertIsNone(row["year_built"])

    def test_listing_without_a_price(self):
        row = parsing.census_row(HOMES[2])
        self.assertIsNone(row["list_price"])
        self.assertEqual(row["status"], "Off Market")

    def test_condo_unit_number(self):
        self.assertTrue(parsing.census_row(HOMES[3])["unit"].startswith("#"))

    def test_row_is_flat_and_json_serialisable(self):
        for home in HOMES:
            json.dumps(parsing.census_row(home))


if __name__ == "__main__":
    unittest.main()
