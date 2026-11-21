"""Tests for parsing.extract_remarks (the listing description). Run:  python -m pytest redfin

The page fragments copy the structure of real redfin.ca listing pages: the description sits in
#marketing-remarks-scroll, inside a #marketingRemarks-preview wrapper that also holds a "Show more" button."""
import unittest

from redfin.transform import parsing

SHOW_MORE = ('<div class="ExpandableLink__bottomLink"><button aria-expanded="false" class="ExpandableLink clickable" '
             'type="button">Show more<svg><path d="M1 1"></path></svg></button></div>')


def page(inner):
    """A listing page around the given content of the remarks block."""
    return ('<html><body><h1>108 Lisgar St</h1>'
            '<div class="expandableSection collapsed marketingRemarks" data-rf-test-id="listingRemarks" '
            'id="marketingRemarks-preview"><div class="sectionContentContainer"><div class="sectionContent">'
            f'<div class="remarks" id="marketing-remarks-scroll">{inner}</div></div></div>{SHOW_MORE}</div>'
            '</body></html>')


class ExtractRemarksTest(unittest.TestCase):
    def test_plain_description(self):
        html = page('<p class=""><span>The Bay Willow is a bungalow with hardwood flooring.</span></p>')
        self.assertEqual(parsing.extract_remarks(html), "The Bay Willow is a bungalow with hardwood flooring.")

    def test_show_more_button_is_not_part_of_the_text(self):
        text = parsing.extract_remarks(page("<p><span>Bright corner unit.</span></p>"))
        self.assertNotIn("Show more", text)

    def test_inline_highlight_tags_add_no_spaces(self):
        html = page('<p><span>A paved driveway, <span class="highlightedTag">covered front porch</span>, and a patio.</span></p>')
        self.assertEqual(parsing.extract_remarks(html), "A paved driveway, covered front porch, and a patio.")

    def test_paragraphs_are_joined_with_a_space(self):
        html = page("<p><span>First paragraph.</span></p><p><span>Second paragraph.</span></p>")
        self.assertEqual(parsing.extract_remarks(html), "First paragraph. Second paragraph.")

    def test_whitespace_and_newlines_are_collapsed(self):
        html = page("<p><span>Sunny   south-facing\n   balcony.</span></p>")
        self.assertEqual(parsing.extract_remarks(html), "Sunny south-facing balcony.")

    def test_banner_asterisks_are_trimmed_but_the_promo_text_is_kept(self):
        html = page("<p><span> *  * OPEN HOUSE Sun Oct 18th, 2-4pm *  *  Space and comfort in this townhouse! </span></p>")
        text = parsing.extract_remarks(html)
        self.assertTrue(text.startswith("OPEN HOUSE Sun Oct 18th"))
        self.assertTrue(text.endswith("townhouse!"))

    def test_html_entities_are_decoded(self):
        html = page("<p><span>Steps to Tom &amp; Jerry&#39;s caf&eacute;.</span></p>")
        self.assertEqual(parsing.extract_remarks(html), "Steps to Tom & Jerry's café.")

    def test_description_without_paragraph_tags(self):
        self.assertEqual(parsing.extract_remarks(page("Just a bare text node.")), "Just a bare text node.")

    def test_page_without_a_remarks_block_returns_none(self):
        self.assertIsNone(parsing.extract_remarks("<html><body><h1>No description here</h1></body></html>"))

    def test_empty_remarks_block_returns_none(self):
        self.assertIsNone(parsing.extract_remarks(page("<p><span>  *  * </span></p>")))


if __name__ == "__main__":
    unittest.main()
