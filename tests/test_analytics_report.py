"""scripts/analytics_report.py: the numbers it derives from the page-view rows."""
import importlib.util
import os
from datetime import date

spec = importlib.util.spec_from_file_location(
    "analytics_report", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "analytics_report.py"))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

TODAY = date(2026, 11, 22)


def row(day, visitor, device="desktop", referrer=""):
    return {"PartitionKey": day, "visitor_hash": visitor, "device": device, "referrer_host": referrer}


def test_views_and_uniques_per_day_with_empty_days_filled_in():
    rows = [row("2026-11-22", "a"), row("2026-11-22", "a"), row("2026-11-22", "b"), row("2026-11-20", "a")]
    summary = report.summarize(rows, days=3, today=TODAY)
    assert summary["days"] == [("2026-11-20", 1, 1), ("2026-11-21", 0, 0), ("2026-11-22", 3, 2)]


def test_rows_before_the_window_are_ignored():
    summary = report.summarize([row("2026-11-01", "a"), row("2026-11-22", "b")], days=7, today=TODAY)
    assert sum(v for _, v, _ in summary["days"]) == 1
    assert sum(summary["devices"].values()) == 1


def test_device_split_and_referrers():
    rows = [row("2026-11-22", "a", "mobile", "www.google.com"), row("2026-11-22", "b", "mobile", "www.google.com"),
            row("2026-11-22", "c", "desktop", "")]
    summary = report.summarize(rows, days=1, today=TODAY)
    assert summary["devices"] == {"mobile": 2, "desktop": 1}
    assert summary["referrers"].most_common() == [("www.google.com", 2), ("(direct / none)", 1)]


def test_the_text_report_renders_and_warns_about_a_broken_visitor_address():
    rows = [row("2026-11-22", "same") for _ in range(60)]
    text = report.format_report(report.summarize(rows, days=2, today=TODAY), top=5)
    assert "2026-11-22" in text and "Device split" in text and "PROXY_HOPS" in text
    assert "No page views" in report.format_report(report.summarize([], days=2, today=TODAY), top=5)
