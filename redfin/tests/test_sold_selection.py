from redfin import config
from redfin.transform import sold


def test_silver_starts_at_min_month(monkeypatch):
    monkeypatch.setattr(config, "SILVER_MIN_MONTH", "2025-06")
    entries = [
        ("2025-05-31", "A1", "b/A1"),   # before the cutoff: dropped
        ("2025-06-01", "A2", "b/A2"),   # first day kept
        ("2026-01-10", "A3", "b/A3"),
    ]
    assert [e[1] for e in sold.latest_bronze_per_name(entries)] == ["A2", "A3"]


def test_newest_page_decides(monkeypatch):
    monkeypatch.setattr(config, "SILVER_MIN_MONTH", "2025-06")
    entries = [("2025-04-02", "A1", "old"), ("2025-07-02", "A1", "new")]
    assert sold.latest_bronze_per_name(entries) == [("2025-07-02", "A1", "new")]
