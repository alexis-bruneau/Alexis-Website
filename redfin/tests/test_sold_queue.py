"""The sold queue's order: discovery puts what it finds (newest sale first) on top, fetch_sold takes from the top, so the
newest sales are fetched first and older queued ones get what is left of the limit. The browser and Azure are faked:
nothing touches redfin.ca or Azure."""
from unittest import mock

import pytest

from redfin import config
from redfin.extract import discover_sold, fetch_sold


def url(pid, slug="ottawa"):
    return f"https://www.redfin.ca/on/{slug}/{pid}-main-st/home/{pid}"


@pytest.fixture
def queue(tmp_path, monkeypatch):
    path = tmp_path / "property_urls.txt"
    monkeypatch.setattr(config, "URLS_FILE", path)

    def write(*urls):
        path.write_text("".join(f"{u}\n" for u in urls), encoding="utf-8")

    write.read = lambda: path.read_text(encoding="utf-8").splitlines()
    return write


def test_found_urls_go_on_top_in_the_order_found(queue):
    queue(url(1), url(2))
    discover_sold.save_urls([url(10), url(11)])
    assert queue.read() == [url(10), url(11), url(1), url(2)]


def test_a_queued_home_found_again_moves_up_and_is_listed_once(queue):
    queue(url(1), url(2, slug="kanata"), url(3))
    discover_sold.save_urls([url(10), url(2)])   # home 2 again, spelled differently
    assert queue.read() == [url(10), url(2), url(1), url(3)]


def test_the_queue_file_is_created_when_missing(queue):
    discover_sold.save_urls([url(10)])
    assert queue.read() == [url(10)]


def discover(search_results, in_silver):
    """Runs discover_sold.main over one page of search results (newest sale first)."""
    links = [mock.Mock(**{"get_attribute.return_value": u}) for u in search_results]
    page = mock.MagicMock()

    def locator(selector):
        found = mock.MagicMock()
        found.all.return_value = links
        found.count.return_value = int(selector.startswith("text=No results") and page.goto.call_count > 1)
        found.first.count.return_value = 0   # no Next button: the loop asks for page-2, which says "No results"
        return found

    page.locator.side_effect = locator
    playwright = mock.MagicMock()
    playwright.__enter__.return_value.chromium.launch.return_value.new_context.return_value.new_page.return_value = page
    with mock.patch.object(discover_sold, "sync_playwright", return_value=playwright), \
            mock.patch.object(discover_sold, "get_latest_azure_urls", return_value=set(in_silver)), \
            mock.patch.object(discover_sold, "load_redfin_cookies", return_value=[]), \
            mock.patch("sys.argv", ["discover_sold"]), mock.patch("time.sleep"):
        discover_sold.main()


def fetched(limit, in_silver):
    """The URLs fetch_sold.main asks for, in order."""
    asked = []
    with mock.patch.object(fetch_sold, "sync_playwright", return_value=mock.MagicMock()), \
            mock.patch.object(fetch_sold, "load_redfin_cookies", return_value=[]), \
            mock.patch.object(fetch_sold, "known_property_ids",
                              return_value={fetch_sold.parsing.property_id(u) for u in in_silver}), \
            mock.patch.object(fetch_sold, "fetch_html", side_effect=lambda u, context: asked.append(u) or "<html></html>"), \
            mock.patch.object(fetch_sold.store, "put_bronze", return_value=True):
        fetch_sold.main(limit=limit)
    return asked


def test_new_sales_are_fetched_before_the_backlog_and_leftover_slots_go_to_older_ones(queue):
    # The queue as committed: a backlog (3, 4) that Redfin lists below the new sales, and one the search no longer shows (5).
    queue(url(1), url(3), url(4), url(5))
    silver = [url(1)]
    discover([url(10), url(11), url(1), url(3), url(4), url(12)], silver)

    assert queue.read() == [url(10), url(11), url(3), url(4), url(12), url(1), url(5)]
    assert fetched(2, silver) == [url(10), url(11)]                                   # the new ones first
    assert fetched(10, silver) == [url(10), url(11), url(3), url(4), url(12), url(5)]  # then the rest, never silver's


def test_the_limit_on_found_urls_counts_everything_not_in_silver(queue, monkeypatch):
    monkeypatch.setattr(config, "MAX_NEW_URLS", 2)
    queue(url(3))
    discover([url(10), url(3), url(11)], in_silver=[])
    assert queue.read()[:2] == [url(10), url(3)]
