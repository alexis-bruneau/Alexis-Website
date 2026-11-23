"""Page-view counting (app/analytics.py). Azure is replaced by in-memory tables (conftest.py): nothing here touches the network."""
import logging
import threading
from datetime import datetime, timedelta, timezone

import pytest
from flask import Flask

from app import analytics

CHROME_WIN = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0.0.0 Safari/537.36")
IPHONE = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
          "Version/17.1 Mobile/15E148 Safari/604.1")
IPAD = ("Mozilla/5.0 (iPad; CPU OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 "
        "Mobile/15E148 Safari/604.1")
ANDROID_TABLET = "Mozilla/5.0 (Linux; Android 13; SM-X700) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
FIREFOX = "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0"
EDGE = CHROME_WIN + " Edg/120.0.0.0"
DAY1 = datetime(2026, 11, 22, 15, 30, tzinfo=timezone.utc)
DAY2 = DAY1 + timedelta(days=1)


# ---------------- Classification -------------------------------------------
@pytest.mark.parametrize("ua, device, browser", [
    (CHROME_WIN, "desktop", "Chrome"), (IPHONE, "mobile", "Safari"), (IPAD, "tablet", "Safari"),
    (ANDROID_TABLET, "tablet", "Chrome"), (FIREFOX, "desktop", "Firefox"), (EDGE, "desktop", "Edge"),
    ("SomethingOdd/1.0 (compatible)", "desktop", "Other"),
])
def test_device_and_browser(ua, device, browser):
    assert analytics.classify_user_agent(ua) == (device, browser)


@pytest.mark.parametrize("ua", [
    "", None, "x", "curl/8.4.0", "python-requests/2.31.0", "Googlebot/2.1", "Mozilla/5.0 (compatible; bingbot/2.0)",
    "Mozilla/5.0 HeadlessChrome/120.0.0.0 Safari/537.36", "kube-probe/1.27", "UptimeRobot/2.0", "Go-http-client/1.1",
    "facebookexternalhit/1.1", "Slackbot-LinkExpanding 1.0",
])
def test_bots_and_empty_user_agents_are_bots(ua):
    assert analytics.is_bot(ua)


@pytest.mark.parametrize("ua", [CHROME_WIN, IPHONE, FIREFOX, EDGE, "Mozilla/5.0 (Linux; Android 11; CUBOT P40) Chrome/120 Mobile Safari/537.36"])
def test_real_browsers_are_not_bots(ua):
    assert not analytics.is_bot(ua)


def test_referrer_keeps_the_host_only():
    assert analytics.referrer_host("https://www.Google.com:443/search?q=my+address#x") == "www.google.com"
    assert analytics.referrer_host("") == ""
    assert analytics.referrer_host(None) == ""
    assert analytics.referrer_host("android-app://com.google.android.gm") == ""


# ---------------- Salt and visitor hash ------------------------------------
def test_same_ip_and_ua_same_day_same_hash(tables):
    h = analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY1)
    assert h == analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY1 + timedelta(hours=5))
    assert len(h) == 16 and int(h, 16) >= 0


def test_other_ip_or_other_browser_other_hash(tables):
    h = analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY1)
    assert h != analytics.visitor_hash("203.0.113.10", CHROME_WIN, DAY1)
    assert h != analytics.visitor_hash("203.0.113.9", FIREFOX, DAY1)


def test_next_day_different_hash(tables):
    assert analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY1) != analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY2)


def test_a_port_in_the_forwarded_address_does_not_change_the_hash(tables):
    h = analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY1)
    assert h == analytics.visitor_hash("203.0.113.9:51234", CHROME_WIN, DAY1)
    assert analytics.visitor_hash("[2001:db8::1]:443", CHROME_WIN, DAY1) == analytics.visitor_hash("2001:db8::1", CHROME_WIN, DAY1)


def test_salt_is_cached_after_the_first_read(tables, monkeypatch):
    analytics.get_salt(DAY1)
    monkeypatch.setattr(analytics, "_get_clients", lambda: pytest.fail("the table was read again"))
    analytics.get_salt(DAY1 + timedelta(hours=1))


def test_two_replicas_agree_on_the_salt(tables, monkeypatch):
    """Each replica has its own empty cache and tries to create the day's salt; the second finds it already there."""
    first = analytics.get_salt(DAY1)
    monkeypatch.setattr(analytics, "_salt_cache", {})            # another replica / worker process
    assert analytics.get_salt(DAY1) == first
    assert len(tables[1].rows) == 1


def test_salts_older_than_two_days_are_deleted(tables):
    salts = tables[1]
    for age in (5, 3, 2, 1):
        day = (DAY1 - timedelta(days=age)).strftime("%Y-%m-%d")
        salts.rows[("salt", day)] = {"PartitionKey": "salt", "RowKey": day, "salt": "old"}
    analytics.get_salt(DAY1)
    assert sorted(rk for _, rk in salts.rows) == ["2026-11-20", "2026-11-21", "2026-11-22"]


def test_monthly_rotation_gives_one_salt_per_month(tables, monkeypatch):
    monkeypatch.setattr(analytics, "SALT_ROTATION", "month")
    a = analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY1)
    assert a == analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY1 + timedelta(days=5))     # 27 Nov
    assert a != analytics.visitor_hash("203.0.113.9", CHROME_WIN, DAY1 + timedelta(days=10))    # 2 Dec
    assert ("salt", "2026-11") in tables[1].rows


# ---------------- What gets written ----------------------------------------
def test_the_row_has_the_documented_fields_and_nothing_else(tables):
    pageviews = tables[0]
    analytics._process({"path": "/ottawa_map", "referrer_host": "www.google.com", "user_agent": CHROME_WIN,
                        "ip": "203.0.113.9", "device": "desktop", "browser": "Chrome", "now": DAY1})
    (row,) = pageviews.rows.values()
    assert set(row) == {"PartitionKey", "RowKey", "viewed_at", "path", "referrer_host", "visitor_hash", "device", "browser"}
    assert row["PartitionKey"] == "2026-11-22"
    assert row["RowKey"].startswith("153000000000-")
    assert row["viewed_at"] == "2026-11-22T15:30:00+00:00"
    assert (row["path"], row["referrer_host"], row["device"], row["browser"]) == ("/ottawa_map", "www.google.com", "desktop", "Chrome")
    assert "203.0.113.9" not in repr(row) and "Mozilla" not in repr(row)


def test_row_keys_sort_by_time_and_never_collide(tables):
    pageviews = tables[0]
    for minute in (5, 1, 3, 3):
        analytics._process({"path": "/", "referrer_host": "", "user_agent": CHROME_WIN, "ip": "1.1.1.1", "device": "desktop",
                            "browser": "Chrome", "now": DAY1.replace(minute=minute)})
    keys = [rk for _, rk in pageviews.rows]
    assert len(keys) == 4
    assert [k[:6] for k in sorted(keys)] == ["150100", "150300", "150300", "150500"]


# ---------------- Which requests count -------------------------------------
@pytest.fixture
def sent(tables, monkeypatch):
    """Page views handed to the writer, captured instead of written."""
    jobs = []
    monkeypatch.setattr(analytics, "_enqueue", lambda job: jobs.append(job) or True)
    return jobs


def make_app(mimetype="text/html", status=200):
    app = Flask(__name__)

    @app.route("/ottawa_map", methods=["GET", "POST"])
    @app.route("/")
    @app.route("/ottawa_active")
    @app.route("/points.json")
    def page():
        from flask import Response
        return Response("x", status=status, mimetype=mimetype)

    @app.after_request
    def after(response):
        from flask import request
        analytics.track_request(request, response)
        return response

    return app.test_client()


HUMAN = {"User-Agent": CHROME_WIN, "Referer": "https://www.google.com/search?q=secret+query"}


def test_a_real_visit_is_counted(sent):
    make_app().get("/ottawa_map", headers=HUMAN, environ_overrides={"REMOTE_ADDR": "203.0.113.9"})
    (job,) = sent
    assert (job["path"], job["referrer_host"], job["ip"], job["device"], job["browser"]) == \
        ("/ottawa_map", "www.google.com", "203.0.113.9", "desktop", "Chrome")
    assert "secret" not in repr({k: v for k, v in job.items() if k != "user_agent"})


def test_the_root_path_is_tracked_too(sent):
    make_app().get("/", headers=HUMAN)
    assert [j["path"] for j in sent] == ["/"]


@pytest.mark.parametrize("path", ["/ottawa_active", "/points.json"])
def test_untracked_paths_are_ignored(sent, path):
    make_app().get(path, headers=HUMAN)
    assert sent == []


def test_head_and_post_are_ignored(sent):
    client = make_app()
    client.head("/ottawa_map", headers=HUMAN)
    client.post("/ottawa_map", headers=HUMAN)
    assert sent == []


@pytest.mark.parametrize("headers", [
    {"DNT": "1"}, {"Sec-GPC": "1"},
    {"Sec-Purpose": "prefetch"}, {"Sec-Purpose": "prefetch;prerender"}, {"Purpose": "prefetch"}, {"X-Moz": "prefetch"},
    {"User-Agent": "Googlebot/2.1"}, {"User-Agent": "curl/8.4.0"}, {"User-Agent": ""},
])
def test_do_not_track_prefetch_and_bots_are_not_counted(sent, headers):
    make_app().get("/ottawa_map", headers={**HUMAN, **headers})
    assert sent == []


def test_dnt_zero_does_not_block(sent):
    make_app().get("/ottawa_map", headers={**HUMAN, "DNT": "0"})
    assert len(sent) == 1


def test_only_200_html_is_counted(sent):
    make_app(status=302).get("/ottawa_map", headers=HUMAN)
    make_app(status=404).get("/ottawa_map", headers=HUMAN)
    make_app(mimetype="application/json").get("/ottawa_map", headers=HUMAN)
    assert sent == []


def test_a_user_agent_without_a_referrer_is_counted_with_an_empty_host(sent):
    make_app().get("/ottawa_map", headers={"User-Agent": CHROME_WIN})
    assert sent[0]["referrer_host"] == ""


# ---------------- No connection string, never breaking a request ------------
def test_without_a_connection_string_nothing_happens(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.setattr(analytics, "_get_clients", lambda: pytest.fail("Azure was contacted"))
    monkeypatch.setattr(analytics, "_enqueue", lambda job: pytest.fail("a job was queued"))
    assert analytics.record_pageview("/ottawa_map", "", CHROME_WIN, "1.1.1.1") is False
    assert make_app().get("/ottawa_map", headers=HUMAN).status_code == 200


def test_record_pageview_ignores_untracked_paths_and_bots(sent):
    assert analytics.record_pageview("/points.json", "", CHROME_WIN, "1.1.1.1") is False
    assert analytics.record_pageview("/ottawa_map", "", "Googlebot/2.1", "1.1.1.1") is False
    assert sent == []


def test_a_query_string_is_never_kept(sent):
    assert analytics.record_pageview("/ottawa_map?street=12+Elm", "", CHROME_WIN, "1.1.1.1")
    assert sent[0]["path"] == "/ottawa_map"


def test_azure_failures_are_swallowed_and_the_connection_string_is_not_logged(tables, monkeypatch, caplog):
    def boom():
        raise RuntimeError("AccountKey=FAKE-SECRET-KEY could not connect")
    monkeypatch.setattr(analytics, "_get_clients", boom)
    with caplog.at_level(logging.DEBUG, logger="analytics"):
        analytics._process({"path": "/", "referrer_host": "", "user_agent": CHROME_WIN, "ip": "203.0.113.9",
                            "device": "desktop", "browser": "Chrome", "now": DAY1})
    assert "FAKE-SECRET-KEY" not in caplog.text and "203.0.113.9" not in caplog.text and "Mozilla" not in caplog.text
    assert "RuntimeError" in caplog.text


def test_the_request_does_not_wait_for_the_write(tables, monkeypatch):
    """The page view is handed to the background thread; a slow Azure call does not hold the response."""
    release, started, written = threading.Event(), threading.Event(), []

    def slow_write(job):
        started.set()
        release.wait(5)
        written.append(job)

    monkeypatch.setattr(analytics, "_write_pageview", slow_write)
    assert analytics.record_pageview("/ottawa_map", "", CHROME_WIN, "1.1.1.1") is True
    assert started.wait(5) and written == []          # the call above already returned while the write is still running
    release.set()
    analytics._queue.join()
    assert len(written) == 1


def test_a_full_queue_drops_the_view_instead_of_blocking(tables, monkeypatch):
    import queue
    from types import SimpleNamespace
    full = queue.Queue(maxsize=1)
    full.put("busy")
    monkeypatch.setattr(analytics, "_queue", full)
    monkeypatch.setattr(analytics, "_worker", SimpleNamespace(is_alive=lambda: True))   # a writer that never consumes
    assert analytics.record_pageview("/ottawa_map", "", CHROME_WIN, "1.1.1.1") is False
