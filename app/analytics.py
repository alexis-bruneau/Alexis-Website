"""Privacy-friendly page-view counting for the sold map. What is and is not collected: docs/ANALYTICS.md.

One row per page view goes to Azure Table Storage (table `pageviews`), written from a background thread so a request
never waits on Azure and never fails because of it. No cookies, no stored IP addresses or user agents: the visitor id is
a salted hash and the salt is thrown away after SALT_KEEP_DAYS days.

This module must stay importable without Flask, the data and the Redfin package (scripts/analytics_report.py uses it).
"""
import hashlib
import logging
import os
import queue
import re
import secrets
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from azure.core.exceptions import ResourceExistsError

log = logging.getLogger("analytics")

# ---------------- Settings -------------------------------------------------
# Page views are counted for these paths only (the sold map). Add "/ottawa_active" here to count the active map too.
TRACKED_PATHS = {"/", "/ottawa_map"}

PAGEVIEWS_TABLE = "pageviews"
SALT_TABLE = "analyticssalt"
SALT_PARTITION = "salt"

# How often the salt changes, hence how long one person keeps the same visitor_hash: "day" or "month".
# "day": uniques exist per day only; a hash cannot be linked from one day to the next (the most private choice).
# "month": the same visitor is recognised for a calendar month, so monthly uniques become possible, at the price of
# being able to follow one (anonymous) visitor across a month. See docs/ANALYTICS.md.
SALT_ROTATION = "day"
SALT_KEEP_DAYS = 2   # day rotation: salts older than this are deleted (the one-month rotation keeps the previous month)

QUEUE_SIZE = 1000    # page views waiting to be written; a burst beyond this is dropped, never queued without limit

# ---------------- What counts as a person ----------------------------------
# Empty / tiny user agents and the common crawlers, uptime monitors, link-preview fetchers, HTTP libraries and headless
# browsers. Real browsers never contain any of these words.
BOT_RE = re.compile(
    r"(?<!cu)bot|crawl|spider|slurp|scrape|fetcher|headless|phantom|lighthouse|pagespeed|gtmetrix|pingdom|uptime|monitor|probe"
    r"|curl|wget|python|java/|go-http|okhttp|axios|node-fetch|httpclient|libwww|facebookexternalhit|embedly|preview"
    r"|whatsapp|telegram|discord|slack|skype|mediapartners|apis-google|validator|azure|kube-",
    re.IGNORECASE,
)
MIN_UA_LENGTH = 10


def is_bot(user_agent):
    """True for an empty / tiny user agent or one that matches BOT_RE."""
    ua = (user_agent or "").strip()
    return len(ua) < MIN_UA_LENGTH or bool(BOT_RE.search(ua))


def classify_user_agent(user_agent):
    """(device, browser): device is 'mobile', 'tablet' or 'desktop'; browser is a family name or 'Other'.
    iPadOS 13+ Safari sends a Mac user agent, so those iPads count as desktop (a known limit)."""
    ua = (user_agent or "").lower()
    if "ipad" in ua or "tablet" in ua or "kindle" in ua or "silk/" in ua or ("android" in ua and "mobile" not in ua):
        device = "tablet"
    elif "mobi" in ua or "iphone" in ua or "ipod" in ua:
        device = "mobile"
    else:
        device = "desktop"
    # Order matters: Edge, Opera and Samsung Internet also say "Chrome"; Chrome also says "Safari".
    if "edg/" in ua or "edga/" in ua or "edgios/" in ua:
        browser = "Edge"
    elif "opr/" in ua or "opera" in ua:
        browser = "Opera"
    elif "samsungbrowser" in ua:
        browser = "Samsung Internet"
    elif "firefox/" in ua or "fxios" in ua:
        browser = "Firefox"
    elif "chrome/" in ua or "crios" in ua:
        browser = "Chrome"
    elif "safari/" in ua:
        browser = "Safari"
    else:
        browser = "Other"
    return device, browser


def referrer_host(referer):
    """The host of a Referer header and nothing else (no path, no query, no port); '' when absent or not http(s)."""
    try:
        parsed = urlparse(referer or "")
        if parsed.scheme not in ("http", "https"):
            return ""
        return (parsed.hostname or "").lower()[:100]
    except ValueError:
        return ""


def _clean_ip(ip):
    """The bare address: drops a ':port' some proxies append. With a port in it, every connection would hash differently
    and one person would look like many."""
    ip = (ip or "").strip()
    if ip.startswith("["):                       # [2001:db8::1]:443
        return ip[1:].split("]")[0]
    if ip.count(":") == 1 and "." in ip:         # 203.0.113.9:51234 (IPv4 only; a bare IPv6 has several colons)
        return ip.split(":")[0]
    return ip


# ---------------- Azure Tables ---------------------------------------------
def _conn_str():
    return os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")


def is_enabled():
    """False (everything below does nothing) without a storage connection string: local development and tests."""
    return bool(_conn_str())


_clients = None
_clients_lock = threading.Lock()


def _get_clients():
    """(pageviews table, salt table), created on first use. The tables are created if they do not exist yet."""
    global _clients
    with _clients_lock:
        if _clients is None:
            from azure.data.tables import TableServiceClient  # not at import: tests and the web app start without it
            service = TableServiceClient.from_connection_string(_conn_str())
            _clients = (service.create_table_if_not_exists(PAGEVIEWS_TABLE),
                        service.create_table_if_not_exists(SALT_TABLE))
        return _clients


def _utcnow():
    return datetime.now(timezone.utc)


def _period_key(now):
    """The salt's name: the UTC date ('2026-11-22'), or the month ('2026-11') when SALT_ROTATION is 'month'."""
    return now.strftime("%Y-%m" if SALT_ROTATION == "month" else "%Y-%m-%d")


def _oldest_kept_key(now):
    """Salts named before this are deleted."""
    if SALT_ROTATION == "month":
        return (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    return (now - timedelta(days=SALT_KEEP_DAYS)).strftime("%Y-%m-%d")


_salt_cache = {}
_salt_lock = threading.Lock()


def get_salt(now=None):
    """The secret salt of `now`'s period. Several workers / replicas agree on it: each tries to create the row (only the
    first succeeds), then every one reads it back. The result is cached in memory. Fetching a new period's salt also
    deletes the salts that are too old, which is what makes older hashes impossible to reproduce."""
    now = now or _utcnow()
    key = _period_key(now)
    with _salt_lock:
        if key in _salt_cache:
            return _salt_cache[key]
        table = _get_clients()[1]
        try:
            table.create_entity({"PartitionKey": SALT_PARTITION, "RowKey": key, "salt": secrets.token_hex(32)})
        except ResourceExistsError:
            pass                                         # another worker created it first: use theirs
        salt = table.get_entity(SALT_PARTITION, key)["salt"]
        oldest = _oldest_kept_key(now)
        for cached in [k for k in _salt_cache if k < oldest]:
            del _salt_cache[cached]
        _salt_cache[key] = salt
        try:
            for row in list(table.query_entities("PartitionKey eq @p and RowKey lt @k",
                                                 parameters={"p": SALT_PARTITION, "k": oldest}, select=["RowKey"])):
                table.delete_entity(SALT_PARTITION, row["RowKey"])
        except Exception as exc:                         # cleanup is retried the next time a salt is fetched
            _log_failure("deleting old salts", exc)
        return salt


def visitor_hash(ip, user_agent, now=None):
    """16 hex characters identifying 'this IP + this browser, in this salt period'. The IP and the user agent only pass
    through memory here; they are never stored or logged."""
    material = "\0".join((get_salt(now), _clean_ip(ip), user_agent or ""))
    return hashlib.sha256(material.encode("utf-8", "replace")).hexdigest()[:16]


# ---------------- Writing --------------------------------------------------
def _log_failure(what, exc):
    """Logs the kind of error only: never its message (which could echo a URL) and never the connection string."""
    code = getattr(exc, "error_code", None) or getattr(exc, "status_code", None) or ""
    log.warning("analytics: %s failed (%s %s)", what, type(exc).__name__, code)


def _write_pageview(job):
    now = job["now"]
    entity = {
        "PartitionKey": now.strftime("%Y-%m-%d"),
        # Sorts by time; the random tail keeps two views in the same microsecond apart.
        "RowKey": f"{now.strftime('%H%M%S%f')}-{secrets.token_hex(4)}",
        "viewed_at": now.isoformat(),
        "path": job["path"],
        "referrer_host": job["referrer_host"],
        "visitor_hash": visitor_hash(job["ip"], job["user_agent"], now),
        "device": job["device"],
        "browser": job["browser"],
    }
    _get_clients()[0].create_entity(entity)


def _process(job):
    try:
        _write_pageview(job)
    except Exception as exc:
        _log_failure("writing a page view", exc)


_queue = queue.Queue(maxsize=QUEUE_SIZE)
_worker = None
_worker_lock = threading.Lock()


def _work():
    while True:
        job = _queue.get()
        try:
            _process(job)
        finally:
            _queue.task_done()


def _enqueue(job):
    """Hands the job to the single writer thread (started on first use, so it exists in each gunicorn worker process)."""
    global _worker
    with _worker_lock:
        if _worker is None or not _worker.is_alive():
            _worker = threading.Thread(target=_work, name="analytics-writer", daemon=True)
            _worker.start()
    try:
        _queue.put_nowait(job)
    except queue.Full:
        log.warning("analytics: write queue full, page view dropped")
        return False
    return True


def record_pageview(path, referrer_host, user_agent, ip, now=None):
    """Counts one page view, without waiting for Azure. Returns True when it was queued. Does nothing (returns False) when
    there is no connection string, the path is not tracked, or the user agent is a bot. The IP and the user agent are
    used only to compute the visitor hash on the writer thread."""
    if not is_enabled():
        return False
    path = (path or "").split("?")[0]
    if path not in TRACKED_PATHS or is_bot(user_agent):
        return False
    device, browser = classify_user_agent(user_agent)
    return _enqueue({"path": path, "referrer_host": referrer_host or "", "user_agent": user_agent, "ip": ip,
                     "device": device, "browser": browser, "now": now or _utcnow()})


def track_request(request, response):
    """The after_request check: records a page view for a plain 200 HTML GET of a tracked path from a real, non-prefetching
    browser that did not ask not to be tracked. Never raises."""
    try:
        if not is_enabled():
            return False
        if request.method != "GET" or request.path not in TRACKED_PATHS:    # HEAD, POST and everything else
            return False
        if response.status_code != 200 or response.mimetype != "text/html":
            return False
        headers = request.headers
        if headers.get("DNT") == "1" or headers.get("Sec-GPC") == "1":      # Do Not Track / Global Privacy Control
            return False
        purpose = " ".join(headers.get(h, "") for h in ("Sec-Purpose", "Purpose", "X-Moz", "X-Purpose")).lower()
        if "prefetch" in purpose:                                           # the browser guessed; nobody looked yet
            return False
        return record_pageview(request.path, referrer_host(headers.get("Referer")),
                               headers.get("User-Agent", ""), request.remote_addr)
    except Exception as exc:
        _log_failure("tracking a request", exc)
        return False
