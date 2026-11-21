"""Shared Playwright helpers for every stage that loads redfin.ca pages (discovery, sold extract, census, active extract).

Keep all redfin.ca browser settings here so a change (user agent, cookie handling, blocked resources) applies to
every fetcher at once.
"""
import json
import time

from redfin import config

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")


def load_redfin_cookies(path=None):
    """The .redfin.ca cookies from the exported Chrome cookie file, ready for context.add_cookies(); [] if no file."""
    path = path or config.COOKIE_FILE
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        raw = json.load(fh)
    expire = int(time.time()) + 30 * 24 * 3600
    return [{**c, "sameSite": "Lax", "expires": expire} for c in raw if ".redfin.ca" in c.get("domain", "")]


def block_heavy(route):
    """Playwright route handler: skip images, media and fonts (faster, and less load on redfin.ca)."""
    if route.request.resource_type in ("image", "media", "font"):
        route.abort()
    else:
        route.continue_()


def fetch_html(url, context):
    """Returns the rendered HTML of one listing page, or None on error / login wall."""
    page = context.new_page()
    page.route("**/*", block_heavy)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(1.5)  # let JS hydrate (Redfin needs this)
        if "/login" in page.url:
            print(f"🔒 Login page encountered at {url}")
            return None
        return page.content()
    except Exception as e:
        print(f"⚠️ Error/Timeout at {url}: {e}")
        return None
    finally:
        page.close()
