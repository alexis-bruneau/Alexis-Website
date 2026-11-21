#!/usr/bin/env python3
"""The daily run, by hand: new SOLD listings, then today's ACTIVE listings, then silver is rebuilt and the app reloaded.

Each step is a subprocess (like redfin.pipeline). Needs AZURE_STORAGE_CONNECTION_STRING and the Redfin cookies, like the
steps themselves. This contacts redfin.ca (discovery, 200 sold pages, the ~41-page census, 200 active pages): run it
yourself, once a day, not in a loop.

  sold block     1 extract.discover_sold   new sold URLs into the queue file (edits the committed data/queue/property_urls.txt)
                 2 extract.fetch_sold      up to --limit-sold pages into bronze/<day>/
                 3 transform.sold          --incremental --out-prefix silver (also when step 2 stopped early)
                 4 transform.text          --incremental --out-prefix silver: listing descriptions (nothing on the maps reads them yet)
  active block   5 extract.census          today's snapshot of every for-sale home
                 6 extract.fetch_active    up to --limit-active detail pages (new listings first, then the stalest pages)
                 7 transform.active        silver/active/listings.parquet, minus homes that are already sold (reconciled_out.parquet)
  at the end     8 verify                  silver is as new as bronze and the census (read-only); stale = failed
                 9 app refresh             POST /refresh-data on the running Flask app, so the maps show the new silver

The maps read silver, not bronze: a fetch without its transform changes nothing on screen, and a running app keeps its
in-memory copy until it is refreshed. Steps 8 and 9 exist so that this cannot go unnoticed.

Failures: if discovery fails, fetch_sold is skipped; the other steps still run. The two blocks are independent: a failed
sold block does not stop the active block, and the reverse. An incomplete census (exit code 2) still goes on to
fetch_active and transform.active with what was saved, and the summary says so. If the census fails outright, fetch_active
is skipped (it would work from an old census) and transform.active still runs. An app that is not running is not a
failure (it loads the new data when it starts). The exit code is 1 when any step exited non-zero.

    python -m redfin.daily
    python -m redfin.daily --limit-sold 50 --limit-active 50
    python -m redfin.daily --skip-sold          # active block only
    python -m redfin.daily --skip-active        # sold block only
    python -m redfin.daily --transform-only     # rebuild silver from the bronze already saved; does NOT contact redfin.ca
"""
import argparse
import os
import subprocess
import sys
import tempfile

import requests

from redfin import config, runstats

DISCOVER = "redfin.extract.discover_sold"
FETCH_SOLD = "redfin.extract.fetch_sold"
TRANSFORM_SOLD = "redfin.transform.sold"
TRANSFORM_TEXT = "redfin.transform.text"
CENSUS = "redfin.extract.census"
FETCH_ACTIVE = "redfin.extract.fetch_active"
TRANSFORM_ACTIVE = "redfin.transform.active"
VERIFY = "redfin.verify"
REFRESH = "app.refresh"       # not a subprocess: a request to the running Flask app
STEPS = (DISCOVER, FETCH_SOLD, TRANSFORM_SOLD, TRANSFORM_TEXT, CENSUS, FETCH_ACTIVE, TRANSFORM_ACTIVE, VERIFY, REFRESH)
SOLD_STEPS = (DISCOVER, FETCH_SOLD, TRANSFORM_SOLD, TRANSFORM_TEXT)
CENSUS_INCOMPLETE = 2   # extract.census: blocked or coverage problems; what it saved is still usable
DEFAULT_REFRESH_URL = "http://127.0.0.1:5000/refresh-data"   # `python app/main.py` listens on port 5000
STOPPED = "   (stopped early after repeated failures: blocked or cookies expired?)"


def run(module, env, *args):
    print(f"\n=== {module} ===", flush=True)
    return subprocess.call([sys.executable, "-m", module, *args], env=env)


def skip(module, why):
    print(f"\n=== {module} === skipped: {why}", flush=True)


def refresh_app(url):
    """Asks the running Flask app to reload silver (POST /refresh-data). Returns (exit code, note): 0 reloaded,
    None the app is not running (not a failure: it loads the new data when it starts), 1 the reload failed."""
    print(f"\n=== {REFRESH} ===", flush=True)
    try:
        resp = requests.post(url, timeout=120)
    except requests.exceptions.ConnectionError:
        print(f"nothing is listening at {url}: the app will load the new data when it is started.", flush=True)
        return None, "not running; it loads the new data when started"
    except requests.RequestException as e:
        print(f"refresh failed: {e}", flush=True)
        return 1, f"FAILED ({type(e).__name__})"
    try:
        body = resp.json()
    except ValueError:
        body = {}
    sold_ok, active_ok = body.get("success"), (body.get("active") or {}).get("success")
    note = f"sold: {body.get('message')}; active: {(body.get('active') or {}).get('message')}"
    print(note, flush=True)
    if not resp.ok or not sold_ok or not active_ok:
        return 1, f"FAILED ({note if body else 'HTTP ' + str(resp.status_code)})"
    return 0, note


def queue_size():
    """Number of URLs in the sold queue file."""
    if not config.URLS_FILE.exists():
        return 0
    with config.URLS_FILE.open("r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def _n(stats, step, key):
    value = stats.get(step, {}).get(key)
    return "n/a" if value is None else f"{value:,}" if isinstance(value, int) and not isinstance(value, bool) else str(value)


def _status(code):
    return "skipped" if code is None else "ok" if code == 0 else f"FAILED (exit {code})"


def summary(codes, stats, queued, refresh_note=None):
    """The final report, as a list of lines. codes is {step: exit code, or None when the step did not run}."""
    lines = ["", "=== summary ===", "steps:"]
    for step, code in codes.items():
        label = _status(code)
        if step == CENSUS and code == CENSUS_INCOMPLETE:
            label = "INCOMPLETE (exit 2)"
        if step == VERIFY and code == 1 and any(stats.get("verify", {}).get(k) is False for k in ("sold_fresh", "active_fresh")):
            label = "STALE (silver is behind)"   # the check ran and found a problem, as opposed to crashing
        if step == REFRESH and code is None and refresh_note:
            label = "skipped (app not running)" if refresh_note.startswith("not running") else "skipped"
        lines.append(f"  {step.removeprefix('redfin.'):<22} {label}")

    if any(codes[s] is not None for s in SOLD_STEPS):
        note = STOPPED if stats.get("fetch_sold", {}).get("stopped_early") else ""
        lines += ["sold:",
                  f"  new URLs queued          {queued:,}",
                  f"  sold pages fetched       {_n(stats, 'fetch_sold', 'saved')}{note}",
                  f"  sold rows added          {_n(stats, 'transform.sold', 'rows_added')}",
                  f"  descriptions added       {_n(stats, 'transform.text', 'rows_added')}"]

    if codes[CENSUS] is not None:
        size = _n(stats, "census", "listings")
        if codes[CENSUS] == 0:
            census_line = f"yes ({size} listings; Redfin's header said {_n(stats, 'census', 'header_total')})"
        elif codes[CENSUS] == CENSUS_INCOMPLETE:
            census_line = (f"NO, incomplete ({size} listings saved). Active rows were built from what was saved, "
                           f"so the active list is partial")
        else:
            census_line = f"NO, the census step failed (exit {codes[CENSUS]}); today's census was not saved"
        note = STOPPED if stats.get("fetch_active", {}).get("stopped_early") else ""
        lines += ["active:",
                  f"  census complete          {census_line}",
                  f"  active pages fetched     {_n(stats, 'fetch_active', 'saved')}{note}"]
    if codes[TRANSFORM_ACTIVE] is not None:
        if codes[CENSUS] is None:
            lines += ["active:"]
        lines += [f"  active rows written      {_n(stats, 'transform.active', 'rows_written')}",
                  f"  removed because sold     {_n(stats, 'transform.active', 'removed_sold')}"]

    if codes[VERIFY] is not None:
        v = stats.get("verify", {})
        lines.append("silver up to date:")
        if "sold_fresh" in v:
            lines.append(f"  sold                     {'yes' if v['sold_fresh'] else 'NO'} "
                         f"({_n(stats, 'verify', 'sold_pending')} of the {_n(stats, 'verify', 'sold_pages')} pages of bronze day "
                         f"{_n(stats, 'verify', 'sold_day')} are not in silver)")
        if "active_fresh" in v:
            lines.append(f"  active                   {'yes' if v['active_fresh'] else 'NO'} "
                         f"(snapshot as of {_n(stats, 'verify', 'active_as_of')}, {_n(stats, 'verify', 'active_rows')} rows; "
                         f"latest census {_n(stats, 'verify', 'census_day')})")
    if codes[REFRESH] is not None or refresh_note:
        lines += ["website:", f"  app reloaded             {refresh_note or _status(codes[REFRESH])}"]
    return lines


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit-sold", type=int, default=200, help="max sold pages to fetch (default 200; 0 = every pending URL)")
    ap.add_argument("--limit-active", type=int, default=200, help="max active detail pages to fetch (default 200)")
    ap.add_argument("--skip-sold", action="store_true", help="skip the sold block")
    ap.add_argument("--skip-active", action="store_true", help="skip the active block")
    ap.add_argument("--transform-only", action="store_true",
                    help="no discovery, fetch or census: only the transforms, the check and the refresh (never contacts redfin.ca)")
    ap.add_argument("--refresh-url", default=DEFAULT_REFRESH_URL, help=f"the app's reload endpoint (default {DEFAULT_REFRESH_URL})")
    ap.add_argument("--no-refresh", action="store_true", help="do not ask the app to reload")
    args = ap.parse_args(argv)
    if args.skip_sold and args.skip_active:
        ap.error("--skip-sold and --skip-active together leave nothing to run")
    fetch = not args.transform_only

    fd, stats_path = tempfile.mkstemp(prefix="redfin_daily_", suffix=".json")
    os.close(fd)
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"                 # the steps print emoji; a redirected Windows console would crash on cp1252
    env["REDFIN_STATS_FILE"] = stats_path   # the steps put their counts here (runstats.report)
    codes = dict.fromkeys(STEPS)
    queue_before = queue_after = queue_size()
    refresh_note = None

    try:
        if not args.skip_sold:
            if fetch:
                codes[DISCOVER] = run(DISCOVER, dict(env))
                queue_after = queue_size()
                if codes[DISCOVER] == 0:
                    codes[FETCH_SOLD] = run(FETCH_SOLD, {**env, "MAX_SCRAPE_COUNT": str(args.limit_sold)})
                else:
                    skip(FETCH_SOLD, "discovery failed")
            # Even when fetch_sold stopped early or was skipped: pages already in bronze should reach silver.
            codes[TRANSFORM_SOLD] = run(TRANSFORM_SOLD, dict(env), "--incremental", "--out-prefix", "silver")
            codes[TRANSFORM_TEXT] = run(TRANSFORM_TEXT, dict(env), "--incremental", "--out-prefix", "silver")

        if not args.skip_active:
            if fetch:
                codes[CENSUS] = run(CENSUS, dict(env))
                if codes[CENSUS] in (0, CENSUS_INCOMPLETE):
                    codes[FETCH_ACTIVE] = run(FETCH_ACTIVE, dict(env), "--n", str(args.limit_active))
                else:
                    skip(FETCH_ACTIVE, "the census failed")
            codes[TRANSFORM_ACTIVE] = run(TRANSFORM_ACTIVE, dict(env))

        # Runs whatever happened above: the check is what notices a step that did not do its job.
        flags = (["--sold"] if not args.skip_sold else []) + (["--active"] if not args.skip_active else [])
        codes[VERIFY] = run(VERIFY, dict(env), *flags)
        if not args.no_refresh:
            codes[REFRESH], refresh_note = refresh_app(args.refresh_url)

        stats = runstats.read(stats_path)
    finally:
        os.remove(stats_path)

    print("\n".join(summary(codes, stats, max(0, queue_after - queue_before), refresh_note)), flush=True)
    failed = [s for s, code in codes.items() if code]
    if failed:
        print(f"\nExit 1: {len(failed)} step(s) failed: {', '.join(s.removeprefix('redfin.') for s in failed)}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
