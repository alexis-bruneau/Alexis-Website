#!/usr/bin/env python3
"""The daily run, by hand: new SOLD listings, then today's ACTIVE listings, then silver is rebuilt.

Each step is a subprocess (like redfin.pipeline). Needs AZURE_STORAGE_CONNECTION_STRING and the Redfin cookies, like the
steps themselves. This contacts redfin.ca (discovery, 200 sold pages, the ~41-page census, 200 active pages): run it
yourself, once a day, not in a loop.

  sold block     1 extract.discover_sold   new sold URLs into the queue file (edits the committed data/queue/property_urls.txt)
                 2 extract.fetch_sold      up to --limit-sold pages into bronze/<day>/
                 3 transform.sold          --incremental --out-prefix silver (also when step 2 stopped early)
  active block   4 extract.census          today's snapshot of every for-sale home
                 5 extract.fetch_active    up to --limit-active detail pages (new listings first, then the stalest pages)
                 6 transform.active        silver/active/listings.parquet, minus homes that are already sold (reconciled_out.parquet)

Failures: if discovery fails, fetch_sold is skipped; the other steps still run. The two blocks are independent: a failed
sold block does not stop the active block, and the reverse. An incomplete census (exit code 2) still goes on to
fetch_active and transform.active with what was saved, and the summary says so. If the census fails outright, fetch_active
is skipped (it would work from an old census) and transform.active still runs. The exit code is 1 when any step exited
non-zero.

    python -m redfin.daily
    python -m redfin.daily --limit-sold 50 --limit-active 50
    python -m redfin.daily --skip-sold          # active block only
    python -m redfin.daily --skip-active        # sold block only
"""
import argparse
import os
import subprocess
import sys
import tempfile

from redfin import config, runstats

DISCOVER = "redfin.extract.discover_sold"
FETCH_SOLD = "redfin.extract.fetch_sold"
TRANSFORM_SOLD = "redfin.transform.sold"
CENSUS = "redfin.extract.census"
FETCH_ACTIVE = "redfin.extract.fetch_active"
TRANSFORM_ACTIVE = "redfin.transform.active"
CENSUS_INCOMPLETE = 2   # extract.census: blocked or coverage problems; what it saved is still usable
STOPPED = "   (stopped early after repeated failures: blocked or cookies expired?)"


def run(module, env, *args):
    print(f"\n=== {module} ===", flush=True)
    return subprocess.call([sys.executable, "-m", module, *args], env=env)


def skip(module, why):
    print(f"\n=== {module} === skipped: {why}", flush=True)


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


def summary(codes, stats, queued):
    """The final report, as a list of lines. codes is {module: exit code, or None when the step did not run}."""
    lines = ["", "=== summary ===", "steps:"]
    for module, code in codes.items():
        label = _status(code)
        if module == CENSUS and code == CENSUS_INCOMPLETE:
            label = "INCOMPLETE (exit 2)"
        lines.append(f"  {module.removeprefix('redfin.'):<22} {label}")

    if any(codes[m] is not None for m in (DISCOVER, FETCH_SOLD, TRANSFORM_SOLD)):
        note = STOPPED if stats.get("fetch_sold", {}).get("stopped_early") else ""
        lines += ["sold:",
                  f"  new URLs queued          {queued:,}",
                  f"  sold pages fetched       {_n(stats, 'fetch_sold', 'saved')}{note}",
                  f"  sold rows added          {_n(stats, 'transform.sold', 'rows_added')}"]

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
                  f"  active pages fetched     {_n(stats, 'fetch_active', 'saved')}{note}",
                  f"  active rows written      {_n(stats, 'transform.active', 'rows_written')}",
                  f"  removed because sold     {_n(stats, 'transform.active', 'removed_sold')}"]
    return lines


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit-sold", type=int, default=200, help="max sold pages to fetch (default 200; 0 = every pending URL)")
    ap.add_argument("--limit-active", type=int, default=200, help="max active detail pages to fetch (default 200)")
    ap.add_argument("--skip-sold", action="store_true", help="skip the sold block (steps 1-3)")
    ap.add_argument("--skip-active", action="store_true", help="skip the active block (steps 4-6)")
    args = ap.parse_args(argv)
    if args.skip_sold and args.skip_active:
        ap.error("--skip-sold and --skip-active together leave nothing to run")

    fd, stats_path = tempfile.mkstemp(prefix="redfin_daily_", suffix=".json")
    os.close(fd)
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"                 # the steps print emoji; a redirected Windows console would crash on cp1252
    env["REDFIN_STATS_FILE"] = stats_path   # the steps put their counts here (runstats.report)
    codes = dict.fromkeys((DISCOVER, FETCH_SOLD, TRANSFORM_SOLD, CENSUS, FETCH_ACTIVE, TRANSFORM_ACTIVE))
    queue_before = queue_size()
    queue_after = queue_before

    try:
        if not args.skip_sold:
            codes[DISCOVER] = run(DISCOVER, dict(env))
            queue_after = queue_size()
            if codes[DISCOVER] == 0:
                codes[FETCH_SOLD] = run(FETCH_SOLD, {**env, "MAX_SCRAPE_COUNT": str(args.limit_sold)})
            else:
                skip(FETCH_SOLD, "discovery failed")
            # Even when fetch_sold stopped early or was skipped: pages already in bronze should reach silver.
            codes[TRANSFORM_SOLD] = run(TRANSFORM_SOLD, dict(env), "--incremental", "--out-prefix", "silver")

        if not args.skip_active:
            codes[CENSUS] = run(CENSUS, dict(env))
            if codes[CENSUS] in (0, CENSUS_INCOMPLETE):
                codes[FETCH_ACTIVE] = run(FETCH_ACTIVE, dict(env), "--n", str(args.limit_active))
            else:
                skip(FETCH_ACTIVE, "the census failed")
            codes[TRANSFORM_ACTIVE] = run(TRANSFORM_ACTIVE, dict(env))

        stats = runstats.read(stats_path)
    finally:
        os.remove(stats_path)

    print("\n".join(summary(codes, stats, max(0, queue_after - queue_before))), flush=True)
    failed = [m for m, code in codes.items() if code]
    if failed:
        print(f"\nExit 1: {len(failed)} step(s) failed: {', '.join(m.removeprefix('redfin.') for m in failed)}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
