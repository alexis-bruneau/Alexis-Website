#!/usr/bin/env python3
"""Run the whole Redfin pipeline locally: discover new listing URLs, fetch them into bronze, transform to silver.

Everything is written to the same Azure container the GitHub Action uses, so a local run and a
scheduled run are interchangeable. Needs AZURE_STORAGE_CONNECTION_STRING in .env (or the environment).

Examples (from the project root):
    python app/Redfin/run_pipeline.py                  # discover + scrape everything that's pending
    python app/Redfin/run_pipeline.py --limit 50       # scrape at most 50 listings
    python app/Redfin/run_pipeline.py --skip-discovery # only scrape URLs already in property_urls.txt
    python app/Redfin/run_pipeline.py --silver-prefix silver_v2   # write the transform output somewhere else
    python app/Redfin/run_pipeline.py --max-new 200    # discover at most 200 new URLs
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent


def run(script, env):
    print(f"\n=== {script} ===", flush=True)
    return subprocess.call([sys.executable, str(HERE / script)], env=env)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=0, help="max listings to scrape (0 = all pending)")
    ap.add_argument("--max-new", type=int, default=None, help="max new URLs to discover (default 500)")
    ap.add_argument("--silver-prefix", default="silver", help="where transform_silver.py writes (default: the live silver)")
    ap.add_argument("--skip-discovery", action="store_true", help="skip step 1, only scrape")
    args = ap.parse_args()

    env = dict(os.environ)
    if args.max_new is not None:
        env["MAX_NEW_URLS"] = str(args.max_new)

    if not args.skip_discovery:
        if run("get_properties.py", dict(env)) != 0:
            sys.exit("Discovery failed; not scraping.")

    scrape_env = dict(env)
    scrape_env["MAX_SCRAPE_COUNT"] = str(args.limit)
    extract_status = run("scrape_properties_prod.py", scrape_env)  # extract: redfin.ca -> bronze
    # Transform even if extract stopped early (blocked): pages already saved should still reach silver.
    transform_status = run("transform_silver.py", dict(env), "--incremental", "--out-prefix", args.silver_prefix)
    sys.exit(extract_status or transform_status)


if __name__ == "__main__":
    main()
