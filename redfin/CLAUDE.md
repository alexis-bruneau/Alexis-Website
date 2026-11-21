# Redfin pipeline

Pulls Ottawa listings from redfin.ca (sold and active) into a bronze/silver pipeline on Azure Blob, feeds the map in
`app/main.py`, and will later produce market insights (gold) and a Q&A layer over listings and insights.

## Data flow

```
redfin.ca sold search  --discover_sold-->  data/queue/property_urls.txt
queue                  --fetch_sold------>  bronze/<day>/<MLS|P<pid>>.html.gz          (raw page, write-once)
bronze/<day>/          --transform.sold-->  silver/<YYYY-MM>/listed_properties.parquet + silver/sale_history.parquet + images/
bronze/<day>/          --transform.text-->  silver/listing_text.parquet                 (agent descriptions)
redfin.ca for-sale     --census---------->  bronze/search/<day>/<lo>_<hi>.json.gz + manifest.json
census listings        --fetch_active---->  bronze/active/<day>/<MLS>.html.gz            (never fetched first, then stalest)
census + newest page   --transform.active->  silver/active/listings.parquet              (as of the census day, sold homes removed)
  per MLS (any day)                          silver/active/reconciled_out.parquet        (the removed ones: mls, sold_date, reason)
silver                 --app/main.py----->  /ottawa_map (sold), /ottawa_active (active)
```

Details, tables and keys: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Layout

```
redfin/
  pyproject.toml           package, deps, dev extra, pytest config
  src/redfin/
    config.py              ALL paths, env vars and run caps. Paths resolve from the checkout, never the cwd
    storage/azure_store.py every blob read/write; connects lazily on first use
    scraping/browser.py    user agent, cookies, fetch_html (shared by every fetcher)

    # the lineage, in order:
    extract/               1. SCRAPE: the ONLY code that contacts redfin.ca (discover_sold, fetch_sold, census, fetch_active) -> bronze
    transform/             2. TRANSFORM: pure; bronze -> silver (parsing, metrics, sold, active, text)
    run/                   3. EXECUTE: the commands you run; they start the steps above, check the result, reload the app
      daily.py               the once-a-day command, with how many listings to take (--limit-sold, --limit-active)
      verify.py              read-only freshness check (silver vs newest bronze day and latest census); daily.py runs it last

    analytics/             read-time helpers for the map (property_types, sale_curve)
    runstats.py            steps report their counts to run/daily.py through a JSON file (no-op outside it)
  scripts/                 one-off maintenance: backfill_images.py, compare_silver.py
  tests/ (+ fixtures/)     pure-function tests, no network, no Azure
  data/queue/              property_urls.txt (sold-URL queue, committed)
  docs/                    see the index below
```

## Invariants (do not break)

- **Blob prefixes and formats are fixed**: `bronze/`, `bronze/search/`, `bronze/active/`, `silver/`, `silver_v2/`,
  `images/`. Changing one is a decision (docs/DECISIONS.md), never a side effect of a refactor.
- **Bronze is write-once and the source of truth.** `put_bronze` never overwrites. Silver and gold must always be
  rebuildable from bronze without contacting redfin.ca.
- **Transform never contacts redfin.ca and never reads the clock** (`now()`, `today()`). Dates come from the bronze path
  or the census day, so re-running on the same bronze gives the same rows.
- **The map loads every `*listed_properties.parquet` under `silver/`** and treats each row as a sale. Nothing else may use
  that file-name suffix; new tables pick other names.
- **Keys**: sold listings `(property_id, MLS)` (MLS empty when the page has none); sale-history events
  `(property_id, event_date, event_type, source_id, price)`; active listings `MLS`.
- Active detail pages live under `bronze/active/`, never `bronze/<day>/` (the sold transform would turn them into fake sales).

## Commands (from the repo root)

```bash
pip install -e "./redfin[dev]"        # once; also: playwright install chromium
python -m pytest redfin               # tests (offline, < 1 s)

python -m redfin.run.daily                                  # THE daily run: up to 200 sold + 200 active pages, census, silver rebuilt
python -m redfin.run.daily --limit-sold 150 --limit-active 150 [--skip-sold | --skip-active]   # how many listings / one block only
python -m redfin.run.daily --transform-only                 # rebuild silver from the bronze already saved + check + reload the app (no redfin.ca)
python -m redfin.run.verify                                 # read-only: is silver as new as bronze and the census? exit 1 if not

python -m redfin.extract.fetch_sold --limit 20          # one stage at a time; every module takes --help
python -m redfin.extract.census --local-dir census_out  # census without Azure

# Parser change -> rebuild into silver_v2, compare, then switch:
python -m redfin.transform.sold --limit 200             # quick trial (default output prefix is silver_v2)
python -m redfin.transform.sold                         # full rebuild into silver_v2/
python redfin/scripts/compare_silver.py --show 5        # silver_v2 vs silver on (property_id, MLS)
SILVER_PREFIX=silver_v2 python app/main.py              # look at it on the map
python -m redfin.transform.sold --out-prefix silver     # switch (back up silver/ first)
```

Production is `.github/workflows/daily_scrape.yml` on **master** (06:00 UTC): discover_sold, then fetch_sold
(`MAX_SCRAPE_COUNT=300`), then `transform.sold --incremental --out-prefix silver`. Needs secrets
`AZURE_STORAGE_CONNECTION_STRING` and `REDFIN_COOKIES_JSON` (written to `redfin/chrome_cookies.json`).

## Redfin rate limits and Terms of Use

- Redfin's Terms of Use prohibit automated access. This is a personal, non-commercial project: keep volume low and never
  redistribute raw listing data (board/CREA MLS® content).
- Caps live in `config.py`: 300 detail pages per scheduled run, stop after 8 failed fetches in a row, discovery stops after
  3 empty pages, census pauses 3–5 s between its ~41 page loads.
- **Do not run anything in `extract/` or `run/daily.py` (without `--transform-only`) unless the user asks for it in this session.**
  (`run/daily.py` runs the census plus up to 400 detail pages, and edits the committed `data/queue/property_urls.txt`.) No loops, no
  parallel fetchers, no retries beyond what the code does. Tests and transforms never touch redfin.ca.
- Blocked (403, empty pages, login wall): [docs/runbooks/fix-403.md](docs/runbooks/fix-403.md).

## Docs

| File | What |
|---|---|
| [docs/STATUS.md](docs/STATUS.md) | done / in progress / next / known issues. Read this first |
| [docs/DECISIONS.md](docs/DECISIONS.md) | append-only decision log, incl. proposed (open) decisions |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | layers, tables, keys, metric definitions |
| [docs/ACTIVE_LISTINGS.md](docs/ACTIVE_LISTINGS.md) | active-listings design: state machine, planned tables, validation |
| [docs/runbooks/fix-403.md](docs/runbooks/fix-403.md) | refreshing cookies when Redfin blocks us |

## End of task

When a change alters behaviour or makes a decision, update **docs/STATUS.md** and append to **docs/DECISIONS.md** in the
same change (same commit). Never edit an old decision entry: add a new one that supersedes it.

## Model routing

If the request looks mismatched to the current model, say so in one line and recommend a switch before starting.

| Model (alias) | Relative cost | Use for | Redfin examples |
|---|---|---|---|
| Haiku 5.5 (`haiku`) | lowest, fastest | lookups, mechanical edits, doc/status upkeep | find where a column is written; update STATUS.md after a merged change; rename a CLI flag everywhere |
| Sonnet 5.5 (`sonnet`) | ~20× Haiku | everyday features and fixes with clear scope | add a parser field + test; wire `transform.text` into the workflow; map/UI tweaks in `static/js/map.js` |
| Opus 5.5 (`opus`) | 2× Sonnet | multi-file design, debugging across layers, data-model changes | sold-only `listed_properties` migration via silver_v2; listing state machine (SCD2); gold tables |
| Fable 5.2 (`fable`) | 2× Opus | hardest, long-horizon reasoning where Opus falls short | RAG architecture end to end; validation against OREB statistics with survival analysis |

The `.claude/agents/redfin-scribe` subagent (Haiku) handles doc/status updates and code search.
