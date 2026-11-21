# Decisions

Append-only. One entry per decision; never edit an old entry. To change one, add a new entry that says
"supersedes D<n>" and set the old entry's status line to `superseded by D<m>` (the only edit allowed).
Status: `accepted` (in force), `accepted, not implemented`, `proposed` (open question), `superseded by D<n>`, `rejected`.

---

## D1. Store everything in one Azure Blob container as Parquet; the map queries it with DuckDB
- **Date**: 2026-02-19 (`a433f7e`, `04f1a4d`)
- **Decision**: one container `redfin-data` (account `stredfinprod`). Parsed rows are Parquet; the Flask app loads them
  into an in-memory DuckDB view.
- **Why**: cheap, no database server to run, and the data is small (thousands of rows).
- **Alternatives rejected**: a hosted SQL database (cost and upkeep for a one-person project); CSV files (the first scraper
  wrote CSV; no types, slow to load).
- **Status**: accepted

## D2. Split the pipeline into extract (bronze) and transform (silver)
- **Date**: 2026-10-18 (`fe8d9bb`)
- **Decision**: extract only saves raw HTML to `bronze/<day>/<name>.html.gz`, write-once. Transform rebuilds silver from
  bronze with pure parsing code and never contacts redfin.ca.
- **Why**: a parser fix used to need a new visit to every page. With raw pages kept, a fix is a re-run, and requests to
  Redfin (rate limits, ToS) are spent only on new pages.
- **Alternatives rejected**: parse at fetch time and keep only rows (the old scraper; lost information for good).
- **Status**: accepted

## D3. Rebuild into `silver_v2/`, compare, then switch
- **Date**: 2026-10-18 (`fe8d9bb`)
- **Decision**: full rebuilds default to `--out-prefix silver_v2`; `scripts/compare_silver.py` diffs it against `silver/`
  on the listing key; only then is live `silver/` rewritten. The map can preview with `SILVER_PREFIX=silver_v2`.
- **Why**: the live map never shows a half-checked rebuild.
- **Alternatives rejected**: rewrite `silver/` in place with manual backups (`silver_backup/` was that; error-prone).
- **Status**: accepted

## D4. Key sold listings by `(property_id, MLS)`
- **Date**: 2026-10-18 (`6961a33`)
- **Decision**: `property_id` (the number in `/home/<id>`) identifies the home; the MLS number identifies one listing of it
  (empty when the page has none). Bronze pages are named by MLS, or `P<property_id>` without one.
- **Why**: a home can sell several times; MLS alone is missing on some pages; URL alone is spelled differently between
  discovery and the page's canonical URL.
- **Alternatives rejected**: URL as key; MLS alone.
- **Status**: accepted

## D5. Sale history is its own event table
- **Date**: 2026-10-18 (`606023d`, `91be9bf`)
- **Decision**: `silver/sale_history.parquet`, one row per event, keyed by `property_id` and tagged with each event's `mls`.
  Sale metrics are computed from the sold listing's own events only (matched by MLS).
- **Why**: one Redfin page carries the history of every listing of the home; metrics from earlier listings were wrong.
- **Alternatives rejected**: a history CSV per scrape; metrics from the whole page's timeline.
- **Status**: accepted

## D6. pandas + Parquet for silver, DuckDB for gold and validation; no dbt
- **Date**: 2026-10-18 (`05e98ed`, ACTIVE_LISTINGS.md §8)
- **Decision**: keep transforms as pure Python functions over pandas; write gold and validation as DuckDB SQL over Parquet.
- **Why**: MBs of data, one daily job, plain pytest on pure functions already works. dbt snapshots record when dbt ran, not
  when data was observed, which breaks rebuild-from-bronze.
- **Alternatives rejected**: dbt / dbt-duckdb (reconsider only if gold grows into many interdependent models).
- **Status**: accepted (gold not built yet)

## D7. `listed_properties` holds sold listings only
- **Date**: 2026-10-18 (`05e98ed`, ACTIVE_LISTINGS.md §1, §9)
- **Decision**: transform writes a `listed_properties` row only when that MLS has a Sold event. Timelines of unsold
  listings live in separate tables.
- **Why**: the map treats every `listed_properties` row as a sale; ~113 unsold rows currently show up as sales.
- **Alternatives rejected**: a status column the map filters on (every reader would have to remember it).
- **Status**: accepted, not implemented (see STATUS.md)

## D8. Active listings get their own prefixes and are keyed by MLS
- **Date**: 2026-10-18 (`05e98ed`, `69dab2a`, `9691de7`)
- **Decision**: census in `bronze/search/<day>/`, detail pages in `bronze/active/<day>/`, snapshot in
  `silver/active/listings.parquet`. The census row is the listing's identity; the detail page is used only for events and
  must contain the census row's `listingId`.
- **Why**: the sold transform reads every `bronze/<day>/*.html.gz`; active pages there would become fake sales. 143 MLS
  numbers sit on two propertyIds, and one detail page can name several MLS numbers, so MLS from the census is the
  reliable key.
- **Alternatives rejected**: mixing active pages into `bronze/<day>/`; keying active listings by `(property_id, MLS)`.
- **Status**: accepted

## D9. Census by price-band slicing of the for-sale search
- **Date**: 2026-10-18 (`69dab2a`)
- **Decision**: a search page embeds at most 350 homes and page 2 repeats them, so the area is cut into price bands, and a
  band with more than `CENSUS_SLICE_MAX` (340) results is split at its median price. Each band is saved raw as it is
  fetched; a manifest written last says whether the day is complete.
- **Why**: ~41 page loads cover ~5,000 listings, with no API calls and resumable after a block.
- **Alternatives rejected**: pagination (returns the same 350); fetching every detail page daily (thousands of requests).
- **Status**: accepted

## D10. Listing descriptions come from a separate text-only transform pass
- **Date**: 2026-11-21 (`c14a670`)
- **Decision**: `transform.text` writes `<prefix>/listing_text.parquet` (`property_id, MLS, text, bronze_day`), same key
  and page-selection rule as `transform.sold`, without touching images, history or `listed_properties`.
- **Why**: backfills every bronze page without rewriting the other silver tables; the file name keeps it off the map.
  Promo text is kept raw; cleaning belongs to the embedding step.
- **Alternatives rejected**: adding a `text` column to `listed_properties` (forces a full sold rebuild, bloats map loads).
- **Status**: accepted

## D11. Restructure the Redfin code into an installable package `redfin/` (same repo)
- **Date**: 2026-11-21 (`ba2be69`)
- **Decision**: src-layout package with `config.py`, `storage/`, `scraping/`, `extract/`, `transform/`, `analytics/`;
  entry points run as `python -m redfin.<module>`; maintenance scripts in `redfin/scripts/`. No blob prefix or format changed.
  `metrics.py` sits in `transform/`, not `analytics/`, because the transform computes silver columns with it. The Azure
  client connects lazily so imports and `--help` never need credentials.
- **Why**: scripts imported each other through `sys.path` hacks and cwd-relative paths like `app/Redfin/...`; constants were
  duplicated across files.
- **Alternatives rejected**: a separate repo now (see P4); a flat package without layers.
- **Status**: accepted

## D12. Remove the Heroku deploy files
- **Date**: 2026-11-21 (`7b0ad84`)
- **Decision**: deleted `Procfile`, `runtime.txt`, `wsgi.py`, `composer.json`; the public site is the GitHub Pages build.
- **Why**: nothing in the repo used them anymore.
- **Alternatives rejected**: keeping them unused.
- **Status**: accepted. If the Flask app needs hosting again, that is a new decision.

## D13. The "how long do homes take to sell" curve is computed at read time from the sold rows, not stored
- **Date**: 2026-11-21
- **Decision**: `POST /active-points` also reads the sold `properties` view (the one the sold map uses) and returns
  `sold_curve`, `sold_sample` and `sold_window`. The curve is the share of sales with `days_on_market <= N` for N = 0..180,
  from sales inside the same circle that pass the price, beds and property-type filters, in the 180 days ending on the
  latest sale in the data (not the clock, so the result is reproducible). Below 20 sales the curve is `null`. The maths is
  the pure helper `redfin.analytics.sale_curve` (unit-tested); windowing and filtering stay in `app/main.py`.
- **Why**: it has to follow the search circle and filters on every request, the sold set is small (a few thousand rows,
  already in memory), and there is no gold layer yet to hold precomputed curves.
- **Known limit**: it only counts homes that sold. Withdrawn, expired and still-unsold listings are not in the data, so it
  understates how long a typical listing takes (survivorship bias). The page says so.
- **Alternatives rejected**: a precomputed gold table (nothing to build it in yet, and a free circle plus filters makes the
  set of curves too big to precompute); a survival estimate that treats active listings as censored (the right fix for the
  bias, but it needs listing history we do not keep; revisit with the observations table in ACTIVE_LISTINGS.md).
- **Status**: accepted

## D14. One daily command; the active list is the newest page per census MLS, minus homes already sold
- **Date**: 2026-11-21 (branch `feature/daily-run`)
- **Decision**:
  1. `python -m redfin.daily` (run by hand, once a day) runs `discover_sold`, `fetch_sold` (`MAX_SCRAPE_COUNT` = `--limit-sold`,
     default 200), `transform.sold --incremental --out-prefix silver`, `census`, `fetch_active --n <--limit-active>` (default
     200) and `transform.active` as subprocesses, and prints one summary. Discovery failing skips only `fetch_sold`; the sold
     and active blocks are independent; an incomplete census (exit 2) still feeds `fetch_active` and `transform.active`; a
     census that fails outright skips `fetch_active` but not `transform.active`. Exit code 1 if any step exited non-zero.
     The steps report their counts through a JSON file named in `REDFIN_STATS_FILE` (`redfin/runstats.py`; a no-op when unset),
     because a subprocess cannot return numbers and `subprocess.call` cannot capture output.
  2. `fetch_active` defaults to a "daily" selection: census rows that pass `eligible()` and have no `bronze/active/` page for
     any day first (newest listings first, by Redfin's days on market), then those whose newest page is the oldest; listings
     already fetched on the census day are skipped. `--sample` keeps the old biased test sample.
  3. `transform.active` builds a row for every census MLS from the **newest page of that MLS across all days** (up to the census
     day), with identity, price, status and coordinates from the census row and `as_of` = census day. New column `page_day`
     says how old the page is. The default census day is now the latest census, not the latest day with pages.
  4. Reconcile with sold, inside `transform.active`: a census MLS that is a sale in silver is left out of `listings.parquet`
     and written to `silver/active/reconciled_out.parquet` (`mls, sold_date, reason`). **Matched on MLS only.** A
     `listed_properties` row is a sale only with proof for its own MLS: a Sold/Closed event for that MLS in
     `sale_history.parquet`, or a sale metric on the row (those exist only when the MLS has its own Sold event). `Sold Date`
     alone is not proof, and `Sold-Conditional` / `Pending` are not sales.
  5. `/active-points` shows only snapshot rows whose `status` is `Active` (a row with no status stays).
- **Why**: a 200/day batch must add to the active list, not replace it with that day's 200. Matching on `property_id` would
  hide a home relisted after a sale (same property, new MLS). The parser fills `Sold Date` from the previous sale or the
  asking price (ACTIVE_LISTINGS.md findings 2 and 7), so it cannot say whether this MLS sold; hiding an active listing by
  mistake is worse than showing a just-sold one for one more day. The census returns non-Active homes (the fixture has an
  `Off Market` one).
- **Alternatives rejected**: matching sold on `property_id`; trusting `Sold Date` alone as the sale test (see the proof rule
  above); deleting reconciled rows instead of writing them aside;
  parsing the steps' stdout for the summary; adding the daily run to `daily_scrape.yml` (not changed here: it needs the
  census, which the Action has never run).
- **Status**: accepted. Refines D8 (what the active snapshot contains). `daily_scrape.yml` is unchanged. A daily run edits the
  committed `redfin/data/queue/property_urls.txt` (discovery appends to it).

---

## Proposed (open questions)

## P1. RAG / Q&A design over listings and insights
- **Date**: 2026-11-21
- **Question**: how should "ask a question about the market" work?
- **Options**: (a) text-to-SQL over gold tables in DuckDB (exact numbers, needs gold first); (b) semantic search over
  `listing_text` embeddings (good for "homes with a finished basement near transit"); (c) both, routed by question type.
  Embedding provider still open; storage candidates: DuckDB `vss` extension over Parquet, or a hosted vector store.
- **Leaning**: (c), starting with (a) once gold exists. Embeddings computed in a separate pass, like D10.
- **Status**: proposed

## P2. Make bronze and silver private
- **Date**: 2026-10-18 (ACTIVE_LISTINGS.md §11)
- **Question**: the container allows public blob reads (the map links images directly), so raw pages and silver are
  downloadable by anyone who guesses a name.
- **Options**: move `bronze/` and `silver/` to a private container and keep only `images/` public; or serve images through
  the Flask app / SAS URLs. Either way this changes blob locations, so it needs its own migration plan.
- **Status**: proposed

## P3. Redfin Flask routes as a blueprint
- **Date**: 2026-11-21
- **Question**: move `/ottawa_map`, `/ottawa_active`, `/points.json`, `/filtered-points`, `/active-points` out of
  `app/main.py` into a blueprint (e.g. `redfin.web`), leaving `app/main.py` for the portfolio.
- **Status**: proposed

## P4. Split Redfin into its own repository
- **Date**: 2026-11-21
- **Question**: the package is now self-contained under `redfin/`; a separate repo would decouple its CI and history from
  the portfolio. Cost: the map templates/JS and the Flask routes live in the portfolio repo.
- **Status**: proposed (revisit after P3)
