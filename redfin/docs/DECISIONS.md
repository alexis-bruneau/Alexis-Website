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

## D15. The daily run also refreshes text, checks that silver is fresh, and reloads the app
- **Date**: 2026-11-21 (branch `feature/daily-run`)
- **Decision**: `redfin.daily` gains, after the transforms: `transform.text --incremental --out-prefix silver`; `redfin.verify`
  (read-only) as the last subprocess step; and a `POST /refresh-data` to the running Flask app (default
  `http://127.0.0.1:5000/refresh-data`, `--refresh-url`, `--no-refresh`). `verify` fails the run when the newest bronze day has
  more than max(3 pages, 5%) not in silver `listed_properties`, or when `silver/active/listings.parquet` is not as of the
  latest census day or is empty. `--transform-only` skips discovery, fetches and the census and runs only the transforms,
  the check and the refresh. An app that is not running is not a failure.
- **Why**: the maps read silver and the app keeps an in-memory copy, so a fetch without its transform, or a transform
  without a reload, changes nothing on screen and raises no error. That is what happened on 2026-11-21: 150 sold and 150
  active pages were fetched, no transform ran, and both maps stayed on the 2026-10-18 data.
- **Alternatives rejected**: only documenting "run the transforms after fetching" (the failure is silent); having the app
  poll silver for changes (more moving parts than one POST); counting every bronze page not in silver (a few pages are
  legitimately out of scope or unreadable and would fail every run); adding these steps to `daily_scrape.yml` (a production
  change that needs its own decision; the Action stays sold-only).
- **Status**: accepted. Refines D14.

## D16. The commands you run live in `redfin/run/`; `pipeline.py` is removed
- **Date**: 2026-11-21 (branch `feature/daily-run`)
- **Decision**: the lineage is three folders in `redfin/src/redfin/`: `extract/` (1. scrape, the only code that contacts
  redfin.ca), `transform/` (2. bronze -> silver) and `run/` (3. execute). `daily.py` and `verify.py` moved there, so the
  commands are `python -m redfin.run.daily --limit-sold N --limit-active N` and `python -m redfin.run.verify`
  (D14 and D15 name them `redfin.daily` / `redfin.verify`, their names when written). `redfin/pipeline.py` (sold-only
  discover, fetch, transform) is deleted: `run.daily --skip-active` does the same and also checks silver.
  `extract/` and `transform/` keep their names and module paths, so `daily_scrape.yml` is unchanged.
- **Why**: the entry points were loose files next to the layer folders, so the order of the lineage was not visible, and two
  runners overlapped. The `src/` layout (`redfin/src/redfin`) stays: it is the layout the Python packaging guide recommends
  and tests, CI and the editable install already depend on it.
- **Alternatives rejected**: numbered folders (`1_extract`: not importable with a normal `import`); renaming `extract/` to
  `scrape/` (edits `daily_scrape.yml`, a production change for a nicer name); flattening `src/redfin` to `redfin/redfin`.
- **Status**: accepted. Refines D11.

## D17. Ottawa neighbourhoods on the sold map: ONS Gen 3 boundaries, assigned at read time
- **Date**: 2026-11-21 (branch `feature/neighbourhoods`)
- **Decision**: the sold map (`/ottawa_map`) gets a second way to pick an area, next to the draggable circle: one of the 116
  **Ottawa Neighbourhood Study (ONS) Gen 3** neighbourhoods (City of Ottawa open data, Open Government Licence - City of Ottawa
  v2.0, downloaded 2026-11-21). Each sale is placed with a point-in-polygon test on its own latitude/longitude, in
  `redfin.analytics.neighbourhoods`, **at read time**: the app assigns every sale once when it loads the data and keeps the id in
  memory. Nothing is added to silver and no blob prefix or file name changes. Redfin's own `neighbourhood` text is not used.
- **Boundary research** (all queried 2026-11-21; "sales/area" = the 7,852 sales with coordinates in live silver that day, so it
  changes as data grows):

  | Set | Licence | Areas | Sales/area (median; share of areas with >= 20 sales) | Names | Coverage | Verdict |
  |---|---|---|---|---|---|---|
  | **ONS Gen 3** (City of Ottawa) | OGL - City of Ottawa 2.0 | 116 | 46; 86% | residents' names (Glebe-Dows Lake, Old Ottawa South, Stittsville, Carp) | the whole city, urban, suburban and rural (rural areas are 60-260 km2) | **chosen** |
  | ONS Gen 2 (City of Ottawa) | same | 111 | not run | same, older | whole city | superseded by Gen 3; published as "Historical" |
  | City wards 2022-2026 and 2026-2030 | same | 24 each | 296; 100% | "Ward 15 Kitchissippi": political, not how people name areas | whole city | too coarse: one ward mixes Westboro with Hintonburg |
  | StatCan census tracts 2021 | OGL - Canada | 231 (Ottawa side of the Ottawa-Gatineau CMA) | 30; 75% | numbers (5050001.00) | whole city | no names; many small tracts |
  | StatCan forward sortation areas 2021 | OGL - Canada (the reference guide says the equivalent Statistics Canada Open Licence) | 44 (K0A, K1, K2, K4) | 152; 86% | postal prefixes (K2J) | whole city | recognised by some, but not neighbourhoods |
  | StatCan dissemination areas 2021 | OGL - Canada | 1,392 in the Ottawa census division | 4; 4% | numbers | whole city | far too small for our sample |
  | OpenStreetMap place tags | ODbL (share-alike on a derived database) | not downloaded | not run | residents' names | volunteer-drawn, not checked | not evaluated: ONS already fits, and ODbL would add obligations |

  Other City sets found and dismissed: community design plan boundaries, child-care planning areas and police divisions (not
  neighbourhoods). Census files are from 2021 (published 2022-09-21; the next set follows the 2026 census). ONS Gen 3 was created
  2024-02-09 and last modified 2026-08-12, with "official review every 5 years". Its description says the boundaries come from
  census tracts, barriers (waterways, highways), **maps used by the real estate profession (the Ottawa MLS)**, community
  consultation and fieldwork, which is why its areas line up with how homes are advertised.
- **Why ONS Gen 3**: the only set that is at once the right size for our data (116 areas, 91% with at least 10 sales), named the way
  residents and listings name places, current, openly licensed for redistribution with attribution, and complete across suburban
  and rural Ottawa (its rural areas are 60-260 km2 each). The publisher warns the
  boundaries "are not indicative of actual neighbourhood limits" and that small communities can sit inside a larger one; the page
  repeats the attribution and the README says so.
- **The file**: `redfin/data/neighbourhoods/ottawa_neighbourhoods.geojson` (123 KB, 31 KB gzipped): WGS84, only `id` (the ONS id,
  3001-3117) and `name`. Built by `redfin/scripts/build_neighbourhoods.py`, which downloads from the City's servers (never
  redfin.ca), repairs the two Greenbelt polygons whose rings self-intersect in the source, and simplifies all polygons together
  with `shapely.coverage_simplify` (about 15 m) so shared borders cannot open gaps. Checked on the download: 116 valid polygons,
  0 overlap, no holes between neighbours, union 2,890 km2 against 2,892 km2 for the ward outline (shoreline generalisation). The
  same file is served to the browser (`/neighbourhood-boundaries.geojson`), so what you see is what is counted. Attribution is on
  the map and in the table footer. Source, date and licence: `redfin/data/neighbourhoods/README.md`.
- **Assignment rules** (`analytics/neighbourhoods.py`): inside a polygon gets its id; a point exactly on a border counts as
  inside, and on a border shared by two areas the **lowest id wins** (never file order); a point in no polygon, or with a missing
  coordinate, gets id 0 and is returned as its own row, **"Outside Ottawa / unassigned"**, which is always listed (last, even with
  0 sales) so the rows add up to the total. Nothing is dropped. Sales with no coordinates at all (25 today: the longitude parser
  gap in Known issues) are on no map; the summary reports their number separately instead of putting them in the bucket. On
  2026-11-21 the bucket was empty: all 7,852 placed sales fall inside an ONS polygon.
- **Minimum sample**: a neighbourhood with fewer than **10 sales** (`MIN_SAMPLE`) after the filters is flagged `low_sample`: the
  table greys the row and the map greys the polygon and leaves it out of the colour scale. The row is still returned with its `n`.
  Why 10: sold prices here have a coefficient of variation near 0.5 (sd 339k on a mean of 697k), which puts the standard error
  of a median price at about 19% with 10 sales and 11% with 30, and below 10 one sale moves an average by 10% or more. 20 (the
  sell-time curve's floor, D13, which needs a whole distribution) would grey out most areas as soon as price, beds or type
  filters are used. Statistics: median price (prices are right-skewed), average and median days on market (long tail), average
  % sold vs final and vs original ask. The colour scale is clipped at the 5th/95th percentile of the areas that qualify.
- **One filter path**: `redfin.analytics.sale_filters.apply_sale_filters` holds the sold filters (date, price, beds, property
  type). `/filtered-points` takes `area` = `{"type": "circle", "center": [lat, lng], "radius_km": r}` or
  `{"type": "neighbourhood", "id": n}`; the old top-level `center` / `radius_km` still means a circle. `POST /neighbourhood-summary`
  takes the same `filters` and returns the rows. The sold frame is built once per data load (asking differences and ids
  included) and cleared by `/refresh-data`, so the point-in-polygon test never runs per request. The sold map's "5+ beds" button
  still matches exactly 5 (the active map uses `beds_mask`); kept as is so circle mode behaves exactly as before.
- **Alternatives rejected**: a column in silver (would tie every silver rebuild to a boundary version; see P5); grouping by Redfin's
  `neighbourhood` string (spelling varies, does not match official areas); dropping unassigned sales (hides data problems);
  a hosted boundary service or tiles (one 123 KB file is enough); reducing the file to the census set because it is "more official"
  (231 unnamed tracts are not an area a buyer recognises).
- **Consequences**: a changed boundary file needs no re-scrape and no silver rebuild, only an app reload. The change is read-only for
  the pipeline: no extract or transform code runs, the daily scrape and `daily_scrape.yml` are untouched, and the Pages build is
  byte-identical (`neighbourhoods.js` is on the exclusion list of `build_site.py`, like `map.js`). It adds `shapely>=2.0`.
- **Status**: accepted

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

## P5. Store `neighbourhood_id` in silver (and use it in gold)
- **Date**: 2026-11-21
- **Question**: D17 assigns neighbourhoods at read time, in the Flask app. Should the id become a silver column (and a gold
  dimension) once something other than the map needs it, such as gold tables, the Q&A layer (P1) or the active map?
- **For**: one place for the assignment, usable from DuckDB and notebooks; consistent between the sold and active maps.
- **Against**: it ties silver to one boundary file (a new Gen of ONS would mean a full silver rebuild via `silver_v2`, D3), and it
  puts a derived value next to parsed facts. A middle way: a separate gold dimension `sale_neighbourhood(property_id, MLS,
  neighbourhood_id, boundary_version)` built from silver and the committed GeoJSON, leaving silver as it is.
- **Leaning**: the gold dimension, when gold exists. Until then, read time.
- **Status**: proposed
