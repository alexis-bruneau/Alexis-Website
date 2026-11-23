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

## D18. Neighbourhoods on the active map too: a lower minimum, a count that needs none, and phone tabs
- **Date**: 2026-11-21 (same branch as D17, `feature/neighbourhoods`)
- **Decision**: `/ottawa_active` gets the same Circle | Neighbourhood switch as the sold map (D17, same ONS Gen 3 polygons, same
  point-in-polygon assignment, read time only, nothing stored). `POST /active-points` takes the same `area` (circle or
  neighbourhood; the old top-level `center` / `radius_km` still means a circle), and `POST /active-neighbourhood-summary` returns
  one row per neighbourhood plus the unassigned bucket: `n`, median asking price, average and median days on market, `with_cut`
  and `cut_pct` (the listings whose last asking-price change was a cut: the page's "With a Price Cut" card). The sell-time curve
  (D13) is computed for the selected neighbourhood the same way as for a circle: `comparable_sales` takes an area.
- **Minimum sample 5, not 10 (`ACTIVE_MIN_SAMPLE`)**: on 2026-11-21 the snapshot held 326 active listings, about 3 per
  neighbourhood. Listings per neighbourhood: 90 of 116 areas have at least 1, 27 have 5 or more, only 5 have 10 or more. With
  D17's floor of 10 almost every area would be grey, and with a filter or two (condo, a price band) none would. 5 keeps the 27
  busiest areas; below that a median asking price of a handful of homes says little. It is a constant in
  `analytics/neighbourhoods.py` and moves with the snapshot (it grows by up to 200 pages a day once `run.daily` runs).
- **The count needs no minimum**: the number of listings for sale in an area is a fact, not an estimate, so "Listing count" is
  the default colour of the active map and colours every area that has at least one listing, whatever the sample. Only the
  median, days on market and cut share are greyed below the floor (in the table the count stays dark and only those cells are
  grey). The legend says "No listings" for empty areas and "Fewer than 5 listings" for the estimates.
- **One front-end file for both maps**: `static/js/neighbourhoods.js` now holds one `NB` object per page (wording, metrics,
  columns, which request to repeat, the "nothing picked yet" state). `active.js` keeps its own state and calls into it; the
  active page's price-change buttons (applied in the browser for the map) are also sent to the summary as `changes`, so the table
  and the map count the same listings. The table follows the filter panel, not the chart selections (brush, bars).
- **Filters in one place**: `analytics/active_filters.py` holds `prepare_active_frame`, `apply_active_filters` (asking price, days
  on market, beds, property type, price changes) and `beds_mask` (moved from `app/main.py` unchanged). The active frame is built
  once per snapshot load and cleared by `load_active_data()`, like the sold frame.
- **Hover and selection on the polygons (both maps)**: the area under the pointer glows (white edge, warm halo, brighter fill; no
  glow on touch screens, where a tap selects) and the selected area has a white edge with a blue halo. D17's near-black selected
  outline (`#0f172a`) is gone: it stayed on after a click and read as a black border on hover.
- **Phones**: the active page had no tab bar, and its "Listings" button showed nothing on a phone (the desktop sidebar is hidden
  there and there was no phone panel). It now has the sold map's tabs, Insights | Listings | Areas (Areas only in Neighbourhood
  mode). On both pages the Sold | Active switch, which the controls bar used to cover on phones, is a slim strip above the map; the
  controls are [Circle | Neighbourhood] [Filters] with the radius on a second row in Circle mode, and the tab bar replaces the
  Listings and Neighbourhoods buttons.
- **Alternatives rejected**: keeping 10 (a map of grey); letting the user set the minimum (one more control for a number that should
  follow the data); wards for the active map (a second boundary set to explain; the same polygons answer "where" on both pages);
  a separate `active_neighbourhoods.js` (the two files would drift).
- **Consequences**: changes the production Flask pages `/ottawa_active` and `/ottawa_map` and adds routes; no extract, transform or
  workflow code changes, so the daily scrape and `daily_scrape.yml` are untouched; none of the changed pages or scripts is in the
  Pages build (`ottawa_active.html`, `active.js`, `neighbourhoods.js`, `map.css` are all on the exclusion lists of `build_site.py`).
  A bad `area` on `/active-points` is now a 400 (it was a 500). P5 (store the id in silver or a gold dimension) now has two users.
- **Status**: accepted

## D19. The selected neighbourhood is an outline with a clear middle
- **Date**: 2026-11-22 (same branch, `feature/neighbourhoods`)
- **Decision**: the selected neighbourhood is drawn as a blue 4 px edge with a white and a blue glow and a fully clear fill, so the
  map underneath and the listings on it show through. The other neighbourhoods keep their colours, and the hover glow (D18) is
  unchanged (on the selected area it only adds a faint 10 % veil). This supersedes D18's "white edge with a blue halo" for the
  selected state, which kept a 75 % fill over the street map.
- **Why not `fill: false`**: a clear fill with `fillOpacity` 0 still counts as a click target, so clicking inside the selected area
  behaves like clicking any area.
- **Consequences**: front end only (`static/js/neighbourhoods.js` `styleFor`, `static/css/map.css`); no endpoint, daily scrape or
  Pages build change.
- **Status**: accepted

## D20. In Neighbourhood mode, closing the listings goes back to the neighbourhood table
- **Date**: 2026-11-22 (same branch, `feature/neighbourhoods`)
- **Decision**: clicking a listing marker opens the Listings sidebar, which replaces the neighbourhood table (both are 420 px
  panels on the left). Closing the sidebar (the "View Insights" button, a click on the map) now returns to the table in
  Neighbourhood mode and to the insights in Circle mode (`homeView()` in `map.js` and `active.js`). Before, the table was gone
  after the first marker click and only the "Neighbourhoods" button brought it back, so it looked as if the comparison never showed.
- **Controls bar**: while a sidebar is open (`body.side-open`, desktop only) the Circle | Neighbourhood bar sits at the sidebar's edge
  and wraps when the map is narrow. It was centred on the window and covered the "View Insights" button under about 1250 px, so the
  listings sidebar could not be closed with its button.
- **Consequences**: front end only (`static/js/map.js`, `static/js/active.js`, `static/css/map.css`); no endpoint, daily scrape or
  Pages build change.
- **Status**: accepted

## D21. The side panels keep their width in Chrome
- **Date**: 2026-11-22 (same branch, `feature/neighbourhoods`)
- **Decision**: `#areasPanel` and `#listingsPanel` are `flex: none` (always 420 px) and `#mainPanel` gets `min-width: 0` with a
  `minmax(0, 1fr)` column, so the map and insights shrink beside an open panel. In current desktop Chrome (not the Chromium
  build used for the earlier checks) the active page's neighbourhood table opened at 0 px wide: the insights charts gave
  `#mainPanel` a large minimum width and the flex layout squeezed the panel instead. That is why the comparison never showed on
  `/ottawa_active` although the code opened it. Checked in desktop Chrome at 976 to 1464 px wide on both pages; phones unchanged.
- **Consequences**: CSS only (`static/css/map.css`); no endpoint, daily scrape or Pages build change.
- **Status**: accepted

## D22. Silver starts at 2025-06; the sold map opens on the last 6 months
- **Date**: 2026-11-22
- **Decision**: (1) `transform.sold` (and `transform.text`, `run.verify`, which share `latest_bronze_per_name`) only take listings
  whose newest bronze day is in `config.SILVER_MIN_MONTH` (`2025-06`, env-overridable) or later. Older bronze pages stay in bronze
  (write-once, source of truth) but never reach silver, so a full rebuild gives silver from 2025-06 and the sale history of the
  dropped listings goes with them. The cutoff is on the bronze day (the `silver/<YYYY-MM>/` folder), not on the sale date.
  (2) `/ottawa_map` loads with the "Sold Date Range" slider on the last 6 months of sold data (same rule as the "6 months" quick
  pick); Clear All returns to that default, "All" is one click. The active map is unchanged (its period is time on market, and
  6 months would hide listings of 180+ days).
- **Consequences**: month files before 2025-06 already in live `silver/` are not deleted by an incremental run; they disappear
  only on a full rewrite (`--out-prefix silver`, back up first). Daily scrape workflow and Pages build are unchanged in shape,
  but the scheduled run on master will use the cutoff once merged.
- **Status**: accepted

## D23. Phone: a marker tap always returns to the Listings tab and shows the listing
- **Date**: 2026-11-22 (same branch as D22)
- **Decision**: on both maps, `showListings()` opens the list when the desktop view state **or** the phone tab (`body[data-mtab]`)
  is not "listings". Before, only `currentView` was checked, and the bottom tab bar never updates it, so after one marker tap
  (or the scatter on the active page) later taps while on the Map tab did nothing. On the sold page the phone list also had no
  highlight: it now opens on the page that holds the tapped listing (sorted as the phone list is), marks the row and scrolls to
  it; new data clears the mark. The active page's phone list already pins the picked listing at the top: it now scrolls back to the top.
- **Consequences**: JS and CSS only (`map.js`, `active.js`, `v2.css`); no endpoint, silver, daily scrape or Pages build change.
- **Status**: accepted

## D24. The web app ships as a Docker image: one gunicorn worker with 4 threads, web-only requirements
- **Date**: 2026-11-22 (`feature/docker`, step 1 of the hosting plan; nothing is deployed yet)
- **Decision**: `Dockerfile` (python:3.10-slim) runs `gunicorn app.main:app` on `0.0.0.0:$PORT` (default 8000) with
  `--workers 1 --worker-class gthread --threads 4 --timeout 120`. The redfin package is installed editable and `--no-deps`.
  The connection string is never in the image: `docker run --env-file .env` locally, an Azure secret later.
- **Why one worker**: `app/main.py` loads the sold and active data from Azure when it is imported, so every worker process holds
  its own full copy (measured: about 250 MB for one worker, 290 MB peak). Threads share one copy, and `/refresh-data` (called
  by `redfin.run.daily`) reloads module globals in the one process that answers, so with several workers only a random one
  would refresh. `--timeout 120` because the Azure download runs inside the worker's start-up.
- **Why editable**: `redfin/config.py` finds `redfin/data/neighbourhoods/` relative to its own file; a normal install copies the
  package into site-packages and the file would not be found. `--no-deps` keeps playwright, bs4 and requests out of the image.
- **Requirements**: neither workflow reads the root `requirements.txt` (`daily_scrape.yml` installs `-e ./redfin`,
  `deploy_pages.yml` only runs `build_site.py`), so it was trimmed to what the web app imports (playwright, beautifulsoup4,
  Pillow and the `requests==2.24.0` pin removed; `redfin[dev]` still brings the scraping packages for local work).
- **Consequences**: new `Dockerfile`, `.dockerignore`, `docs/DOCKER.md`; no change to `daily_scrape.yml`, `deploy_pages.yml`,
  blobs or endpoints. Open point: a second worker would need `/refresh-data` to reach every worker, or the data moved out of
  process. The first sold request after start takes about 6 s (it builds the neighbourhood frame).
- **Status**: accepted

---

## D25. Master pushes deploy the web app to Azure Container Apps (blue-green, OpenID Connect)
- **Date**: 2026-11-22 (`feature/docker`, step 2 of the hosting plan; first run happens when the branch is merged)
- **Decision**: `.github/workflows/deploy_azure.yml` runs on pushes to master that touch `app/`, `templates/`, `static/`,
  `redfin/` (not its docs, tests, scripts, data/queue: all in `.dockerignore`), `requirements.txt`, `Dockerfile`,
  `.dockerignore` or the workflow files, and on manual dispatch (`build_only` option). One job: `az acr build` into
  `acralexisbruneau` tagged with the commit SHA (an existing tag is never rebuilt), then `az containerapp update` creates a new
  revision of `ca-alexis-web`, which is tested on its own revision address, gets 100% of the traffic, and is then smoke tested
  on `quartierinsights.ca` (`/`, `/ottawa_map`) and `www.quartierinsights.ca`. Concurrency group `deploy-azure`,
  `cancel-in-progress: false` (a running deploy finishes; only the newest waiting one is kept).
- **Why Multiple revision mode**: the new revision is created with 0% traffic and tested on its own address before visitors
  arrive, and a failed test after the switch sends the traffic back automatically (the previous revision is still running at
  that point). In Single mode there is no test before visitors arrive. The workflow stops with a message if the app is still
  in Single mode.
- **Cost choice**: once both tests pass, every revision except the new one is deactivated, so only one copy (0.5 CPU / 1 GiB,
  min replicas 1) is billed. Rollback after a green deploy is then "activate the previous revision in the portal, then move
  the traffic" (about a minute) instead of an instant traffic switch. Keeping the previous revision running was rejected: it
  roughly doubles the compute bill for an instant rollback this site does not need.
- **Authentication**: OpenID Connect with `azure/login` (federated credential for `repo:alexis-bruneau/Alexis-Website:ref:refs/heads/master`);
  no client secret exists. Only `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID` are stored in GitHub (identifiers, not
  passwords). `permissions: id-token: write, contents: read`. The storage connection string stays in the Container App secret
  `conn`; the workflow never reads it.
- **Least privilege**: Contributor on the resource group `rg-alexis-web` only (also covers registry push/pull). Narrower
  role sets were not adopted because their exact minimum could not be verified without running against Azure.
- **Consequences**: a third workflow triggered by master pushes (next to `daily_scrape.yml` and `deploy_pages.yml`, both unchanged);
  new `docs/AZURE_DEPLOY.md` and `.github/scripts/wait_for_200.sh`. The Azure steps were written without being run: the first
  real run is the merge. Open point: the running app loads its data only at start, so new daily-scrape data reaches the
  site only after a deploy or restart; `/refresh-data` has no authentication and must be protected before anything calls it
  from outside (to be decided separately, see STATUS "Next").
- **Status**: accepted

---

## Proposed (open questions)

## D26. The scheduled run is the whole daily pipeline, and it reloads the live site through a protected /refresh-data
- **Date**: 2026-11-22 (`feature/daily-pipeline-ci`)
- **Decision**: `daily_scrape.yml` (06:00 UTC, master) now runs one command, `python -m redfin.run.daily --limit-sold 200
  --limit-active 200 --refresh-url https://quartierinsights.ca/refresh-data --require-refresh`, instead of three hand-wired steps
  (discover, fetch_sold, transform.sold). That adds `transform.text`, the census, `fetch_active`, `transform.active` (homes that
  sold leave the active list, D14), `redfin.run.verify` (stale silver fails the run, D15) and the reload of the live site.
  The job fails early when a required secret is missing, uses a `daily-scrape` concurrency group (no two runs at once), has a
  180-minute timeout and writes the run's summary to the run page.
- **`/refresh-data` is protected** (it was open, D25's open point): when the env var `REFRESH_TOKEN` is set the caller must send
  it in the `X-Refresh-Token` header (constant-time compare); when it is not set only loopback callers are accepted. Behind
  Azure's proxy the app never sees a loopback caller, so a deployed app that lost its secret refuses everyone. A refused call is
  HTTP 403. `run.daily` reads the token from the `REFRESH_TOKEN` environment variable (never from an argument, never printed).
- **`--require-refresh`**: an app that cannot be reached is a failed step. Without the flag (local runs) it stays "not running,
  not a failure" (D15). A refused token or a reload that reports `success: false` fails the run either way.
- **Volume**: a scheduled run now fetches up to 200 sold + 200 active detail pages plus the ~41-page census (before: up to 300
  sold pages). Redfin's Terms of Use still prohibit automated access (redfin/CLAUDE.md); the stop-after-8-failures and
  3-empty-pages rules in `config.py` apply unchanged.
- **Consequences**: new GitHub secret `REFRESH_TOKEN` and a Container App secret/env var of the same value (setup in
  docs/AZURE_DEPLOY.md). The deployed app has one worker, so one reload reaches every request (D24). The run edits
  `data/queue/property_urls.txt` inside the runner only; nothing commits it (as before).
- **Status**: accepted

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
