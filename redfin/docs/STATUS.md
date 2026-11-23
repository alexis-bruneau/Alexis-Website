# Status

Last verified against the code and the live container: **2026-11-21**. Update this file in the same change as any
behaviour change (see redfin/CLAUDE.md, "End of task").

## Done

- **Sold pipeline in production** (daily on master): discover_sold → fetch_sold (≤ 300 pages/run) → `transform.sold
  --incremental --out-prefix silver`. Live silver: 7,577 listings in 3 monthly `listed_properties` files, plus
  `silver/sale_history.parquet`.
- Per-listing sale metrics (`sold_vs_last_list`, `sold_vs_original_list`, `days_on_market`, `cumulative_dom`) from the
  sold MLS's own events.
- Photos in `images/`, Street View fallback behind `SAVE_STREETVIEW`, `scripts/backfill_images.py`.
- **Active listings (manual, not scheduled)**: `extract.census` (price-band slices, manifest), `extract.fetch_active`,
  `transform.active` → `silver/active/listings.parquet`; `/ottawa_active` map with a Sold | Active switch. One census
  before the daily command existed (2026-10-18); the live snapshot is still that one (50 listings) until the first daily run.
- **`python -m redfin.run.daily`** (D14, written offline, **not yet run against redfin.ca**): one command a day: up to 200 new sold
  listings + up to 200 active detail pages, census, silver rebuilt. `fetch_active` picks never-fetched listings first, then
  the stalest page; `transform.active` uses the newest page per census MLS across all days (so the active list grows by
  ~200 a day) and moves homes that are already sold into `silver/active/reconciled_out.parquet`. `/active-points` hides
  non-Active census rows. Tests cover the selection, the reconcile and the orchestrator's order and failure rules.
- **Folders show the lineage** (D16): `extract/` (1. scrape) -> `transform/` (2. bronze -> silver) -> `run/` (3. the commands:
  `python -m redfin.run.daily --limit-sold N --limit-active N`, `python -m redfin.run.verify`). `pipeline.py` was removed
  (`run.daily --skip-active` replaces it).
- **The daily run also finishes the job** (D15): after the transforms it runs `transform.text`, then `redfin.run.verify` (silver is as
  new as the newest bronze day and the latest census, else the run fails) and asks the running app to reload
  (`POST /refresh-data`). `python -m redfin.run.daily --transform-only` rebuilds silver from the bronze already saved without
  contacting redfin.ca. Found on 2026-11-21: 150 sold and 150 active pages had been fetched but the transforms had not run, so
  the maps still showed the 2026-10-18 data (50 active listings, no 2026-11 sold file).
- **The scheduled run is the whole pipeline** (D26, `feature/daily-pipeline-ci`, not merged yet): `daily_scrape.yml` calls
  `redfin.run.daily` (sold block, census, active block with sold homes removed, verify, reload of quartierinsights.ca). `/refresh-data`
  now needs the `X-Refresh-Token` header (env `REFRESH_TOKEN`); without that variable only loopback is accepted.
- **Listing text**: `transform.text` → `silver/listing_text.parquet` (already run against live silver).
- **Package restructure** (2026-11-21): `redfin/` package, `config.py`, shared browser helpers, lazy Azure client, docs
  under `redfin/docs/`. See D11.
- **Insight charts on /ottawa_active** (branch `feature/active-insights-charts`, not merged yet): scatter of % change from the
  original listing price vs days on market (linked to the map and list; header, tabs, legend and grid styled like the sold chart), clickable days-on-market and cut-depth histograms that cross-filter, and a sell-time curve from the sold
  data (`redfin.analytics.sale_curve`, D13). Flask-only: not in the Pages build or the daily scrape.

- **Neighbourhoods on the sold and the active map** (branch `feature/neighbourhoods`, D17, D18; not merged yet): `/ottawa_map` and
  `/ottawa_active` have a Circle | Neighbourhood switch. Neighbourhood mode draws the 116 Ottawa Neighbourhood Study (Gen 3, City of
  Ottawa open data) polygons shaded by a metric (sold: median price, days on market, sold vs ask; active: listing count, median
  asking price, days on market, share with a price cut) with a legend; the area under the pointer glows; a click or a table row
  selects one, and the insights, markers and listings then show that neighbourhood under the same filters. A sortable table lists
  every neighbourhood, reruns on every filter change, and is the "Areas" tab on phones. Estimates are greyed below 10 sales (sold) or
  5 listings (active); the active listing count is exact and always coloured. The active page also got the phone tab bar it lacked
  (its "Listings" button showed nothing on a phone), and on both pages the Sold | Active switch is no longer hidden by the controls
  bar on phones. Read time only (`redfin.analytics.neighbourhoods`, `sale_filters`, `active_filters`): nothing new in silver, no blob
  change; the daily scrape and the Pages build are untouched. Boundaries and licence: `redfin/data/neighbourhoods/README.md`.

- **Silver from 2025-06 only, sold map opens on 6 months** (D22): `config.SILVER_MIN_MONTH`; the old month files still in live
  `silver/` go away only on a full rewrite. The sold map's date slider defaults to the last 6 months (Clear All too).

- **Docker image for the web app** (D24, `feature/docker`): runs locally (`docker run --env-file .env`), about 250 MB of memory,
  not deployed yet. Root `requirements.txt` is now web-only, which also removes the old `requests==2.24.0` pin. Guide: `docs/DOCKER.md`.

- **Automatic deploy to Azure** (D25, `feature/docker`, written offline, **not yet run**): `.github/workflows/deploy_azure.yml` builds the image
  in `acralexisbruneau` (tag = commit SHA) on master pushes, creates a new revision of `ca-alexis-web`, tests it, moves the traffic,
  smoke tests quartierinsights.ca, and rolls the traffic back if that fails. Needs the one-time setup in `docs/AZURE_DEPLOY.md`
  (Multiple revision mode, app registration with a federated credential, Contributor on `rg-alexis-web`, three GitHub secrets).
  The first run is the merge of this branch.

## In progress

- `feature/docker` (D24, D25): Docker image and Azure deploy workflow; PR to master not opened yet. Azure/GitHub setup per `docs/AZURE_DEPLOY.md` first.
- `feature/daily-run` (D14, D15): the daily command, merged with master (which has the restructure). PR to master not merged yet.
- `feature/active-insights-charts` (insight charts, D13).
- `feature/neighbourhoods` (D17, D18): PR to master not merged yet.

## Next

0. Merge `feature/daily-pipeline-ci` (D26) and do its one-time setup (the `REFRESH_TOKEN` secret in GitHub and in the Container
   App, docs/AZURE_DEPLOY.md), then watch the first scheduled run. After that, `daily_scrape.yml` runs the whole pipeline and
   reloads the live site by itself.
2. Implement D7 (sold-only `listed_properties`): rebuild into `silver_v2`, expect `compare_silver.py` to show only the ~113
   unsold keys dropped, then switch.
3. Fix the longitude parser gap (25 rows, see known issues), via the same silver_v2 pattern.
4. Run `python -m redfin.run.daily` for a week and check the numbers; then decide whether the active path goes into the workflow
   (`daily_scrape.yml` has never run the census), then observations / listing state per
   [ACTIVE_LISTINGS.md](ACTIVE_LISTINGS.md) "First slice".
5. Gold tables (DuckDB), then the Q&A layer (P1).
6. Neighbourhoods on the map, what is left: judge the active minimum sample (5, D18) after a few weeks of daily runs, when each
   neighbourhood has more listings; decide P5 (store the id) once gold or the Q&A layer needs the assignment.

## Known issues

| Issue | Where | Notes |
|---|---|---|
| Sold "5+ beds" button matches exactly 5 bedrooms; the active map's matches 5 or more | `analytics/sale_filters.py` (sold) vs `beds_mask` in `app/main.py` (active) | 204 of 7,852 sold sales have 6+ bedrooms and are excluded by "5+". Kept as it was so circle mode did not change in the neighbourhood work; one-line fix if wanted |
| Neighbourhood boundaries are generalised by the publisher (about 15 m after our simplification) | `data/neighbourhoods/` | a home within roughly 15-50 m of a border can be placed in the neighbour. ONS neighbourhoods are statistical areas, not official limits |
| Active neighbourhood estimates are thin: 326 listings (2026-11-21) is about 3 per neighbourhood, and with a filter or two almost every area is below the minimum of 5 | `analytics/neighbourhoods.py` (`ACTIVE_MIN_SAMPLE`) | the listing count is exact and still coloured; medians and averages grey out. Improves as the active snapshot grows (D18) |
| The neighbourhood table follows the filter panel but not the chart picks (scatter brush, histogram bars) on the active page | `static/js/active.js` | by design: the table counts the listings for sale, the charts narrow what is on the map |
| The 25 sales with no longitude are on no map, so also in no neighbourhood | `transform/parsing.py` | `/neighbourhood-summary` reports them as `no_coordinates`; fixed by the longitude parser fix below |
| 25 silver rows have `longitude = NaN` (latitude is fine) | `transform/parsing.py` `extract_between(html, 'longitude":', "}")` misses a variant | rows can't be placed on the map. Re-checked 2026-11-21 |
| ~113 unsold listings in `listed_properties` show as sales | transform writes every parsed page | D7, not implemented |
| `transform.text` and the whole active path are not in `daily_scrape.yml` | workflow | run by hand for now |
| Re-sales of a known home are never fetched | `discover_sold` and `fetch_sold` dedup on `property_id` | fixed by the planned `fetch_plan()` (ACTIVE_LISTINGS.md §6) |
| Discovered URLs beyond the 300-page cap are not kept by the Action | the runner appends to `data/queue/property_urls.txt` but never commits it | queue updates only land when someone commits them locally |
| `fetch_sold` / `fetch_active` stop after 8 failures in a row; `fetch_sold` still exits 0 then | `extract/fetch_sold.py` | `redfin.run.daily` can't see it from the exit code, so it reads `stopped_early` from the step's counts and prints a warning (exit code unchanged: `daily_scrape.yml` depends on it) |
| One failed page makes `fetch_active` exit 2 | `extract/fetch_active.py` | `redfin.run.daily` then reports that step as failed (exit 1) although the other pages were saved |
| Active rows with a stale page keep an approximate `last_change_date` | `transform.active` | a price change since the page was fetched is dated as the census day; `page_day` shows how old the page is |
| A partial census shrinks the active list | `transform.active` | rows exist only for MLS in that day's census; `redfin.run.daily` says so in the summary |
| Reconcile only knows sales that reached silver | `transform.active` | a home sold since the last `transform.sold` shows as active until the next run; matched on MLS only (`listed_properties` rows whose MLS is the page's first `TREB #` can differ from the sold MLS, ACTIVE_LISTINGS.md finding 7) |
| `/active-points` hides every non-`Active` status, including `Pending` | `app/main.py` | only `Active` and `Off Market` have been seen (the 4-home fixture); check the real census before relying on it |
| `redfin.run.daily` reloads `http://127.0.0.1:5000/refresh-data` unless told otherwise | `daily.py` | locally an app that isn't running is not an error. The scheduled run passes `--refresh-url https://quartierinsights.ca/refresh-data --require-refresh` and the token in `REFRESH_TOKEN` (D26); unreachable, refused (403) or `success: false` fails the run |
| The first scheduled run of the whole pipeline (D26) has not happened yet | `daily_scrape.yml` | it was written and tested offline (158 tests, endpoint cases through Flask's test client); the Azure and Redfin sides run for the first time on the first dispatch after merge. Volume rises from 300 sold pages to 200 sold + 200 active + the census |
| Container is publicly readable, including bronze/silver | Azure | P2 |
| Manual backups under `silver_backup/<timestamp>/` contain `listed_properties` files | Azure | harmless (the map only reads `silver/`), but nothing documents or cleans them |
| Map still reads the legacy columns `Days On Market` / `Sold Price Difference` | `app/main.py` | dropping them needs the app switched to the new names first |
| Heroku files were removed (D12) | | if the Flask app is still deployed on Heroku, that deploy is broken |

Fixed by the 2026-11-21 docs pass (kept here once, for the record): ACTIVE_LISTINGS.md said "design only" although census,
fetch_active and transform_active existed; the 403 runbook said "never more than 50 per day" while the Action uses
`MAX_SCRAPE_COUNT=300`, and it referred to variables of the deleted legacy scraper.
