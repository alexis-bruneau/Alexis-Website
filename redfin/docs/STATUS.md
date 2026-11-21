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
- **Active listings (manual, not scheduled)**: `extract.census` (price-band slices, manifest), `extract.fetch_active`
  (a 50-listing sample by default), `transform.active` → `silver/active/listings.parquet`; `/ottawa_active` map with a
  Sold | Active switch. One census so far (2026-10-18); the snapshot holds 50 listings as of that day.
- **Listing text**: `transform.text` → `silver/listing_text.parquet` (already run against live silver).
- **Package restructure** (2026-11-21): `redfin/` package, `config.py`, shared browser helpers, lazy Azure client, docs
  under `redfin/docs/`. See D11.

## In progress

- Nothing open on a branch beyond `refactor/redfin-package` (this restructure).

## Next

1. Wire `transform.text --incremental --out-prefix silver` into `daily_scrape.yml` after the sold transform.
2. Implement D7 (sold-only `listed_properties`): rebuild into `silver_v2`, expect `compare_silver.py` to show only the ~113
   unsold keys dropped, then switch.
3. Fix the longitude parser gap (25 rows, see known issues), via the same silver_v2 pattern.
4. Schedule the active path (census → fetch_active → transform.active) in the workflow; then observations / listing state
   per [ACTIVE_LISTINGS.md](ACTIVE_LISTINGS.md) "First slice".
5. Gold tables (DuckDB), then the Q&A layer (P1).

## Known issues

| Issue | Where | Notes |
|---|---|---|
| 25 silver rows have `longitude = NaN` (latitude is fine) | `transform/parsing.py` `extract_between(html, 'longitude":', "}")` misses a variant | rows can't be placed on the map. Re-checked 2026-11-21 |
| ~113 unsold listings in `listed_properties` show as sales | transform writes every parsed page | D7, not implemented |
| `transform.text` and the whole active path are not in `daily_scrape.yml` | workflow | run by hand for now |
| Re-sales of a known home are never fetched | `discover_sold` and `fetch_sold` dedup on `property_id` | fixed by the planned `fetch_plan()` (ACTIVE_LISTINGS.md §6) |
| Discovered URLs beyond the 300-page cap are not kept by the Action | the runner appends to `data/queue/property_urls.txt` but never commits it | queue updates only land when someone commits them locally |
| Container is publicly readable, including bronze/silver | Azure | P2 |
| Manual backups under `silver_backup/<timestamp>/` contain `listed_properties` files | Azure | harmless (the map only reads `silver/`), but nothing documents or cleans them |
| Map still reads the legacy columns `Days On Market` / `Sold Price Difference` | `app/main.py` | dropping them needs the app switched to the new names first |
| Root `requirements.txt` pins `requests==2.24.0`; the package takes any `requests` | Flask app vs package | can conflict in one environment; unpin when touching the Flask app |
| Heroku files were removed (D12) | | if the Flask app is still deployed on Heroku, that deploy is broken |

Fixed by the 2026-11-21 docs pass (kept here once, for the record): ACTIVE_LISTINGS.md said "design only" although census,
fetch_active and transform_active existed; the 403 runbook said "never more than 50 per day" while the Action uses
`MAX_SCRAPE_COUNT=300`, and it referred to variables of the deleted legacy scraper.
