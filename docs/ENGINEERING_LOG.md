# Engineering log (resume-oriented)

Only items a senior engineer or hiring manager would ask a follow-up question about: architecture and trade-offs, new
infrastructure, non-trivial algorithms and data modelling, reliability / security / cost work, production concerns.
**Not here:** bug fixes, refactors, renames, formatting, small UI tweaks, dependency bumps, minor features.
Rule of thumb: "Would an interviewer ask a follow-up about this?" If not, leave it out.
Never write a metric that was not measured; use `TBD` instead.

**Maintaining it:** add an entry in the same PR as the change (rule in the root [CLAUDE.md](../CLAUDE.md)), newest first, and refresh
"Resume bullets" when the change is significant. Fine-grained rationale lives in
[redfin/docs/DECISIONS.md](../redfin/docs/DECISIONS.md) (`D<n>`); this file is the interview-level view and links to it.

Entry format:

```
### YYYY-MM-DD — <title>
**Area:** architecture | infra | data | algorithm | security | devops | product
**Decision / what:** ...
**Why / trade-offs:** ... (alternatives considered)
**Tech:** ...
**Impact:** ... (metrics if known, otherwise "TBD")
**Refs:** PR #, commit, or file links
```

---

## Project summary

A personal portfolio site plus **QuartierInsights** (quartierinsights.ca): a market-analytics web app for Ottawa real estate.
A scheduled scraper (Playwright) lands raw redfin.ca pages in Azure Blob (bronze), pure transforms rebuild typed Parquet tables
(silver), and a Flask + DuckDB app serves interactive sold and active-listing maps with neighbourhood analytics. The web app runs as
a container on Azure Container Apps, deployed from GitHub Actions; the static portfolio is published to GitHub Pages.
Stack: Python 3.10, Playwright, pandas / Parquet / DuckDB, Flask + gunicorn, Leaflet + Chart.js, Azure Blob / Container Registry /
Container Apps, Docker, GitHub Actions. Scale (2026-11-21): about 7.6K sold listings in silver, ~5,000 active listings seen per
census, 116 neighbourhood polygons, 154 offline unit tests. Single developer, personal and non-commercial.

## Resume bullets

- Architected a bronze/silver data pipeline on Azure Blob (write-once raw HTML, rebuildable Parquet tables) fed by Playwright scrapers,
  so parser fixes are re-runs instead of re-scrapes of a rate-limited source; about 7.6K sold listings in silver, 154 offline tests.
- Designed a price-band slicing census that snapshots ~5,000 active listings in ~41 page loads despite a 350-result cap per search,
  with raw slices saved as fetched and a completeness manifest so a blocked run resumes instead of restarting.
- Modelled sale history as an event table keyed by `(property_id, MLS)` and computed per-listing days-on-market from each listing's own
  events; listings showing more than a year on market dropped from 465 to 3.
- Built a one-command daily orchestrator (sold + active, text, freshness check, app reload) with independent failure domains and a
  read-only verifier, closing a silent failure in which 300 fetched pages never reached the maps and nothing errored.
- Shipped neighbourhood analytics for 116 Ottawa areas: compared seven boundary datasets on licence, size and coverage, assigned ~7.9K
  sales by vectorised point-in-polygon at read time (no re-scrape or silver rebuild when boundaries change), and set minimum samples from the
  price variance (CV ~0.5).
- Containerised the Flask / DuckDB app (gunicorn, one worker with 4 threads sized from a measured ~250 MB per process, non-root user,
  web-only dependencies) and kept credentials out of the image.
- Implemented blue-green CI/CD to Azure Container Apps with GitHub Actions: OpenID Connect login (no stored cloud secret), commit-SHA
  image tags built in ACR, a new revision tested at 0% traffic, public smoke tests, automatic traffic rollback, and a single billed revision.
- Automated a daily GitHub Actions scrape (06:00 UTC) with capped runs, a circuit breaker on consecutive blocks, checkpointing to Azure,
  and cookies injected from a repository secret; the transform step still runs when extraction stops early.
- Built a read-time sell-time curve (share of comparable sales closed by day N, 180-day window anchored on the latest sale, suppressed
  below 20 sales) and documented its survivorship bias rather than presenting it as a listing-time estimate.

---

## Log

### 2026-11-22 — Blue-green deploy of the web app to Azure Container Apps (OIDC, one billed revision)
**Area:** devops
**Decision / what:** `deploy_azure.yml` runs on master pushes that touch the web app. It builds the image in Azure Container Registry
(`az acr build`, tag = commit SHA, never rebuilt), creates a new revision of the Container App at 0% traffic, tests it on its own
address, moves 100% of the traffic, smoke-tests the public domains, then deactivates every other revision. Any failure after the
traffic pin sends the traffic back to the previous revision. One deploy at a time (concurrency group, no cancel of a running deploy).
**Why / trade-offs:** Multiple-revision mode gives a test before visitors arrive, which Single mode cannot. Deactivating old revisions
makes rollback about a minute (reactivate in the portal) instead of instant, but avoids roughly doubling the compute bill for a rollback
speed this site does not need. Login is OpenID Connect with a federated credential scoped to `refs/heads/master`: no client secret
exists, only three identifiers are stored in GitHub, and the storage connection string lives only in the Container App secret. Role is
Contributor on one resource group; narrower custom roles were rejected because their minimum could not be verified without running
against Azure.
**Tech:** GitHub Actions, Azure Container Apps, Azure Container Registry, Azure CLI, OIDC / federated credentials, bash smoke-test helper.
**Impact:** Merges to master ship to production with a pre-switch test and automatic rollback; one replica billed (0.5 CPU / 1 GiB).
The workflow was written without being run against Azure; outcome of the first production run: TBD.
**Refs:** D25 in redfin/docs/DECISIONS.md; PR #12; `7e51caf`, `ad3f0a5`; `.github/workflows/deploy_azure.yml`, `docs/AZURE_DEPLOY.md`.

### 2026-11-22 — Containerised the web app: one gunicorn worker, four threads, web-only image
**Area:** infra
**Decision / what:** Dockerfile on `python:3.10-slim`, gunicorn `--workers 1 --worker-class gthread --threads 4 --timeout 120`, non-root
user, dependency layer before code layer, redfin installed editable with `--no-deps`, `.dockerignore` keeping `.env`, cookies, `.git` and
tests out of the image. The root `requirements.txt` was cut to what the web app imports (scraper packages and an old `requests` pin removed).
**Why / trade-offs:** `app/main.py` loads the Azure data at import time, so each worker process holds a full copy (measured about
250 MB, 290 MB peak) and `/refresh-data` would only reload whichever worker answered. One worker with threads shares one copy and
makes the reload deterministic; the cost is no multi-process parallelism, accepted at this traffic. Editable install because
`config.py` locates `redfin/data/neighbourhoods/` relative to its own file and a normal install would copy the package away from it.
Alternatives: several workers (needs reload fan-out or data moved out of process), a hosted database (rejected in D1).
**Tech:** Docker, gunicorn, Python 3.10, pip layer caching.
**Impact:** about 250 MB resident per process (measured); first sold request about 6 s while the neighbourhood frame builds. Image size: TBD.
**Refs:** D24; PR #12; `880ea0f`; `Dockerfile`, `.dockerignore`, `docs/DOCKER.md`.

### 2026-11-21 — Neighbourhood analytics: boundary selection and read-time spatial assignment
**Area:** algorithm
**Decision / what:** Both maps get a Circle | Neighbourhood switch over the 116 Ottawa Neighbourhood Study (Gen 3) polygons. Sales and
listings are assigned by vectorised point-in-polygon at app load and held in memory; nothing is added to silver. Border points go to
the lowest id (deterministic, never file order); points in no polygon land in an always-listed "unassigned" row so counts add up. The
polygon file (123 KB) is built by a script that repairs self-intersecting rings and simplifies all polygons together
(`shapely.coverage_simplify`, about 15 m) so shared borders cannot open gaps; the browser is served the same file that is counted.
**Why / trade-offs:** Seven boundary sets were compared on licence, area count, sales per area, names and coverage; ONS Gen 3 won (right
size, residents' names, whole city, open licence). Wards are too coarse, census tracts and dissemination areas have no names or too few
sales, OSM would add ODbL obligations. Minimum sample is 10 sales (sold) from the price variance (CV ~0.5 gives a ~19% standard error on
a median at n=10, ~11% at n=30) and 5 listings (active) because the snapshot averages about 3 per area; the active listing count needs no
minimum because it is a fact, not an estimate. Rejected: storing the id in silver (ties every rebuild to a boundary version, kept open as P5),
grouping by Redfin's free-text neighbourhood (inconsistent spelling).
**Tech:** shapely, GeoJSON, pandas, Flask, Leaflet, City of Ottawa open data (OGL).
**Impact:** all 7,852 sales with coordinates fell inside a polygon on 2026-11-21; boundary changes need only an app reload; the daily
scrape and the static-site build are byte-for-byte unaffected.
**Refs:** D17, D18; PR #9; `135a417`, `5ede195`; `redfin/src/redfin/analytics/neighbourhoods.py`, `redfin/data/neighbourhoods/`.

### 2026-11-21 — Daily orchestrator with failure isolation and a freshness gate
**Area:** devops
**Decision / what:** `python -m redfin.run.daily` runs discover, fetch, transform, census, active fetch, active transform and a text
transform as subprocesses, with the sold and active blocks independent (a failed discovery skips only the sold fetch; an incomplete
census still feeds the rest), one summary, and a non-zero exit if any step failed. Steps report counts to the parent through a JSON file
named in an environment variable (a subprocess cannot return numbers). A read-only `run.verify` fails the run if silver is not as new as the
newest bronze day (tolerance max(3 pages, 5%)) or the active snapshot is not as of the latest census; the run then asks the running app to
reload (`/refresh-data`). `--transform-only` rebuilds silver from saved bronze without touching redfin.ca.
**Why / trade-offs:** The trigger was a real incident: 150 sold and 150 active pages were fetched but no transform ran, and both maps
silently stayed on the 2026-10-18 data. A tolerance instead of "every bronze page is in silver" because a few pages are legitimately out
of scope. Rejected: parsing step stdout for the summary, having the app poll silver, adding the steps to the scheduled workflow (a
production change that needs its own decision).
**Tech:** Python subprocess orchestration, Parquet, Flask reload endpoint, pytest.
**Impact:** the "fetched but never reached the map" failure now fails the run; orchestration order and failure rules are covered by tests.
**Refs:** D14, D15, D16; `88672c7`, `57a7619`, `2e26453`, merge `3deb74a`; `redfin/src/redfin/run/`.

### 2026-11-21 — Active snapshot rebuilt from the newest page per MLS, reconciled against sold
**Area:** data
**Decision / what:** `transform.active` emits one row per census MLS using the newest detail page across all days, so a 200-page daily
batch grows the active list instead of replacing it; identity, price, status and coordinates come from the census row and a `page_day`
column records page age. Homes already sold are removed from the snapshot and written to `reconciled_out.parquet` with a reason.
**Why / trade-offs:** The match is on MLS only, and a row counts as a sale only with proof for its own MLS (a Sold event for that MLS or a
sale metric), never on a `Sold Date` alone, because the parser fills that field from earlier sales or the asking price. Matching on
`property_id` would hide a relisted home. The stated bias: hiding a live listing by mistake is worse than showing a just-sold one a day
longer. Rejected: deleting reconciled rows instead of keeping them aside.
**Tech:** pandas, Parquet, pytest on pure functions (selection and reconcile cases).
**Impact:** TBD (first multi-day daily runs pending; see STATUS "Next").
**Refs:** D14; `88672c7`; `redfin/src/redfin/transform/active.py`.

### 2026-11-21 — Sell-time curve computed at read time, with the survivorship bias stated
**Area:** algorithm
**Decision / what:** For the selected area and filters, the share of sales with `days_on_market <= N` for N = 0..180, over the 180 days
ending on the latest sale in the data (not the clock, so results are reproducible); suppressed below 20 sales. The maths is a pure,
unit-tested helper; windowing and filtering stay in the Flask layer.
**Why / trade-offs:** The circle and filters are free-form per request, so the set of curves is too large to precompute and there is no gold
layer yet. Known limit: only homes that sold are counted, so it understates how long a typical listing takes; the page says so. The
right fix (a survival estimate treating active listings as censored) needs listing history that is not yet kept, so it is deferred to the
observations table design.
**Tech:** pandas, DuckDB-backed sold view, Chart.js.
**Impact:** n/a (analytic feature); bias documented in the UI.
**Refs:** D13; `3da587a`; `redfin/src/redfin/analytics/sale_curve.py`.

### 2026-11-21 — Append-only decision log and an agent-assisted workflow
**Area:** devops
**Decision / what:** `DECISIONS.md` (ADR-style, append-only, supersede instead of edit), `STATUS.md`, committed invariants in
`redfin/CLAUDE.md` (write-once bronze, transforms never read the clock or contact redfin.ca, fixed blob prefixes), a read-only tool
allowlist, and a cheap helper agent for doc upkeep and code search. Branch policy: feature branches, PRs to master, master is production.
**Why / trade-offs:** One person, long-lived pipeline: the reasons for decisions otherwise get lost between sessions. Tool permissions are
limited to read-only commands; the invariants are written so refactors cannot silently change storage formats.
**Tech:** Markdown ADRs, Claude Code project instructions and subagent.
**Impact:** 25 decisions and 5 open questions recorded; rate-limit rules prevent unrequested scraping.
**Refs:** `23550fd`; `redfin/CLAUDE.md`, `redfin/docs/DECISIONS.md`, `.claude/agents/redfin-scribe.md`.

### 2026-10-18 — Census of active listings by recursive price-band slicing
**Area:** algorithm
**Decision / what:** A search page embeds at most 350 homes and page 2 repeats them, so pagination cannot enumerate the market. The area is cut
into price bands and any band over 340 results is split at its median price. Each band is stored raw (`bronze/search/<day>/`) as it is
fetched and a manifest written last states whether the day is complete. Active detail pages go to a separate prefix so the sold transform
cannot turn them into fake sales; active listings are keyed by MLS taken from the census, because 143 MLS numbers sit on two property ids.
**Why / trade-offs:** About 41 page loads cover ~5,000 listings with no private API calls and resume after a block. Alternatives
rejected: pagination (returns the same 350), fetching every detail page daily (thousands of requests against a rate-limited site).
**Tech:** Playwright, gzip JSON, Azure Blob prefixes, manifest-last commit marker.
**Impact:** ~41 page loads per ~5,000-listing census (as designed; full-day completeness tracked by the manifest).
**Refs:** D8, D9; `69dab2a`, `9691de7`; merged in PR #8; `redfin/src/redfin/extract/census.py`.

### 2026-10-18 — Sale-history event model and per-listing metrics
**Area:** data
**Decision / what:** Listings keyed by `(property_id, MLS)`; the full sale history stored as its own event table keyed by `property_id`
and tagged with each event's MLS; days on market, price-vs-ask and cumulative DOM computed from the sold listing's own events only.
**Why / trade-offs:** One Redfin page carries the history of every listing of a home, and the old code measured from the home's first-ever
listing. URL as key fails because discovery and canonical URLs are spelled differently; MLS alone is missing on some pages.
**Tech:** pandas, Parquet, pytest.
**Impact:** listings showing more than a year on market fell from 465 to 3 (148 had shown more than 1,000 days).
**Refs:** D4, D5; `6961a33`, `606023d`, `91be9bf`; `redfin/src/redfin/transform/metrics.py`.

### 2026-10-18 — Bronze / silver split with write-once raw pages and a rebuild-compare-switch release
**Area:** architecture
**Decision / what:** Extract only saves gzipped raw HTML (`bronze/<day>/`, never overwritten); transform rebuilds silver from bronze with pure
parsing code that never contacts the site or reads the clock. Full rebuilds default to `silver_v2/`, `compare_silver.py` diffs it
against `silver/` on the listing key, and only then is live silver rewritten.
**Why / trade-offs:** Before, a parser fix meant revisiting every page and the old scraper lost information for good by parsing at fetch
time. Now a fix is a re-run and requests to the source are spent only on new pages. Rejected: in-place rewrite with manual backups
(error-prone), a hosted database or dbt (MBs of data; dbt snapshots record when dbt ran, not when data was observed, which breaks
rebuild-from-bronze).
**Tech:** Azure Blob, Parquet, pandas, DuckDB, pytest.
**Impact:** deterministic rebuilds from bronze; live map never shows an unchecked rebuild. Row counts of the first full rebuild: TBD.
**Refs:** D2, D3, D6; `fe8d9bb`; `redfin/docs/ARCHITECTURE.md`.

### 2026-10-14 — Scraper reliability: bounded runs, checkpointing, circuit breakers
**Area:** devops
**Decision / what:** Run caps (300 detail pages per scheduled run), checkpoint to Azure every N listings, stop after 8 consecutive fetch
failures, discovery stops after 3 blocked pages and exits non-zero, images uploaded in parallel and a photo reference recorded only if the
upload succeeded, one blob listing replacing per-listing existence checks. The transform step runs `if: always()` so pages already saved
still reach silver after an early stop.
**Why / trade-offs:** A 403 used to make discovery loop forever (194 debug screenshots in one run) and partial runs lost their work. Hard caps
also keep volume low for a site whose terms prohibit automated access. Rejected: unbounded "scrape everything" runs.
**Tech:** Playwright, GitHub Actions, Azure Blob, Python concurrency.
**Impact:** runs terminate and keep partial progress; runbook for blocks in `redfin/docs/runbooks/fix-403.md`.
**Refs:** `b8de642`, `d86d007`; `.github/workflows/daily_scrape.yml`.

### 2026-10-14 — Secrets and scraper-state hygiene
**Area:** security
**Decision / what:** Stopped tracking the exported Redfin cookies and the Chrome profile (browser databases, crash metrics) and gitignored them;
the scheduled job receives cookies from the `REDFIN_COOKIES_JSON` repository secret and writes them to an ignored file at run time; the
storage connection string is only ever an environment variable or Actions / Container App secret.
**Why / trade-offs:** Session cookies and a browser profile are credentials. The cost is a manual cookie refresh every few weeks when the
anti-bot token expires (documented). The previously committed cookie file remains in git history.
**Tech:** GitHub Actions secrets, `.gitignore`.
**Impact:** TBD (no secrets in the working tree); history rewrite not done.
**Refs:** `52f93cf`, `b8de642`; `redfin/docs/runbooks/fix-403.md`.

### 2026-10-14 — Static portfolio build for GitHub Pages, with backend pages excluded
**Area:** devops
**Decision / what:** `build_site.py` assembles `_site/` from templates and static files, excludes the pages and
scripts that need the Flask / Azure backend (and strips the dashboard link from the published menu with an assertion that fails the build
if the link moves), and a Pages workflow deploys it on every master push.
**Why / trade-offs:** A free, zero-maintenance host for the portfolio while the data app lives elsewhere; explicit exclusion lists mean new
backend pages cannot leak into the public build by accident. Alternative: serving everything from Flask (cost and uptime for static content).
**Tech:** GitHub Pages, GitHub Actions, Python build script.
**Impact:** TBD.
**Refs:** `a6f059e`, `df4cef7`; `build_site.py`, `.github/workflows/deploy_pages.yml`.

### 2026-02-19 — Scheduled scraping to Azure Blob, Parquet and an in-process DuckDB for the map
**Area:** architecture
**Decision / what:** A GitHub Actions cron job scrapes new sold listings and writes Parquet to one Azure Blob container; the Flask app
loads the files into an in-memory DuckDB view and queries it per request. Later fixes in the same arc: merge new rows into existing data
instead of overwriting, load every historical month file, reuse one browser and block image requests during scraping, and set a CA bundle
for DuckDB's HTTPS access on the previous host.
**Why / trade-offs:** Data is thousands of rows, so a database server is cost and upkeep without benefit; Parquet gives types and fast loads
where the first scraper's CSV did not. Trade-off: data is loaded at process start, so new data reaches the site only on restart or reload
(addressed later in D15, D24, D25).
**Tech:** GitHub Actions, Azure Blob Storage, Parquet, DuckDB, Playwright, Flask.
**Impact:** automated daily refresh with no server to run; scrape speed gain from browser reuse and image blocking: TBD (not measured).
**Refs:** D1; `a433f7e`, `04f1a4d`, `58cc10f`, `199c8b6`, `f361164`.

### 2023-05-22 — ASL classifier served from Flask on a CPU-only host
**Area:** product
**Decision / what:** A PyTorch American Sign Language classifier exposed through a Flask endpoint on Heroku. The first TensorFlow model was
replaced with a PyTorch one, installed from CPU-only wheels, with weights loaded on CPU.
**Why / trade-offs:** The history shows repeated dependency work to get a CPU-only install to deploy (requirements pinned to CPU builds, an older
torch, no TensorFlow); the cost was a larger weights file (about 64 MB committed to the repo). The Heroku files were later removed (D12)
and the page is excluded from the static site. Why Heroku and the exact limits hit are not recorded in the repo.
**Tech:** PyTorch (CPU), Flask, Heroku.
**Impact:** TBD.
**Refs:** `4954e1a`, `ddaeb43`, `33cdfd2`; D12.
