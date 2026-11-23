# Visitor analytics (sold map)

Counts page views and unique visitors of the sold map on quartierinsights.ca, with no cookies, no stored IP addresses and no
third-party script. Code: [app/analytics.py](../app/analytics.py), hooked in by `count_map_pageview` in
[app/main.py](../app/main.py). Numbers: `python scripts/analytics_report.py`.

## What is counted

A page view is a `GET` of a path in `TRACKED_PATHS` (`app/analytics.py`) that answers `200` with HTML:

| Path | Counted |
|---|---|
| `/ottawa_map` | yes |
| `/` | only where it answers 200. On quartierinsights.ca it answers 302 to `/ottawa_map`, so the visit is counted once, at `/ottawa_map`. On any other host (the `azurecontainerapps.io` address, local) `/` is the portfolio home page, which is **not** counted |
| `/ottawa_active` | no (add it to `TRACKED_PATHS` to count it) |
| JSON / POST routes, static files, the GitHub Pages portfolio | no |

Not counted: bots and crawlers (user-agent patterns, empty or tiny user agents, headless browsers, uptime monitors, link-preview
fetchers), `HEAD` requests, browser prefetches (`Sec-Purpose` / `Purpose: prefetch`), and visitors who send `DNT: 1` or
`Sec-GPC: 1`.

## What is stored

One row per page view in the `pageviews` table of the storage account behind `AZURE_STORAGE_CONNECTION_STRING`:

| Field | Example | Notes |
|---|---|---|
| `PartitionKey` | `2026-11-22` | UTC date |
| `RowKey` | `153000123456-9f2c01ab` | time of day (UTC) + random tail: sorted by time, unique |
| `viewed_at` | `2026-11-22T15:30:00.123456+00:00` | UTC |
| `path` | `/ottawa_map` | no query string |
| `referrer_host` | `www.google.com` | the host of the Referer header only; empty when none |
| `visitor_hash` | `3f9a0c11d2b84e07` | see below |
| `device` | `mobile` | `mobile` / `tablet` / `desktop`, from the user agent |
| `browser` | `Chrome` | family only |

**Never stored or logged:** the IP address, the user agent, the full Referer, the query string, any cookie or browser storage
id. The connection string is never logged either; errors are logged as the exception type only.

## How unique visitors work without storing IPs

`visitor_hash = sha256(salt + ip + user_agent)`, cut to 16 hex characters. The IP and user agent exist only in memory while the
hash is computed on a background thread.

* The **salt** is a random secret, one per UTC day, kept in the `analyticssalt` table. Several workers or replicas agree on it:
  each tries to create the day's row (only the first succeeds), then all read it back. Each process also caches it in memory.
* **Salts older than 2 days are deleted** (`SALT_KEEP_DAYS`, done when a new day's salt is first fetched). Without the salt a
  hash cannot be recomputed from a guessed IP, so stored hashes cannot be reversed, and hashes of different days cannot be
  linked (the salts differ anyway).
* **Daily unique visitors** = the number of distinct `visitor_hash` values in a day's partition.

Limits, to keep in mind when reading the numbers:

* **People behind one network merge.** An office, a university or a family on the same Wi-Fi and the same browser version
  looks like one visitor. Counts are slightly low.
* **A changing IP splits one person.** Phone moving between Wi-Fi and mobile data, VPNs, IPv6 privacy addresses: one person
  can count twice in a day. Counts are slightly high.
* **Uniques are per day only.** Adding the daily uniques of a week counts a person who came on 3 days 3 times. The report says
  so. There is no weekly or monthly unique figure.
* iPads that ask for the desktop site look like Macs (`desktop`). Visitors who block the Referer header count as direct.
* Page views are written from a queue in memory: a view still waiting when the container stops is lost, and a burst of more
  than 1000 waiting views is dropped. Fine for statistics, not for billing.

### Monthly uniques (a choice, not enabled)

`SALT_ROTATION` in `app/analytics.py` is `"day"`. With `"month"` the salt (and so the hash) lasts a calendar month, and the
count of distinct hashes over a month's partitions is a monthly unique figure (the report script does not compute it yet). The
price: the same anonymous visitor can be followed for up to a month instead of a day. The previous month's salt is then kept
until the month after. Decide that before you have data you would like to compare: switching mid-month mixes the two meanings.

## The client address behind Azure

The Container Apps ingress is one proxy in front of the app and appends the address it saw to `X-Forwarded-For`. `app/main.py`
trusts exactly that one hop (`PROXY_HOPS = 1`, werkzeug `ProxyFix`, client address only): the **last** entry is the real
visitor, and whatever a visitor typed into the header before it is ignored. More trusted hops than real proxies would let a
visitor pick their own address; fewer would give the proxy's address for everybody.

If a CDN or Front Door is ever put in front of the Container App, set `PROXY_HOPS = 2`. **How to notice a wrong value:** the
report prints a warning when there are many views but never more than 1 unique visitor a day (everybody has the proxy's
address). `PROXY_HOPS` was chosen from Azure's documented behaviour and has not been verified against the live site: look at
the first day's report after deploying.

## Storage and permissions

* Nothing to configure: the app creates both tables on first use, with the connection string the app already has. The Container
  App needs **no new secret**.
* The storage account must offer the Table service (a standard general-purpose v2 account does) and the connection string must
  allow it (an account-key string does; a blob-only SAS token does not). If not, page views are skipped and one warning line per
  failure (exception type only) appears in the container log.
* Without `AZURE_STORAGE_CONNECTION_STRING` (local development, tests) the module does nothing.
* To switch counting off: empty `TRACKED_PATHS`, or remove the `after_request` hook in `app/main.py`.

## Reading the numbers

```bash
python scripts/analytics_report.py              # last 30 days
python scripts/analytics_report.py --days 7 --top 5
```

Reads `AZURE_STORAGE_CONNECTION_STRING` from the environment or `.env`. It prints page views and unique visitors per day, the
device split and the top referrers. It only reads. (Azure portal: Storage account, Storage browser, Tables, `pageviews`, for
raw rows.)

## Retention

Rows have no expiry. Keep at most about 13 months (a full year plus the same month last year, for comparisons) and delete the
rest from time to time:

```bash
python scripts/analytics_purge.py --older-than-days 400           # dry run: counts the rows it would delete
python scripts/analytics_purge.py --older-than-days 400 --yes     # deletes them
```

## Privacy notice

This is designed to need no consent banner (no cookies or other identifiers on the device, no IP kept, no profile across
sites or days), in the spirit of PIPEDA and Quebec's Law 25. It is a design, not legal advice: a short line in a privacy notice
("we count visits without cookies; we keep a daily anonymous hash, never your IP address") is good practice, and
`DNT` / `Sec-GPC` are honoured.
