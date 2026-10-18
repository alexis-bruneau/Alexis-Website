# Redfin pipeline architecture

## Data flow

```mermaid
flowchart LR
    RF[redfin.ca] -->|get_properties.py<br/>discovery| Q[property_urls.txt<br/>pending URLs]
    Q --> EX[scrape_properties_prod.py<br/>EXTRACT]
    RF -->|listing pages| EX
    EX -->|raw HTML, write-once| BR[(bronze/<br/>day/MLS.html.gz)]
    BR --> TR[transform_silver.py<br/>TRANSFORM<br/>parsing.py - pure, no network]
    TR --> SL[(silver/<br/>listed_properties.parquet)]
    TR --> SH[(silver/<br/>sale_history.parquet)]
    TR -->|photo URL from page| IM[(images/)]
    SL --> APP[Flask app / DuckDB]
    SH -.->|next: gold models| GD[(gold/<br/>insights)]
    SL -.-> GD
    IM --> APP
    APP --> MAP[Ottawa map]
```

Bronze is the source of truth. Silver and gold can always be rebuilt from it without contacting redfin.ca.

## Tables and keys

```mermaid
erDiagram
    PROPERTY ||--o{ LISTING : "has (property_id)"
    PROPERTY ||--o{ SALE_HISTORY_EVENT : "has (property_id)"
    LISTING  ||--o{ SALE_HISTORY_EVENT : "own events (property_id + mls)"
    LISTING  ||--o| PHOTO : "images/key_1.jpg"

    LISTING {
        string property_id PK "from the URL: /home/149231176"
        string MLS PK "real MLS number, NULL if the page has none"
        string url
        float  sold_price
        string sold_date
        string address
        string property_type "raw Redfin value"
        float  latitude
        float  longitude
        int    sold_vs_last_list
        int    sold_vs_original_list
        int    days_on_market
        int    cumulative_dom
        string photo_blob
    }
    SALE_HISTORY_EVENT {
        string property_id FK
        int    event_seq
        date   event_date
        string event_type "Listed, Price Changed, Pending, Sold (MLS), Delisted, Relisted"
        int    price
        string source "TREB or CREA"
        string source_id "raw id from Redfin"
        string mls "FK to LISTING.MLS, NULL for CREA events"
        string status
    }
    PHOTO {
        string key "MLS, or P+property_id when no MLS"
    }
```

* A **property** is a physical home (`property_id`). It can have several **listings** over the years, each with its
  own MLS number. One Redfin page shows the events of *all* of them, which is why sale history is keyed by
  `property_id` and carries an `mls` per event.
* Join events to the listing they belong to with `property_id` **and** `mls`. Join on `property_id` alone to get
  the whole life of the home.
* Event identity: `(property_id, event_date, event_type, source_id, price)`.

## Sale metrics (metrics.py)

Computed per listing from the sold listing's **own** events (matched by MLS), never from earlier listings of the
same home. Unit-tested in `tests/test_metrics.py`.

| Column | Definition |
|---|---|
| `sold_vs_last_list` | sold price - last Listed / Price Changed price of that MLS on or before the sale (sale-to-list) |
| `sold_vs_original_list` | sold price - first Listed price of that MLS |
| `days_on_market` | sold date - first Listed date of that MLS |
| `cumulative_dom` | days on market plus the earlier unsold listings of the same home, back to the previous sale (a relist does not reset it) |

The map's legacy columns `Days On Market` and `Sold Price Difference` hold `days_on_market` and `sold_vs_last_list`
until the app reads the new names. Listings whose sold event has no MLS (e.g. Sold (Public Records) only) have no metrics.

## Layers

| Layer | Location | Written by | Mutable? |
|---|---|---|---|
| bronze | `bronze/<day>/<MLS or P+property_id>.html.gz` | extract | never (write-once) |
| silver | `silver/<YYYY-MM>/listed_properties.parquet`, `silver/sale_history.parquet` | transform | rebuilt from bronze |
| images | `images/<key>_1.jpg` / `_sv.jpg` | transform | idempotent |
| gold | not built yet | | |
