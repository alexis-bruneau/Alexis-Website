# Ottawa neighbourhood boundaries

`ottawa_neighbourhoods.geojson`: 116 polygons, WGS84 (EPSG:4326), properties `id` and `name` only. Used by the sold map
(`/ottawa_map`, Neighbourhood mode) through `redfin.analytics.neighbourhoods`. The app serves this same file to the browser
(`/neighbourhood-boundaries.geojson`), so what you see is what is counted.

## Source

| | |
|---|---|
| Dataset | **Ottawa Neighbourhood Study (ONS) - Neighbourhood Boundaries Gen 3** |
| Publisher | City of Ottawa (open data), created by the Ottawa Neighbourhood Study |
| Item page | https://www.arcgis.com/home/item.html?id=439875b741f34985a7001a1491c2d79b (listed on https://open.ottawa.ca) |
| Feature service | https://services.arcgis.com/G6F8XLCl5KtAlZ2G/arcgis/rest/services/GEN3_OTT_1_3_3/FeatureServer/0 |
| Downloaded | **2026-11-21**. The publisher's item was created 2024-02-09 and last modified 2026-08-12; update frequency "as needed (official review every 5 years)" |
| Licence | [Open Government Licence - City of Ottawa, version 2.0](https://ottawa.ca/en/city-hall/get-know-your-city/open-data#open-data-licence-version-2-0) (a copy of the Government of Canada's OGL 2.0 for the City). Redistribution and display on a public site are allowed with attribution |
| Attribution (shown on the map) | Contains information licensed under the Open Government Licence – City of Ottawa. Neighbourhood boundaries: Ottawa Neighbourhood Study. |

The publisher notes that the boundaries "were created by the Ottawa Neighbourhood Study (ONS) to analyse population
statistics and are not indicative of actual neighbourhood limits". Small communities can sit inside a larger ONS
neighbourhood.

## What was changed from the download

Done by [`../../scripts/build_neighbourhoods.py`](../../scripts/build_neighbourhoods.py) (re-run it to rebuild; it contacts
only the City's servers):

1. Downloaded as GeoJSON already converted from Web Mercator to WGS84 by the service (`outSR=4326`).
2. Two polygons (Greenbelt East and West) had self-intersecting rings in the source; repaired with `make_valid` (area unchanged).
3. All polygons simplified **together** as one coverage (`shapely.coverage_simplify`, tolerance 0.0002 degrees, about 15 m), so a shared border
   is simplified once and no gaps or overlaps open between neighbours. 12,044 vertices became 5,407; the file is 123 KB (31 KB gzipped).
4. Coordinates rounded to 5 decimals (about 1 m). Attributes reduced to `id` (the ONS id, 3001-3117) and `name` (title case; the source is upper case).

Checks on the download (2026-11-21): 116 polygons, 0 overlaps, no holes between neighbours, union area 2,890 km² against 2,892 km² for the
City's 2022-2026 ward outline (the difference is shoreline generalisation along the Ottawa River). Boundaries are generalised
by the publisher (not densified), so a home within about 15-50 m of a border can land on the wrong side.

`id` 0 is reserved for "Outside Ottawa / unassigned" and is not in this file.
