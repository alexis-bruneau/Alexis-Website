"""Rebuilds redfin/data/neighbourhoods/ottawa_neighbourhoods.geojson from the City of Ottawa's open data.

Source: "Ottawa Neighbourhood Study (ONS) - Neighbourhood Boundaries Gen 3" (City of Ottawa open data, ArcGIS feature
service). Licence: Open Government Licence - City of Ottawa v2.0. See redfin/data/neighbourhoods/README.md.

What it does:
  1. downloads the 116 polygons already converted to WGS84 (EPSG:4326) by the service (`outSR=4326`), or reads a file
     you downloaded yourself (`--raw`),
  2. repairs invalid rings (two Greenbelt polygons self-intersect in the source),
  3. simplifies ALL polygons together as one coverage (`shapely.coverage_simplify`), so a shared border is simplified
     once and no gaps or overlaps appear between neighbours (simplifying each polygon alone would open slivers),
  4. keeps only `id`, `name` (title case) and the geometry, rounds to 5 decimals (about 1 m),
  5. checks the result (valid, no overlap, no new gaps, area change) and writes it.

Contacts the City of Ottawa's servers only, never redfin.ca. Run from the repo root:

    python redfin/scripts/build_neighbourhoods.py                      # download + build
    python redfin/scripts/build_neighbourhoods.py --raw ons_raw.geojson --tolerance 0.0002
"""
import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import shapely
from shapely.geometry import mapping, shape
from shapely.ops import unary_union

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "redfin" / "data" / "neighbourhoods" / "ottawa_neighbourhoods.geojson"
SERVICE = "https://services.arcgis.com/G6F8XLCl5KtAlZ2G/arcgis/rest/services/GEN3_OTT_1_3_3/FeatureServer/0"
DEFAULT_TOLERANCE = 0.0002  # degrees; about 15 m north-south and 11 m east-west at Ottawa's latitude
DECIMALS = 5


def download():
    query = urllib.parse.urlencode({"where": "1=1", "outFields": "*", "outSR": "4326", "f": "geojson"})
    with urllib.request.urlopen(f"{SERVICE}/query?{query}", timeout=120) as response:
        return json.load(response)


def title_name(name):
    """'OLD OTTAWA SOUTH' -> 'Old Ottawa South', 'GREENBELT (WEST)' -> 'Greenbelt (West)'. The source is upper case."""
    return re.sub(r"[A-Za-z]+", lambda m: m.group().capitalize(), name.strip())


def polygonal(geom):
    """The polygon parts of a repaired geometry (make_valid can return lines or points next to the polygons)."""
    if geom.geom_type in ("Polygon", "MultiPolygon"):
        return geom
    parts = [g for g in getattr(geom, "geoms", []) if g.geom_type in ("Polygon", "MultiPolygon")]
    return unary_union(parts)


def build(raw, tolerance):
    rows = []
    for feature in raw["features"]:
        props = feature["properties"]
        geom = shape(feature["geometry"])
        if not geom.is_valid:
            geom = polygonal(shapely.make_valid(geom))
        rows.append((int(props["ONS_ID"]), title_name(props["ONS_Name"]), geom))
    rows.sort(key=lambda row: row[0])
    ids = [r[0] for r in rows]
    assert len(set(ids)) == len(ids), "duplicate ONS_ID"

    simplified = shapely.coverage_simplify([r[2] for r in rows], tolerance)
    features = []
    for (nid, name, _), geom in zip(rows, simplified):
        geom = shapely.set_precision(geom, 10 ** -DECIMALS)
        assert geom.is_valid and not geom.is_empty, f"{name}: invalid after simplification"
        features.append({"type": "Feature", "properties": {"id": nid, "name": name}, "geometry": mapping(geom)})
    return rows, features


def check(rows, features):
    """Prints what the simplification changed. Raises if neighbours now overlap."""
    before = [r[2] for r in rows]
    after = [shape(f["geometry"]) for f in features]
    union_before, union_after = unary_union(before), unary_union(after)
    tree = shapely.STRtree(after)
    overlap = 0.0
    for i, geom in enumerate(after):
        for j in tree.query(geom):
            if j > i:
                overlap += geom.intersection(after[j]).area
    # degrees^2 -> km^2 at Ottawa's latitude (1 deg lat = 111.2 km, 1 deg lon = 78.4 km)
    to_km2 = 111.2 * 78.4
    print(f"polygons: {len(after)}  vertices: {sum(shapely.get_num_coordinates(g) for g in after):,} "
          f"(was {sum(shapely.get_num_coordinates(g) for g in before):,})")
    print(f"overlap between neighbours: {overlap * to_km2:.4f} km2 (must be ~0)")
    print(f"area change of the whole: {(union_after.area - union_before.area) * to_km2:+.3f} km2; "
          f"area not in both: {union_before.symmetric_difference(union_after).area * to_km2:.3f} km2")
    if overlap * to_km2 > 0.01:
        raise SystemExit("neighbours overlap after simplification")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw", help="a GeoJSON you already downloaded (WGS84) instead of downloading")
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE, help="simplification, in degrees")
    parser.add_argument("--out", default=str(OUT))
    args = parser.parse_args()

    raw = json.load(open(args.raw, encoding="utf-8")) if args.raw else download()
    rows, features = build(raw, args.tolerance)
    check(rows, features)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # compact separators: the browser downloads this file
    out.write_text(json.dumps({"type": "FeatureCollection", "features": features}, separators=(",", ":")),
                   encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    sys.exit(main())
