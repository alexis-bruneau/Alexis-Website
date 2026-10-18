#!/usr/bin/env python3
"""Compare a rebuilt silver (default silver_v2/) against the live silver/, joined on MLS.

Read-only. Use it after transform_silver.py to see whether the rebuild reproduces what the old scraper
wrote, column by column:
    python app/Redfin/compare_silver.py
    python app/Redfin/compare_silver.py --new silver_v2 --old silver --show 5
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import azure_store as store  # noqa: E402


def load(prefix):
    frames = [store.read_parquet(b) for b in store.list_silver(prefix + "/")]
    if not frames:
        sys.exit(f"No parquet files under {prefix}/")
    return pd.concat(frames, ignore_index=True)


def same(a, b):
    """Equal, treating NaN/None/'N/A'/'None' as the same 'missing'."""
    def norm(v):
        return None if v is None or pd.isna(v) or str(v) in ("N/A", "None", "nan", "") else v
    return norm(a) == norm(b)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", default="silver_v2")
    ap.add_argument("--old", default="silver")
    ap.add_argument("--show", type=int, default=3, help="example differing rows per column")
    args = ap.parse_args()

    new, old = load(args.new), load(args.old)
    for name, df in (("new", new), ("old", old)):
        print(f"{name}: {len(df)} rows, {df['MLS'].nunique()} distinct MLS, {df['MLS'].duplicated().sum()} duplicate MLS")

    old = old.drop_duplicates("MLS", keep="last").set_index("MLS")
    new = new.drop_duplicates("MLS", keep="last").set_index("MLS")
    both = new.index.intersection(old.index)
    print(f"in both: {len(both)} | only in new: {len(new.index.difference(old.index))} | only in old: {len(old.index.difference(new.index))}")

    print("\ncolumn differences on shared MLS:")
    for col in [c for c in new.columns if c in old.columns]:
        diffs = [m for m in both if not same(new.at[m, col], old.at[m, col])]
        print(f"  {col:<24} {len(diffs):>6} differ")
        for m in diffs[: args.show]:
            print(f"      {m}: new={new.at[m, col]!r}  old={old.at[m, col]!r}")
    print("\ncolumns only in new:", [c for c in new.columns if c not in old.columns])
    print("columns only in old:", [c for c in old.columns if c not in new.columns])


if __name__ == "__main__":
    main()
