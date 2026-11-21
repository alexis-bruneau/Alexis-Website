"""Listings that were purged from silver on purpose and must stay out.

The list is a committed text file (config.EXCLUDED_FILE), one listing per line:

    <property_id> <MLS or -> <sold date>        # anything after '#' is a comment

Why it exists: bronze keeps the raw pages, and the sold search still lists old sales, so without the list `discover_sold`
would queue them again, `fetch_sold` would fetch them again and `transform.sold` would rebuild them from bronze.

    discover_sold / fetch_sold   skip a property id that is in the list (they match on property_id)
    transform.sold / .text       skip a (property_id, MLS) that is in the list (they match on the listing)

Pure file reading; no network.
"""
from pathlib import Path

from redfin import config
from redfin.transform import parsing


def load(path=None):
    """The list as [(property_id, MLS or None, sold date string or None)]. Missing file = empty list."""
    path = Path(path or config.EXCLUDED_FILE)
    if not path.exists():
        return []
    entries = []
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split("#", 1)[0].split()
        if not fields:
            continue
        mls = fields[1] if len(fields) > 1 and fields[1] != "-" else None
        sold = fields[2] if len(fields) > 2 and fields[2] != "-" else None
        entries.append((fields[0], mls, sold))
    return entries


def property_ids(entries):
    return {pid for pid, _, _ in entries}


def keys(entries):
    """{(property_id, MLS or None)}: the key transform.sold gives a listing."""
    return {(pid, mls) for pid, mls, _ in entries}


def names(entries):
    """{bronze file name}: the MLS, or P<property_id> when the page had none (parsing.listing_key)."""
    return {parsing.listing_key(pid, mls) for pid, mls, _ in entries}


def without_excluded_pages(bronze_entries, entries):
    """[(day, name, blob)] minus the pages named in the list. A cheap check on the file name, before any download."""
    skip = names(entries)
    return [e for e in bronze_entries if e[1] not in skip]


def append(new_entries, path=None):
    """Adds entries (property_id, MLS, sold date) that are not in the file yet. Returns how many were added."""
    path = Path(path or config.EXCLUDED_FILE)
    have = keys(load(path))
    fresh = [e for e in new_entries if (e[0], e[1]) not in have]
    if fresh:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as f:
            if not path.stat().st_size:
                f.write("# property_id MLS sold_date: purged from silver on purpose; see redfin/docs/DECISIONS.md (D17)\n")
            for pid, mls, sold in sorted(fresh):
                f.write(f"{pid} {mls or '-'} {sold or '-'}\n")
    return len(fresh)
