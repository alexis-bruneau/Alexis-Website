"""PURGE PLAN: which sold listings to remove from silver, and everything that hangs off them. Pure: frames in, plan out.

A listing is purged when its OWN sale (a Sold/Closed event for that MLS in sale_history) happened before the cutoff.
That is the same proof transform.active uses for "this MLS really sold"; the `Sold Date` column is not used because the
parser can fill it from an earlier sale. Rows with no own sale event (listings that never sold) are never purged.

For every purged listing the plan removes:
  * its row from silver/<month>/listed_properties.parquet (a month file left empty is deleted)
  * its image(s): images/<key>_1.jpg and images/<key>_sv.jpg, key = MLS or P<property_id>
  * its row in silver/listing_text.parquet
  * the sale-history events of its property, but only when NO listing of that property stays in silver
Bronze is not touched. The listings are returned as exclusions so the pipeline does not bring them back (exclusions.py).
"""
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from redfin.storage import azure_store as store
from redfin.transform import parsing
from redfin.transform.active import is_sale_event

MISSING = {"", "None", "nan", "N/A"}


def _mls(value):
    return None if value is None or pd.isna(value) or str(value).strip() in MISSING else str(value).strip()


@dataclass
class Plan:
    listings: list = field(default_factory=list)       # purged: (property_id, MLS or None, sold date 'YYYY-MM-DD')
    silver: dict = field(default_factory=dict)         # {blob: DataFrame to write, or None to delete the blob}
    history: pd.DataFrame = None                       # sale_history after the purge (None = unchanged)
    history_dropped: int = 0
    text: pd.DataFrame = None                          # listing_text after the purge (None = unchanged)
    text_dropped: int = 0
    images: list = field(default_factory=list)         # image blob names to delete (may not all exist)
    partial_properties: set = field(default_factory=set)   # purged a listing but the property keeps another one


def own_sale_dates(history):
    """{MLS: date of its latest Sold/Closed event} from sale_history (events with a date and an MLS only)."""
    if history is None or history.empty:
        return {}
    sales = history[history["mls"].notna() & history["event_date"].notna() & history["event_type"].map(is_sale_event)]
    return sales.groupby("mls")["event_date"].max().to_dict()


def with_property_id(df):
    """Older silver files have no property_id column; derive it from the url like transform.sold does."""
    if "property_id" not in df.columns:
        df = df.copy()
        df.insert(0, "property_id", df["url"].map(parsing.property_id))
    return df


def image_names(property_id, mls, photo_blob=None):
    key = parsing.listing_key(property_id, mls)
    names = {store.image_blob_name(key, "listing"), store.image_blob_name(key, "streetview")}
    if photo_blob is not None and not pd.isna(photo_blob) and str(photo_blob) not in MISSING:
        names.add(str(photo_blob))
    return sorted(names)


def plan(frames, history, text, cutoff):
    """frames: {silver blob: listed_properties DataFrame}; history: sale_history (or None); text: listing_text (or None);
    cutoff: date, listings whose own sale is strictly before it are purged. -> Plan."""
    sales = own_sale_dates(history)
    result = Plan()
    purged = set()                       # (property_id, MLS)
    remaining_properties = set()
    for blob, raw in frames.items():
        df = with_property_id(raw)
        gone = []
        for i, (pid, mls, photo) in enumerate(zip(df["property_id"], df["MLS"], df.get("photo_blob", pd.Series([None] * len(df))))):
            mls = _mls(mls)
            when = sales.get(mls) if mls else None
            when = pd.Timestamp(when).date() if when is not None and pd.notna(when) else None
            if when is not None and when < cutoff:
                gone.append(i)
                purged.add((pid, mls))
                result.listings.append((pid, mls, str(when)))
                result.images += image_names(pid, mls, photo)
        keep = df.drop(df.index[gone]) if gone else df
        remaining_properties.update(keep["property_id"])
        if gone:
            result.silver[blob] = keep.reset_index(drop=True) if len(keep) else None
    result.images = sorted(set(result.images))
    result.listings = sorted(set(result.listings))

    purged_properties = {pid for pid, _ in purged}
    result.partial_properties = purged_properties & remaining_properties
    full = purged_properties - remaining_properties

    if history is not None and not history.empty and full:
        keep = history[~history["property_id"].isin(full)]
        result.history_dropped = len(history) - len(keep)
        result.history = keep.reset_index(drop=True)
    if text is not None and not text.empty and purged:
        gone = [(pid, _mls(m)) in purged for pid, m in zip(text["property_id"], text["MLS"])]
        result.text_dropped = sum(gone)
        result.text = text[[not g for g in gone]].reset_index(drop=True)
    return result
