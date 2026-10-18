"""Maps the raw Redfin property type stored in silver to the few groups the map filters on.

Silver keeps whatever Redfin said ('Single Family Residential', 'Townhouse', 'Condo/Co-Op', ...), and the
spelling has changed between parser versions. Grouping happens here, at read time, so a new spelling only
needs a new rule here and never a re-scrape. The group names are exactly what the filter buttons in
templates/ottawa_map.html send (data-ptype).
"""

CONDO = "Condo"
TOWNHOME = "Townhome"
SINGLE_FAMILY = "Single-family"
MULTI_FAMILY = "Multi-family"
OTHER = "Other"

# First match wins. Checked against the lower-cased raw value.
_RULES = (
    ("multi", MULTI_FAMILY),     # 'Multi-family', 'Multi-Family (2-4 Unit)'
    ("town", TOWNHOME),          # 'Townhome', 'Townhouse'
    ("condo", CONDO),            # 'Condo', 'Condo/Co-Op', 'Condo (co-op)'
    ("co-op", CONDO),
    ("single", SINGLE_FAMILY),   # 'Single-family', 'Single Family Residential'
    ("ranch", SINGLE_FAMILY),    # a detached single-storey house
)


def property_group(raw):
    """Raw property type -> one of the group names above. Vacant land, mobile homes and missing values are 'Other'."""
    if raw is None or raw != raw:  # None or NaN
        return OTHER
    value = str(raw).strip().lower()
    for needle, group in _RULES:
        if needle in value:
            return group
    return OTHER
