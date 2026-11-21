"""Optional, machine-readable results of a pipeline step, so `redfin.daily` can print one summary.

The steps run as subprocesses, so their counts cannot be returned. When REDFIN_STATS_FILE is set (only redfin.daily
sets it), each step merges its numbers into that JSON file under its own name; otherwise report() does nothing and
every step behaves exactly as before.

    runstats.report("fetch_sold", pending=40, saved=38)
"""
import json
from pathlib import Path

from redfin import config


def read(path):
    """The stats file as {step: {name: value}}; {} when it is missing or unreadable."""
    try:
        return json.loads(Path(path).read_text(encoding="utf-8")) or {}
    except (OSError, ValueError):
        return {}


def report(step, **values):
    if not config.STATS_FILE:
        return
    data = read(config.STATS_FILE)
    data.setdefault(step, {}).update(values)
    Path(config.STATS_FILE).write_text(json.dumps(data), encoding="utf-8")
