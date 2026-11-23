import os
import sys

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# `app` has no __init__.py: it imports as a namespace package from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from app import analytics  # noqa: E402


class FakeTable:
    """Just enough of azure.data.tables.TableClient, in memory."""

    def __init__(self):
        self.rows = {}

    def create_entity(self, entity):
        key = (entity["PartitionKey"], entity["RowKey"])
        if key in self.rows:
            raise ResourceExistsError("exists")
        self.rows[key] = dict(entity)

    def get_entity(self, partition_key, row_key):
        try:
            return dict(self.rows[(partition_key, row_key)])
        except KeyError:
            raise ResourceNotFoundError("missing")

    def delete_entity(self, partition_key, row_key):
        self.rows.pop((partition_key, row_key), None)

    def query_entities(self, query_filter, parameters=None, select=None):
        """Understands only the filters the code uses: 'PartitionKey eq @p and RowKey lt @k', 'PartitionKey ge @start'."""
        p = parameters or {}
        out = []
        for (pk, rk), row in sorted(self.rows.items()):
            if "@p" in query_filter and pk != p["p"]:
                continue
            if "RowKey lt @k" in query_filter and not rk < p["k"]:
                continue
            if "PartitionKey ge @start" in query_filter and not pk >= p["start"]:
                continue
            out.append(dict(row))
        return iter(out)


@pytest.fixture
def tables(monkeypatch):
    """Analytics wired to two in-memory tables and a fresh salt cache. Returns (pageviews, salts)."""
    pageviews, salts = FakeTable(), FakeTable()
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true;FAKE-SECRET-KEY")
    monkeypatch.setattr(analytics, "_get_clients", lambda: (pageviews, salts))
    monkeypatch.setattr(analytics, "_salt_cache", {})
    return pageviews, salts
