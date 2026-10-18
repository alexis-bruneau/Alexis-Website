"""Azure Blob helpers shared by the pipeline stages (extract, transform, backfill).

Container layout:
    bronze/<YYYY-MM-DD>/<MLS>.html.gz   raw page, zlib-compressed. IMMUTABLE: written once, never overwritten
                                        or edited. Blob metadata carries the url and fetch time.
    silver/<YYYY-MM>/listed_properties.parquet   parsed rows, rebuilt from bronze by transform_silver.py
    images/<MLS>_1.jpg | <MLS>_sv.jpg   listing photo / Street View fallback
"""
import io
import os
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load .env from the current directory or its parent (same behaviour as the old scraper).
_env = Path(".env")
load_dotenv(dotenv_path=_env if _env.exists() else Path("..") / ".env")

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "redfin-data"

if not CONN_STR:
    raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in .env")

container_client = BlobServiceClient.from_connection_string(CONN_STR).get_container_client(CONTAINER_NAME)
if not container_client.exists():
    container_client.create_container()


# ---------------- bronze (raw, immutable) ---------------------------------

def bronze_blob_name(day, mls):
    return f"bronze/{day}/{mls}.html.gz"


def put_bronze(html, mls, url, day=None):
    """Writes one raw page. Never overwrites: returns False if this MLS is already saved for that day."""
    day = day or datetime.now().strftime("%Y-%m-%d")
    blob = container_client.get_blob_client(bronze_blob_name(day, mls))
    metadata = {"url": url, "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    try:
        blob.upload_blob(zlib.compress(html.encode("utf-8")), overwrite=False, metadata=metadata)
    except ResourceExistsError:
        return False
    return True


def list_bronze():
    """Every bronze page as [(day, mls, blob_name)]."""
    out = []
    for b in container_client.list_blobs(name_starts_with="bronze/"):
        parts = b.name.split("/")
        if len(parts) == 3 and parts[2].endswith(".html.gz"):
            out.append((parts[1], parts[2][:-len(".html.gz")], b.name))
    return out


def read_bronze(blob_name):
    """The decompressed HTML of one bronze page."""
    data = container_client.get_blob_client(blob_name).download_blob().readall()
    return zlib.decompress(data).decode("utf-8")


def bronze_url_metadata(blob_name):
    return container_client.get_blob_client(blob_name).get_blob_properties().metadata.get("url")


# ---------------- images --------------------------------------------------

IMAGE_WORKERS = 8
_existing_images = set()


def load_existing_images():
    _existing_images.update(b.name for b in container_client.list_blobs(name_starts_with="images/"))
    return _existing_images


def image_blob_name(mls, kind="listing"):
    return f"images/{mls}_{'sv' if kind == 'streetview' else '1'}.jpg"


def upload_image(mls, image_url, kind="listing", retries=3):
    """Uploads one image. Returns the blob name only if the image is really in Azure, else None."""
    blob_name = image_blob_name(mls, kind)
    if blob_name in _existing_images:
        return blob_name

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(image_url, timeout=20)
            # Only accept real JPEGs (not an HTML error page that happens to be HTTP 200)
            if resp.status_code == 200 and resp.content[:3] == b"\xff\xd8\xff":
                container_client.get_blob_client(blob_name).upload_blob(resp.content, overwrite=True)
                _existing_images.add(blob_name)
                return blob_name
            print(f"   -> Image {mls} ({kind}): HTTP {resp.status_code} (attempt {attempt}/{retries})")
        except Exception as e:
            print(f"   -> Image {mls} ({kind}) failed (attempt {attempt}/{retries}): {e}")
        time.sleep(attempt)  # simple backoff
    return None


# ---------------- parquet -------------------------------------------------

def read_parquet(blob_name):
    return pd.read_parquet(io.BytesIO(container_client.get_blob_client(blob_name).download_blob().readall()))


def write_parquet(df, blob_name):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    container_client.upload_blob(name=blob_name, data=buf, overwrite=True)


def list_silver(prefix="silver/"):
    return [b.name for b in container_client.list_blobs(name_starts_with=prefix)
            if b.name.endswith("listed_properties.parquet")]
