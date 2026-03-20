# STEP 1: Search NARA API for records matching the query
# Find the NAIDs and ObjectIDs for each page in the query.

# API Documentation: 
# Swagger: https://catalog.archives.gov/api/v2/api-docs/#/
# Github: https://github.com/usnationalarchives/Catalog-API/tree/master


import argparse
import logging
import time
import sys
from pathlib import Path

import requests

# Allow importing apikeys from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from apikeys import nara_key
from database_init import get_connection

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_URL = "https://catalog.archives.gov/api/v2/records/search"
HEADERS = {"x-api-key": nara_key}
DEFAULT_QUERY = '"Kings Mountain" AND "Co 4479"'
PAGE_SIZE = 50          # rows per API request (max may be 100)
MAX_RETRIES = 5
INITIAL_BACKOFF = 2     # seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_page(query: str, offset: int) -> dict:
    """Fetch a single page of search results with retry + exponential backoff."""
    params = {
        "q": query,
        "rows": PAGE_SIZE,
        "offset": offset,
    }
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                log.info("Retrying in %ds …", backoff)
                time.sleep(backoff)
                backoff *= 2
            else:
                log.error("All %d attempts exhausted. Aborting.", MAX_RETRIES)
                raise

def extract_hits(data: dict) -> list[dict]:
    """Pull the list of hit dicts from the API response."""
    return (
        data.get("body", {})
            .get("hits", {})
            .get("hits", [])
    )

def total_hits(data: dict) -> int:
    """Return the total number of hits reported by the API."""
    return (
        data.get("body", {})
            .get("hits", {})
            .get("total", {})
            .get("value", 0)
    )

# ---------------------------------------------------------------------------
# Main indexing logic
# ---------------------------------------------------------------------------

# Since you can only request upto 100 records at a time we need to keep track of what
# page we are on.

def index_query(query: str):
    """Run the full pagination loop and store results in SQLite."""
    conn = get_connection()
    offset = 0

    # First request — also tells us the total
    log.info("Searching NARA for: %s", query)
    data = fetch_page(query, offset)
    total = total_hits(data)
    log.info("Total hits: %d", total)

    records_added = 0
    pages_added = 0

    while True:
        hits = extract_hits(data)
        if not hits:
            break

        for hit in hits:
            na_id = hit.get("_id")
            source = hit.get("_source", {})
            record = source.get("record", {})
            title = record.get("title", "")

            # Upsert each digital object into 'pages'
            digital_objects = record.get("digitalObjects") or []
            for obj in digital_objects:
                obj_id = obj.get("objectId")
                if not obj_id:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO pages (object_id, na_id, filename) "
                    "VALUES (?, ?, ?)",
                    (obj_id, na_id, obj.get("objectFilename", "")),
                )
                pages_added += 1

        conn.commit()

        offset += PAGE_SIZE
        if offset >= total:
            break

        log.info("Fetching offset %d / %d …", offset, total)
        data = fetch_page(query, offset)

    log.info("Indexing complete. Records: %d | Pages: %d", records_added, pages_added)
    conn.close()
