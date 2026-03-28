# STEP 1: Search NARA API for records matching the query
# Find the NAIDs and ObjectIDs for each page in the query.

# API Documentation: 
# Swagger: https://catalog.archives.gov/api/v2/api-docs/#/
# Github: https://github.com/usnationalarchives/Catalog-API/tree/master



import argparse
import json
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
### Arguments
DEFAULT_QUERY = '"Kings Mountain" AND "Co 4479"'
DEFAULT_RECORD_GROUP = "146" 
# Don't set this to None. It will break unless you do some modifications on the code.
# Basically what you would have to do is get it to take the record group from the 
# response which wouldn't be too hard

API_URL = "https://catalog.archives.gov/api/v2/records/search"
HEADERS = {"x-api-key": nara_key}

PAGE_LIMIT = 100         # max results per page (limit param)
MAX_RETRIES = 5
INITIAL_BACKOFF = 2     # seconds

API_PAGES_DIR = Path(__file__).resolve().parent.parent / "api_pages"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_page(query: str, page: int, record_group: int | None = None) -> dict:
    """Fetch a single page of search results with retry + exponential backoff."""
    params = {
        "q": query,
        "limit": PAGE_LIMIT,
        "page": page,
    }
    if record_group is not None:
        params["recordGroupNumber"] = record_group
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

def main(query: str, record_group: str | None = None):
    """Run the full pagination loop and store results in SQLite."""
    conn = get_connection()
    page = 1

    # Ensure output directory exists
    API_PAGES_DIR.mkdir(parents=True, exist_ok=True)

    # First request — also tells us the total
    log.info("Searching NARA for: %s (record group: %s)", query, record_group)
    data = fetch_page(query, page, record_group)
    total = total_hits(data)
    log.info("Total hits: %d", total)

    records_added = 0

    while True:
        hits = extract_hits(data)
        if not hits:
            break

        # Save raw API response for this page
        page_file = API_PAGES_DIR / f"page_{page:03d}.json"
        with open(page_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        log.info("Saved page %d to %s", page, page_file.name)

        for hit in hits:
            na_id = hit.get("_id")
            source = hit.get("_source", {})
            record = source.get("record", {})
            title = record.get("title", "")

            digital_objects = record.get("digitalObjects") or []
            if not digital_objects:
                continue
            object_id_start = digital_objects[0].get("objectId")
            object_id_end   = digital_objects[-1].get("objectId")

            # Upsert into 'hits'
            conn.execute(
                "INSERT OR IGNORE INTO hits (na_id, record_group, record_title, object_id_start, object_id_end) VALUES (?, ?, ?, ?, ?)",
                (na_id, record_group, title, object_id_start, object_id_end),
            )
            records_added += 1

        conn.commit()

        page += 1
        fetched_so_far = (page - 1) * PAGE_LIMIT
        if fetched_so_far >= total:
            break
        
        time.sleep(0.2)
        log.info("Fetching page %d (≈offset %d / %d) …", page, fetched_so_far, total)
        data = fetch_page(query, page, record_group)

    log.info("Indexing complete. Records added: %d", records_added)
    conn.close()

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collects data for extracting cases data from NARA")
    parser.add_argument(
        "query",
        nargs="?",
        default=DEFAULT_QUERY,
        help=f'Search query (default: {DEFAULT_QUERY})',
    )
    parser.add_argument(
        "--record-group",
        type=str,
        default=DEFAULT_RECORD_GROUP,
        metavar="NUM",
        help=f"NARA record group number to filter by (default: {DEFAULT_RECORD_GROUP})",
    )
    args = parser.parse_args()
    main(args.query, args.record_group)
