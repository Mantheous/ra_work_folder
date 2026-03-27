# STEP 2: Extract text from NARA proxy server (async version)
# Reads hits from the database that don't have raw_text yet,
# fetches OCR text in chunks of 100 via the proxy API,
# tracks progress in the extraction_chunks table,
# and assembles the final raw_text once all chunks are done.
#
# Chunks are fetched concurrently using asyncio + aiohttp so that
# other chunks/NAIDs can proceed while one is waiting on backoff.

import asyncio
import logging
import math
import random
from datetime import datetime, timezone

import aiohttp

from database_init import get_connection

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://catalog.archives.gov/proxy/extractedText/"
CHUNK_LIMIT = 100  # Always request 100 objects per page

MAX_HTML_RETRIES = 30      # HTML = server glitch, retry immediately many times
MAX_EMPTY_RETRIES = 8      # Empty JSON = server processing, backoff patiently
INITIAL_BACKOFF = 5        # Starting delay for empty-JSON backoff (seconds)
MAX_BACKOFF = 600          # Cap backoff at 10 minutes
JITTER_FRACTION = 0.25     # Add 0-25% random jitter to backoff delays
POLITE_DELAY = 0.1         # Seconds between successful fetches
REQUEST_TIMEOUT = 60       # Seconds before a request times out
MAX_CONCURRENT = 10        # Max simultaneous chunk fetches

# Browser-like headers to reduce likelihood of being blocked/rate-limited
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Cache-Control": "no-cache",  # Prevents 304 responses so each request is like a hard reload
    "Pragma": "no-cache",  # Ditto
    "Referer": "https://catalog.archives.gov/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


class ConsecutiveTimeoutError(Exception):
    """Raised when two requests in a row time out, indicating a connectivity problem."""
    pass


# ---------------------------------------------------------------------------
# Shared mutable state (protected by asyncio.Lock)
# ---------------------------------------------------------------------------
class TimeoutTracker:
    """Thread-safe tracker for consecutive timeouts across concurrent tasks."""

    def __init__(self):
        self._count = 0
        self._lock = asyncio.Lock()

    async def record_success(self):
        async with self._lock:
            self._count = 0

    async def record_timeout(self, na_id: str, page: int):
        async with self._lock:
            self._count += 1
            current = self._count
        log.error(
            "  na_id=%s page=%d: REQUEST TIMED OUT (%d consecutive)",
            na_id, page, current,
        )
        if current >= 2:
            raise ConsecutiveTimeoutError(
                f"Two consecutive timeouts. Stopping program. "
                f"Last: na_id={na_id} page={page}"
            )


# ---------------------------------------------------------------------------
# Fetching with tiered retry strategy (async)
# ---------------------------------------------------------------------------
async def fetch_chunk(session: aiohttp.ClientSession, na_id: str, page: int,
                      timeout_tracker: TimeoutTracker,
                      semaphore: asyncio.Semaphore | None = None) -> dict | None:
    """
    Fetch a single chunk (page of up to 100 digitalObjects) from the proxy.

    Retry strategy based on failure type:
      - HTML response  -> retry IMMEDIATELY (server glitch, just needs another shot)
      - Empty JSON     -> exponential backoff up to 10 minutes (server is processing)
      - Timeout        -> if 2 in a row, raise ConsecutiveTimeoutError to stop program

    Returns the parsed JSON dict on success, or None after exhausting retries.

    When a semaphore is provided, the semaphore is held during active network
    requests but RELEASED during backoff sleeps so other tasks can proceed.
    """
    url = f"{BASE_URL}{na_id}"
    html_attempts = 0
    empty_attempts = 0
    backoff_delay = INITIAL_BACKOFF
    client_timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    max_total_attempts = MAX_HTML_RETRIES + MAX_EMPTY_RETRIES  # absolute safety cap

    for attempt in range(1, max_total_attempts + 1):
        # Acquire the semaphore for the actual network request
        if semaphore is not None:
            await semaphore.acquire()

        # Track whether we already released the semaphore (for backoff/error paths).
        # The finally block will release it if we haven't already.
        sem_released = False

        try:
            params = {"page": page, "limit": CHUNK_LIMIT}
            if attempt > 1:
                params["_t"] = int(asyncio.get_event_loop().time() * 1000)

            async with session.get(url, headers=HEADERS, params=params,
                                   timeout=client_timeout) as resp:
                # --- SUCCESS: reset timeout counter ---
                await timeout_tracker.record_success()

                # --- CHECK: HTML response (server returned the React SPA shell) ---
                content_type = resp.headers.get("Content-Type", "")
                if "html" in content_type.lower():
                    html_attempts += 1
                    if html_attempts >= MAX_HTML_RETRIES:
                        log.error(
                            "  na_id=%s page=%d: Got HTML %d times in a row. Giving up.",
                            na_id, page, html_attempts,
                        )
                        return None
                    log.warning(
                        "  [HTML %d/%d] na_id=%s page=%d: Got HTML, retrying immediately...",
                        html_attempts, MAX_HTML_RETRIES, na_id, page,
                    )
                    continue  # Retry immediately, no delay

                resp.raise_for_status()
                data = await resp.json()

                # --- CHECK: Missing digitalObjects key ---
                if "digitalObjects" not in data:
                    log.warning(
                        "  na_id=%s page=%d: Response missing 'digitalObjects': %s",
                        na_id, page, list(data.keys()),
                    )
                    return None

                # --- CHECK: Empty digitalObjects (server is processing/not ready) ---
                if len(data.get("digitalObjects", [])) == 0:
                    empty_attempts += 1
                    if empty_attempts >= MAX_EMPTY_RETRIES:
                        log.error(
                            "  na_id=%s page=%d: Got empty JSON %d times. Giving up.",
                            na_id, page, empty_attempts,
                        )
                        return None
                    jitter = backoff_delay * random.uniform(0, JITTER_FRACTION)
                    wait = min(backoff_delay + jitter, MAX_BACKOFF)
                    log.warning(
                        "  [EMPTY %d/%d] na_id=%s page=%d: Empty JSON. Backing off %.0fs...",
                        empty_attempts, MAX_EMPTY_RETRIES, na_id, page, wait,
                    )
                    # Release semaphore BEFORE sleeping so other tasks can proceed
                    if semaphore is not None:
                        semaphore.release()
                        sem_released = True
                    await asyncio.sleep(wait)
                    backoff_delay = min(backoff_delay * 2, MAX_BACKOFF)
                    continue  # Will re-acquire semaphore at top of loop

                # --- SUCCESS: Got real data ---
                return data

        except asyncio.TimeoutError:
            await timeout_tracker.record_timeout(na_id, page)
            continue  # Try once more before potentially stopping

        except aiohttp.ClientError as e:
            await timeout_tracker.record_success()  # Not a timeout, reset counter
            log.warning(
                "  [Attempt %d] na_id=%s page=%d: %s",
                attempt, na_id, page, e,
            )
            # Release semaphore before sleeping on error
            if semaphore is not None:
                semaphore.release()
                sem_released = True
            await asyncio.sleep(1)
            continue  # Will re-acquire semaphore at top of loop

        finally:
            # Release semaphore if not already released (normal return, HTML retry, timeout, etc.)
            if semaphore is not None and not sem_released:
                semaphore.release()

    return None


# ---------------------------------------------------------------------------
# Step 1: Populate extraction_chunks for hits that need text
# ---------------------------------------------------------------------------
async def populate_chunks(conn, session: aiohttp.ClientSession,
                          timeout_tracker: TimeoutTracker):
    """
    For each hit with raw_text IS NULL, ensure extraction_chunks rows exist.
    If no rows exist yet for a hit, fetch page 1 to discover the total,
    then create rows for all chunks.
    """
    pending_hits = conn.execute(
        "SELECT na_id FROM hits WHERE raw_text IS NULL"
    ).fetchall()

    if not pending_hits:
        log.info("No hits need text extraction.")
        return 0

    log.info("Found %d hits without raw_text.", len(pending_hits))
    populated = 0

    for row in pending_hits:
        na_id = row["na_id"]

        # Check if chunks already exist for this na_id
        existing = conn.execute(
            "SELECT COUNT(*) as cnt FROM extraction_chunks WHERE na_id = ?",
            (na_id,),
        ).fetchone()["cnt"]

        if existing > 0:
            continue  # Already populated

        # Fetch page 1 to discover total (no semaphore — sequential discovery)
        log.info("Discovering total for na_id=%s …", na_id)
        data = await fetch_chunk(session, na_id, page=1,
                                 timeout_tracker=timeout_tracker)

        if data is None:
            log.error("  Could not reach proxy for na_id=%s. Skipping chunk creation.", na_id)
            continue

        total = data.get("total", 0)
        if total == 0:
            log.warning("  na_id=%s has total=0 objects. Marking as extracted with empty text.", na_id)
            conn.execute(
                "UPDATE hits SET raw_text = '', status = 'extracted' WHERE na_id = ?",
                (na_id,),
            )
            conn.commit()
            continue

        num_chunks = math.ceil(total / CHUNK_LIMIT)
        log.info("  na_id=%s: total=%d objects → %d chunks", na_id, total, num_chunks)

        # Insert chunk rows
        for chunk_page in range(1, num_chunks + 1):
            conn.execute(
                """INSERT OR IGNORE INTO extraction_chunks
                   (na_id, chunk_page, total_objects, status, attempts)
                   VALUES (?, ?, ?, 'pending', 0)""",
                (na_id, chunk_page, total),
            )

        # The page-1 data we already fetched — store it right away
        text = _extract_text_from_response(data)
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """UPDATE extraction_chunks
               SET extracted_text = ?, status = 'done', attempts = 1, last_attempt = ?
               WHERE na_id = ? AND chunk_page = 1""",
            (text, now, na_id),
        )
        conn.commit()
        populated += 1
        log.info("  Stored page 1 for na_id=%s (%d objects in chunk)", na_id, len(data.get("digitalObjects", [])))

        await asyncio.sleep(POLITE_DELAY)

    return populated


# ---------------------------------------------------------------------------
# Step 2: Fetch all pending/failed chunks (concurrently)
# ---------------------------------------------------------------------------
async def _fetch_one_chunk(conn, session: aiohttp.ClientSession,
                           chunk_id: int, na_id: str, chunk_page: int,
                           timeout_tracker: TimeoutTracker,
                           semaphore: asyncio.Semaphore,
                           results: dict):
    """Fetch a single chunk and record the result in the database."""
    now = datetime.now(timezone.utc).isoformat()

    log.info("Fetching na_id=%s chunk page=%d …", na_id, chunk_page)

    data = await fetch_chunk(session, na_id, chunk_page,
                             timeout_tracker=timeout_tracker,
                             semaphore=semaphore)

    if data is not None:
        text = _extract_text_from_response(data)
        conn.execute(
            """UPDATE extraction_chunks
               SET extracted_text = ?, status = 'done',
                   attempts = attempts + 1, last_attempt = ?
               WHERE id = ?""",
            (text, now, chunk_id),
        )
        results["done"] += 1
    else:
        conn.execute(
            """UPDATE extraction_chunks
               SET status = 'failed',
                   attempts = attempts + 1, last_attempt = ?
               WHERE id = ?""",
            (now, chunk_id),
        )
        results["failed"] += 1

    conn.commit()  # Checkpoint after every chunk


async def fetch_pending_chunks(conn, session: aiohttp.ClientSession,
                               timeout_tracker: TimeoutTracker):
    """Fetch text for all extraction_chunks that are not yet done, concurrently."""
    pending = conn.execute(
        """SELECT id, na_id, chunk_page
           FROM extraction_chunks
           WHERE status != 'done'
           ORDER BY na_id, chunk_page"""
    ).fetchall()

    if not pending:
        log.info("No pending chunks to fetch.")
        return 0, 0

    log.info("Found %d pending/failed chunks to fetch (max %d concurrent).",
             len(pending), MAX_CONCURRENT)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results = {"done": 0, "failed": 0}

    tasks = [
        _fetch_one_chunk(conn, session, row["id"], row["na_id"], row["chunk_page"],
                         timeout_tracker, semaphore, results)
        for row in pending
    ]

    await asyncio.gather(*tasks, return_exceptions=True)

    return results["done"], results["failed"]


# ---------------------------------------------------------------------------
# Step 3: Assemble completed hits
# ---------------------------------------------------------------------------
def assemble_completed_hits(conn):
    """
    For each hit where ALL extraction_chunks are 'done',
    concatenate the text in chunk_page order and write to hits.raw_text.
    """
    # Find na_ids where raw_text is still NULL
    candidates = conn.execute(
        "SELECT na_id FROM hits WHERE raw_text IS NULL"
    ).fetchall()

    assembled = 0
    for row in candidates:
        na_id = row["na_id"]

        # Check if all chunks for this na_id are done
        stats = conn.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done
               FROM extraction_chunks WHERE na_id = ?""",
            (na_id,),
        ).fetchone()

        if stats["total"] == 0:
            continue  # No chunks yet (populate step may have failed)

        if stats["done"] < stats["total"]:
            continue  # Not all chunks done yet

        # All done — concatenate in page order
        chunks = conn.execute(
            """SELECT extracted_text FROM extraction_chunks
               WHERE na_id = ? ORDER BY chunk_page""",
            (na_id,),
        ).fetchall()

        full_text = "\n".join(
            chunk["extracted_text"] for chunk in chunks
            if chunk["extracted_text"]
        )

        conn.execute(
            "UPDATE hits SET raw_text = ?, status = 'extracted' WHERE na_id = ?",
            (full_text, na_id),
        )
        assembled += 1
        log.info("Assembled raw_text for na_id=%s (%d chunks)", na_id, stats["total"])

    conn.commit()
    return assembled


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _extract_text_from_response(data: dict) -> str:
    """
    Concatenate all extractedText fields from the digitalObjects in a response.
    Objects with null extractedText are included as empty strings.
    """
    digital_objects = data.get("digitalObjects", [])
    parts = []
    for obj in digital_objects:
        text = obj.get("extractedText")
        if text:
            parts.append(text.strip())
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    conn = get_connection()
    timeout_tracker = TimeoutTracker()

    try:
        async with aiohttp.ClientSession() as session:
            # Step 1: Populate chunks for hits that need extraction
            log.info("=" * 60)
            log.info("STEP 1: Populating extraction chunks")
            log.info("=" * 60)
            populated = await populate_chunks(conn, session, timeout_tracker)
            log.info("Populated chunks for %d new hits.", populated)

            # Step 2: Fetch all pending/failed chunks (concurrently)
            log.info("=" * 60)
            log.info("STEP 2: Fetching pending chunks (up to %d concurrent)", MAX_CONCURRENT)
            log.info("=" * 60)
            done, failed = await fetch_pending_chunks(conn, session, timeout_tracker)
            log.info("Fetched: %d done, %d failed.", done, failed)

            # Step 3: Assemble completed hits
            log.info("=" * 60)
            log.info("STEP 3: Assembling completed hits")
            log.info("=" * 60)
            assembled = assemble_completed_hits(conn)
            log.info("Assembled raw_text for %d hits.", assembled)

    except ConsecutiveTimeoutError as e:
        log.error("STOPPING: %s", e)
        log.error("Progress has been saved. Re-run when connectivity is restored.")
        # Still assemble any completed hits before exiting
        assembled = assemble_completed_hits(conn)
        if assembled:
            log.info("Assembled raw_text for %d hits before stopping.", assembled)

    # Summary
    log.info("=" * 60)
    total_chunks = conn.execute("SELECT COUNT(*) as cnt FROM extraction_chunks").fetchone()["cnt"]
    done_chunks = conn.execute("SELECT COUNT(*) as cnt FROM extraction_chunks WHERE status = 'done'").fetchone()["cnt"]
    failed_chunks = conn.execute("SELECT COUNT(*) as cnt FROM extraction_chunks WHERE status = 'failed'").fetchone()["cnt"]
    pending_chunks = conn.execute("SELECT COUNT(*) as cnt FROM extraction_chunks WHERE status = 'pending'").fetchone()["cnt"]
    extracted_hits = conn.execute("SELECT COUNT(*) as cnt FROM hits WHERE status = 'extracted'").fetchone()["cnt"]
    remaining_hits = conn.execute("SELECT COUNT(*) as cnt FROM hits WHERE raw_text IS NULL").fetchone()["cnt"]

    log.info("SUMMARY")
    log.info("  Chunks: %d total | %d done | %d failed | %d pending", total_chunks, done_chunks, failed_chunks, pending_chunks)
    log.info("  Hits: %d extracted | %d remaining", extracted_hits, remaining_hits)

    if failed_chunks > 0 or pending_chunks > 0:
        log.info("  Re-run this script to retry the %d remaining chunks.", failed_chunks + pending_chunks)

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())