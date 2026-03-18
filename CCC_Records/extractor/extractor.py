"""
Stage 2: Asynchronous Text Extraction
Reads pending pages from SQLite, fetches OCR text from the NARA proxy URL
using aiohttp with a concurrency limiter, and checkpoints progress.

Usage:
    python extractor.py              # Default concurrency (10)
    python extractor.py --workers 20
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import aiohttp

# Allow importing apikeys from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from apikeys import nara_key
from db import get_connection

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROXY_URL = "https://catalog.archives.gov/proxy/extractedText/{na_id}?objectId={object_id}"
HEADERS = {"x-api-key": nara_key}
DEFAULT_WORKERS = 10
MAX_RETRIES = 3
INITIAL_BACKOFF = 2  # seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Async fetching
# ---------------------------------------------------------------------------
async def fetch_text(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    na_id: str,
    object_id: str,
) -> tuple[str, str | None]:
    """Fetch extracted text for a single object_id. Returns (object_id, text)."""
    url = PROXY_URL.format(na_id=na_id, object_id=object_id)
    backoff = INITIAL_BACKOFF

    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        log.warning(
                            "HTTP %d for object %s (attempt %d/%d)",
                            resp.status, object_id, attempt, MAX_RETRIES,
                        )
                        if attempt < MAX_RETRIES:
                            await asyncio.sleep(backoff)
                            backoff *= 2
                            continue
                        return (object_id, None)

                    data = await resp.json(content_type=None)
                    # The proxy returns a JSON object with digitalObjects list
                    digital_objects = data.get("digitalObjects", [])
                    if digital_objects:
                        text = digital_objects[0].get("extractedText", "")
                    else:
                        text = ""
                    return (object_id, text)

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.warning(
                    "Error fetching object %s (attempt %d/%d): %s",
                    object_id, attempt, MAX_RETRIES, exc,
                )
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(backoff)
                    backoff *= 2
                else:
                    return (object_id, None)

    return (object_id, None)


async def run_extraction(workers: int):
    """Main extraction loop: read pending pages, fetch text, update DB."""
    conn = get_connection()

    pending = conn.execute(
        "SELECT object_id, na_id FROM pages WHERE status = 'pending'"
    ).fetchall()

    if not pending:
        log.info("No pending pages to extract.")
        conn.close()
        return

    log.info("Found %d pending pages. Starting extraction (workers=%d) …", len(pending), workers)

    semaphore = asyncio.Semaphore(workers)
    done_count = 0
    fail_count = 0

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = [
            fetch_text(session, semaphore, row["na_id"], row["object_id"])
            for row in pending
        ]

        # Process results as they complete
        for coro in asyncio.as_completed(tasks):
            object_id, text = await coro
            if text is not None:
                conn.execute(
                    "UPDATE pages SET raw_text = ?, status = 'done' WHERE object_id = ?",
                    (text, object_id),
                )
                done_count += 1
            else:
                fail_count += 1

            # Checkpoint every 50 pages
            if (done_count + fail_count) % 50 == 0:
                conn.commit()
                log.info(
                    "Progress: %d done, %d failed out of %d",
                    done_count, fail_count, len(pending),
                )

    conn.commit()
    conn.close()
    log.info(
        "Extraction complete. Done: %d | Failed: %d | Total: %d",
        done_count, fail_count, len(pending),
    )
    if fail_count > 0:
        log.info("Re-run this script to retry the %d failed pages.", fail_count)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract OCR text from NARA.")
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of concurrent requests (default: {DEFAULT_WORKERS})",
    )
    args = parser.parse_args()
    asyncio.run(run_extraction(args.workers))
