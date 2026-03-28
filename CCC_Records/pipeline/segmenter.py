# STEP: 3
# breaks extracted_text into cases

import asyncio
import json
import logging
from database_init import get_connection
from collections import Counter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

MAX_CONCURRENT = 10
MATCH_THRESHOLD = 0.50

def is_case_marker(text: str) -> bool:
    # Rule 1: Reject if it's 50 characters or longer
    # Rule 2: Check if we found at least 75% of the 14 letters (10.5 letters)
    if len(text) >= 50:
        return False

    target_phrase = "NEXTCASEBEGINS"
    target_counts = Counter(target_phrase)
    text_counts = Counter(text.upper())

    matched_letters = 0
    for char, required_amount in target_counts.items():
        matched_letters += min(required_amount, text_counts.get(char, 0))
        
    threshold = len(target_phrase) * MATCH_THRESHOLD
    return matched_letters >= threshold

def get_extracted_hits(conn):
    """Return na_ids of hits that have been fully extracted."""
    # status = 'extracted' means all chunks are done
    return [row["na_id"] for row in conn.execute(
        "SELECT na_id FROM hits WHERE status = 'extracted'"
    ).fetchall()]

def is_hit_segmented(conn, na_id: str) -> bool:
    """Check if this na_id already has cases in the database."""
    count = conn.execute(
        "SELECT COUNT(*) as cnt FROM cases WHERE na_id = ?", (na_id,)
    ).fetchone()["cnt"]
    return count > 0

def get_all_pages_for_hit(conn, na_id: str) -> list[dict]:
    """
    Fetch all extraction_chunks for a hit, parse their JSON extracted_text,
    and return a single list of page objects ordered by chunk_page.
    """
    chunks = conn.execute(
        """SELECT extracted_text FROM extraction_chunks
           WHERE na_id = ? AND status = 'done'
           ORDER BY chunk_page""",
        (na_id,)
    ).fetchall()

    pages = []
    for chunk in chunks:
        text = chunk["extracted_text"]
        if text:
            try:
                pages.extend(json.loads(text))
            except json.JSONDecodeError:
                log.warning("Malformed JSON in chunk for na_id=%s", na_id)
    return pages

async def segment_and_save_cases(conn, na_id: str, pages: list[dict]):
    """
    Iterate through pages. When "NEXT CASE BEGINS" is found, save the
    accumulated pages as a case, then start a new accumulation.
    """
    cases_inserted = 0
    current_case_pages = []

    def save_current_case():
        nonlocal cases_inserted
        if not current_case_pages:
            return
        
        start_id = current_case_pages[0].get("objectId", "")
        end_id = current_case_pages[-1].get("objectId", "")
        
        # If there's no objectId (e.g. empty API response artifact), skip or use a fallback. 
        # For our schema, case_start_id is the primary key.
        if not start_id:
            return

        case_text_json = json.dumps(current_case_pages, ensure_ascii=False)
        
        conn.execute(
            """INSERT OR IGNORE INTO cases (case_start_id, case_end_id, na_id, extracted_text)
               VALUES (?, ?, ?, ?)""",
            (start_id, end_id, na_id, case_text_json)
        )
        cases_inserted += 1
        current_case_pages.clear()

    for page in pages:
        text = page.get("text", "")
        # This checks if the marker is anywhere in the text.
        if is_case_marker(text):
            # Save whatever we have so far as the prior case
            save_current_case()
        
        # Add the current page to the accumulator.
        # (If the marker is on this page, this page becomes the FIRST page of the new case)
        current_case_pages.append(page)
        
        # Yield to the event loop occasionally to allow other tasks to progress
        await asyncio.sleep(0)

    # Save the final accumulated case
    save_current_case()
    
    conn.commit()
    return cases_inserted

async def _process_one_hit(conn, na_id: str, semaphore: asyncio.Semaphore, results: dict):
    """Process a single hit concurrently."""
    async with semaphore:
        if is_hit_segmented(conn, na_id):
            log.info("Skipping na_id=%s (already segmented).", na_id)
            return

        pages = get_all_pages_for_hit(conn, na_id)
        if not pages:
            log.warning("na_id=%s has no valid pages to segment.", na_id)
            return

        inserted = await segment_and_save_cases(conn, na_id, pages)
        log.info("Segmented na_id=%s into %d cases.", na_id, inserted)
        results["total_cases"] += inserted

async def main():
    conn = get_connection()
    
    hits = get_extracted_hits(conn)
    if not hits:
        log.info("No extracted hits found to segment.")
        conn.close()
        return

    log.info("Found %d extracted hits.", len(hits))
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results = {"total_cases": 0}
    
    tasks = [
        _process_one_hit(conn, na_id, semaphore, results)
        for na_id in hits
    ]
    
    # Run all segmentation tasks concurrently
    await asyncio.gather(*tasks)

    log.info("=" * 60)
    log.info("SEGMENTATION COMPLETE")
    log.info("Total new cases added: %d", results["total_cases"])
    log.info("=" * 60)

    conn.close()

if __name__ == "__main__":
    asyncio.run(main())