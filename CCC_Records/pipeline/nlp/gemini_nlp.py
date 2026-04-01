# NLP Extraction with Gemini API
# Processes all cases that don't have NLP JSON yet, respecting API rate limits.

from google import genai
import json
import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))
from apikeys import gemini_key
from output_format.pydantic_data_models import CCCRecord

import sqlite3

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent.parent / "ccc_records.db"
MODEL = "gemma-3-1b"
REQUESTS_PER_MINUTE = 25       # stay well under 30 RPM free-tier limit
INPUT_TOKENS_PER_MIN = 10_000  # stay under 15k free-tier limit
SLEEP_BETWEEN = 60 / REQUESTS_PER_MINUTE  # seconds between requests
MAX_RETRIES = 5                 # per-case retry limit
BATCH_COMMIT_SIZE = 5           # commit to DB every N successful writes

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gemini_nlp")

# Sentinel written to the DB while a case is being processed.
# Prevents other instances from picking up the same row.
IN_PROGRESS = "__in_progress__"

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def connect_db() -> sqlite3.Connection:
    """Open DB with WAL mode and a generous busy-timeout for parallel access."""
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.row_factory = sqlite3.Row
    # WAL lets multiple readers and one writer coexist without blocking.
    conn.execute("PRAGMA journal_mode=WAL")
    # If SQLite can't get a write lock immediately, retry for up to 60 s.
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def ensure_nlp_column(conn: sqlite3.Connection):
    """Add json column to cases table if it doesn't exist yet."""
    cols = [row[1] for row in conn.execute("PRAGMA table_info(cases)")]
    if "json" not in cols:
        conn.execute("ALTER TABLE cases ADD COLUMN json TEXT")
        conn.commit()
        log.info("Added 'json' column to cases table.")


def count_remaining(conn: sqlite3.Connection) -> int:
    """Count cases that still need processing (NULL or stuck in-progress)."""
    row = conn.execute(
        "SELECT COUNT(*) FROM cases WHERE json IS NULL AND extracted_text IS NOT NULL"
    ).fetchone()
    return row[0]


def claim_next_case(conn: sqlite3.Connection) -> tuple[str, str, str] | None:
    """
    Atomically claim one unprocessed case for this instance.

    Strategy:
      1. SELECT a candidate row where json IS NULL.
      2. UPDATE that exact row to IN_PROGRESS *only if* json IS still NULL.
      3. Check rowcount — if 1, we won the race; if 0, another instance
         claimed it first, so loop and try the next candidate.

    Returns (case_start_id, na_id, extracted_text) or None if no work left.
    """
    while True:
        row = conn.execute("""
            SELECT case_start_id, na_id, extracted_text
            FROM cases
            WHERE json IS NULL
              AND extracted_text IS NOT NULL
            LIMIT 1
        """).fetchone()

        if row is None:
            return None  # Nothing left to process

        cur = conn.execute("""
            UPDATE cases
            SET json = ?
            WHERE case_start_id = ? AND json IS NULL
        """, (IN_PROGRESS, row["case_start_id"]))
        conn.commit()

        if cur.rowcount == 1:
            # We claimed it successfully
            return (row["case_start_id"], row["na_id"], row["extracted_text"])
        # Another instance claimed it first — try the next row


def save_nlp_result(conn: sqlite3.Connection, case_start_id: str, nlp_json: str):
    """Overwrite the IN_PROGRESS sentinel with the real NLP JSON."""
    conn.execute(
        "UPDATE cases SET json = ? WHERE case_start_id = ?",
        (nlp_json, case_start_id),
    )
    conn.commit()


def release_case(conn: sqlite3.Connection, case_start_id: str):
    """Reset a failed case back to NULL so it can be retried later."""
    conn.execute(
        "UPDATE cases SET json = NULL WHERE case_start_id = ? AND json = ?",
        (case_start_id, IN_PROGRESS),
    )
    conn.commit()

# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------

def preprocess_case(extracted_text_json: str) -> str:
    """Strip objectIds; return a JSON array of page-text strings."""
    pages = json.loads(extracted_text_json)
    texts = []
    for page in pages:
        if isinstance(page, dict) and "text" in page:
            texts.append(page["text"])
        elif isinstance(page, str):
            texts.append(page)
    return json.dumps(texts)

# ---------------------------------------------------------------------------
# Gemini call with retry + backoff
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are extracting structured data from a scanned CCC enrollee record.
The data is a JSON array of strings. Each item in the array is a page.
The text was produced by OCR and may contain noise, garbled words,
split numbers, and illegible sections.

Rules:
- Dates must be ISO-8601 (YYYY-MM-DD). Use YYYY-MM if day is unknown.
  Set to null if the date is illegible.
- Always-present array sections (service_record, inoculations, etc.)
  must be [] if no data is found — never omit them.
- Preserve quoted speech verbatim (e.g. member_statement).
- If a field is genuinely absent or illegible, use null.
- Do not invent or infer values. Only extract what is on the page.
"""

client = genai.Client(api_key=gemini_key)
SCHEMA = CCCRecord.model_json_schema()


def call_gemini(case_text: str) -> str | None:
    """Call Gemini with exponential backoff.  Returns JSON string or None."""
    backoff = 10  # initial wait on rate-limit
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=MODEL,
                config={
                    "system_instruction": SYSTEM_PROMPT,
                    "response_mime_type": "application/json",
                    "response_json_schema": SCHEMA,
                },
                contents=[case_text],
            )
            return response.text
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                # Parse retry delay if present
                try:
                    import re
                    match = re.search(r"retry in ([\d.]+)s", msg)
                    wait = float(match.group(1)) + 5 if match else backoff
                except Exception:
                    wait = backoff
                log.warning(
                    "Rate limited (attempt %d/%d). Waiting %.0fs …",
                    attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                backoff = min(backoff * 2, 120)
            elif "400" in msg or "INVALID_ARGUMENT" in msg:
                log.error("Schema / payload error – skipping case: %s", msg[:200])
                return None
            else:
                log.error(
                    "Unexpected error (attempt %d/%d): %s",
                    attempt, MAX_RETRIES, msg[:200],
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
    log.error("Exhausted retries for case.")
    return None

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    conn = connect_db()
    ensure_nlp_column(conn)

    remaining = count_remaining(conn)
    log.info("~%d unprocessed cases visible at startup.", remaining)

    if remaining == 0:
        log.info("Nothing to do – exiting.")
        conn.close()
        return

    successes = 0
    failures = 0
    processed = 0

    while True:
        # Atomically claim the next available case.
        claimed = claim_next_case(conn)
        if claimed is None:
            log.info("No more unprocessed cases – exiting.")
            break

        case_start_id, na_id, extracted_text = claimed
        processed += 1
        log.info(
            "[#%d] Processing case_start_id=%s  na_id=%s",
            processed, case_start_id, na_id,
        )

        # Preprocess
        try:
            case_text = preprocess_case(extracted_text)
        except Exception as exc:
            log.error("  Preprocess failed: %s – releasing back to queue.", exc)
            release_case(conn, case_start_id)
            failures += 1
            continue

        # Call Gemini
        result = call_gemini(case_text)

        if result is not None:
            save_nlp_result(conn, case_start_id, result)  # also commits
            successes += 1
            log.info("  ✓ Saved NLP result.")
        else:
            # Put the row back so another instance or a future run can retry it
            release_case(conn, case_start_id)
            failures += 1
            log.warning("  ✗ No result – released case back to queue.")

        # Rate-limit pacing
        time.sleep(SLEEP_BETWEEN)

    conn.close()
    log.info(
        "Done. %d succeeded, %d failed/released out of %d claimed.",
        successes, failures, processed,
    )


if __name__ == "__main__":
    main()