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
MODEL = "gemini-2.5-flash-lite"
REQUESTS_PER_MINUTE = 13        # stay well under 15 RPM free-tier limit
INPUT_TOKENS_PER_MIN = 200_000  # stay under 250k free-tier limit
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

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def ensure_nlp_column(conn: sqlite3.Connection):
    """Add nlp_json column to cases table if it doesn't exist yet."""
    cols = [row[1] for row in conn.execute("PRAGMA table_info(cases)")]
    if "json" not in cols:
        conn.execute("ALTER TABLE cases ADD COLUMN json TEXT")
        conn.commit()
        log.info("Added 'json' column to cases table.")


def get_unprocessed_cases(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    """Return (case_start_id, na_id, extracted_text) for all cases missing NLP."""
    rows = conn.execute("""
        SELECT case_start_id, na_id, extracted_text
        FROM cases
        WHERE json IS NULL
          AND extracted_text IS NOT NULL
    """).fetchall()
    return rows


def save_nlp_result(conn: sqlite3.Connection, case_start_id: str, nlp_json: str):
    """Write the NLP JSON string to the database for a given case."""
    conn.execute(
        "UPDATE cases SET json = ? WHERE case_start_id = ?",
        (nlp_json, case_start_id),
    )

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
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    ensure_nlp_column(conn)
    cases = get_unprocessed_cases(conn)
    total = len(cases)
    log.info("Found %d unprocessed cases.", total)

    if total == 0:
        log.info("Nothing to do – exiting.")
        return

    successes = 0
    failures = 0
    uncommitted = 0

    for idx, (case_start_id, na_id, extracted_text) in enumerate(cases, 1):
        log.info(
            "[%d/%d] Processing case_start_id=%s  na_id=%s",
            idx, total, case_start_id, na_id,
        )

        # Preprocess
        try:
            case_text = preprocess_case(extracted_text)
        except Exception as exc:
            log.error("  Preprocess failed: %s – skipping.", exc)
            failures += 1
            continue

        # Call Gemini
        result = call_gemini(case_text)

        if result is not None:
            save_nlp_result(conn, case_start_id, result)
            successes += 1
            uncommitted += 1
            log.info("  ✓ Saved NLP result.")
        else:
            failures += 1
            log.warning("  ✗ No result for this case.")

        # Periodic commit
        if uncommitted >= BATCH_COMMIT_SIZE:
            conn.commit()
            uncommitted = 0
            log.info("  Committed batch to DB.")

        # Rate-limit pacing
        time.sleep(SLEEP_BETWEEN)

    # Final commit
    if uncommitted > 0:
        conn.commit()

    conn.close()
    log.info(
        "Done. %d succeeded, %d failed out of %d total.",
        successes, failures, total,
    )


if __name__ == "__main__":
    main()