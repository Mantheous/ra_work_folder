# NLP Extraction with a local Ollama model
# Functionally equivalent to gemini_nlp.py but runs entirely on-device.
# Requires: pip install ollama   (and a running Ollama server with a model pulled)

import json
import sys
import time
import logging
import re
from pathlib import Path

import ollama

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))
from output_format.pydantic_data_models import CCCRecord

import sqlite3

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent.parent / "ccc_records.db"
MODEL = "qwen2.5:7b"              # change to any model you've pulled locally
MAX_RETRIES = 3                    # per-case retry limit
BATCH_COMMIT_SIZE = 5              # commit to DB every N successful writes

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("local_nlp")

# ---------------------------------------------------------------------------
# Database helpers  (identical to gemini_nlp)
# ---------------------------------------------------------------------------

def ensure_nlp_column(conn: sqlite3.Connection):
    """Add json column to cases table if it doesn't exist yet."""
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
# Preprocessing  (identical to gemini_nlp)
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
# Ollama call with retry + backoff
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
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

You MUST respond with ONLY a valid JSON object matching this schema — no markdown, no explanation, no extra text:

"""

SCHEMA = CCCRecord.model_json_schema()


def _extract_json(text: str) -> str | None:
    """Try to extract a JSON object from model output that may contain markdown fences."""
    # Strip markdown code fences if present
    stripped = text.strip()
    if stripped.startswith("```"):
        # Remove opening fence (with optional language tag)
        stripped = re.sub(r"^```[a-zA-Z]*\n?", "", stripped)
        # Remove closing fence
        stripped = re.sub(r"\n?```\s*$", "", stripped)
        stripped = stripped.strip()

    # Try to parse the cleaned string
    try:
        json.loads(stripped)
        return stripped
    except json.JSONDecodeError:
        pass

    # As a fallback, find the first { … } block
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start:end + 1]
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            pass

    return None


def call_ollama(case_text: str) -> str | None:
    """Call a local Ollama model with retry.  Returns JSON string or None."""
    system_msg = SYSTEM_PROMPT + json.dumps(SCHEMA, indent=2)
    backoff = 5
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = ollama.chat(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": case_text},
                ],
                options={
                    "temperature": 0.1,      # low temp for deterministic extraction
                    "num_predict": 4096,      # enough tokens for output
                },
                format="json",               # request JSON mode if model supports it
            )
            raw = response["message"]["content"]

            # Validate it's parseable JSON
            extracted = _extract_json(raw)
            if extracted is None:
                log.warning(
                    "  Model returned non-JSON (attempt %d/%d). Retrying…",
                    attempt, MAX_RETRIES,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            return extracted

        except ollama.ResponseError as exc:
            log.error(
                "Ollama error (attempt %d/%d): %s",
                attempt, MAX_RETRIES, str(exc)[:200],
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as exc:
            log.error(
                "Unexpected error (attempt %d/%d): %s",
                attempt, MAX_RETRIES, str(exc)[:200],
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    log.error("Exhausted retries for case.")
    return None

# ---------------------------------------------------------------------------
# Main loop  (mirrors gemini_nlp structure)
# ---------------------------------------------------------------------------

def main():
    # Quick connectivity check
    try:
        ollama.list()
    except Exception as exc:
        log.error("Cannot reach Ollama server. Is it running?  (%s)", exc)
        raise SystemExit(1)

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

        # Call local model
        result = call_ollama(case_text)

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

        # No rate-limit pacing needed for local models

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
