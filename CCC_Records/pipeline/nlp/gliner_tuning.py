# NLP Extraction with GLiNER (local, no API key needed)
# Functionally equivalent to gemini_nlp.py but uses GLiNER for entity extraction.
# GLiNER is an NER model — it finds entity *spans* in text rather than generating
# structured JSON.  This script runs GLiNER with labels that map to CCCRecord
# fields, then assembles the extracted entities into the Pydantic schema.
#
# Requires:  pip install gliner
#   (NOTE: on Windows you may need long-path support enabled for the torch
#    dependency.  See https://pip.pypa.io/warnings/enable-long-paths)

import json
import sys
import re
import logging
from pathlib import Path

import torch
from gliner import GLiNER

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))
from output_format.pydantic_data_models import CCCRecord

import sqlite3

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent.parent / "ccc_records (1).db"
GLINER_MODEL = "gliner-community/gliner_xxl-v2.5"   # same model used in the notebook
THRESHOLD = 0.10                              # confidence threshold for entity extraction
MAX_TEXT_LEN = 2000                         # GLiNER context window guard
RESULT_COUNT = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gliner_nlp")
# ---------------------------------------------------------------------------
# Entity labels → CCCRecord field mapping
# Each label here will be passed to GLiNER.  After prediction we bucket the
# results by label and feed them into the corresponding Pydantic field.
# GLiNER works best with natural-language-style labels.
# ---------------------------------------------------------------------------

# LABELS = [
#     # General info
#     "person name",
#     "date of birth",
#     "birthplace",
#     "home address",
#     "citizenship status",
#     "race or color",
#     "education grade",
#     "occupation",
#     "nearest relative name",
#     "nearest relative address",
# ]

LABELS = [
    "applicant name",
    "date of birth",
    "birthplace",
    "home address",
    "race or color",
    "education grade",
    "occupation",
    "CCC Co."
]

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def construct_link(na_id, case_id):
    return f"https://catalog.archives.gov/id/{na_id}?objectPage={int(case_id)-int(na_id)}"

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

def get_sample_cases(conn: sqlite3.Connection, n: int) -> list[tuple[str, str, str]]:
    """Return (case_start_id, na_id, extracted_text) for n random cases."""
    rows = conn.execute("""
        SELECT case_start_id, na_id, extracted_text
        FROM cases
        WHERE extracted_text IS NOT NULL
        ORDER BY RANDOM()
        LIMIT ?
    """, (n,)).fetchall()
    return rows

# ---------------------------------------------------------------------------
# Preprocessing  (identical to gemini_nlp)
# ---------------------------------------------------------------------------

def preprocess_case(extracted_text_json: str) -> str:
    """Strip objectIds; return concatenated plain text for GLiNER."""
    pages = json.loads(extracted_text_json)
    texts = []
    for page in pages:
        if isinstance(page, dict) and "text" in page:
            texts.append(page["text"])
        elif isinstance(page, str):
            texts.append(page)
    return "\n\n".join(texts)

# ---------------------------------------------------------------------------
# Regex helpers for values that need normalization
# ---------------------------------------------------------------------------

_RE_DATE = re.compile(
    r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})"
)

def _try_iso_date(raw: str | None) -> str | None:
    """Best-effort conversion of a raw date string to YYYY-MM-DD."""
    if not raw:
        return None
    m = _RE_DATE.search(raw)
    if not m:
        return raw.strip()
    month, day, year = m.group(1), m.group(2), m.group(3)
    if len(year) == 2:
        y = int(year)
        year = str(1900 + y) if y > 25 else str(2000 + y)
    return f"{year}-{int(month):02d}-{int(day):02d}"


def _try_float(raw: str | None) -> float | None:
    if not raw:
        return None
    nums = re.findall(r"[\d.]+", raw)
    if nums:
        try:
            return float(nums[0])
        except ValueError:
            pass
    return None


def _try_int(raw: str | None) -> int | None:
    if not raw:
        return None
    nums = re.findall(r"\d+", raw)
    if nums:
        try:
            return int(nums[0])
        except ValueError:
            pass
    return None


def _try_bool_yes_no(raw: str | None) -> bool | None:
    if not raw:
        return None
    low = raw.strip().lower()
    if "yes" in low:
        return True
    if "no" in low:
        return False
    return None

# ---------------------------------------------------------------------------
# Entity-to-record assembly
# ---------------------------------------------------------------------------

def _first_by_label(entities: list[dict], label: str) -> str | None:
    """Return the text of the highest-scoring entity for a given label, or None."""
    matches = [e for e in entities if e["label"] == label]
    if not matches:
        return None
    matches.sort(key=lambda e: e["score"], reverse=True)
    return matches[0]["text"].strip()


def _all_by_label(entities: list[dict], label: str) -> list[str]:
    """Return all entity texts for a given label, sorted by score desc."""
    matches = [e for e in entities if e["label"] == label]
    matches.sort(key=lambda e: e["score"], reverse=True)
    return [m["text"].strip() for m in matches]


def build_record(entities: list[dict]) -> dict:
    """
    Assemble GLiNER entities into a flat dict using only the configured LABELS.
    """
    record = {}
    for label in LABELS:
        record[label] = _first_by_label(entities, label)
    return record

# ---------------------------------------------------------------------------
# GLiNER extraction wrapper
# ---------------------------------------------------------------------------

def extract_with_gliner(model: GLiNER, case_text: str) -> str | None:
    """
    Run GLiNER on case_text with the configured labels.
    Returns a JSON string matching the CCCRecord schema, or None on failure.
    """
    try:
        # GLiNER has a limited context window; truncate if needed
        text = case_text[:MAX_TEXT_LEN]

        entities = model.predict_entities(text, LABELS, threshold=THRESHOLD)

        log.debug("  Found %d raw entities.", len(entities))

        record = build_record(entities)
        return json.dumps(record, ensure_ascii=False, indent=2)

    except Exception as exc:
        log.error("GLiNER extraction failed: %s", str(exc)[:300])
        return None

# ---------------------------------------------------------------------------
# Main loop  (mirrors gemini_nlp structure)
# ---------------------------------------------------------------------------

def main():
    # Pick the best available hardware accelerator
    if torch.cuda.is_available():
        device = torch.device("cuda")
    elif torch.backends.mps.is_available():
        device = torch.device("mps")
    else:
        device = torch.device("cpu")
    log.info("Using device: %s", device)

    log.info("Loading GLiNER model '%s' …", GLINER_MODEL)
    model = GLiNER.from_pretrained(GLINER_MODEL)
    model = model.to(device)
    model.eval()
    log.info("Model loaded.")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    ensure_nlp_column(conn)
    # cases = get_unprocessed_cases(conn)
    cases = get_sample_cases(conn, RESULT_COUNT)
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
            "[%d/%d] Processing %s",
            idx, total, construct_link(na_id, case_start_id),
        )

        # Preprocess
        try:
            case_text = preprocess_case(extracted_text)
        except Exception as exc:
            log.error("  Preprocess failed: %s – skipping.", exc)
            failures += 1
            continue

        # Run GLiNER
        result = extract_with_gliner(model, case_text)

        if result is not None:
            print(result)
            successes += 1
            uncommitted += 1
            # log.info("  ✓ Saved NLP result.")
        else:
            failures += 1
            log.warning("  ✗ No result for this case.")


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
