"""
Stage 3: Text Segmentation & Data Extraction
Reads extracted text from SQLite, segments it by person using the
"NEXT CASE BEGINS" marker, and attempts regex-based extraction of
genealogical fields.

Usage:
    python segmenter.py
"""

import json
import logging
import re

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# Case separator (case-insensitive)
CASE_SEPARATOR = re.compile(r"NEXT\s+CASE\s+BEGINS", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Regex patterns for field extraction
# These are best-effort patterns for typical CCC enrollment documents.
# They will need tuning as more record formats are encountered.
# ---------------------------------------------------------------------------
PATTERNS = {
    "name": re.compile(
        r"(?:Name|Enrollee)[:\s]+([A-Z][a-zA-Z\-\']+(?:\s+[A-Z][a-zA-Z\-\']+){1,3})",
        re.IGNORECASE,
    ),
    "enrollee_id": re.compile(
        r"(?:CCC[-\s]*(?:No|ID|Number)?|Enrollee\s*(?:No|ID|Number)?)[:\s#]*(\d{3,})",
        re.IGNORECASE,
    ),
    "home_town": re.compile(
        r"(?:Home\s*(?:Address|Town|City))[:\s]+(.+?)(?:\n|$)",
        re.IGNORECASE,
    ),
    "home_state": re.compile(
        r"(?:State)[:\s]+([A-Za-z\s]+?)(?:\n|$)",
        re.IGNORECASE,
    ),
    "enlistment_date": re.compile(
        r"(?:Date\s*(?:of\s*)?(?:Enlistment|Enrollment|Enrolled))[:\s]+(.+?)(?:\n|$)",
        re.IGNORECASE,
    ),
    "discharge_date": re.compile(
        r"(?:Date\s*(?:of\s*)?(?:Discharge|Discharged|Separation))[:\s]+(.+?)(?:\n|$)",
        re.IGNORECASE,
    ),
    "nearest_kin": re.compile(
        r"(?:Nearest\s*(?:of\s*)?Kin|Next\s*of\s*Kin|Kin)[:\s]+(.+?)(?:\n|$)",
        re.IGNORECASE,
    ),
    "company": re.compile(
        r"(?:Co\.?|Company)\s*(\d{1,5})",
        re.IGNORECASE,
    ),
}


def extract_fields(text: str) -> dict:
    """Run all regex patterns against the text and return matches."""
    fields = {}
    for key, pattern in PATTERNS.items():
        match = pattern.search(text)
        if match:
            fields[key] = match.group(1).strip()
    return fields


def segment_and_store():
    """Main segmentation loop."""
    conn = get_connection()

    # Get all distinct na_ids that have extracted text
    na_ids = conn.execute(
        "SELECT DISTINCT na_id FROM pages WHERE status = 'done' AND raw_text IS NOT NULL"
    ).fetchall()

    if not na_ids:
        log.info("No extracted text to segment.")
        conn.close()
        return

    # Clear previous cases (re-run safe)
    conn.execute("DELETE FROM cases")
    conn.commit()

    total_cases = 0

    for row in na_ids:
        na_id = row["na_id"]

        # Get pages for this record, ordered by object_id (sequential page order)
        pages = conn.execute(
            "SELECT object_id, raw_text FROM pages "
            "WHERE na_id = ? AND status = 'done' AND raw_text IS NOT NULL "
            "ORDER BY object_id",
            (na_id,),
        ).fetchall()

        if not pages:
            continue

        # Build case segments
        current_case_parts = []
        cases_for_record = []

        for page in pages:
            text = page["raw_text"] or ""

            # Check if this page is a separator
            if CASE_SEPARATOR.search(text):
                # Save the current case if it has content
                if current_case_parts:
                    cases_for_record.append("\n".join(current_case_parts))
                current_case_parts = []
            else:
                current_case_parts.append(text)

        # Don't forget the last case
        if current_case_parts:
            cases_for_record.append("\n".join(current_case_parts))

        # Store each case
        for case_text in cases_for_record:
            parsed = extract_fields(case_text)
            conn.execute(
                "INSERT INTO cases (na_id, case_text, parsed_data) VALUES (?, ?, ?)",
                (na_id, case_text, json.dumps(parsed)),
            )
            total_cases += 1

        conn.commit()

    conn.close()
    log.info("Segmentation complete. Total cases found: %d", total_cases)


if __name__ == "__main__":
    segment_and_store()
