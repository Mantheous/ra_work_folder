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

from gliner import GLiNER

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))
from output_format.pydantic_data_models import CCCRecord

import sqlite3

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent.parent / "ccc_records.db"
GLINER_MODEL = "urchade/gliner_mediumv2.1"   # same model used in the notebook
THRESHOLD = 0.35                              # confidence threshold for entity extraction
BATCH_COMMIT_SIZE = 5                         # commit to DB every N successful writes
MAX_TEXT_LEN = 50_000                         # GLiNER context window guard

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
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

LABELS = [
    # General info
    "person name",
    "date of birth",
    "birthplace",
    "home address",
    "citizenship status",
    "race or color",
    "military service",
    "education grade",
    "occupation",
    "unemployed since date",
    "nearest relative name",
    "nearest relative address",
    "state",
    "enrollee class",

    # IDs
    "enrollee ID",
    "serial number",

    # Enrollment
    "enrollment date",
    "enrollment place",
    "enrollment term end date",
    "enrolling officer",
    "selecting agency",

    # Service record
    "service start date",
    "service end date",
    "department",
    "company",
    "camp name",
    "camp location",
    "work type",
    "work performance",

    # Physical exam
    "height",
    "weight",
    "vision",
    "hearing",
    "complexion",
    "eye color",
    "hair color",

    # Discharge
    "discharge date",
    "discharge type",
    "discharge location",
    "discharge reason",

    # Other
    "inoculation",
    "illness or injury",
    "allottee name",
    "allotment amount",
    "disciplinary charge",
    "disciplinary punishment",
    "leader appointment",
]

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
    Assemble GLiNER entities into a dict matching CCCRecord schema.
    """
    # --- General Info ---
    general_info = {}

    name = _first_by_label(entities, "person name")
    if name:
        general_info["name"] = name

    cls_ = _first_by_label(entities, "enrollee class")
    if cls_:
        general_info["class"] = cls_

    state = _first_by_label(entities, "state")
    if state:
        general_info["state"] = state

    dob = _try_iso_date(_first_by_label(entities, "date of birth"))
    if dob:
        general_info["dob"] = dob

    birthplace = _first_by_label(entities, "birthplace")
    if birthplace:
        general_info["birthplace"] = birthplace

    address = _first_by_label(entities, "home address")
    if address:
        general_info["address"] = address

    citizenship = _first_by_label(entities, "citizenship status")
    if citizenship:
        general_info["citizenship"] = citizenship

    color = _first_by_label(entities, "race or color")
    if color:
        general_info["color"] = color

    military = _first_by_label(entities, "military service")
    if military:
        general_info["military_service"] = military

    edu = _try_int(_first_by_label(entities, "education grade"))
    if edu is not None:
        general_info["education_grade"] = edu

    occ = _first_by_label(entities, "occupation")
    if occ:
        general_info["occupation"] = occ

    unemp = _first_by_label(entities, "unemployed since date")
    if unemp:
        general_info["unemployed_since"] = unemp

    nr_name = _first_by_label(entities, "nearest relative name")
    if nr_name:
        general_info["nearest_relative"] = nr_name

    # --- IDs ---
    enrollee_id = _first_by_label(entities, "enrollee ID")
    serial_no = _first_by_label(entities, "serial number")

    # --- Enrollment ---
    enrollment = {}
    edate = _try_iso_date(_first_by_label(entities, "enrollment date"))
    if edate:
        enrollment["date"] = edate
    eplace = _first_by_label(entities, "enrollment place")
    if eplace:
        enrollment["place"] = eplace
    eterm = _try_iso_date(_first_by_label(entities, "enrollment term end date"))
    if eterm:
        enrollment["term_end"] = eterm
    eofficer = _first_by_label(entities, "enrolling officer")
    if eofficer:
        enrollment["enrolling_officer"] = eofficer
    eagency = _first_by_label(entities, "selecting agency")
    if eagency:
        enrollment["selecting_agency"] = eagency

    # --- Service Record ---
    service_record = []
    start_dates = _all_by_label(entities, "service start date")
    end_dates = _all_by_label(entities, "service end date")
    departments = _all_by_label(entities, "department")
    companies = _all_by_label(entities, "company")
    camps = _all_by_label(entities, "camp name")
    locations = _all_by_label(entities, "camp location")
    work_types = _all_by_label(entities, "work type")
    performances = _all_by_label(entities, "work performance")

    n_service = max(len(start_dates), len(end_dates), 1) if start_dates or end_dates else 0
    for i in range(n_service):
        item = {}
        if i < len(start_dates):
            item["from"] = _try_iso_date(start_dates[i])
        if i < len(end_dates):
            item["to"] = _try_iso_date(end_dates[i])
        if i < len(departments):
            item["department"] = departments[i]
        if i < len(companies):
            item["company"] = companies[i]
        if i < len(camps):
            item["camp"] = camps[i]
        if i < len(locations):
            item["location"] = locations[i]
        if i < len(work_types):
            item["work_type"] = work_types[i]
        if i < len(performances):
            item["performance"] = performances[i]
        if item:
            service_record.append(item)

    # --- Physical Exam ---
    physical_exam = {}
    pe = {}
    height = _try_float(_first_by_label(entities, "height"))
    if height is not None:
        pe["height_in"] = height
    weight = _try_int(_first_by_label(entities, "weight"))
    if weight is not None:
        pe["weight_lbs"] = weight
    vision = _first_by_label(entities, "vision")
    if vision:
        pe["vision"] = vision
    hearing = _first_by_label(entities, "hearing")
    if hearing:
        pe["hearing"] = hearing
    complexion = _first_by_label(entities, "complexion")
    if complexion:
        pe["complexion"] = complexion
    eye_color = _first_by_label(entities, "eye color")
    if eye_color:
        pe["eye_color"] = eye_color
    hair_color = _first_by_label(entities, "hair color")
    if hair_color:
        pe["hair_color"] = hair_color
    if pe:
        physical_exam["enrollment"] = pe

    # --- Discharge ---
    discharge = {}
    ddate = _try_iso_date(_first_by_label(entities, "discharge date"))
    if ddate:
        discharge["date"] = ddate
    dtype = _first_by_label(entities, "discharge type")
    if dtype:
        discharge["type"] = dtype
    dloc = _first_by_label(entities, "discharge location")
    if dloc:
        discharge["location"] = dloc
    dreason = _first_by_label(entities, "discharge reason")
    if dreason:
        discharge["reason"] = dreason

    # --- List fields ---
    inoculations = _all_by_label(entities, "inoculation")
    illness_injury = _all_by_label(entities, "illness or injury")
    leader_appointments = _all_by_label(entities, "leader appointment")

    # --- Allotments ---
    allotments = []
    allottee_names = _all_by_label(entities, "allottee name")
    allotment_amounts = _all_by_label(entities, "allotment amount")
    for i in range(max(len(allottee_names), len(allotment_amounts))):
        a = {}
        if i < len(allottee_names):
            a["allottee_name"] = allottee_names[i]
        if i < len(allotment_amounts):
            a["amount"] = _try_float(allotment_amounts[i])
        if a:
            allotments.append(a)

    # --- Disciplinary ---
    disciplinary = []
    charges = _all_by_label(entities, "disciplinary charge")
    punishments = _all_by_label(entities, "disciplinary punishment")
    for i in range(max(len(charges), len(punishments))):
        d = {}
        if i < len(charges):
            d["charge"] = charges[i]
        if i < len(punishments):
            d["punishment"] = punishments[i]
        if d:
            disciplinary.append(d)

    # --- Assemble top-level record ---
    record = {
        "enrollee_id": enrollee_id,
        "serial_no": serial_no,
        "general_info": general_info or None,
        "enrollment": enrollment or None,
        "service_record": service_record,
        "physical_exam": physical_exam or None,
        "inoculations": inoculations,
        "education_activities": None,      # GLiNER can't easily fill nested sub-lists
        "leader_appointments": leader_appointments,
        "illness_injury": illness_injury,
        "allotments": allotments,
        "disciplinary": disciplinary,
        "discharge": discharge or None,
    }
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
    log.info("Loading GLiNER model '%s' …", GLINER_MODEL)
    model = GLiNER.from_pretrained(GLINER_MODEL)
    model.eval()
    log.info("Model loaded.")

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

        # Run GLiNER
        result = extract_with_gliner(model, case_text)

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
