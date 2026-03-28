# STEP 4: Extract structured case data with spaCy, validate with Pydantic, store JSON.

"""
Reads case rows from the pipeline SQLite DB in this folder (``ccc_records.db``), writes
validated JSON into ``cases.json``.

**How pipeline data is stored** (see ``segmenter.py`` / ``database_init.py``):

- ``cases.extracted_text``: a JSON **array** of page dicts, same shape as ``text_extractor``
  stores per chunk: each element is ``{"objectId": "<string>", "text": "<OCR/plain text>"}``,
  ordered from first page of the enrollee file to last.
- ``cases.case_start_id`` / ``case_end_id``: the ``objectId`` of the first and last page in
  that array (primary key is ``case_start_id``).
- ``cases.na_id``: catalogue NA ID for the record series row in ``hits``; we ``LEFT JOIN hits``
  to fill ``source.record-group`` from ``hits.record_group`` when present.

Requires: pip install spacy pydantic && python -m spacy download en_core_web_sm

Edit the configuration constants below before running.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import spacy
from pydantic import BaseModel, ConfigDict, Field, ValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

PIPELINE_DIR = Path(__file__).resolve().parent
# Same path as ``database_init.DB_PATH`` — DB lives next to this script.
DB_PATH = PIPELINE_DIR / "ccc_records.db"

# --- run configuration (edit here; no CLI) ---------------------------------
FORCE_RECOMPUTE = False
SPACY_MODEL = "en_core_web_sm"
RECORD_GROUP: int | None = 146

# ---------------------------------------------------------------------------
# Pydantic models (mirror CCC_Records/pipeline/example_format.json)
# ---------------------------------------------------------------------------


class Source(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    naid: str
    url: str = ""
    start_page: int | None = Field(default=None, alias="start-page")
    end_page: int | None = Field(default=None, alias="end-page")
    record_group: int | None = Field(default=None, alias="record-group")
    access_date: str | None = Field(default=None, alias="access-date")


class PersonName(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    last: str | None = None
    first: str | None = None
    middle: str | None = None


class NearestRelative(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str | None = None
    address: str | None = None
    relation: str | None = None


class GeneralInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: PersonName | None = None
    class_: str | None = Field(default=None, alias="class")
    state: str | None = None
    dob: str | None = None
    dob_raw: str | None = Field(default=None, alias="dob_raw")
    birthplace: str | None = None
    address: str | None = None
    citizenship: str | None = None
    color: str | None = None
    military_service: str | None = None
    education_grade: int | None = None
    occupation: str | None = None
    unemployed_since: str | None = None
    previous_ccc_member: bool | None = None
    nearest_relative: NearestRelative | None = None


class Enrollment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date: str | None = None
    place: str | None = None
    term_end: str | None = None
    enrolling_officer: str | None = None
    selecting_agency: str | None = None


class Reenrollment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    sequence: int
    company: str | None = None
    camp: str | None = None
    date: str | None = None
    commander: str | None = None


class ServiceDebts(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    clothing: float = 0.0
    equipment: float = 0.0
    camp_exchange: float = 0.0
    company_fund: float = 0.0
    usamps: float = 0.0
    other: float = 0.0


class ServiceRecordItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    sequence: int
    from_: str | None = Field(default=None, alias="from")
    to: str | None = None
    department: str | None = None
    company: str | None = None
    camp: str | None = None
    location: str | None = None
    work_type: str | None = None
    performance: str | None = None
    awol_days: int | None = None
    awop_days: int | None = None
    transferred_to: str | None = None
    last_paid_date: str | None = None
    disbursing_officer: str | None = None
    debts: ServiceDebts | None = None
    remarks: str | None = None
    verified_by: str | None = None


class VisionBlock(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    right: str | None = None
    left: str | None = None
    conditions: str | None = None


class HearingBlock(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    right: str | None = None
    left: str | None = None
    conditions: str | None = None


class TeethBlock(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    upper_missing: str | None = None
    lower_missing: str | None = None
    mouth_gums: str | None = None


class PhysicalAcceptance(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    place: str | None = None
    date: str | None = None
    qualified: bool | None = None
    rejection_reason: str | None = None
    examiner: str | None = None


class PhysicalEnrollment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    height_in: float | None = None
    weight_lbs: int | None = None
    vision: VisionBlock | None = None
    hearing: HearingBlock | None = None
    teeth: TeethBlock | None = None
    complexion: str | None = None
    eye_color: str | None = None
    hair_color: str | None = None
    scars_marks: str | None = None
    general_exam: str | None = None
    surgical: str | None = None
    locomotion: str | None = None
    cardiovascular: str | None = None
    lungs: str | None = None
    genitourinary: str | None = None
    neuropsychiatric: str | None = None
    urinalysis: str | None = None
    qualified: bool | None = None
    disqualification_reason: str | None = None
    remarks: str | None = None
    examiner: str | None = None
    date: str | None = None


class PhysicalDischargeExam(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    place: str | None = None
    date: str | None = None
    no_change: bool | None = None
    notes: str | None = None
    examiner: str | None = None


class PhysicalExam(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    acceptance: PhysicalAcceptance | None = None
    enrollment: PhysicalEnrollment | None = None
    discharge_exam: PhysicalDischargeExam | None = None
    enrollee_disability_statement: str | None = None


class Fingerprints(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    taken_at: str | None = None
    date: str | None = None
    forwarded_to: str | None = None
    date_accepted: str | None = None


class Inoculation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    vaccine: str | None = None
    dose: int | None = None
    date: str | None = None
    result: str | None = None


class VocationalItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    subject: str | None = None
    date: str | None = None
    rating: str | None = None


class JobTrainingItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str | None = None
    date: str | None = None
    rating: str | None = None


class EducationActivities(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    general_vocational: list[VocationalItem] = Field(default_factory=list)
    job_training: list[JobTrainingItem] = Field(default_factory=list)
    special_schools: list[dict[str, Any]] = Field(default_factory=list)
    certificates: list[dict[str, Any]] = Field(default_factory=list)
    personal_qualities: str | None = None
    adviser_estimate: str | None = None
    outstanding_accomplishments: str | None = None


class Absence(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str | None = Field(default=None, alias="from")
    to: str | None = None
    type: str | None = None
    authority: str | None = None


class Allotment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    sequence: int
    allottee_name: str | None = None
    address: str | None = None
    amount: float | None = None
    from_: str | None = Field(default=None, alias="from")
    to: str | None = None
    termination_notified: bool | None = None
    termination_date: str | None = None


class PriorOffense(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date: str | None = None
    offense: str | None = None
    punishment: str | None = None


class DisciplinaryHearing(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    hearing_no: str | None = None
    date: str | None = None
    company: str | None = None
    camp: str | None = None
    location: str | None = None
    charge: str | None = None
    offense_location: str | None = None
    represented_by: str | None = None
    plea: str | None = None
    finding: str | None = None
    witnesses_against: list[str] = Field(default_factory=list)
    witnesses_for: list[str] = Field(default_factory=list)
    member_statement: str | None = None
    prior_offenses: list[PriorOffense] = Field(default_factory=list)
    punishment: str | None = None
    commanding_officer: str | None = None
    approved_by: str | None = None
    approval_date: str | None = None


class Discharge(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date: str | None = None
    type: str | None = None
    location: str | None = None
    reason: str | None = None
    transport_to: str | None = None
    forfeiture_of_pay: bool | None = None
    superintendent_estimate: str | None = None
    commander_certificate: str | None = None


class CCCRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    enrollee_id: str | None = None
    serial_no: str | None = None
    source: Source | None = None
    general_info: GeneralInfo | None = None
    enrollment: Enrollment | None = None
    reenrollments: list[Reenrollment] = Field(default_factory=list)
    service_record: list[ServiceRecordItem] = Field(default_factory=list)
    physical_exam: PhysicalExam | None = None
    fingerprints: Fingerprints | None = None
    inoculations: list[Inoculation] = Field(default_factory=list)
    education_activities: EducationActivities | None = None
    leader_appointments: list[dict[str, Any]] = Field(default_factory=list)
    absences: list[Absence] = Field(default_factory=list)
    illness_injury: list[dict[str, Any]] = Field(default_factory=list)
    allotments: list[Allotment] = Field(default_factory=list)
    disciplinary: list[DisciplinaryHearing] = Field(default_factory=list)
    discharge: Discharge | None = None


# ---------------------------------------------------------------------------
# Regex + spaCy extraction
# ---------------------------------------------------------------------------

RE_ENROLLEE_ID = re.compile(
    r"\b(?:CC4|CCA|CCC|CC)[-\s]*(\d{4,})\b",
    re.IGNORECASE,
)
RE_SERIAL_LINE = re.compile(
    r"(?:^|\n)\s*(\d{1,3}-\d{4,7})\b",
    re.MULTILINE,
)
RE_NAME_FORM = re.compile(
    r"\(\s*1\s*\)\s*Name\s+([A-Za-z][A-Za-z'\-]+)\s*,\s*([A-Za-z][A-Za-z'\-]+)",
    re.IGNORECASE,
)
RE_DOB = re.compile(
    r"\(\s*3\s*\)\s*Date of birth\s+(.+?)(?:\n|$)",
    re.IGNORECASE,
)
RE_ADDRESS = re.compile(
    r"\(\s*2\s*\)\s*Address\s+(.+?)(?:\n|$)",
    re.IGNORECASE,
)
RE_BIRTHPLACE = re.compile(
    r"\(\s*4\s*\)\s*Birthplace\s+(.+?)(?:\n|$)",
    re.IGNORECASE,
)
RE_NEAREST_REL = re.compile(
    r"\(\s*5\s*\)\s*Nearest relative\s+(.+?)(?:\n|$)",
    re.IGNORECASE,
)
RE_CITIZENSHIP = re.compile(
    r"(Native born|Naturalized|Declarant)",
    re.IGNORECASE,
)
RE_PREV_CCC = re.compile(
    r"\(\s*9\s*\).*?Civilian Conservation Corps\?\s*(Yes|No)\b",
    re.IGNORECASE | re.DOTALL,
)
RE_COLOR = re.compile(
    r"\(\s*8\s*\)\s*Color\s*\n?\s*(White|Black|Colored|Indian)",
    re.IGNORECASE,
)
RE_SERVICE_SPAN = re.compile(
    r"From\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+to\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+under\s+([^\n]+?)\s+at\s+([^\n]+)",
    re.IGNORECASE,
)
RE_HEIGHT_WEIGHT = re.compile(
    r"Height\s+[^\n]*?(\d{1,2}(?:\.\d+)?)\s*(?:in|inches|--)",
    re.IGNORECASE,
)
RE_WEIGHT_LBS = re.compile(
    r"weight[:\s]+(\d{2,3})\s*pounds?",
    re.IGNORECASE,
)
RE_VISION = re.compile(
    r"Vision:.*?Right\s*(?:eye)?\s*([\d/]+).*?Left\s*(?:eye)?\s*([\d/]+)",
    re.IGNORECASE | re.DOTALL,
)
RE_HEARING = re.compile(
    r"Hearing:.*?Right\s*(?:ear)?\s*([\d/]+).*?Left\s*(?:ear)?\s*([\d/]+)",
    re.IGNORECASE | re.DOTALL,
)


def _normalize_ws(s: str | None) -> str | None:
    if s is None:
        return None
    t = " ".join(s.split())
    return t if t else None


def _parse_bool_yes_no(s: str | None) -> bool | None:
    if not s:
        return None
    sl = s.strip().lower()
    if sl == "yes":
        return True
    if sl == "no":
        return False
    return None


def _first_person_name(nlp, text: str) -> PersonName | None:
    doc = nlp(text[:500000])
    persons = [e for e in doc.ents if e.label_ == "PERSON"]
    if not persons:
        return None
    # Prefer longer spans (full names)
    persons.sort(key=lambda e: len(e.text), reverse=True)
    parts = persons[0].text.replace(",", " ").split()
    if len(parts) >= 2:
        return PersonName(first=parts[0], last=parts[-1], middle=" ".join(parts[1:-1]) or None)
    if len(parts) == 1:
        return PersonName(last=parts[0])
    return None


def extract_case_record(
    text: str,
    na_id: str,
    *,
    nlp,
    object_ids: list[str] | None = None,
    record_group: int | None = None,
) -> CCCRecord:
    """Build a CCCRecord from OCR text plus catalogue metadata."""
    t = text if text else ""
    access = date.today().isoformat()
    start_page: int | None = None
    end_page: int | None = None
    if object_ids:
        start_page = 1
        end_page = len(object_ids)

    url = f"https://catalog.archives.gov/id/{na_id}"
    if start_page is not None:
        url = f"{url}?objectPage={start_page}"

    source = Source(
        naid=na_id,
        url=url,
        start_page=start_page,
        end_page=end_page,
        record_group=record_group,
        access_date=access,
    )

    enrollee_id: str | None = None
    m = RE_ENROLLEE_ID.search(t)
    if m:
        prefix = m.group(0).upper().split("-")[0].replace(" ", "")
        enrollee_id = f"{prefix}-{m.group(1)}"

    serial_no = None
    sm = RE_SERIAL_LINE.search(t)
    if sm:
        serial_no = sm.group(1)

    gn = GeneralInfo()
    nm = RE_NAME_FORM.search(t)
    if nm:
        gn.name = PersonName(last=nm.group(1).strip(), first=nm.group(2).strip())
    elif nlp is not None:
        guess = _first_person_name(nlp, t)
        if guess:
            gn.name = guess

    dm = RE_DOB.search(t)
    if dm:
        raw = _normalize_ws(dm.group(1))
        gn.dob_raw = raw

    am = RE_ADDRESS.search(t)
    if am:
        gn.address = _normalize_ws(am.group(1))

    bm = RE_BIRTHPLACE.search(t)
    if bm:
        gn.birthplace = _normalize_ws(bm.group(1))

    nm2 = RE_NEAREST_REL.search(t)
    if nm2:
        line = _normalize_ws(nm2.group(1)) or ""
        rel = NearestRelative(name=line)
        if "," in line:
            parts = line.split(",", 1)
            rel.name = _normalize_ws(parts[0])
            rel.address = _normalize_ws(parts[1])
        gn.nearest_relative = rel

    cm = RE_CITIZENSHIP.search(t)
    if cm:
        gn.citizenship = cm.group(1).title()

    colm = RE_COLOR.search(t)
    if colm:
        gn.color = colm.group(1)

    prev = RE_PREV_CCC.search(t)
    if prev:
        gn.previous_ccc_member = _parse_bool_yes_no(prev.group(1))

    # State: two-letter near city or from address tail
    st = re.search(r"\b([A-Z]{2})\s*(?:\.|$|\n)", t)
    if st and st.group(1) not in {"AT", "OR", "NO", "IN", "TO", "OF", "CO"}:
        gn.state = st.group(1)

    physical = PhysicalExam()
    pe = PhysicalEnrollment()
    vm = RE_VISION.search(t)
    if vm:
        pe.vision = VisionBlock(right=vm.group(1), left=vm.group(2))
    hm = RE_HEARING.search(t)
    if hm:
        pe.hearing = HearingBlock(right=hm.group(1), left=hm.group(2))
    hgt = RE_HEIGHT_WEIGHT.search(t)
    if hgt:
        try:
            pe.height_in = float(hgt.group(1))
        except ValueError:
            pass
    wg = RE_WEIGHT_LBS.search(t)
    if wg:
        try:
            pe.weight_lbs = int(wg.group(1))
        except ValueError:
            pass
    if pe.vision or pe.hearing or pe.height_in or pe.weight_lbs:
        physical.enrollment = pe

    service_items: list[ServiceRecordItem] = []
    for i, sm in enumerate(RE_SERVICE_SPAN.finditer(t), start=1):
        service_items.append(
            ServiceRecordItem(
                sequence=i,
                from_=sm.group(1),
                to=sm.group(2),
                department=_normalize_ws(sm.group(3)),
                camp=_normalize_ws(sm.group(4)),
                debts=ServiceDebts(),
            )
        )

    return CCCRecord(
        enrollee_id=enrollee_id,
        serial_no=serial_no,
        source=source,
        general_info=gn if gn.model_dump(exclude_none=True) else None,
        enrollment=None,
        reenrollments=[],
        service_record=service_items,
        physical_exam=physical if physical.model_dump(exclude_none=True) else None,
        fingerprints=None,
        inoculations=[],
        education_activities=EducationActivities(),
        leader_appointments=[],
        absences=[],
        illness_injury=[],
        allotments=[],
        disciplinary=[],
        discharge=None,
    )


def _load_nlp(model_name: str = "en_core_web_sm"):
    try:
        return spacy.load(model_name)
    except OSError:
        log.error(
            "spaCy model %r not found. Run: python -m spacy download %s",
            model_name,
            model_name,
        )
        raise


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _sql_quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _ensure_json_column(conn: sqlite3.Connection, columns: set[str]) -> str:
    """Return the column name for NLP output (pipeline ``cases.json``)."""
    if "json" in columns:
        return "json"
    conn.execute("ALTER TABLE cases ADD COLUMN json TEXT")
    conn.commit()
    return "json"


def _case_rows(conn: sqlite3.Connection):
    """
    Iterate pipeline ``cases`` rows with ``hits.record_group`` for source metadata.
    Expects ``database_init`` schema: ``case_start_id``, ``extracted_text``, ``json``.
    """
    cols = _table_columns(conn, "cases")
    if "case_start_id" not in cols:
        raise RuntimeError(
            "This script expects pipeline ccc_records.db next to nlp.py "
            "(table cases.case_start_id missing)."
        )
    json_col = _ensure_json_column(conn, cols)
    jref = _sql_quote_ident(json_col)
    sel = f"""
        SELECT c.case_start_id,
               c.case_end_id,
               c.na_id,
               c.extracted_text,
               {jref} AS existing_json,
               h.record_group AS hit_record_group
        FROM cases c
        LEFT JOIN hits h ON h.na_id = c.na_id
    """
    for row in conn.execute(sel):
        yield row, json_col


def _concatenate_pipeline_pages(extracted_text_json: str | None) -> tuple[str, list[str]]:
    """
    Parse ``cases.extracted_text``: a JSON list of
    ``{"objectId": "...", "text": "..."}`` dicts in order; return full OCR text
    and ordered ``objectId`` strings (see ``segmenter.save_current_case``).
    """
    if not extracted_text_json:
        return "", []
    try:
        pages = json.loads(extracted_text_json)
    except json.JSONDecodeError:
        return "", []
    if not isinstance(pages, list):
        return "", []
    parts: list[str] = []
    ids: list[str] = []
    for p in pages:
        if isinstance(p, dict):
            txt = p.get("text")
            parts.append(txt.strip() if isinstance(txt, str) else "")
            oid = p.get("objectId")
            if oid is not None:
                ids.append(str(oid))
    return "\n\n".join(parts), ids


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def process_database(
    db_path: Path,
    *,
    force: bool = False,
    spacy_model: str = "en_core_web_sm",
    record_group: int | None = 146,
) -> int:
    conn = open_db(db_path)
    nlp = _load_nlp(spacy_model)
    updated = 0

    for row, json_col in _case_rows(conn):
        pk = row["case_start_id"]
        na_id = row["na_id"]
        full_text, oids = _concatenate_pipeline_pages(row["extracted_text"])

        hit_rg = row["hit_record_group"]
        record_group_effective: int | None = record_group
        if hit_rg is not None and str(hit_rg).strip() != "":
            try:
                record_group_effective = int(str(hit_rg).strip())
            except ValueError:
                pass

        if not force and row["existing_json"]:
            try:
                CCCRecord.model_validate_json(row["existing_json"])
                log.debug("Skip case %s (valid JSON already present).", pk)
                continue
            except (ValidationError, json.JSONDecodeError):
                pass

        try:
            record = extract_case_record(
                full_text,
                na_id,
                nlp=nlp,
                object_ids=oids or None,
                record_group=record_group_effective,
            )
            payload = record.model_dump(mode="json", by_alias=True)
            json_str = json.dumps(payload, ensure_ascii=False)
            CCCRecord.model_validate_json(json_str)
        except ValidationError as e:
            log.error("Validation failed for case %s: %s", pk, e)
            continue
        except Exception as e:
            log.exception("Extraction error for case %s: %s", pk, e)
            continue

        jref = _sql_quote_ident(json_col)
        conn.execute(
            f"UPDATE cases SET {jref} = ? WHERE case_start_id = ?",
            (json_str, pk),
        )
        updated += 1

    conn.commit()
    conn.close()
    return updated


def main():
    if not DB_PATH.is_file():
        log.error("Database not found: %s", DB_PATH)
        raise SystemExit(1)

    n = process_database(
        DB_PATH,
        force=FORCE_RECOMPUTE,
        spacy_model=SPACY_MODEL,
        record_group=RECORD_GROUP,
    )
    log.info("Updated %d case row(s) in %s", n, DB_PATH)


if __name__ == "__main__":
    main()
