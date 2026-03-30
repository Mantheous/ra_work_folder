from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Simplified Pydantic models (mirror CCC_Records/pipeline/example_format.json)
# ---------------------------------------------------------------------------

class Source(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    naid: str
    url: str = ""
    start_page: int | None = Field(default=None, alias="start-page")
    end_page: int | None = Field(default=None, alias="end-page")
    record_group: int | None = Field(default=None, alias="record-group")
    access_date: str | None = Field(default=None, alias="access-date")


class GeneralInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str | None = None
    class_: str | None = Field(default=None, alias="class")
    state: str | None = None
    dob: str | None = None
    # dob_raw: str | None = Field(default=None, alias="dob_raw") # removed to reduce token usage
    birthplace: str | None = None
    address: str | None = None
    citizenship: str | None = None
    color: str | None = None
    military_service: str | None = None
    education_grade: int | None = None
    occupation: str | None = None
    unemployed_since: str | None = None
    previous_ccc_member: bool | None = None
    nearest_relative: str | None = None


class Enrollment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date: str | None = None
    place: str | None = None
    term_end: str | None = None
    enrolling_officer: str | None = None
    selecting_agency: str | None = None


class ServiceRecordItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str | None = Field(default=None, alias="from")
    to: str | None = None
    department: str | None = None
    company: str | None = None
    camp: str | None = None
    location: str | None = None
    work_type: str | None = None
    performance: str | None = None
    awol_days: int | None = None
    transferred_to: str | None = None
    last_paid_date: str | None = None
    debts: str | None = None
    remarks: str | None = None


class PhysicalEnrollment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    height_in: float | None = None
    weight_lbs: int | None = None
    vision: str | None = None
    hearing: str | None = None
    teeth: str | None = None
    complexion: str | None = None
    eye_color: str | None = None
    hair_color: str | None = None
    systems_evaluation: str | None = None
    qualified: bool | None = None
    disqualification_reason: str | None = None
    remarks: str | None = None


class PhysicalExam(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    enrollment: PhysicalEnrollment | None = None
    discharge_exam_notes: str | None = None
    enrollee_disability_statement: str | None = None


class EducationActivities(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    general_vocational: list[str] = Field(default_factory=list)
    job_training: list[str] = Field(default_factory=list)
    special_schools: list[str] = Field(default_factory=list)
    certificates: list[str] = Field(default_factory=list)
    personal_qualities: str | None = None


class Allotment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    allottee_name: str | None = None
    address: str | None = None
    amount: float | None = None


class DisciplinaryHearing(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    hearing_no: str | None = None
    date: str | None = None
    charge: str | None = None
    finding: str | None = None
    punishment: str | None = None
    prior_offenses: list[str] = Field(default_factory=list)


class Discharge(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date: str | None = None
    type: str | None = None
    location: str | None = None
    reason: str | None = None
    superintendent_estimate: str | None = None


class CCCRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    enrollee_id: str | None = None
    serial_no: str | None = None
    # source: Source | None = None  # This is added in programatically 
    general_info: GeneralInfo | None = None
    enrollment: Enrollment | None = None
    # reenrollments: list[str] = Field(default_factory=list) # removed to reduce token usage
    service_record: list[ServiceRecordItem] = Field(default_factory=list)
    physical_exam: PhysicalExam | None = None
    # fingerprints: str | None = None # removed to reduce token usage
    inoculations: list[str] = Field(default_factory=list)
    education_activities: EducationActivities | None = None
    leader_appointments: list[str] = Field(default_factory=list)
    # absences: list[str] = Field(default_factory=list) # removed to reduce token usage
    illness_injury: list[str] = Field(default_factory=list)
    allotments: list[Allotment] = Field(default_factory=list)
    disciplinary: list[DisciplinaryHearing] = Field(default_factory=list)
    discharge: Discharge | None = None