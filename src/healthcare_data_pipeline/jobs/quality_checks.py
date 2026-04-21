from __future__ import annotations

from datetime import UTC, datetime

from healthcare_data_pipeline.config import settings
from healthcare_data_pipeline.models import QualityResult


def validate_required_fields(
    dataset_name: str, records: list[dict[str, object]], required_fields: tuple[str, ...]
) -> QualityResult:
    missing_rows = []
    for index, record in enumerate(records):
        for field_name in required_fields:
            value = record.get(field_name)
            if value in (None, ""):
                missing_rows.append(f"row={index},field={field_name}")
    if missing_rows:
        return QualityResult(
            rule_name=f"{dataset_name}_required_fields",
            passed=False,
            detail="; ".join(missing_rows[:5]),
        )
    return QualityResult(
        rule_name=f"{dataset_name}_required_fields",
        passed=True,
        detail="All required fields present.",
    )


def validate_patient_references(
    claims: list[dict[str, object]], patients: list[dict[str, object]]
) -> QualityResult:
    patient_ids = {str(record["patient_id"]) for record in patients}
    missing_refs = sorted(
        {
            str(record["patient_id"])
            for record in claims
            if str(record["patient_id"]) not in patient_ids
        }
    )
    if missing_refs:
        return QualityResult(
            rule_name="claims_patient_referential_integrity",
            passed=False,
            detail=", ".join(missing_refs),
        )
    return QualityResult(
        rule_name="claims_patient_referential_integrity",
        passed=True,
        detail="All claims reference a valid patient.",
    )


def validate_encounter_claim_coverage(
    appointments: list[dict[str, object]], claims: list[dict[str, object]]
) -> QualityResult:
    appointment_ids = {str(record["encounter_id"]) for record in appointments}
    claim_ids = {str(record["encounter_id"]) for record in claims}
    missing_claims = sorted(appointment_ids - claim_ids)
    if missing_claims:
        return QualityResult(
            rule_name="encounter_claim_coverage",
            passed=False,
            detail=f"missing_claims={','.join(missing_claims[:5])}",
        )
    return QualityResult(
        rule_name="encounter_claim_coverage",
        passed=True,
        detail="All encounters have matching claims.",
    )


def validate_paid_amount_not_exceed_allowed(
    claims: list[dict[str, object]],
) -> QualityResult:
    invalid_claims = sorted(
        str(record["claim_id"])
        for record in claims
        if float(record["paid_amount"]) > float(record["allowed_amount"])
    )
    if invalid_claims:
        return QualityResult(
            rule_name="paid_amount_not_exceed_allowed",
            passed=False,
            detail="invalid_claim_ids=" + ",".join(invalid_claims[:5]),
        )
    return QualityResult(
        rule_name="paid_amount_not_exceed_allowed",
        passed=True,
        detail="All paid amounts are within allowed amounts.",
    )


def validate_freshness(loaded_at: datetime, current_time: datetime | None = None) -> QualityResult:
    current = current_time or datetime.now(UTC)
    age_hours = (current - loaded_at).total_seconds() / 3600
    passed = age_hours <= settings.quality.freshness_sla_hours
    return QualityResult(
        rule_name="source_freshness",
        passed=passed,
        detail=f"age_hours={age_hours:.2f}, sla_hours={settings.quality.freshness_sla_hours}",
    )


def run_quality_suite(
    patients: list[dict[str, object]],
    appointments: list[dict[str, object]],
    claims: list[dict[str, object]],
    loaded_at: datetime,
) -> list[QualityResult]:
    return [
        validate_required_fields("patients", patients, settings.quality.required_patient_fields),
        validate_required_fields("claims", claims, settings.quality.required_claim_fields),
        validate_patient_references(claims, patients),
        validate_encounter_claim_coverage(appointments, claims),
        validate_paid_amount_not_exceed_allowed(claims),
        validate_freshness(loaded_at=loaded_at),
    ]
