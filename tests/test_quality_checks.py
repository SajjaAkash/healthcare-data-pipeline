from datetime import UTC, datetime, timedelta

from healthcare_data_pipeline.jobs.quality_checks import (
    run_quality_suite,
    validate_encounter_claim_coverage,
    validate_freshness,
    validate_paid_amount_not_exceed_allowed,
    validate_patient_references,
    validate_required_fields,
)


def test_validate_required_fields_fails_when_field_is_blank() -> None:
    result = validate_required_fields(
        "patients",
        [{"patient_id": "P1", "gender": "", "state": "TX"}],
        ("patient_id", "gender", "state"),
    )

    assert result.passed is False


def test_validate_patient_references_detects_missing_patient() -> None:
    result = validate_patient_references(
        claims=[{"patient_id": "P404"}],
        patients=[{"patient_id": "P1"}],
    )

    assert result.passed is False
    assert "P404" in result.detail


def test_validate_freshness_respects_sla() -> None:
    loaded_at = datetime.now(UTC) - timedelta(hours=2)
    current_time = datetime.now(UTC)

    result = validate_freshness(loaded_at=loaded_at, current_time=current_time)

    assert result.passed is True


def test_validate_encounter_claim_coverage_fails_for_missing_claim() -> None:
    result = validate_encounter_claim_coverage(
        appointments=[{"encounter_id": "E1"}],
        claims=[],
    )
    assert result.passed is False


def test_validate_paid_amount_not_exceed_allowed_detects_issue() -> None:
    result = validate_paid_amount_not_exceed_allowed(
        [{"claim_id": "C1", "allowed_amount": 100.0, "paid_amount": 120.0}]
    )
    assert result.passed is False


def test_run_quality_suite_returns_multiple_results() -> None:
    results = run_quality_suite(
        patients=[{"patient_id": "P1", "gender": "Female", "state": "TX"}],
        appointments=[{"encounter_id": "E1"}],
        claims=[
            {
                "claim_id": "C1",
                "patient_id": "P1",
                "allowed_amount": 100.0,
                "paid_amount": 100.0,
                "encounter_id": "E1",
            }
        ],
        loaded_at=datetime.now(UTC),
    )

    assert len(results) == 6
