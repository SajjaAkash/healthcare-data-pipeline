from healthcare_data_pipeline.jobs.silver_transform import (
    normalize_appointments,
    normalize_claims,
    normalize_patients,
)


def test_normalize_patients_standardizes_text_fields() -> None:
    rows = normalize_patients(
        [
            {
                "patient_id": " P100 ",
                "full_name": "  Jane Doe ",
                "gender": "female",
                "state": "tx",
                "birth_year": 1988,
            }
        ]
    )

    assert rows[0]["patient_id"] == "P100"
    assert rows[0]["gender"] == "Female"
    assert rows[0]["state"] == "TX"


def test_normalize_claims_sets_numeric_defaults() -> None:
    rows = normalize_claims(
        [
            {
                "claim_id": "C1",
                "patient_id": "P1",
                "provider_id": "DR1",
                "encounter_id": "E1",
                "service_date": "2026-04-20",
            }
        ]
    )

    assert rows[0]["allowed_amount"] == 0.0
    assert rows[0]["paid_amount"] == 0.0


def test_normalize_appointments_keeps_wait_minutes_as_int() -> None:
    rows = normalize_appointments(
        [
            {
                "encounter_id": "E1",
                "patient_id": "P1",
                "provider_id": "DR1",
                "appointment_date": "2026-04-20",
                "wait_minutes": "9",
            }
        ]
    )

    assert rows[0]["wait_minutes"] == 9
