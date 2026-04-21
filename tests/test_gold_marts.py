from healthcare_data_pipeline.jobs.gold_marts import (
    build_claims_reconciliation,
    build_dim_date,
    build_dim_patient_current,
    build_fact_encounters,
    build_kpi_metrics,
)


def test_build_fact_encounters_merges_claim_details() -> None:
    facts = build_fact_encounters(
        appointments=[
            {
                "encounter_id": "E1",
                "patient_id": "P1",
                "provider_id": "DR1",
                "appointment_date": "2026-04-20",
                "clinic_name": "Primary Care",
                "appointment_status": "completed",
                "wait_minutes": 15,
                "outcome_score": 4.5,
            }
        ],
        claims=[
            {
                "encounter_id": "E1",
                "allowed_amount": 120.0,
                "paid_amount": 95.0,
                "payer_name": "Aetna",
                "procedure_category": "Preventive",
            }
        ],
    )

    assert facts[0]["paid_amount"] == 95.0
    assert facts[0]["payer_name"] == "Aetna"


def test_build_dim_date_creates_expected_keys() -> None:
    dim_date = build_dim_date([{"appointment_date": "2026-04-20"}])

    assert dim_date == [
        {"date_key": "20260420", "calendar_date": "2026-04-20", "year": 2026, "month": 4, "day": 20}
    ]


def test_build_kpi_metrics_includes_total_encounters() -> None:
    metrics = build_kpi_metrics(
        [
            {
                "appointment_status": "completed",
                "paid_amount": 100.0,
                "wait_minutes": 12,
                "payer_name": "Aetna",
            }
        ],
        report_date="2026-04-20",
    )

    metric_names = {metric.metric_name for metric in metrics}
    assert "total_encounters" in metric_names
    assert "payer_mix_aetna" in metric_names


def test_build_dim_patient_current_aggregates_encounters_and_payers() -> None:
    patient_dim = build_dim_patient_current(
        patients=[{"patient_id": "P1", "state": "TX", "gender": "Female", "birth_year": 1985}],
        appointments=[{"patient_id": "P1"}],
        claims=[{"patient_id": "P1", "payer_name": "Aetna", "allowed_amount": 125.0}],
    )
    assert patient_dim[0]["encounter_count"] == 1
    assert patient_dim[0]["payer_count"] == 1


def test_build_claims_reconciliation_marks_missing_claim() -> None:
    reconciliation = build_claims_reconciliation(
        appointments=[{"encounter_id": "E1", "appointment_status": "completed"}],
        claims=[],
    )
    assert reconciliation[0]["reconciliation_status"] == "missing_claim"
