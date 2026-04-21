from pathlib import Path

from healthcare_data_pipeline.demo_pipeline import run_demo_pipeline


def test_run_demo_pipeline_writes_gold_outputs(tmp_path: Path) -> None:
    sample_root = tmp_path / "data"
    bronze_dir = sample_root / "sample" / "bronze"
    bronze_dir.mkdir(parents=True)

    (bronze_dir / "patients.json").write_text(
        (
            '[{"patient_id":"P1","full_name":"Jane Doe","gender":"female",'
            '"state":"tx","birth_year":1990}]'
        ),
        encoding="utf-8",
    )
    (bronze_dir / "claims.json").write_text(
        (
            '[{"claim_id":"C1","patient_id":"P1","provider_id":"DR1","encounter_id":"E1",'
            '"service_date":"2026-04-20","allowed_amount":120.0,"paid_amount":100.0}]'
        ),
        encoding="utf-8",
    )
    (bronze_dir / "appointments.json").write_text(
        (
            '[{"encounter_id":"E1","patient_id":"P1","provider_id":"DR1",'
            '"appointment_date":"2026-04-20","wait_minutes":12,"clinic_name":"Primary Care",'
            '"appointment_status":"completed","outcome_score":4.8}]'
        ),
        encoding="utf-8",
    )

    result = run_demo_pipeline(base_dir=sample_root)

    assert result["fact_encounters"]
    assert (sample_root / "demo_output" / "gold" / "fact_encounters.json").exists()
    assert (sample_root / "demo_output" / "quality" / "quality_results.json").exists()
