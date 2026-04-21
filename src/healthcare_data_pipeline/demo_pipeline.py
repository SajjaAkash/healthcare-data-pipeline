from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from healthcare_data_pipeline.config import settings
from healthcare_data_pipeline.io_utils import read_json_file, write_json_file
from healthcare_data_pipeline.jobs.gold_marts import (
    build_claims_reconciliation,
    build_dim_date,
    build_dim_patient_current,
    build_fact_encounters,
    build_kpi_metrics,
)
from healthcare_data_pipeline.jobs.quality_checks import run_quality_suite
from healthcare_data_pipeline.jobs.silver_transform import (
    normalize_appointments,
    normalize_claims,
    normalize_patients,
)


def _sample_dir(base_dir: str | Path | None = None) -> Path:
    root = Path(base_dir or settings.platform.local_data_dir)
    return root / "sample" / "bronze"


def _output_dir(base_dir: str | Path | None = None) -> Path:
    root = Path(base_dir or settings.platform.local_data_dir)
    return root / "demo_output"


def run_demo_pipeline(base_dir: str | Path | None = None) -> dict[str, object]:
    sample_dir = _sample_dir(base_dir)
    output_dir = _output_dir(base_dir)

    bronze_patients = read_json_file(sample_dir / "patients.json")
    bronze_claims = read_json_file(sample_dir / "claims.json")
    bronze_appointments = read_json_file(sample_dir / "appointments.json")

    silver_patients = normalize_patients(bronze_patients)
    silver_claims = normalize_claims(bronze_claims)
    silver_appointments = normalize_appointments(bronze_appointments)

    dim_date = build_dim_date(silver_appointments)
    dim_patient_current = build_dim_patient_current(
        silver_patients, silver_appointments, silver_claims
    )
    fact_encounters = build_fact_encounters(silver_appointments, silver_claims)
    claims_reconciliation = build_claims_reconciliation(silver_appointments, silver_claims)
    report_date = (
        dim_date[-1]["calendar_date"] if dim_date else datetime.now(UTC).date().isoformat()
    )
    kpi_metrics = [asdict(metric) for metric in build_kpi_metrics(fact_encounters, report_date)]
    quality_results = [
        asdict(result)
        for result in run_quality_suite(
            patients=silver_patients,
            appointments=silver_appointments,
            claims=silver_claims,
            loaded_at=datetime.now(UTC),
        )
    ]

    write_json_file(output_dir / "silver" / "patients.json", silver_patients)
    write_json_file(output_dir / "silver" / "claims.json", silver_claims)
    write_json_file(output_dir / "silver" / "appointments.json", silver_appointments)
    write_json_file(output_dir / "gold" / "dim_date.json", dim_date)
    write_json_file(output_dir / "gold" / "dim_patient_current.json", dim_patient_current)
    write_json_file(output_dir / "gold" / "fact_encounters.json", fact_encounters)
    write_json_file(output_dir / "gold" / "claims_reconciliation.json", claims_reconciliation)
    write_json_file(output_dir / "gold" / "kpi_metrics.json", kpi_metrics)
    write_json_file(output_dir / "quality" / "quality_results.json", quality_results)

    return {
        "silver_patients": silver_patients,
        "silver_claims": silver_claims,
        "silver_appointments": silver_appointments,
        "dim_date": dim_date,
        "dim_patient_current": dim_patient_current,
        "fact_encounters": fact_encounters,
        "claims_reconciliation": claims_reconciliation,
        "kpi_metrics": kpi_metrics,
        "quality_results": quality_results,
        "output_dir": str(output_dir),
    }


if __name__ == "__main__":
    result = run_demo_pipeline()
    print(f"Demo pipeline complete. Outputs written to {result['output_dir']}")
