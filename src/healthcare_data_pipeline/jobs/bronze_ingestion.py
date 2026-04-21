from __future__ import annotations

from datetime import UTC, datetime

from healthcare_data_pipeline.config import settings


def build_source_requests(execution_date: str) -> list[dict[str, str]]:
    ingested_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    return [
        {
            "source_name": "patients",
            "endpoint": settings.sources.patient_api_url,
            "destination_path": (
                f"abfss://{settings.azure.bronze_container}@"
                f"{settings.azure.storage_account}.dfs.core.windows.net/"
                f"patients/execution_date={execution_date}"
            ),
            "execution_date": execution_date,
            "ingested_at": ingested_at,
        },
        {
            "source_name": "claims",
            "endpoint": settings.sources.claims_api_url,
            "destination_path": (
                f"abfss://{settings.azure.bronze_container}@"
                f"{settings.azure.storage_account}.dfs.core.windows.net/"
                f"claims/execution_date={execution_date}"
            ),
            "execution_date": execution_date,
            "ingested_at": ingested_at,
        },
        {
            "source_name": "appointments",
            "endpoint": settings.sources.appointments_api_url,
            "destination_path": (
                f"abfss://{settings.azure.bronze_container}@"
                f"{settings.azure.storage_account}.dfs.core.windows.net/"
                f"appointments/execution_date={execution_date}"
            ),
            "execution_date": execution_date,
            "ingested_at": ingested_at,
        },
    ]


def run_job(args: list[str] | None = None) -> list[dict[str, str]]:
    execution_date = "2026-01-01" if not args else args[0]
    return build_source_requests(execution_date=execution_date)
