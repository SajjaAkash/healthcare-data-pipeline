from __future__ import annotations

import json

from healthcare_data_pipeline.config import settings


def build_databricks_run_now_payload(job_id: int, execution_date: str) -> dict[str, object]:
    return {
        "job_id": job_id,
        "job_parameters": {
            "execution_date": execution_date,
            "project_name": settings.platform.project_name,
            "environment": settings.platform.environment,
        },
    }


def build_databricks_run_now_command(job_id: int, execution_date: str) -> str:
    payload = json.dumps(build_databricks_run_now_payload(job_id, execution_date))
    command = (
        "curl -X POST "
        f"{settings.azure.databricks_workspace_url}/api/2.1/jobs/run-now "
        '-H "Authorization: Bearer $DATABRICKS_TOKEN" '
        '-H "Content-Type: application/json" '
        f"-d '{payload}'"
    )
    if settings.azure.dry_run:
        return f"echo {json.dumps(command)}"
    return command


def build_dashboard_publish_command(execution_date: str) -> str:
    destination = (
        f"abfss://{settings.azure.gold_container}@"
        f"{settings.azure.storage_account}.dfs.core.windows.net/"
        f"streamlit_extracts/date={execution_date}"
    )
    if settings.azure.dry_run:
        return f'echo "Publishing KPI dataset to {destination}"'
    return (
        "python -m healthcare_data_pipeline.demo_pipeline && "
        f'echo "Publishing KPI dataset to {destination}"'
    )
