from __future__ import annotations

import os
from dataclasses import dataclass, field


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw.strip())


@dataclass(frozen=True)
class AzureSettings:
    location: str = os.getenv("AZURE_LOCATION", "eastus2")
    resource_group: str = os.getenv("AZURE_RESOURCE_GROUP", "rg-healthcare-data-platform")
    storage_account: str = os.getenv("AZURE_STORAGE_ACCOUNT", "sthealthcareanalytics")
    bronze_container: str = os.getenv("ADLS_BRONZE_CONTAINER", "bronze")
    silver_container: str = os.getenv("ADLS_SILVER_CONTAINER", "silver")
    gold_container: str = os.getenv("ADLS_GOLD_CONTAINER", "gold")
    key_vault_name: str = os.getenv("KEY_VAULT_NAME", "kv-healthcare-data-platform")
    use_azure_databricks: bool = _bool_env("USE_AZURE_DATABRICKS", True)
    dry_run: bool = _bool_env("AZURE_DRY_RUN", True)
    databricks_workspace_url: str = os.getenv(
        "DATABRICKS_WORKSPACE_URL",
        "https://adb-1234567890123456.7.azuredatabricks.net",
    )
    databricks_bronze_job_id: int = _int_env("DATABRICKS_BRONZE_JOB_ID", 1001)
    databricks_silver_job_id: int = _int_env("DATABRICKS_SILVER_JOB_ID", 1002)
    databricks_gold_job_id: int = _int_env("DATABRICKS_GOLD_JOB_ID", 1003)


@dataclass(frozen=True)
class SourceSettings:
    patient_api_url: str = os.getenv("PATIENT_API_URL", "https://example.com/api/patients")
    claims_api_url: str = os.getenv("CLAIMS_API_URL", "https://example.com/api/claims")
    appointments_api_url: str = os.getenv(
        "APPOINTMENTS_API_URL", "https://example.com/api/appointments"
    )
    source_system_name: str = os.getenv("SOURCE_SYSTEM_NAME", "clinical-ops-demo")


@dataclass(frozen=True)
class QualitySettings:
    freshness_sla_hours: int = _int_env("FRESHNESS_SLA_HOURS", 24)
    required_claim_fields: tuple[str, ...] = ("claim_id", "patient_id", "allowed_amount")
    required_patient_fields: tuple[str, ...] = ("patient_id", "gender", "state")


@dataclass(frozen=True)
class PlatformSettings:
    project_name: str = os.getenv("PROJECT_NAME", "healthcare-data-pipeline")
    environment: str = os.getenv("ENVIRONMENT", "dev")
    airflow_schedule: str = os.getenv("AIRFLOW_SCHEDULE", "@daily")
    local_data_dir: str = os.getenv("LOCAL_DATA_DIR", "data")


@dataclass(frozen=True)
class Settings:
    azure: AzureSettings = field(default_factory=AzureSettings)
    sources: SourceSettings = field(default_factory=SourceSettings)
    quality: QualitySettings = field(default_factory=QualitySettings)
    platform: PlatformSettings = field(default_factory=PlatformSettings)


settings = Settings()
