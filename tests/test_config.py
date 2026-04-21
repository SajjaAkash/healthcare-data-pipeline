from healthcare_data_pipeline.config import settings


def test_default_config_values_are_available() -> None:
    assert settings.azure.location
    assert settings.azure.bronze_container == "bronze"
    assert settings.quality.freshness_sla_hours == 24
    assert settings.platform.airflow_schedule == "@daily"
