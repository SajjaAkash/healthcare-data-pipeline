from healthcare_data_pipeline.orchestration import (
    build_dashboard_publish_command,
    build_databricks_run_now_payload,
)


def test_build_databricks_run_now_payload_includes_job_and_parameters() -> None:
    payload = build_databricks_run_now_payload(1001, "2026-04-21")

    assert payload["job_id"] == 1001
    assert payload["job_parameters"]["execution_date"] == "2026-04-21"


def test_build_dashboard_publish_command_points_to_gold_extract_location() -> None:
    command = build_dashboard_publish_command("2026-04-21")

    assert "streamlit_extracts/date=2026-04-21" in command
