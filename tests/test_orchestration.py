from healthcare_data_pipeline.orchestration import (
    build_dashboard_publish_command,
    build_databricks_run_now_payload,
    build_release_gate_summary,
)


def test_build_databricks_run_now_payload_includes_job_and_parameters() -> None:
    payload = build_databricks_run_now_payload(1001, "2026-04-21")

    assert payload["job_id"] == 1001
    assert payload["job_parameters"]["execution_date"] == "2026-04-21"


def test_build_dashboard_publish_command_points_to_gold_extract_location() -> None:
    command = build_dashboard_publish_command("2026-04-21")

    assert "streamlit_extracts/date=2026-04-21" in command


def test_build_release_gate_summary_includes_blockers() -> None:
    summary = build_release_gate_summary(
        {"missing_claim_rate": 0.1, "orphan_claim_rate": 0.0},
        {"publish_allowed": False, "blockers": ["quality_rules_failed"]},
    )
    assert "publish_allowed=False" in summary
