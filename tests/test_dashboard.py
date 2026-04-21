from healthcare_data_pipeline.dashboard import build_dashboard_payload


def test_build_dashboard_payload_produces_headline_metrics() -> None:
    payload = build_dashboard_payload(
        fact_encounters=[
            {"paid_amount": 125.0, "wait_minutes": 10, "payer_name": "Aetna"},
            {"paid_amount": 175.0, "wait_minutes": 20, "payer_name": "Blue Cross"},
        ],
        quality_results=[
            {"rule_name": "rule_1", "passed": True},
            {"rule_name": "rule_2", "passed": False},
        ],
    )

    assert payload["headline_metrics"]["total_encounters"] == 2
    assert payload["headline_metrics"]["total_paid_amount"] == 300.0
    assert payload["headline_metrics"]["quality_pass_rate"] == 0.5
