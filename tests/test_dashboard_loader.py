import json
from pathlib import Path

from healthcare_data_pipeline.dashboard import load_dashboard_payload


def test_load_dashboard_payload_reads_demo_output(tmp_path: Path) -> None:
    output_root = tmp_path / "demo_output"
    (output_root / "gold").mkdir(parents=True)
    (output_root / "quality").mkdir(parents=True)

    (output_root / "gold" / "fact_encounters.json").write_text(
        json.dumps(
            [
                {
                    "paid_amount": 150.0,
                    "wait_minutes": 10,
                    "payer_name": "Aetna",
                }
            ]
        ),
        encoding="utf-8",
    )
    (output_root / "quality" / "quality_results.json").write_text(
        json.dumps([{"rule_name": "rule", "passed": True, "detail": "ok"}]),
        encoding="utf-8",
    )
    (output_root / "gold" / "claims_reconciliation.json").write_text(
        json.dumps([{"reconciliation_status": "matched"}]),
        encoding="utf-8",
    )
    (output_root / "governance").mkdir(parents=True, exist_ok=True)
    (output_root / "governance" / "release_decision.json").write_text(
        json.dumps({"publish_allowed": True, "blockers": []}),
        encoding="utf-8",
    )

    payload = load_dashboard_payload(base_dir=tmp_path)

    assert payload is not None
    assert payload["headline_metrics"]["total_paid_amount"] == 150.0
