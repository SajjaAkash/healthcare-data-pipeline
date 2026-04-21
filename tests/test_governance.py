from healthcare_data_pipeline.governance import (
    build_reconciliation_audit,
    build_release_decision,
    pseudonymize_patient_id,
)


def test_pseudonymize_patient_id_is_deterministic() -> None:
    assert pseudonymize_patient_id("P1", "salt") == pseudonymize_patient_id("P1", "salt")


def test_build_reconciliation_audit_counts_statuses() -> None:
    audit = build_reconciliation_audit(
        reconciliation_results=[
            {"reconciliation_status": "matched"},
            {"reconciliation_status": "missing_claim"},
        ],
        quality_results=[{"rule_name": "freshness", "passed": True}],
    )
    assert audit["status_counts"]["matched"] == 1
    assert audit["missing_claim_rate"] == 0.5


def test_build_release_decision_blocks_on_rate_threshold() -> None:
    decision = build_release_decision(
        {"failed_quality_rules": [], "missing_claim_rate": 0.2, "orphan_claim_rate": 0.0},
        max_missing_claim_rate=0.1,
        max_orphan_claim_rate=0.05,
    )
    assert decision["publish_allowed"] is False
    assert "missing_claim_rate_exceeded" in decision["blockers"]
