from __future__ import annotations

from collections import Counter
from hashlib import sha256


def pseudonymize_patient_id(patient_id: str, salt: str) -> str:
    token = f"{salt}:{patient_id}".encode("utf-8")
    return sha256(token).hexdigest()[:16]


def build_reconciliation_audit(
    reconciliation_results: list[dict[str, object]],
    quality_results: list[dict[str, object]],
) -> dict[str, object]:
    status_counts = Counter(
        str(row.get("reconciliation_status", "unknown")) for row in reconciliation_results
    )
    failed_quality_rules = [
        str(row.get("rule_name", "unknown"))
        for row in quality_results
        if not bool(row.get("passed"))
    ]
    total_rows = max(len(reconciliation_results), 1)
    missing_claim_rate = round(status_counts.get("missing_claim", 0) / total_rows, 4)
    orphan_claim_rate = round(status_counts.get("orphan_claim", 0) / total_rows, 4)
    return {
        "status_counts": dict(status_counts),
        "failed_quality_rules": failed_quality_rules,
        "missing_claim_rate": missing_claim_rate,
        "orphan_claim_rate": orphan_claim_rate,
    }


def build_release_decision(
    audit_summary: dict[str, object],
    max_missing_claim_rate: float,
    max_orphan_claim_rate: float,
) -> dict[str, object]:
    failed_quality_rules = list(audit_summary.get("failed_quality_rules", []))
    missing_claim_rate = float(audit_summary.get("missing_claim_rate", 0.0))
    orphan_claim_rate = float(audit_summary.get("orphan_claim_rate", 0.0))
    blockers = []
    if failed_quality_rules:
        blockers.append("quality_rules_failed")
    if missing_claim_rate > max_missing_claim_rate:
        blockers.append("missing_claim_rate_exceeded")
    if orphan_claim_rate > max_orphan_claim_rate:
        blockers.append("orphan_claim_rate_exceeded")
    return {
        "publish_allowed": not blockers,
        "blockers": blockers,
        "missing_claim_rate": missing_claim_rate,
        "orphan_claim_rate": orphan_claim_rate,
    }
