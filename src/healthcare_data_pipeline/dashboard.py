from __future__ import annotations

from collections import Counter
from pathlib import Path

from healthcare_data_pipeline.config import settings
from healthcare_data_pipeline.io_utils import read_json_file


def build_dashboard_payload(
    fact_encounters: list[dict[str, object]],
    quality_results: list[dict[str, object]],
    reconciliation_results: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    total_encounters = len(fact_encounters)
    total_paid_amount = round(sum(float(row["paid_amount"]) for row in fact_encounters), 2)
    average_wait = round(
        sum(int(row["wait_minutes"]) for row in fact_encounters) / max(total_encounters, 1), 2
    )
    payer_mix = Counter(str(row["payer_name"]) for row in fact_encounters)
    quality_pass_rate = round(
        sum(1 for row in quality_results if row["passed"]) / max(len(quality_results), 1), 2
    )
    reconciliation_results = reconciliation_results or []
    missing_claim_count = sum(
        1
        for row in reconciliation_results
        if row.get("reconciliation_status") == "missing_claim"
    )
    matched_count = sum(
        1 for row in reconciliation_results if row.get("reconciliation_status") == "matched"
    )
    orphan_claim_count = sum(
        1 for row in reconciliation_results if row.get("reconciliation_status") == "orphan_claim"
    )
    watchlist = [
        row
        for row in reconciliation_results
        if row.get("reconciliation_status") != "matched"
    ]
    quality_alerts = [
        row for row in quality_results if not bool(row.get("passed"))
    ]
    risk_band = "stable"
    if missing_claim_count or orphan_claim_count:
        risk_band = "watch"
    if quality_alerts:
        risk_band = "critical"
    return {
        "headline_metrics": {
            "total_encounters": total_encounters,
            "total_paid_amount": total_paid_amount,
            "average_wait_minutes": average_wait,
            "quality_pass_rate": quality_pass_rate,
            "missing_claim_count": missing_claim_count,
            "matched_count": matched_count,
            "orphan_claim_count": orphan_claim_count,
            "risk_band": risk_band,
        },
        "payer_mix": dict(payer_mix),
        "payer_mix_rows": [
            {"payer_name": payer_name, "encounter_count": count}
            for payer_name, count in payer_mix.most_common()
        ],
        "encounters": fact_encounters,
        "quality_results": quality_results,
        "quality_alerts": quality_alerts,
        "reconciliation_results": reconciliation_results,
        "reconciliation_watchlist": watchlist,
    }


def load_dashboard_payload(base_dir: str | Path | None = None) -> dict[str, object] | None:
    root = Path(base_dir or settings.platform.local_data_dir) / "demo_output"
    fact_path = root / "gold" / "fact_encounters.json"
    reconciliation_path = root / "gold" / "claims_reconciliation.json"
    quality_path = root / "quality" / "quality_results.json"
    if not fact_path.exists() or not quality_path.exists():
        return None

    fact_encounters = read_json_file(fact_path)
    quality_results = read_json_file(quality_path)
    reconciliation_results = (
        read_json_file(reconciliation_path) if reconciliation_path.exists() else []
    )
    return build_dashboard_payload(fact_encounters, quality_results, reconciliation_results)
