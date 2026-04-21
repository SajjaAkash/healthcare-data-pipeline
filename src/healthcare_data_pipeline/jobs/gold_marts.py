from __future__ import annotations

from collections import Counter

from healthcare_data_pipeline.models import PipelineMetric


def build_dim_date(appointments: list[dict[str, object]]) -> list[dict[str, object]]:
    dates = sorted({str(row["appointment_date"]) for row in appointments})
    return [
        {
            "date_key": date_value.replace("-", ""),
            "calendar_date": date_value,
            "year": int(date_value[0:4]),
            "month": int(date_value[5:7]),
            "day": int(date_value[8:10]),
        }
        for date_value in dates
    ]


def build_fact_encounters(
    appointments: list[dict[str, object]], claims: list[dict[str, object]]
) -> list[dict[str, object]]:
    claims_by_encounter = {str(claim["encounter_id"]): claim for claim in claims}
    facts: list[dict[str, object]] = []
    for appointment in appointments:
        encounter_id = str(appointment["encounter_id"])
        claim = claims_by_encounter.get(encounter_id, {})
        facts.append(
            {
                "encounter_id": encounter_id,
                "patient_id": appointment["patient_id"],
                "provider_id": appointment["provider_id"],
                "date_key": str(appointment["appointment_date"]).replace("-", ""),
                "clinic_name": appointment["clinic_name"],
                "appointment_status": appointment["appointment_status"],
                "wait_minutes": appointment["wait_minutes"],
                "outcome_score": appointment["outcome_score"],
                "allowed_amount": float(claim.get("allowed_amount", 0.0)),
                "paid_amount": float(claim.get("paid_amount", 0.0)),
                "payer_name": claim.get("payer_name", "Unknown"),
                "procedure_category": claim.get("procedure_category", "Unknown"),
            }
        )
    return facts


def build_kpi_metrics(
    fact_encounters: list[dict[str, object]], report_date: str
) -> list[PipelineMetric]:
    total_encounters = len(fact_encounters)
    completed = sum(1 for row in fact_encounters if row["appointment_status"] == "completed")
    total_paid = sum(float(row["paid_amount"]) for row in fact_encounters)
    average_wait = sum(int(row["wait_minutes"]) for row in fact_encounters) / max(
        total_encounters, 1
    )
    payer_mix = Counter(
        str(row["payer_name"])
        for row in fact_encounters
        if row["payer_name"] != "Unknown"
    )

    metrics = [
        PipelineMetric("total_encounters", float(total_encounters), "daily", report_date),
        PipelineMetric("completed_encounters", float(completed), "daily", report_date),
        PipelineMetric("total_paid_amount", round(total_paid, 2), "daily", report_date),
        PipelineMetric("average_wait_minutes", round(average_wait, 2), "daily", report_date),
    ]
    for payer_name, count in payer_mix.items():
        metrics.append(
            PipelineMetric(
                f"payer_mix_{payer_name.lower().replace(' ', '_')}",
                float(count),
                "daily",
                report_date,
            )
        )
    return metrics
