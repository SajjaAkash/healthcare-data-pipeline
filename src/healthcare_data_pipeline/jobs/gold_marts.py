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


def build_dim_patient_current(
    patients: list[dict[str, object]],
    appointments: list[dict[str, object]],
    claims: list[dict[str, object]],
) -> list[dict[str, object]]:
    appointments_by_patient = Counter(str(row["patient_id"]) for row in appointments)
    allowed_by_patient: dict[str, float] = {}
    payer_mix_by_patient: dict[str, set[str]] = {}
    for claim in claims:
        patient_id = str(claim["patient_id"])
        allowed_by_patient[patient_id] = allowed_by_patient.get(patient_id, 0.0) + float(
            claim["allowed_amount"]
        )
        payer_mix_by_patient.setdefault(patient_id, set()).add(str(claim["payer_name"]))

    return [
        {
            "patient_id": patient["patient_id"],
            "state": patient["state"],
            "gender": patient["gender"],
            "birth_year": patient["birth_year"],
            "encounter_count": appointments_by_patient.get(str(patient["patient_id"]), 0),
            "payer_count": len(payer_mix_by_patient.get(str(patient["patient_id"]), set())),
            "total_allowed_amount": round(
                allowed_by_patient.get(str(patient["patient_id"]), 0.0), 2
            ),
        }
        for patient in patients
    ]


def build_claims_reconciliation(
    appointments: list[dict[str, object]],
    claims: list[dict[str, object]],
) -> list[dict[str, object]]:
    appointment_index = {str(row["encounter_id"]): row for row in appointments}
    claim_index = {str(row["encounter_id"]): row for row in claims}
    encounter_ids = sorted(set(appointment_index) | set(claim_index))
    reconciliation: list[dict[str, object]] = []
    for encounter_id in encounter_ids:
        appointment = appointment_index.get(encounter_id)
        claim = claim_index.get(encounter_id)
        status = "matched"
        if appointment and not claim:
            status = "missing_claim"
        elif claim and not appointment:
            status = "orphan_claim"

        reconciliation.append(
            {
                "encounter_id": encounter_id,
                "appointment_present": appointment is not None,
                "claim_present": claim is not None,
                "reconciliation_status": status,
                "appointment_status": appointment.get("appointment_status", "unknown")
                if appointment
                else "not_found",
                "allowed_amount": float(claim.get("allowed_amount", 0.0)) if claim else 0.0,
                "paid_amount": float(claim.get("paid_amount", 0.0)) if claim else 0.0,
            }
        )
    return reconciliation


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
