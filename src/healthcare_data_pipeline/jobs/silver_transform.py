from __future__ import annotations

from collections.abc import Iterable


def normalize_patients(records: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for record in records:
        normalized.append(
            {
                "patient_id": str(record["patient_id"]).strip(),
                "full_name": str(record.get("full_name", "")).strip(),
                "gender": str(record.get("gender", "unknown")).strip().title(),
                "state": str(record.get("state", "unknown")).strip().upper(),
                "birth_year": int(record["birth_year"]),
            }
        )
    return normalized


def normalize_claims(records: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for record in records:
        billed_amount = float(record.get("billed_amount", 0.0))
        allowed_amount = float(record.get("allowed_amount", 0.0))
        normalized.append(
            {
                "claim_id": str(record["claim_id"]).strip(),
                "patient_id": str(record["patient_id"]).strip(),
                "provider_id": str(record["provider_id"]).strip(),
                "encounter_id": str(record["encounter_id"]).strip(),
                "procedure_category": str(record.get("procedure_category", "General")).strip(),
                "payer_name": str(record.get("payer_name", "Unknown")).strip(),
                "billed_amount": billed_amount,
                "allowed_amount": allowed_amount,
                "paid_amount": float(record.get("paid_amount", allowed_amount)),
                "service_date": str(record["service_date"]).strip(),
            }
        )
    return normalized


def normalize_appointments(records: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for record in records:
        normalized.append(
            {
                "encounter_id": str(record["encounter_id"]).strip(),
                "patient_id": str(record["patient_id"]).strip(),
                "provider_id": str(record["provider_id"]).strip(),
                "appointment_date": str(record["appointment_date"]).strip(),
                "appointment_status": str(record.get("appointment_status", "completed")).strip(),
                "clinic_name": str(record.get("clinic_name", "General Medicine")).strip(),
                "wait_minutes": int(record.get("wait_minutes", 0)),
                "outcome_score": float(record.get("outcome_score", 0.0)),
            }
        )
    return normalized
