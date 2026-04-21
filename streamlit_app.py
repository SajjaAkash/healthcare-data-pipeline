from __future__ import annotations

import streamlit as st

from healthcare_data_pipeline.dashboard import build_dashboard_payload, load_dashboard_payload

SAMPLE_FACTS = [
    {
        "encounter_id": "E100",
        "patient_id": "P100",
        "provider_id": "DR10",
        "date_key": "20260420",
        "clinic_name": "Cardiology",
        "appointment_status": "completed",
        "wait_minutes": 18,
        "outcome_score": 4.4,
        "allowed_amount": 310.0,
        "paid_amount": 285.0,
        "payer_name": "Blue Cross",
        "procedure_category": "Cardiology",
    },
    {
        "encounter_id": "E101",
        "patient_id": "P101",
        "provider_id": "DR11",
        "date_key": "20260420",
        "clinic_name": "Primary Care",
        "appointment_status": "completed",
        "wait_minutes": 11,
        "outcome_score": 4.8,
        "allowed_amount": 140.0,
        "paid_amount": 140.0,
        "payer_name": "Aetna",
        "procedure_category": "Preventive",
    },
]

SAMPLE_QUALITY = [
    {
        "rule_name": "patients_required_fields",
        "passed": True,
        "detail": "All required fields present.",
    },
    {
        "rule_name": "claims_patient_referential_integrity",
        "passed": True,
        "detail": "All claims reference a valid patient.",
    },
    {"rule_name": "source_freshness", "passed": True, "detail": "age_hours=4.00, sla_hours=24"},
]


def main() -> None:
    st.set_page_config(page_title="Healthcare Data Pipeline", layout="wide")
    payload = load_dashboard_payload()
    if payload is None:
        payload = build_dashboard_payload(SAMPLE_FACTS, SAMPLE_QUALITY)
        st.info(
            "Showing bundled sample data. Run `python -m healthcare_data_pipeline.demo_pipeline` "
            "to generate local Gold outputs for the dashboard."
        )
    else:
        st.success("Loaded dashboard data from local demo pipeline outputs.")
    metrics = payload["headline_metrics"]

    st.title("Healthcare Operations KPI Dashboard")
    st.caption("Streamlit delivery layer for curated healthcare analytics datasets.")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Encounters", metrics["total_encounters"])
    col2.metric("Total Paid Amount", f"${metrics['total_paid_amount']:.2f}")
    col3.metric("Average Wait (min)", metrics["average_wait_minutes"])
    col4.metric("Quality Pass Rate", f"{metrics['quality_pass_rate'] * 100:.0f}%")

    st.subheader("Payer Mix")
    st.bar_chart(payload["payer_mix"])

    st.subheader("Encounter Detail")
    st.dataframe(payload["encounters"], use_container_width=True)

    st.subheader("Quality Checks")
    st.dataframe(payload["quality_results"], use_container_width=True)


if __name__ == "__main__":
    main()
