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

SAMPLE_RECONCILIATION = [
    {
        "encounter_id": "E100",
        "appointment_present": True,
        "claim_present": True,
        "reconciliation_status": "matched",
        "appointment_status": "completed",
        "allowed_amount": 310.0,
        "paid_amount": 285.0,
    },
    {
        "encounter_id": "E101",
        "appointment_present": True,
        "claim_present": False,
        "reconciliation_status": "missing_claim",
        "appointment_status": "completed",
        "allowed_amount": 0.0,
        "paid_amount": 0.0,
    },
]


def main() -> None:
    st.set_page_config(page_title="Clinical Integrity Console", layout="wide")
    payload = load_dashboard_payload()
    if payload is None:
        payload = build_dashboard_payload(SAMPLE_FACTS, SAMPLE_QUALITY, SAMPLE_RECONCILIATION)
        st.info(
            "Showing bundled sample data. Run `python -m healthcare_data_pipeline.demo_pipeline` "
            "to generate local Gold outputs for the dashboard."
        )
    else:
        st.success("Loaded dashboard data from local demo pipeline outputs.")
    metrics = payload["headline_metrics"]

    risk_band = str(metrics["risk_band"]).upper()
    risk_color = {"STABLE": "#2f6f57", "WATCH": "#a56a00", "CRITICAL": "#9f2d2d"}.get(
        risk_band, "#36536b"
    )

    st.markdown(
        f"""
        <style>
        .stApp {{
            background:
                radial-gradient(circle at top right, rgba(167, 43, 43, 0.10), transparent 28%),
                linear-gradient(180deg, #f6f8fb 0%, #eef2f7 100%);
        }}
        .hero {{
            background: linear-gradient(135deg, #14324a 0%, #0f1d2b 100%);
            padding: 1.5rem 1.7rem;
            border-radius: 18px;
            color: #f7fbff;
            border: 1px solid rgba(255,255,255,0.08);
            margin-bottom: 1rem;
        }}
        .status-pill {{
            display: inline-block;
            padding: 0.3rem 0.7rem;
            border-radius: 999px;
            background: {risk_color};
            color: white;
            font-size: 0.85rem;
            font-weight: 600;
            letter-spacing: 0.04em;
        }}
        .panel {{
            background: rgba(255,255,255,0.82);
            border: 1px solid rgba(20,50,74,0.08);
            border-radius: 16px;
            padding: 1rem 1.1rem;
            box-shadow: 0 10px 30px rgba(15, 29, 43, 0.06);
        }}
        </style>
        <div class="hero">
            <div class="status-pill">RISK BAND: {risk_band}</div>
            <h1 style="margin: 0.8rem 0 0.2rem 0;">Clinical Integrity Console</h1>
            <p style="margin: 0; max-width: 52rem;">
                A reconciliation-first monitoring view for encounter coverage, claim matching,
                and healthcare data quality posture.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    summary_col, throughput_col, finance_col, ops_col = st.columns(4)
    summary_col.metric("Matched Encounters", metrics["matched_count"])
    throughput_col.metric("Total Encounters", metrics["total_encounters"])
    finance_col.metric("Total Paid Amount", f"${metrics['total_paid_amount']:.2f}")
    ops_col.metric("Missing Claims", metrics["missing_claim_count"])

    left, right = st.columns([1.25, 1])

    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Coverage Watchlist")
        st.caption("Exceptions that would typically drive an analyst or billing follow-up queue.")
        st.dataframe(payload["reconciliation_watchlist"], use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Encounter Detail")
        st.dataframe(payload["encounters"], use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Payer Exposure")
        st.bar_chart(payload["payer_mix"])
        st.dataframe(payload["payer_mix_rows"], use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Quality Exceptions")
        if payload["quality_alerts"]:
            st.error("One or more governance checks failed in the current batch.")
        else:
            st.success("No failed quality rules in the current batch.")
        st.dataframe(payload["quality_results"], use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    reconciliation_tab, quality_tab = st.tabs(["Reconciliation Ledger", "Quality Ledger"])
    with reconciliation_tab:
        st.dataframe(payload["reconciliation_results"], use_container_width=True)
    with quality_tab:
        st.dataframe(payload["quality_results"], use_container_width=True)


if __name__ == "__main__":
    main()
