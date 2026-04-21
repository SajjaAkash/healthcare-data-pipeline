# Architecture Notes

## Platform Overview

This project models a healthcare analytics platform on Azure using a medallion-style data design:

- Bronze: raw API extracts for patients, claims, and appointments land in ADLS Gen2 containers.
- Silver: PySpark standardization aligns data types, normalizes dimensions, and resolves source inconsistencies.
- Gold: dimensional marts and KPI datasets support operational and reporting use cases.
- Delivery: Streamlit surfaces curated KPIs to stakeholders without requiring direct warehouse access.

## Domain Design

The project centers on a clinical operations and revenue-cycle workflow:

- `dim_patient` supports population slicing by demographics and geography.
- `dim_date` supports daily reporting and freshness tracking.
- `fact_encounter` combines appointment behavior, provider activity, and claims economics.
- KPI outputs summarize utilization, collections, wait times, and payer mix.

## Quality Design

The quality suite is intentionally explicit and interview-friendly:

- Required-field checks protect against null-heavy source data.
- Referential integrity validates that every claim maps to a known patient.
- Freshness checks ensure SLA compliance before downstream publication.

## Azure Deployment Shape

- Azure Resource Group for project isolation.
- ADLS Gen2-compatible storage account for Bronze, Silver, Gold, and dashboard extracts.
- Key Vault for source credentials and Airflow secrets.
- User-assigned managed identity for dashboard and pipeline access to storage and secrets.
- Log Analytics workspace for pipeline observability.
- Linux App Service plan and web app placeholder for dashboard hosting.
- Azure Databricks job ids and workspace URL passed into Airflow runtime configuration.

## Orchestration Pattern

The Airflow DAG is designed around an Azure-native execution path while remaining safe to inspect locally:

- Bronze, Silver, and Gold tasks generate Databricks `jobs/run-now` payloads.
- `AZURE_DRY_RUN=true` causes the DAG to echo the exact `curl` commands instead of submitting them.
- Switching `AZURE_DRY_RUN=false` and supplying `DATABRICKS_TOKEN` turns the same tasks into runnable Databricks API calls.
