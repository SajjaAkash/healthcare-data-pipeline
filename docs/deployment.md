# Deployment Guide

## Prerequisites

- Azure CLI authenticated to the target subscription.
- Terraform 1.6+ installed.
- Python 3.11+ for local validation.
- An Airflow runtime available locally or in Azure-hosted infrastructure.

## Terraform Setup

1. Copy the example variables file:

   ```powershell
   Copy-Item infrastructure/terraform/terraform.tfvars.example infrastructure/terraform/terraform.tfvars
   ```

2. Initialize and review the plan:

   ```powershell
   cd infrastructure/terraform
   terraform init
   terraform plan
   ```

3. Apply the infrastructure:

   ```powershell
   terraform apply
   ```

## Airflow Deployment

- Deploy the DAG under `dags/` to your Airflow environment.
- Install the package and dashboard dependencies in the Airflow runtime image.
- Provide `DATABRICKS_TOKEN` as a secret in the Airflow runtime.
- Set `AZURE_DRY_RUN=false` when you are ready for real Databricks submissions.
- Configure `DATABRICKS_BRONZE_JOB_ID`, `DATABRICKS_SILVER_JOB_ID`, and `DATABRICKS_GOLD_JOB_ID` for your workspace.
- The DAG will call the Databricks `jobs/run-now` API using the configured workspace URL.

## Streamlit Deployment

- The repo includes `streamlit_app.py` as a lightweight presentation layer.
- For local demos, generate dashboard-ready outputs with `python -m healthcare_data_pipeline.demo_pipeline`.
- Point the app at Gold KPI extracts in ADLS or a serving table once infrastructure is live.
- Deploy on Azure App Service, Container Apps, or another hosting target aligned with your platform standards.
