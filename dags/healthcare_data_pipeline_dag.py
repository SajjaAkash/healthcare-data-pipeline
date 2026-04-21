from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from healthcare_data_pipeline.config import settings
from healthcare_data_pipeline.jobs.bronze_ingestion import run_job as preview_bronze_job
from healthcare_data_pipeline.orchestration import (
    build_dashboard_publish_command,
    build_databricks_run_now_command,
    build_databricks_run_now_payload,
)


def preview_ingestion() -> None:
    print(preview_bronze_job(["{{ ds }}"]))


def preview_databricks_payload(job_id: int) -> None:
    print(build_databricks_run_now_payload(job_id, "{{ ds }}"))


with DAG(
    dag_id="healthcare_data_pipeline_daily",
    start_date=datetime(2026, 1, 1),
    schedule=settings.platform.airflow_schedule,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2},
    tags=["healthcare", "azure", "pyspark", "streamlit"],
    description="End-to-end healthcare data pipeline orchestrated in Airflow.",
) as dag:
    start = EmptyOperator(task_id="start")

    preview_bronze = PythonOperator(
        task_id="preview_bronze_requests",
        python_callable=preview_ingestion,
    )

    preview_silver_payload = PythonOperator(
        task_id="preview_silver_databricks_payload",
        python_callable=preview_databricks_payload,
        op_kwargs={"job_id": settings.azure.databricks_silver_job_id},
    )

    preview_gold_payload = PythonOperator(
        task_id="preview_gold_databricks_payload",
        python_callable=preview_databricks_payload,
        op_kwargs={"job_id": settings.azure.databricks_gold_job_id},
    )

    run_bronze = BashOperator(
        task_id="run_bronze_ingestion",
        bash_command=build_databricks_run_now_command(
            settings.azure.databricks_bronze_job_id,
            "{{ ds }}",
        ),
    )

    run_silver = BashOperator(
        task_id="run_silver_transform",
        bash_command=build_databricks_run_now_command(
            settings.azure.databricks_silver_job_id,
            "{{ ds }}",
        ),
    )

    run_gold = BashOperator(
        task_id="run_gold_marts",
        bash_command=build_databricks_run_now_command(
            settings.azure.databricks_gold_job_id,
            "{{ ds }}",
        ),
    )

    run_quality = BashOperator(
        task_id="run_quality_checks",
        bash_command=(
            "echo python -m healthcare_data_pipeline.jobs.quality_checks "
            "--execution-date {{ ds }}"
        ),
    )

    publish_dashboard_extract = BashOperator(
        task_id="publish_dashboard_extract",
        bash_command=build_dashboard_publish_command("{{ ds }}"),
    )

    end = EmptyOperator(task_id="end")

    chain(
        start,
        preview_bronze,
        preview_silver_payload,
        preview_gold_payload,
        run_bronze,
        run_silver,
        run_gold,
        run_quality,
        publish_dashboard_extract,
        end,
    )
