from healthcare_data_pipeline.jobs.bronze_ingestion import build_source_requests


def test_build_source_requests_returns_three_healthcare_sources() -> None:
    requests = build_source_requests("2026-04-20")

    assert len(requests) == 3
    assert {request["source_name"] for request in requests} == {
        "patients",
        "claims",
        "appointments",
    }
    assert all("abfss://" in request["destination_path"] for request in requests)
