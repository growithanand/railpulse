from __future__ import annotations

import json
import re
from pathlib import Path


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[2]
    / "dashboards"
    / "railpulse_maintenance_dashboard.lvdash.json"
)


def _dashboard() -> dict[str, object]:
    return json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))


def test_dashboard_has_six_read_only_datasets() -> None:
    dashboard = _dashboard()
    datasets = dashboard["datasets"]

    assert len(datasets) == 6
    assert len({dataset["name"] for dataset in datasets}) == 6
    for dataset in datasets:
        assert len(dataset["queryLines"]) == 1
        query = dataset["queryLines"][0].strip()
        assert query.startswith(("SELECT", "WITH"))
        assert not re.search(
            r"\b(CREATE|INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|COPY|CALL)\b",
            query,
            flags=re.IGNORECASE,
        )


def test_dashboard_layout_references_known_datasets_and_fields() -> None:
    dashboard = _dashboard()
    dataset_names = {dataset["name"] for dataset in dashboard["datasets"]}
    pages = dashboard["pages"]

    assert len(pages) == 1
    assert pages[0]["pageType"] == "PAGE_TYPE_CANVAS"
    assert pages[0]["layoutVersion"] == "GRID_V1"
    assert len(pages[0]["layout"]) == 13

    widget_names: set[str] = set()
    for item in pages[0]["layout"]:
        widget = item["widget"]
        widget_names.add(widget["name"])
        assert 0 <= item["position"]["x"] < 12
        assert item["position"]["x"] + item["position"]["width"] <= 12
        for query_wrapper in widget.get("queries", []):
            query = query_wrapper["query"]
            assert query["datasetName"] in dataset_names
            field_names = {field["name"] for field in query["fields"]}
            encodings = widget["spec"]["encodings"]
            encoded_names = {
                encoding["fieldName"]
                for encoding in encodings.values()
                if isinstance(encoding, dict) and "fieldName" in encoding
            }
            encoded_names.update(
                column["fieldName"] for column in encodings.get("columns", [])
            )
            assert encoded_names <= field_names

    assert len(widget_names) == len(pages[0]["layout"])


def test_dashboard_keeps_the_final_result_and_deployment_guardrails_visible() -> None:
    dashboard_text = DASHBOARD_PATH.read_text(encoding="utf-8")

    assert "NOT_PRODUCTION_READY" in dashboard_text
    assert "0/1 failures detected" in dashboard_text
    assert "1/1 missed" in dashboard_text
    assert "73 false alert episodes" in dashboard_text
    assert "Do not rerun, retune" in dashboard_text
    assert "warehouse_id" not in dashboard_text.lower()
    assert "dashboard_id" not in dashboard_text.lower()
    assert "schedule" not in dashboard_text.lower()
    assert "@" not in dashboard_text
