"""End-to-end integration test for the FastAPI backend.

Exercises all 10 endpoints against the existing fixture (tests/fixtures/config.json
+ tests/fixtures/input.csv) and asserts the same 16/2/6 outcome as the engine-level
integration test — proving the HTTP layer is a pass-through.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Fresh TestClient with an isolated storage root per test."""
    monkeypatch.setenv("REC_ENGINE_STORAGE_ROOT", str(tmp_path))
    # Force-reimport modules that read the env at import time
    import importlib
    import rec_engine_api.storage as storage
    importlib.reload(storage)
    import rec_engine_api.main as main
    importlib.reload(main)
    from fastapi.testclient import TestClient
    return TestClient(main.app)


def _config_json() -> dict:
    return json.loads((FIXTURES / "config.json").read_text())


def _input_path() -> str:
    return str(FIXTURES / "input.csv")


def test_validate_config_happy(client):
    r = client.post("/configs/validate", json=_config_json())
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["valid"] is True
    assert body.get("error") is None


def test_validate_config_rejects_bad_setid(client):
    cfg = _config_json()
    cfg["setId"] = "has a space"
    r = client.post("/configs/validate", json=cfg)
    assert r.status_code == 400
    body = r.json()
    assert body["valid"] is False
    assert "setId" in body["error"]


def test_full_e2e_flow(client):
    """Save config → run → list → fetch → filter matches/breaks → download."""
    # 1. Save config
    r = client.post("/configs", json=_config_json())
    assert r.status_code == 200, r.text
    save = r.json()
    config_id = save["configId"]
    assert config_id.startswith("cfg_USD_NOSTRO_CITIBANK_")

    # 2. List configs
    r = client.get("/configs")
    assert r.status_code == 200
    configs = r.json()["configs"]
    assert any(c["configId"] == config_id and c["setId"] == "USD_NOSTRO_CITIBANK" and c["matchPassCount"] == 4 for c in configs)

    # 3. Fetch specific config
    r = client.get(f"/configs/{config_id}")
    assert r.status_code == 200
    assert r.json()["setId"] == "USD_NOSTRO_CITIBANK"

    r = client.get("/configs/cfg_missing_abc")
    assert r.status_code == 404

    # 4. Trigger a run
    r = client.post("/runs", json={
        "configId": config_id,
        "inputPath": _input_path(),
        "cycleDate": "2024-01-15",
    })
    assert r.status_code == 200, r.text
    run = r.json()
    run_id = run["runId"]
    assert run["metrics"]["inputRows"] == 18
    assert run["metrics"]["matchedRows"] == 16
    assert run["metrics"]["unmatchedRows"] == 2
    assert run["metrics"]["matchGroups"] == 6
    assert len(run["passBreakdown"]) == 4
    pass_ids = {p["passId"] for p in run["passBreakdown"]}
    assert pass_ids == {"MP_O2O", "MP_O2M", "MP_M2O", "MP_M2M"}

    # 5. List + fetch run
    r = client.get("/runs")
    assert r.status_code == 200
    runs = r.json()["runs"]
    assert any(rr["runId"] == run_id for rr in runs)

    r = client.get(f"/runs/{run_id}")
    assert r.status_code == 200
    manifest = r.json()
    assert manifest["totals"]["matchedRows"] == 16
    assert manifest["setId"] == "USD_NOSTRO_CITIBANK"

    r = client.get("/runs/run_missing")
    assert r.status_code == 404

    # 6. Matches — unfiltered
    r = client.get(f"/runs/{run_id}/matches")
    assert r.status_code == 200
    page = r.json()
    assert page["total"] == 16
    assert page["limit"] == 100
    assert page["offset"] == 0
    assert len(page["rows"]) == 16
    for row in page["rows"]:
        assert row["STATUS"] == "MATCHED"

    # 7. Matches — filter by matched_by_pass=MP_O2M
    r = client.get(f"/runs/{run_id}/matches", params={"matched_by_pass": "MP_O2M"})
    assert r.status_code == 200
    page = r.json()
    assert page["total"] == 3  # 1 GL + 2 STMTs per the fixture 1:N pattern
    assert all(row["MATCHED_BY_PASS"] == "MP_O2M" for row in page["rows"])

    # 8. Matches — filter by currency=EUR (TRX003 pair)
    r = client.get(f"/runs/{run_id}/matches", params={"currency": "EUR"})
    assert r.status_code == 200
    page = r.json()
    assert page["total"] == 2
    assert all(row["CURRENCY"] == "EUR" for row in page["rows"])

    # 9. Matches — pagination
    r = client.get(f"/runs/{run_id}/matches", params={"limit": 5, "offset": 0})
    assert r.status_code == 200
    page = r.json()
    assert page["total"] == 16
    assert page["limit"] == 5
    assert len(page["rows"]) == 5

    r = client.get(f"/runs/{run_id}/matches", params={"limit": 5, "offset": 14})
    page = r.json()
    assert len(page["rows"]) == 2   # 16 total, offset 14 leaves 2

    # 10. Breaks — the 2 ORPHAN rows
    r = client.get(f"/runs/{run_id}/breaks")
    assert r.status_code == 200
    page = r.json()
    assert page["total"] == 2
    refs = sorted(row["REFERENCE"] for row in page["rows"])
    assert refs == ["ORPHAN1", "ORPHAN2"]

    # 11. Download — manifest
    r = client.get(f"/runs/{run_id}/download/manifest")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    manifest_dl = r.json()
    assert manifest_dl["totals"]["matchedRows"] == 16

    # 12. Download — output CSV
    r = client.get(f"/runs/{run_id}/download/output")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    lines = r.text.splitlines()
    assert len(lines) == 19  # header + 18 rows

    # 13. Download — invalid kind
    r = client.get(f"/runs/{run_id}/download/bogus")
    assert r.status_code == 400


def test_run_rejects_missing_config(client):
    r = client.post("/runs", json={
        "configId": "cfg_missing_xyz",
        "inputPath": _input_path(),
        "cycleDate": "2024-01-15",
    })
    assert r.status_code == 400


def test_run_rejects_missing_input(client):
    # Save a config first
    r = client.post("/configs", json=_config_json())
    cfg_id = r.json()["configId"]
    r = client.post("/runs", json={
        "configId": cfg_id,
        "inputPath": "/nonexistent/path/input.csv",
        "cycleDate": "2024-01-15",
    })
    assert r.status_code == 400
