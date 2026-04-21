"""FastAPI app: route registration, exception handlers, storage bootstrap.

Runs are synchronous — the HTTP request blocks until the engine finishes. The
blocking Polars work is dispatched to a threadpool via run_in_threadpool so it
doesn't stall the event loop.
"""

from __future__ import annotations

import json
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from rec_engine.config import ConfigError, _parse_config

from rec_engine_api import runs as runs_module
from rec_engine_api import storage
from rec_engine_api.filters import MAX_LIMIT, VALID_FILTERS, page_output
from rec_engine_api.schemas import (
    ConfigListResponse,
    ConfigSaveResponse,
    ConfigValidateResponse,
    PageResponse,
    RunListResponse,
    RunRequest,
    RunResponse,
)

_SET_ID_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_-]+")


@asynccontextmanager
async def _lifespan(app: FastAPI):
    storage.ensure_dirs()
    yield


app = FastAPI(
    title="Reconciliation Matching Engine API",
    version="0.1.0",
    lifespan=_lifespan,
)

# CORS — permissive by default for local dev so a browser-based UI on any port
# (e.g. Vite on 5173, CRA on 3000) can hit the API directly. Override via the
# REC_ENGINE_API_CORS_ORIGINS env var — comma-separated list of allowed origins.
# Set it to a specific origin in production (e.g. "https://recon.yourdomain.com").
_cors_env = os.environ.get("REC_ENGINE_API_CORS_ORIGINS", "*").strip()
_cors_origins: list[str] = ["*"] if _cors_env == "*" else [o.strip() for o in _cors_env.split(",") if o.strip()]
# `allow_credentials=True` requires explicit origins; Starlette silently ignores
# it when origins=["*"], so wildcard + no-credentials is the correct pairing.
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=(_cors_origins != ["*"]),
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],  # so fetch() can read the filename on downloads
)


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(ConfigError)
async def _config_error_handler(request: Request, exc: ConfigError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": str(exc)})


@app.exception_handler(FileNotFoundError)
async def _not_found_handler(request: Request, exc: FileNotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": str(exc)})


@app.exception_handler(ValueError)
async def _value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": str(exc)})


@app.exception_handler(Exception)
async def _unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
    # Don't leak tracebacks to clients in POC — just a generic 500.
    return JSONResponse(status_code=500, content={"error": "internal server error"})


# ---------------------------------------------------------------------------
# /configs
# ---------------------------------------------------------------------------


@app.post("/configs/validate", response_model=ConfigValidateResponse)
async def validate_config(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        _parse_config(body)
    except ConfigError as exc:
        return JSONResponse(status_code=400, content={"valid": False, "error": str(exc)})
    return JSONResponse(status_code=200, content={"valid": True, "error": None})


@app.post("/configs", response_model=ConfigSaveResponse)
async def save_config(request: Request) -> JSONResponse:
    body = await request.json()
    try:
        _parse_config(body)
    except ConfigError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    raw_set_id = body.get("setId", "unknown")
    safe_set_id = _SET_ID_SANITIZE_RE.sub("_", str(raw_set_id)) or "unknown"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    config_id = f"cfg_{safe_set_id}_{ts}"

    storage.save_config(config_id, body)
    saved_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return JSONResponse(
        status_code=200,
        content={
            "configId": config_id,
            "savedAt": saved_at,
            "path": str(storage.config_path(config_id)),
        },
    )


@app.get("/configs", response_model=ConfigListResponse)
async def list_configs() -> dict:
    return {"configs": [c.model_dump() for c in storage.list_configs()]}


@app.get("/configs/{config_id}")
async def get_config(config_id: str) -> JSONResponse:
    try:
        raw = storage.load_config_raw(config_id)
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "config not found"})
    return JSONResponse(status_code=200, content=raw)


# ---------------------------------------------------------------------------
# /runs
# ---------------------------------------------------------------------------


@app.post("/runs", response_model=RunResponse)
async def create_run(req: RunRequest) -> JSONResponse:
    try:
        result = await run_in_threadpool(
            runs_module.execute_run, req.configId, req.inputPath, req.cycleDate
        )
    except FileNotFoundError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except ConfigError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    return JSONResponse(status_code=200, content=result)


@app.get("/runs", response_model=RunListResponse)
async def list_runs() -> dict:
    return {"runs": [r.model_dump() for r in storage.list_runs()]}


@app.get("/runs/{run_id}")
async def get_run(run_id: str) -> JSONResponse:
    try:
        manifest = storage.load_manifest(run_id)
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "run not found"})
    meta = storage.load_meta(run_id)
    merged: dict[str, Any] = dict(manifest)
    # Keep manifest keys authoritative; meta fills in configId / startedAt / cycleDate-as-requested.
    for k, v in meta.items():
        merged.setdefault(k, v)
    merged["runId"] = run_id
    return JSONResponse(status_code=200, content=merged)


def _parse_page_params(limit: int, offset: int) -> tuple[int, int]:
    if limit < 0:
        limit = 0
    if limit > MAX_LIMIT:
        limit = MAX_LIMIT
    if offset < 0:
        offset = 0
    return limit, offset


def _collect_filters(
    matched_by_pass: str | None,
    currency: str | None,
    bank_account: str | None,
) -> dict[str, str]:
    filters: dict[str, str] = {}
    if matched_by_pass is not None:
        filters["matched_by_pass"] = matched_by_pass
    if currency is not None:
        filters["currency"] = currency
    if bank_account is not None:
        filters["bank_account"] = bank_account
    return filters


@app.get("/runs/{run_id}/matches", response_model=PageResponse)
async def get_matches(
    run_id: str,
    limit: int = Query(100, ge=0, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    matched_by_pass: str | None = Query(None),
    currency: str | None = Query(None),
    bank_account: str | None = Query(None),
) -> JSONResponse:
    csv_path = storage.output_csv_path(run_id)
    if not csv_path.exists():
        return JSONResponse(status_code=404, content={"error": "run not found"})
    limit, offset = _parse_page_params(limit, offset)
    filters = _collect_filters(matched_by_pass, currency, bank_account)
    payload = await run_in_threadpool(page_output, csv_path, "MATCHED", filters, limit, offset)
    return JSONResponse(status_code=200, content=payload)


@app.get("/runs/{run_id}/breaks", response_model=PageResponse)
async def get_breaks(
    run_id: str,
    limit: int = Query(100, ge=0, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    matched_by_pass: str | None = Query(None),
    currency: str | None = Query(None),
    bank_account: str | None = Query(None),
) -> JSONResponse:
    csv_path = storage.output_csv_path(run_id)
    if not csv_path.exists():
        return JSONResponse(status_code=404, content={"error": "run not found"})
    limit, offset = _parse_page_params(limit, offset)
    filters = _collect_filters(matched_by_pass, currency, bank_account)
    payload = await run_in_threadpool(page_output, csv_path, "UNMATCHED", filters, limit, offset)
    return JSONResponse(status_code=200, content=payload)


@app.get("/runs/{run_id}/download/{kind}")
async def download(run_id: str, kind: str):
    if kind not in ("output", "manifest"):
        return JSONResponse(
            status_code=400,
            content={"error": f"invalid kind {kind!r}; expected 'output' or 'manifest'"},
        )
    if not storage.run_dir(run_id).exists():
        return JSONResponse(status_code=404, content={"error": "run not found"})

    if kind == "output":
        p = storage.output_csv_path(run_id)
        if not p.exists():
            return JSONResponse(status_code=404, content={"error": "output.csv not found"})
        return FileResponse(
            path=str(p),
            media_type="text/csv",
            filename=f"{run_id}_output.csv",
        )

    # kind == "manifest"
    p = storage.manifest_path(run_id)
    if not p.exists():
        return JSONResponse(status_code=404, content={"error": "manifest.json not found"})
    return FileResponse(
        path=str(p),
        media_type="application/json",
        filename=f"{run_id}_manifest.json",
    )
