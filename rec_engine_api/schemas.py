"""Pydantic request/response models for the API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ConfigValidateResponse(BaseModel):
    valid: bool
    error: str | None = None


class ConfigSaveResponse(BaseModel):
    configId: str
    savedAt: str
    path: str


class ConfigListItem(BaseModel):
    configId: str
    setId: str
    savedAt: str
    matchPassCount: int


class ConfigListResponse(BaseModel):
    configs: list[ConfigListItem]


class RunRequest(BaseModel):
    configId: str
    inputPath: str
    cycleDate: str | None = None


class RunTotals(BaseModel):
    inputRows: int
    matchedRows: int
    unmatchedRows: int
    matchGroups: int


class RunResponse(BaseModel):
    runId: str
    metrics: RunTotals
    outputPath: str
    manifestPath: str
    passBreakdown: list[dict[str, Any]]
    durationSec: float


class RunListItem(BaseModel):
    runId: str
    configId: str | None
    setId: str | None
    startedAt: str
    durationSec: float
    totals: RunTotals


class RunListResponse(BaseModel):
    runs: list[RunListItem]


class PageResponse(BaseModel):
    total: int
    limit: int
    offset: int
    rows: list[dict[str, Any]]


class ErrorResponse(BaseModel):
    error: str
