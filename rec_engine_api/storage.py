"""Filesystem read/write for configs + runs.

Layout (rooted at REC_ENGINE_STORAGE_ROOT, default "."):
  ./configs/{configId}.json
  ./runs/{runId}/output.csv
  ./runs/{runId}/manifest.json
  ./runs/{runId}/meta.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from rec_engine_api.schemas import ConfigListItem, RunListItem, RunTotals

STORAGE_ROOT = Path(os.environ.get("REC_ENGINE_STORAGE_ROOT", "."))
CONFIGS_DIR = STORAGE_ROOT / "configs"
RUNS_DIR = STORAGE_ROOT / "runs"


def ensure_dirs() -> None:
    """mkdir -p both roots; safe to call repeatedly."""
    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)


def config_path(config_id: str) -> Path:
    return CONFIGS_DIR / f"{config_id}.json"


def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id


def output_csv_path(run_id: str) -> Path:
    return run_dir(run_id) / "output.csv"


def manifest_path(run_id: str) -> Path:
    return run_dir(run_id) / "manifest.json"


def meta_path(run_id: str) -> Path:
    return run_dir(run_id) / "meta.json"


def save_config(config_id: str, raw: dict) -> None:
    """Write config JSON (indent=3 to match fixture style)."""
    ensure_dirs()
    p = config_path(config_id)
    p.write_text(json.dumps(raw, indent=3))


def load_config_raw(config_id: str) -> dict:
    p = config_path(config_id)
    if not p.exists():
        raise FileNotFoundError(f"config not found: {config_id}")
    return json.loads(p.read_text())


def list_configs() -> list[ConfigListItem]:
    ensure_dirs()
    items: list[ConfigListItem] = []
    for p in CONFIGS_DIR.glob("*.json"):
        try:
            raw = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        set_id = raw.get("setId") or ""
        passes = raw.get("matchPasses") or []
        saved_at = (
            datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
        items.append(
            ConfigListItem(
                configId=p.stem,
                setId=set_id,
                savedAt=saved_at,
                matchPassCount=len(passes) if isinstance(passes, list) else 0,
            )
        )
    items.sort(key=lambda i: i.savedAt, reverse=True)
    return items


def save_meta(run_id: str, meta: dict) -> None:
    p = meta_path(run_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(meta, indent=2, default=str))


def save_manifest(run_id: str, manifest: dict) -> None:
    """Persist the manifest dict (as returned by writer.write) to manifest.json."""
    p = manifest_path(run_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, indent=2, default=str))


def load_manifest(run_id: str) -> dict:
    p = manifest_path(run_id)
    if not p.exists():
        raise FileNotFoundError(f"run not found: {run_id}")
    return json.loads(p.read_text())


def load_meta(run_id: str) -> dict:
    p = meta_path(run_id)
    if not p.exists():
        return {}
    return json.loads(p.read_text())


def list_runs() -> list[RunListItem]:
    ensure_dirs()
    items: list[RunListItem] = []
    for rdir in RUNS_DIR.iterdir():
        if not rdir.is_dir():
            continue
        mpath = rdir / "manifest.json"
        if not mpath.exists():
            continue
        try:
            manifest = json.loads(mpath.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        meta: dict = {}
        metap = rdir / "meta.json"
        if metap.exists():
            try:
                meta = json.loads(metap.read_text())
            except (json.JSONDecodeError, OSError):
                meta = {}
        totals_raw = manifest.get("totals") or {}
        totals = RunTotals(
            inputRows=int(totals_raw.get("inputRows", 0)),
            matchedRows=int(totals_raw.get("matchedRows", 0)),
            unmatchedRows=int(totals_raw.get("unmatchedRows", 0)),
            matchGroups=int(totals_raw.get("matchGroups", 0)),
        )
        started_at = manifest.get("runTimestamp") or meta.get("startedAt") or ""
        items.append(
            RunListItem(
                runId=rdir.name,
                configId=meta.get("configId"),
                setId=manifest.get("setId"),
                startedAt=started_at,
                durationSec=float(manifest.get("durationSec", 0.0) or 0.0),
                totals=totals,
            )
        )
    items.sort(key=lambda i: i.startedAt, reverse=True)
    return items
