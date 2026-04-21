"""Run execution wrapper: glue between the HTTP layer and rec_engine."""

from __future__ import annotations

import secrets
from datetime import datetime, timezone
from pathlib import Path

from rec_engine import config as rec_config
from rec_engine import engine as rec_engine
from rec_engine import writer as rec_writer

from rec_engine_api import storage


def _mint_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{ts}_{secrets.token_hex(3)}"


def execute_run(config_id: str, input_path: str, cycle_date: str | None) -> dict:
    """Run the engine for a saved config + input path.

    Returns a dict compatible with RunResponse.

    Raises:
        FileNotFoundError: config or input not found.
        rec_engine.config.ConfigError: config validation failed on load.
        ValueError: bad cycleDate or other engine errors.
    """
    cfg_path = storage.config_path(config_id)
    if not cfg_path.exists():
        raise FileNotFoundError(f"config not found: {config_id}")

    input_p = Path(input_path)
    if not input_p.exists():
        raise FileNotFoundError(f"inputPath not found: {input_path}")

    config = rec_config.load(cfg_path)

    run_id = _mint_run_id()
    rdir = storage.run_dir(run_id)
    rdir.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    storage.save_meta(
        run_id,
        {
            "configId": config_id,
            "inputPath": str(input_p),
            "cycleDate": cycle_date,
            "startedAt": started_at,
        },
    )

    result = rec_engine.run(config, str(input_p), cycle_date_override=cycle_date)
    out_csv = storage.output_csv_path(run_id)
    manifest = rec_writer.write(
        result,
        config,
        str(cfg_path),
        str(input_p),
        str(out_csv),
    )

    # writer.write drops a sidecar at "{out_csv}.manifest.json"; we persist the
    # returned dict to the canonical ./runs/{runId}/manifest.json path instead.
    storage.save_manifest(run_id, manifest)
    sidecar = Path(str(out_csv) + ".manifest.json")
    if sidecar.exists() and sidecar.resolve() != storage.manifest_path(run_id).resolve():
        try:
            sidecar.unlink()
        except OSError:
            pass

    totals = manifest.get("totals") or {}
    return {
        "runId": run_id,
        "metrics": {
            "inputRows": int(totals.get("inputRows", 0)),
            "matchedRows": int(totals.get("matchedRows", 0)),
            "unmatchedRows": int(totals.get("unmatchedRows", 0)),
            "matchGroups": int(totals.get("matchGroups", 0)),
        },
        "outputPath": str(out_csv),
        "manifestPath": str(storage.manifest_path(run_id)),
        "passBreakdown": manifest.get("passBreakdown", []),
        "durationSec": float(manifest.get("durationSec", 0.0) or 0.0),
    }
