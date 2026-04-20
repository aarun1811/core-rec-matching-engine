"""Assemble the output LazyFrame (preserving input column order) and write CSV + manifest."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import polars as pl

from rec_engine.engine import EngineResult
from rec_engine.schema import OUTPUT_COLUMNS
from rec_engine.types import Config


def write(
    result: EngineResult,
    config: Config,
    config_path: str,
    input_path: str,
    output_path: str,
) -> dict:
    input_lf = result.input_lf
    matches_lf = result.matches_lf

    # Use .columns (Polars 0.20.31 LazyFrames don't expose collect_schema()).
    original_cols = [c for c in input_lf.columns if c != "_row_idx"]

    joined = input_lf.join(matches_lf, on="_row_idx", how="left")

    # STATUS: MATCHED if MATCHING_ID is not null, else UNMATCHED (overwriting any existing STATUS)
    joined = joined.with_columns([
        pl.when(pl.col("MATCHING_ID").is_not_null())
          .then(pl.lit("MATCHED"))
          .otherwise(pl.lit("UNMATCHED"))
          .alias("STATUS"),
    ])

    # Final column order: original columns first (with STATUS overwritten),
    # then any OUTPUT_COLUMNS not already in original order.
    final_cols: list[str] = list(original_cols)
    for oc in OUTPUT_COLUMNS:
        if oc not in final_cols:
            final_cols.append(oc)

    # Drop STATUS_right if a STATUS_right column was produced by the join
    # (can happen when input also had STATUS and matches_lf did not — safe to check).
    avail_before = joined.columns
    if "STATUS_right" in avail_before:
        joined = joined.drop("STATUS_right")

    # Restore input row order via _row_idx, then drop it
    joined = joined.sort("_row_idx").drop("_row_idx")

    # Select final columns (only those present in the frame)
    avail = joined.columns
    select_cols = [c for c in final_cols if c in avail]
    joined = joined.select(select_cols)

    # Sink CSV (streaming) — this is the one materialization for the output.
    # Polars 0.20.31's standard engine does not support sink_csv for our plan shape;
    # fall back to collect(streaming=True).write_csv(path).
    # Also: 0.20.31's write_csv cannot serialize Decimal columns — cast any Decimal
    # columns to Utf8 to preserve exact formatting (e.g. "1000.0000").
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    materialized = joined.collect(streaming=True)
    decimal_cols = [
        name for name, dtype in materialized.schema.items()
        if isinstance(dtype, pl.Decimal)
    ]
    if decimal_cols:
        materialized = materialized.with_columns([
            pl.col(c).cast(pl.Utf8) for c in decimal_cols
        ])
    materialized.write_csv(output_path)

    # Manifest
    manifest = _build_manifest(result, config, config_path, input_path, output_path)
    manifest_path = Path(str(output_path) + ".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    return manifest


def _build_manifest(
    result: EngineResult,
    config: Config,
    config_path: str,
    input_path: str,
    output_path: str,
) -> dict:
    n_input = result.input_lf.select(pl.len()).collect().item()
    matches_df = result.matches_lf.collect()
    matched_rows = matches_df.height
    match_groups = matches_df.get_column("MATCHING_ID").n_unique() if matched_rows else 0

    unmatched_input_lf = result.input_lf.join(
        result.matches_lf.select("_row_idx"), on="_row_idx", how="anti",
    )
    unmatched_df = unmatched_input_lf.select(["LS_TYPE", "CURRENCY"]).collect()
    by_side = Counter(unmatched_df.get_column("LS_TYPE").to_list())
    by_ccy = Counter(unmatched_df.get_column("CURRENCY").to_list())

    return {
        "setId": config.set_id,
        # Record the *effective* cycle date (CLI override takes precedence over
        # config.cycle_date) so the manifest matches the MATCH_DATE values in
        # the output CSV.
        "cycleDate": result.cycle_date.isoformat(),
        "configPath": str(Path(config_path).resolve()),
        "inputPath": str(Path(input_path).resolve()),
        "outputPath": str(Path(output_path).resolve()),
        "runTimestamp": result.run_started.isoformat(),
        "durationSec": result.run_duration_sec,
        "totals": {
            "inputRows": int(n_input),
            "matchedRows": int(matched_rows),
            "unmatchedRows": int(n_input - matched_rows),
            "matchGroups": int(match_groups),
        },
        "passBreakdown": [
            {
                "passId": s.pass_id,
                "passName": s.pass_name,
                "matchType": s.match_type,
                "ruleOrder": s.rule_order,
                "matchedGroups": s.matched_groups,
                "matchedRows": s.matched_rows,
                "avgQuality": s.avg_quality,
                "durationSec": s.duration_sec,
            } for s in result.pass_stats
        ],
        "unmatchedBreakdown": {
            "bySide": dict(by_side),
            "byCurrency": dict(by_ccy),
        },
    }
