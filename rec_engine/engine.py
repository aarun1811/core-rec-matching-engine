"""Engine orchestrator: load → passes → emit combined match-groups LazyFrame."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from rec_engine.loader import load as load_input
from rec_engine.matchers import dispatch
from rec_engine.matchers.base import MGIDAllocator
from rec_engine.populations import apply as apply_population
from rec_engine.types import Config, MatchPass


@dataclass
class PassStat:
    pass_id: str
    pass_name: str
    match_type: str
    rule_order: int
    matched_groups: int
    matched_rows: int
    avg_quality: float
    duration_sec: float


@dataclass
class EngineResult:
    input_lf: pl.LazyFrame
    matches_lf: pl.LazyFrame            # long-form: _row_idx, MATCHING_ID, MATCHED_BY_PASS, MATCH_QUALITY
    pass_stats: list[PassStat]
    run_started: datetime
    run_duration_sec: float


def run(config: Config, input_path: str | Path, cycle_date_override: str | None = None) -> EngineResult:
    t_start_all = time.time()
    run_started = datetime.now(timezone.utc)

    cycle_date_str = cycle_date_override or config.cycle_date
    if not cycle_date_str:
        raise ValueError("cycleDate must be provided via config or CLI")
    cycle_date = date.fromisoformat(cycle_date_str)

    mgid = MGIDAllocator(set_id=config.set_id, cycle_date=cycle_date)

    input_lf = load_input(input_path, config.schema)

    active_passes: list[MatchPass] = sorted(
        [p for p in config.match_passes if p.status == "ACTIVE"],
        key=lambda p: p.pqr.rule_order,
    )

    matched_idx: set[int] = set()
    all_match_rows: list[pl.DataFrame] = []
    pass_stats: list[PassStat] = []

    for p in active_passes:
        t_start = time.time()

        # Exclude already-matched rows for this pass
        if matched_idx:
            excl_series = pl.Series("_matched_ids", list(matched_idx), dtype=pl.UInt64)
            remaining = input_lf.filter(~pl.col("_row_idx").is_in(excl_series))
        else:
            remaining = input_lf

        left_lf = apply_population(remaining, p.population_rule.left)
        right_lf = apply_population(remaining, p.population_rule.right)

        match_rows_lf = dispatch(left_lf, right_lf, p, mgid)
        match_rows_df = match_rows_lf.collect()

        if not match_rows_df.is_empty():
            all_match_rows.append(match_rows_df)
            new_idx = match_rows_df.get_column("_row_idx").to_list()
            matched_idx.update(new_idx)
            n_groups = match_rows_df.get_column("MATCHING_ID").n_unique()
            n_rows = match_rows_df.height
            avg_q = float(match_rows_df.get_column("MATCH_QUALITY").mean() or 0.0)
        else:
            n_groups = 0
            n_rows = 0
            avg_q = 0.0

        pass_stats.append(PassStat(
            pass_id=p.id, pass_name=p.name, match_type=p.match_type, rule_order=p.pqr.rule_order,
            matched_groups=n_groups, matched_rows=n_rows,
            avg_quality=round(avg_q, 2), duration_sec=round(time.time() - t_start, 3),
        ))

    if all_match_rows:
        matches_df = pl.concat(all_match_rows)
    else:
        matches_df = pl.DataFrame(
            schema={
                "_row_idx": pl.UInt64,
                "MATCHING_ID": pl.Utf8,
                "MATCHED_BY_PASS": pl.Utf8,
                "MATCH_QUALITY": pl.Int64,
            }
        )

    matches_df = matches_df.with_columns(
        pl.lit(cycle_date).alias("MATCH_DATE"),
    )

    return EngineResult(
        input_lf=input_lf,
        matches_lf=matches_df.lazy(),
        pass_stats=pass_stats,
        run_started=run_started,
        run_duration_sec=round(time.time() - t_start_all, 3),
    )
