"""1:1 matcher: greedy highest-quality pairing within hard-key buckets."""

from __future__ import annotations

import polars as pl

from rec_engine.expressions import compile_attr_expr
from rec_engine.matchers.base import (
    MGIDAllocator,
    attr_bare_name,
    hard_key_columns,
    tolerance_attrs,
    contains_attrs,
)
from rec_engine.scorer import score_expr
from rec_engine.types import MatchPass


def match(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
) -> pl.LazyFrame:
    """
    Returns a LazyFrame with one row per participating record (left + right):
        columns: _row_idx, MATCHING_ID, MATCHED_BY_PASS, MATCH_QUALITY
    """
    hkeys = hard_key_columns(pass_config)
    left = left_lf.rename({"_row_idx": "_row_idx_L"})
    right = right_lf.rename({"_row_idx": "_row_idx_R"})

    # Right-side column suffix strategy: rename non-key columns of right with _R suffix,
    # but keep the join keys un-suffixed so the inner join works cleanly.
    right_cols = right.columns
    to_suffix = [c for c in right_cols if c not in hkeys and c != "_row_idx_R"]
    right = right.rename({c: f"{c}_R" for c in to_suffix})

    candidates = left.join(right, on=hkeys, how="inner")

    # Apply WITHIN filters
    for a in tolerance_attrs(pass_config):
        bare_left = attr_bare_name(a.left_attribute)
        le = compile_attr_expr(a.left_attribute)
        re_ = compile_attr_expr(a.right_attribute + "_R")
        if bare_left == "VALUE_DATE" and a.tolerance_days is not None:
            candidates = candidates.filter(
                (le - re_).dt.total_days().abs() <= a.tolerance_days
            )
        else:
            tol_abs = a.tolerance_amount or 0.0
            tol_pct = a.tolerance_percent or 0.0
            tol_eff = pl.max_horizontal(pl.lit(tol_abs), (le.cast(pl.Float64).abs() * tol_pct))
            candidates = candidates.filter(
                (le.cast(pl.Float64) - re_.cast(pl.Float64)).abs() <= tol_eff
            )

    # Apply CONTAINS filters (literal=True disables regex; accepts column-vs-column)
    for a in contains_attrs(pass_config):
        le = compile_attr_expr(a.left_attribute).cast(pl.Utf8)
        re_ = compile_attr_expr(a.right_attribute + "_R").cast(pl.Utf8)
        candidates = candidates.filter(le.str.contains(re_, literal=True))

    # The scorer assumes right-side columns carry a _R suffix. Join keys were shared
    # un-suffixed; duplicate them as _R so the scorer can resolve them uniformly.
    candidates = candidates.with_columns(
        [pl.col(c).alias(f"{c}_R") for c in hkeys if f"{c}_R" not in right_cols]
    )

    # Score
    candidates = candidates.with_columns(score_expr(pass_config).alias("_quality"))
    candidates = candidates.filter(pl.col("_quality") >= pass_config.pqr.quality)

    # Greedy 1:1: left-uniqueness then right-uniqueness.
    # sort().unique(keep="first") is unambiguous — takes the first (i.e., highest quality)
    # row per subset. Secondary sort by _row_idx_L/_R makes tie-breaks deterministic
    # across Polars versions and parallel execution orderings.
    candidates = (
        candidates.sort(["_quality", "_row_idx_L"], descending=[True, False])
        .unique(subset=["_row_idx_L"], keep="first", maintain_order=True)
    )
    candidates = (
        candidates.sort(["_quality", "_row_idx_R"], descending=[True, False])
        .unique(subset=["_row_idx_R"], keep="first", maintain_order=True)
    )

    # Collect survivors and allocate MGIDs
    pairs = candidates.select(["_row_idx_L", "_row_idx_R", "_quality"]).collect()
    if pairs.is_empty():
        return _empty_result_lf()

    n = pairs.height
    mgids = mgid.allocate_batch(n)
    pairs = pairs.with_columns(pl.Series("MATCHING_ID", mgids))

    # Explode to long-form: one row per participating record.
    left_rows = pairs.select(
        pl.col("_row_idx_L").alias("_row_idx"),
        pl.col("MATCHING_ID"),
        pl.lit(pass_config.id).alias("MATCHED_BY_PASS"),
        pl.col("_quality").alias("MATCH_QUALITY"),
    )
    right_rows = pairs.select(
        pl.col("_row_idx_R").alias("_row_idx"),
        pl.col("MATCHING_ID"),
        pl.lit(pass_config.id).alias("MATCHED_BY_PASS"),
        pl.col("_quality").alias("MATCH_QUALITY"),
    )
    return pl.concat([left_rows, right_rows]).lazy()


def _empty_result_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            "_row_idx": pl.UInt64,
            "MATCHING_ID": pl.Utf8,
            "MATCHED_BY_PASS": pl.Utf8,
            "MATCH_QUALITY": pl.Int64,
        }
    ).lazy()
