"""1:N matcher: full-bucket aggregation within hard-key buckets."""

from __future__ import annotations

import polars as pl

from rec_engine.expressions import compile_attr_expr
from rec_engine.matchers.base import (
    MGIDAllocator,
    attr_bare_name,
    hard_key_columns,
)
from rec_engine.types import AttributeMatch, MatchPass


def match(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
) -> pl.LazyFrame:
    """
    1:N: one-side is left, many-side is right.
    Full-bucket aggregation: SUM(right rows in bucket) must match left row within tolerance.
    All-within date rule: every right row's date must be within tolerance of left date.
    """
    return _match_one_to_many(left_lf, right_lf, pass_config, mgid, one_is_left=True)


def _match_one_to_many(
    one_lf: pl.LazyFrame,
    many_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
    one_is_left: bool,
) -> pl.LazyFrame:
    hkeys = hard_key_columns(pass_config)

    # Identify aggregation attribute (SUM on the many-side)
    agg_attr = _find_agg_attr(pass_config, many_is_side="RIGHT" if one_is_left else "LEFT")
    if agg_attr is None:
        # Shouldn't happen: config validation enforces it for 1:N/N:1
        return _empty_result_lf()

    many_bare = attr_bare_name(
        agg_attr.right_attribute if one_is_left else agg_attr.left_attribute
    )

    # Identify date WITHIN attribute (for all-within rule), if any
    date_attr: AttributeMatch | None = next(
        (a for a in pass_config.attributes_to_match
         if a.operator == "WITHIN" and attr_bare_name(a.left_attribute) == "VALUE_DATE"),
        None,
    )

    # Bucket the many side
    many_agg_cols = [
        pl.col("_row_idx").alias("_many_idx_list"),
        pl.col(many_bare).sum().alias("_many_sum"),
    ]
    if date_attr is not None:
        many_agg_cols += [
            pl.col("VALUE_DATE").min().alias("_many_date_min"),
            pl.col("VALUE_DATE").max().alias("_many_date_max"),
        ]
    many_agg_cols.append(pl.len().alias("_many_n"))

    many_bucketed = many_lf.group_by(hkeys).agg(many_agg_cols)

    # Only buckets with >= 2 rows on the many side are 1:N candidates
    many_bucketed = many_bucketed.filter(pl.col("_many_n") >= 2)

    # Rename one-side _row_idx
    one_side = one_lf.rename({"_row_idx": "_one_idx"})

    candidates = one_side.join(many_bucketed, on=hkeys, how="inner")

    # Amount-aggregation filter
    one_amt_bare = attr_bare_name(
        agg_attr.left_attribute if one_is_left else agg_attr.right_attribute
    )
    tol_abs = agg_attr.tolerance_amount or 0.0
    tol_pct = agg_attr.tolerance_percent or 0.0
    if agg_attr.operator == "EQUALS":
        candidates = candidates.filter(
            pl.col(one_amt_bare).cast(pl.Float64) == pl.col("_many_sum").cast(pl.Float64)
        )
    else:  # WITHIN
        tol_eff = pl.max_horizontal(
            pl.lit(tol_abs),
            pl.col(one_amt_bare).cast(pl.Float64).abs() * tol_pct,
        )
        candidates = candidates.filter(
            (pl.col(one_amt_bare).cast(pl.Float64) - pl.col("_many_sum").cast(pl.Float64)).abs() <= tol_eff
        )

    # All-within date filter
    if date_attr is not None and date_attr.tolerance_days is not None:
        td = date_attr.tolerance_days
        candidates = candidates.filter(
            ((pl.col("VALUE_DATE") - pl.col("_many_date_min")).dt.total_days().abs() <= td)
            & ((pl.col("VALUE_DATE") - pl.col("_many_date_max")).dt.total_days().abs() <= td)
        )

    # Quality: simple amount-based score. WITHIN scoring requires pairwise deltas so we compute
    # it directly here rather than calling score_expr (which assumes pairwise join shape).
    if agg_attr.operator == "EQUALS":
        quality_expr = pl.lit(100).cast(pl.Int64)
    else:
        delta = (pl.col(one_amt_bare).cast(pl.Float64) - pl.col("_many_sum").cast(pl.Float64)).abs()
        tol_eff = pl.max_horizontal(
            pl.lit(tol_abs),
            pl.col(one_amt_bare).cast(pl.Float64).abs() * tol_pct,
        ).clip(lower_bound=1e-12)
        quality_expr = (pl.lit(100.0) - (delta / tol_eff * 20.0)).clip(80.0, 100.0).round(0).cast(pl.Int64)
    candidates = candidates.with_columns(quality_expr.alias("_quality"))
    candidates = candidates.filter(pl.col("_quality") >= pass_config.pqr.quality)

    # Left-uniqueness: each one-side row can join at most one bucket; pick highest quality.
    # Secondary sort by _one_idx makes tie-breaks deterministic.
    candidates = (
        candidates.sort(["_quality", "_one_idx"], descending=[True, False])
        .unique(subset=["_one_idx"], keep="first", maintain_order=True)
    )

    # Collect, allocate MGIDs, explode
    candidates_df = candidates.select([
        "_one_idx", "_many_idx_list", "_quality",
    ]).collect()

    if candidates_df.is_empty():
        return _empty_result_lf()

    n = candidates_df.height
    mgids = mgid.allocate_batch(n)
    candidates_df = candidates_df.with_columns(pl.Series("MATCHING_ID", mgids))

    one_rows = candidates_df.select(
        pl.col("_one_idx").alias("_row_idx"),
        pl.col("MATCHING_ID"),
        pl.lit(pass_config.id).alias("MATCHED_BY_PASS"),
        pl.col("_quality").alias("MATCH_QUALITY"),
    )
    many_rows = candidates_df.select([
        pl.col("_many_idx_list"),
        pl.col("MATCHING_ID"),
        pl.lit(pass_config.id).alias("MATCHED_BY_PASS"),
        pl.col("_quality").alias("MATCH_QUALITY"),
    ]).explode("_many_idx_list").rename({"_many_idx_list": "_row_idx"})

    return pl.concat([one_rows, many_rows]).lazy()


def _find_agg_attr(pass_config: MatchPass, many_is_side: str) -> AttributeMatch | None:
    for a in pass_config.attributes_to_match:
        if a.aggregation and a.aggregation.side == many_is_side and a.aggregation.function == "SUM":
            return a
    return None


def _empty_result_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            "_row_idx": pl.UInt64,
            "MATCHING_ID": pl.Utf8,
            "MATCHED_BY_PASS": pl.Utf8,
            "MATCH_QUALITY": pl.Int64,
        }
    ).lazy()
