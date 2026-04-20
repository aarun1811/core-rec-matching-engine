"""N:M matcher: bucketed subset-sum via brute-force enumeration within capped buckets."""

from __future__ import annotations

from itertools import combinations

import polars as pl

from rec_engine.matchers.base import MGIDAllocator, hard_key_columns
from rec_engine.types import AttributeMatch, MatchPass

MAX_N2M_BUCKET_SIDE = 10  # per side. Enumeration is O(2^L × 2^R); 10×10 = ~1M pairs
                          # per bucket is tractable. Raising this materially slows N:M.


def match(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
) -> pl.LazyFrame:
    hkeys = hard_key_columns(pass_config)

    # Identify the amount attribute (there must be exactly one numeric WITHIN/EQUALS)
    amt_attr = _find_amount_attr(pass_config)
    if amt_attr is None:
        return _empty_result_lf()

    # Identify date WITHIN attribute if present
    date_attr = next(
        (a for a in pass_config.attributes_to_match
         if a.operator == "WITHIN" and a.left_attribute == "VALUE_DATE"),
        None,
    )

    # Collect both sides bucketed by hard keys. Bucket rows are small.
    left_groups = (
        left_lf.group_by(hkeys).agg([
            pl.col("_row_idx").alias("idxs"),
            pl.col("AMOUNT").alias("amts"),
            pl.col("VALUE_DATE").alias("dates"),
            pl.len().alias("n"),
        ]).filter(
            (pl.col("n") >= 2) & (pl.col("n") <= MAX_N2M_BUCKET_SIDE)
        ).collect()
    )
    right_groups = (
        right_lf.group_by(hkeys).agg([
            pl.col("_row_idx").alias("idxs"),
            pl.col("AMOUNT").alias("amts"),
            pl.col("VALUE_DATE").alias("dates"),
            pl.len().alias("n"),
        ]).filter(
            (pl.col("n") >= 2) & (pl.col("n") <= MAX_N2M_BUCKET_SIDE)
        ).collect()
    )

    if left_groups.is_empty() or right_groups.is_empty():
        return _empty_result_lf()

    # Align buckets on hard keys via inner join (still small — one row per bucket)
    joined = left_groups.join(
        right_groups, on=hkeys, how="inner", suffix="_R",
    )
    if joined.is_empty():
        return _empty_result_lf()

    tol_abs = amt_attr.tolerance_amount or 0.0
    tol_pct = amt_attr.tolerance_percent or 0.0
    td = date_attr.tolerance_days if date_attr else None

    matched_left_idx: list[list[int]] = []
    matched_right_idx: list[list[int]] = []
    qualities: list[int] = []

    for row in joined.iter_rows(named=True):
        l_idxs = list(row["idxs"])
        l_amts = list(row["amts"])
        l_dates = list(row["dates"])
        r_idxs = list(row["idxs_R"])
        r_amts = list(row["amts_R"])
        r_dates = list(row["dates_R"])

        best = _best_subset_pair(
            l_idxs, l_amts, l_dates,
            r_idxs, r_amts, r_dates,
            tol_abs=tol_abs, tol_pct=tol_pct, tol_days=td,
        )
        if best is None:
            continue

        li, ri, quality = best
        matched_left_idx.append(li)
        matched_right_idx.append(ri)
        qualities.append(quality)

    if not matched_left_idx:
        return _empty_result_lf()

    mgids = mgid.allocate_batch(len(matched_left_idx))

    rows: list[dict] = []
    for mg, li_group, ri_group, q in zip(mgids, matched_left_idx, matched_right_idx, qualities):
        for idx in li_group:
            rows.append({
                "_row_idx": idx, "MATCHING_ID": mg,
                "MATCHED_BY_PASS": pass_config.id, "MATCH_QUALITY": q,
            })
        for idx in ri_group:
            rows.append({
                "_row_idx": idx, "MATCHING_ID": mg,
                "MATCHED_BY_PASS": pass_config.id, "MATCH_QUALITY": q,
            })

    return pl.DataFrame(
        rows,
        schema={
            "_row_idx": pl.UInt64,
            "MATCHING_ID": pl.Utf8,
            "MATCHED_BY_PASS": pl.Utf8,
            "MATCH_QUALITY": pl.Int64,
        },
    ).lazy()


def _best_subset_pair(
    l_idxs: list[int], l_amts: list, l_dates: list,
    r_idxs: list[int], r_amts: list, r_dates: list,
    tol_abs: float, tol_pct: float, tol_days: int | None,
) -> tuple[list[int], list[int], int] | None:
    """
    Brute-force subset-sum: enumerate all size->=2 subsets on each side, find
    pair with SUM(L) ~= SUM(R) within tolerance and dates all-within.
    Prefer higher quality (smaller total rows as tiebreaker).
    Returns (l_subset_idxs, r_subset_idxs, quality) or None.
    Safe only because MAX_N2M_BUCKET_SIDE caps the enumeration size.
    """
    l_subsets = _enumerate_subsets(l_amts, l_dates, min_size=2)
    r_subsets = _enumerate_subsets(r_amts, r_dates, min_size=2)
    if not l_subsets or not r_subsets:
        return None

    # best: (quality, neg_total_rows, li_mask, ri_mask) — tuple comparison prefers
    # higher quality, then fewer rows, then lower li_mask, then lower ri_mask (stable).
    best: tuple[int, int, int, int] | None = None

    for li_mask, l_sum, l_dmin, l_dmax in l_subsets:
        tol_eff = max(tol_abs, abs(float(l_sum)) * tol_pct)
        for ri_mask, r_sum, r_dmin, r_dmax in r_subsets:
            delta = abs(float(l_sum) - float(r_sum))
            if delta > tol_eff:
                continue
            if tol_days is not None:
                # Correct "all-within" condition: every date in the union of both
                # subsets must be within tol_days of every other date. Equivalent to:
                #   max(all dates) - min(all dates) <= tol_days
                full_min = min(l_dmin, r_dmin)
                full_max = max(l_dmax, r_dmax)
                if (full_max - full_min).days > tol_days:
                    continue

            if tol_eff <= 0:
                quality = 100 if delta == 0 else 0
            else:
                quality = max(80, int(round(100 - (delta / tol_eff * 20))))

            total_rows = bin(li_mask).count("1") + bin(ri_mask).count("1")
            candidate = (quality, -total_rows, li_mask, ri_mask)
            if best is None or candidate > best:
                best = candidate

    if best is None:
        return None

    quality, _, li_mask, ri_mask = best
    li_out = [l_idxs[i] for i in range(len(l_idxs)) if li_mask & (1 << i)]
    ri_out = [r_idxs[i] for i in range(len(r_idxs)) if ri_mask & (1 << i)]
    return li_out, ri_out, quality


def _enumerate_subsets(amts, dates, min_size: int) -> list[tuple[int, float, object, object]]:
    """
    Return list of (bitmask, sum_amount, min_date, max_date) for each non-empty subset
    with size >= min_size.
    """
    n = len(amts)
    if n < min_size or n > MAX_N2M_BUCKET_SIDE:
        return []
    out: list[tuple[int, float, object, object]] = []
    for size in range(min_size, n + 1):
        for combo in combinations(range(n), size):
            mask = 0
            s = 0.0
            dmin = dates[combo[0]]
            dmax = dates[combo[0]]
            for i in combo:
                mask |= (1 << i)
                s += float(amts[i])
                if dates[i] < dmin: dmin = dates[i]
                if dates[i] > dmax: dmax = dates[i]
            out.append((mask, s, dmin, dmax))
    return out


def _find_amount_attr(pass_config: MatchPass) -> AttributeMatch | None:
    for a in pass_config.attributes_to_match:
        if a.left_attribute == "AMOUNT" and a.operator in ("EQUALS", "WITHIN"):
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
