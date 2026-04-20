"""Build a Polars expression that scores each candidate match pair 0-100."""

from __future__ import annotations

import re

import polars as pl

from rec_engine.expressions import compile_attr_expr
from rec_engine.matchers.base import attr_bare_name
from rec_engine.types import AttributeMatch, MatchPass

MANDATORY_WEIGHT = 10
OPTIONAL_WEIGHT = 3
WITHIN_FLOOR = 80.0

_SUBSTRING_RE_WITH_SUFFIX = re.compile(
    r"^SUBSTRING\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$"
)


def score_expr(
    pass_config: MatchPass,
    left_suffix: str = "",
    right_suffix: str = "_R",
    date_cols: frozenset[str] = frozenset({"VALUE_DATE"}),
) -> pl.Expr:
    """
    Build an expression that evaluates to an integer quality score for each row of
    a join-candidate LazyFrame.
    `right_suffix` is the suffix Polars applied to right-side columns during the join.
    """
    numer_terms: list[pl.Expr] = []
    denom = 0

    for a in pass_config.attributes_to_match:
        weight = MANDATORY_WEIGHT if a.mandatory else OPTIONAL_WEIGHT
        denom += weight

        le = compile_attr_expr(_suffixed(a.left_attribute,  left_suffix))
        re_ = compile_attr_expr(_suffixed(a.right_attribute, right_suffix))

        if a.operator == "EQUALS":
            per_attr = pl.when(le == re_).then(pl.lit(100.0)).otherwise(pl.lit(0.0))
        elif a.operator == "CONTAINS":
            # str.contains accepts an Expr pattern; literal=True disables regex.
            per_attr = pl.when(
                le.cast(pl.Utf8).str.contains(re_.cast(pl.Utf8), literal=True)
            ).then(pl.lit(100.0)).otherwise(pl.lit(0.0))
        elif a.operator == "WITHIN":
            bare_left = attr_bare_name(a.left_attribute)
            if bare_left in date_cols and a.tolerance_days is not None:
                # Date subtraction yields a Duration; convert to days.
                delta = (le - re_).dt.total_days().abs()
                tol = a.tolerance_days
                partial = pl.lit(100.0) - (delta.cast(pl.Float64) / tol * 20.0)
                per_attr = pl.when(delta <= tol).then(
                    partial.clip(WITHIN_FLOOR, 100.0)
                ).otherwise(pl.lit(0.0))
            else:
                delta = (le.cast(pl.Float64) - re_.cast(pl.Float64)).abs()
                tol_abs = a.tolerance_amount or 0.0
                tol_pct = a.tolerance_percent or 0.0
                tol_eff = pl.max_horizontal(
                    pl.lit(tol_abs),
                    (le.cast(pl.Float64).abs() * tol_pct),
                )
                partial = pl.lit(100.0) - (delta / tol_eff.clip(lower_bound=1e-12) * 20.0)
                per_attr = pl.when(delta <= tol_eff).then(
                    partial.clip(WITHIN_FLOOR, 100.0)
                ).otherwise(pl.lit(0.0))
        else:
            raise ValueError(f"Unsupported operator in scorer: {a.operator}")

        numer_terms.append(per_attr * weight)

    total = numer_terms[0]
    for t in numer_terms[1:]:
        total = total + t
    return (total / denom).round(0).cast(pl.Int64)


def _suffixed(expr_str: str, suffix: str) -> str:
    """Append `suffix` to the bare column inside a possibly-SUBSTRING expression."""
    if not suffix:
        return expr_str
    m = _SUBSTRING_RE_WITH_SUFFIX.match(expr_str)
    if m:
        col, s, ln = m.group(1), m.group(2), m.group(3)
        return f"SUBSTRING({col}{suffix}, {s}, {ln})"
    return expr_str + suffix
