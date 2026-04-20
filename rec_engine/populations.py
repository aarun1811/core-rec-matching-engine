"""Evaluate population filters against a LazyFrame."""

from __future__ import annotations

import polars as pl

from rec_engine.types import Population, PopulationFilter


def apply(lf: pl.LazyFrame, pop: Population) -> pl.LazyFrame:
    """Return `lf` filtered down to rows satisfying all filters in `pop` (AND)."""
    for f in pop.filters:
        lf = lf.filter(_filter_expr(f))
    return lf


def _filter_expr(f: PopulationFilter) -> pl.Expr:
    col = pl.col(f.attribute)
    op = f.operator
    v = f.value
    match op:
        case "=":   return col == v
        case "!=":  return col != v
        case ">":   return col > v
        case "<":   return col < v
        case ">=":  return col >= v
        case "<=":  return col <= v
        case "IN":
            if not isinstance(v, list):
                raise ValueError(f"IN operator requires a list value, got {type(v).__name__}")
            return col.is_in(v)
        case "LIKE":
            if not isinstance(v, str):
                raise ValueError("LIKE operator requires a string pattern")
            regex = _like_to_regex(v)
            return col.cast(pl.Utf8).str.contains(regex)
        case _:
            raise ValueError(f"Unsupported population operator: {op!r}")


def _like_to_regex(pattern: str) -> str:
    """Convert a SQL LIKE pattern (% _) to a Polars-compatible regex."""
    out = ["^"]
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        elif ch in r".\+*?()[]{}|":
            out.append("\\" + ch)
        else:
            out.append(ch)
    out.append("$")
    return "".join(out)
