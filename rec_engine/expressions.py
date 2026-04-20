"""Compile attribute expressions (bare columns and SUBSTRING) into Polars expressions."""

from __future__ import annotations

import re

import polars as pl

SUBSTRING_RE = re.compile(r"^SUBSTRING\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$")


def compile_attr_expr(expr_str: str) -> pl.Expr:
    """
    Turn an attribute expression into a Polars expression.
    Supports:
      - bare column name: 'AMOUNT'
      - SUBSTRING(col, start, length): 1-based start (>= 1), length chars (>= 0)
    """
    m = SUBSTRING_RE.match(expr_str)
    if m:
        col, start, length = m.group(1), int(m.group(2)), int(m.group(3))
        if start < 1:
            raise ValueError(
                f"SUBSTRING start must be >= 1 (1-based, SQL convention), "
                f"got {start} in {expr_str!r}"
            )
        # Polars str.slice takes a 0-based offset and a length (count).
        return pl.col(col).cast(pl.Utf8).str.slice(start - 1, length)
    return pl.col(expr_str)
