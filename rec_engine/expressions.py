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
      - SUBSTRING(col, start, length): 1-based start, length chars
    """
    m = SUBSTRING_RE.match(expr_str)
    if m:
        col, start, length = m.group(1), int(m.group(2)), int(m.group(3))
        # Polars str.slice is 0-based, length is inclusive
        return pl.col(col).cast(pl.Utf8).str.slice(start - 1, length)
    return pl.col(expr_str)
