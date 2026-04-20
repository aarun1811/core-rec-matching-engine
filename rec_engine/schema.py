"""Canonical + extra column schema; produce Polars dtype mapping for scan_csv."""

from __future__ import annotations

import polars as pl

from rec_engine.types import ExtraColumn, Schema

CANONICAL_DTYPES: dict[str, pl.DataType] = {
    "LS_TYPE":      pl.Utf8,
    "DR_CR_IND":    pl.Utf8,
    "STATUS":       pl.Utf8,
    "AMOUNT":       pl.Decimal(18, 4),
    "CURRENCY":     pl.Utf8,
    "VALUE_DATE":   pl.Date,
    "BANK_ACCOUNT": pl.Utf8,
    "REFERENCE":    pl.Utf8,
}

CANONICAL_REQUIRED: frozenset[str] = frozenset(CANONICAL_DTYPES.keys())

OUTPUT_COLUMNS: tuple[str, ...] = (
    "STATUS", "MATCH_DATE", "MATCHING_ID", "MATCHED_BY_PASS", "MATCH_QUALITY",
)


def extra_dtype(col: ExtraColumn) -> pl.DataType:
    match col.type:
        case "string":   return pl.Utf8
        case "integer":  return pl.Int64
        case "decimal":  return pl.Decimal(col.precision or 18, col.scale or 4)
        case "date":     return pl.Date
        case "datetime": return pl.Datetime
        case "boolean":  return pl.Boolean
        case _:          raise ValueError(f"Unknown extra column type: {col.type!r}")


def build_dtype_map(schema: Schema, header_columns: list[str]) -> dict[str, pl.DataType]:
    """Map each header column to a Polars dtype; unknown columns default to Utf8."""
    dtypes: dict[str, pl.DataType] = {}
    for col in header_columns:
        if col in CANONICAL_DTYPES:
            dtypes[col] = CANONICAL_DTYPES[col]
        elif col in schema.extra_columns:
            dtypes[col] = extra_dtype(schema.extra_columns[col])
        else:
            dtypes[col] = pl.Utf8
    return dtypes


def check_required_present(header_columns: list[str]) -> None:
    missing = CANONICAL_REQUIRED - set(header_columns)
    if missing:
        raise ValueError(
            f"Input CSV missing required canonical columns: {sorted(missing)}"
        )
