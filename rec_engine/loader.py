"""Load input CSV as a Polars LazyFrame with canonical + extra types, and a _row_idx column."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from rec_engine.schema import build_dtype_map, check_required_present
from rec_engine.types import Schema

# Polars 0.20.31 `scan_csv` cannot parse these dtypes directly; read as Utf8
# and cast after the scan.
_UNSUPPORTED_IN_SCAN: tuple[type, ...] = (pl.Decimal,)


def _needs_post_cast(dtype: pl.DataType) -> bool:
    return isinstance(dtype, _UNSUPPORTED_IN_SCAN)


def load(input_path: str | Path, schema: Schema) -> pl.LazyFrame:
    """
    Lazily scan the CSV with explicit per-column dtypes.
    Adds an internal `_row_idx` (UInt64) column for row identity.
    """
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    # Peek the header to know which columns are present.
    header_columns = _read_header(path)
    check_required_present(header_columns)
    dtypes = build_dtype_map(schema, header_columns)

    # Split out dtypes that scan_csv can't parse directly; read them as Utf8
    # and cast after the scan.
    scan_dtypes: dict[str, pl.DataType] = {}
    post_casts: dict[str, pl.DataType] = {}
    for col, dt in dtypes.items():
        if _needs_post_cast(dt):
            scan_dtypes[col] = pl.Utf8
            post_casts[col] = dt
        else:
            scan_dtypes[col] = dt

    lf = pl.scan_csv(
        path,
        schema_overrides=scan_dtypes,
        try_parse_dates=False,  # we rely on explicit dtypes
    )
    if post_casts:
        lf = lf.with_columns(
            [pl.col(c).cast(dt) for c, dt in post_casts.items()]
        )
    # Add _row_idx as the first column
    lf = lf.with_row_index("_row_idx")
    return lf


def _read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as f:
        header = f.readline().rstrip("\r\n")
    return [c.strip() for c in header.split(",")]
