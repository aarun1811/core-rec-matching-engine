"""Minimal filter DSL over output CSV using lazy Polars scans."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl

# Maps short query-param names to actual output CSV column names.
VALID_FILTERS: dict[str, str] = {
    "matched_by_pass": "MATCHED_BY_PASS",
    "currency": "CURRENCY",
    "bank_account": "BANK_ACCOUNT",
}

MAX_LIMIT = 10000


def page_output(
    csv_path: Path,
    status: Literal["MATCHED", "UNMATCHED"],
    filters: dict[str, str],
    limit: int,
    offset: int,
) -> dict:
    """Lazy-scan the output CSV, filter by STATUS + optional equality filters,
    and return {total, limit, offset, rows}.
    """
    if limit < 0:
        limit = 0
    if limit > MAX_LIMIT:
        limit = MAX_LIMIT
    if offset < 0:
        offset = 0

    # infer_schema_length=0 → read every column as Utf8. Keeps JSON serialization
    # predictable (AMOUNT already Utf8 at write time, dates string-formatted, etc.)
    lf = pl.scan_csv(str(csv_path), infer_schema_length=0)
    lf = lf.filter(pl.col("STATUS") == status)

    for key, value in filters.items():
        col = VALID_FILTERS.get(key)
        if col is None:
            continue
        lf = lf.filter(pl.col(col) == value)

    total = lf.select(pl.len()).collect().item()
    rows = lf.slice(offset, limit).collect().to_dicts()

    return {
        "total": int(total),
        "limit": limit,
        "offset": offset,
        "rows": rows,
    }
