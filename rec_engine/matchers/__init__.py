"""Cardinality-specific match algorithms."""

from __future__ import annotations

import polars as pl

from rec_engine.matchers.base import MGIDAllocator
from rec_engine.matchers import one_to_one  # populated in later tasks
from rec_engine.types import MatchPass


def dispatch(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
) -> pl.LazyFrame:
    if pass_config.match_type == "ONE_TO_ONE":
        return one_to_one.match(left_lf, right_lf, pass_config, mgid)
    if pass_config.match_type == "ONE_TO_MANY":
        from rec_engine.matchers import one_to_many  # noqa: lazy to avoid circular imports
        return one_to_many.match(left_lf, right_lf, pass_config, mgid)
    if pass_config.match_type == "MANY_TO_ONE":
        from rec_engine.matchers import many_to_one
        return many_to_one.match(left_lf, right_lf, pass_config, mgid)
    if pass_config.match_type == "MANY_TO_MANY":
        from rec_engine.matchers import many_to_many
        return many_to_many.match(left_lf, right_lf, pass_config, mgid)
    raise ValueError(f"Unknown match_type: {pass_config.match_type}")
