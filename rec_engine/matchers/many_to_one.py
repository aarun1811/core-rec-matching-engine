"""N:1 matcher: mirrors 1:N with sides swapped."""

from __future__ import annotations

import polars as pl

from rec_engine.matchers.base import MGIDAllocator
from rec_engine.matchers.one_to_many import _match_one_to_many
from rec_engine.types import MatchPass


def match(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
) -> pl.LazyFrame:
    """
    N:1: many-side is LEFT, one-side is RIGHT.
    Reuse the 1:N machinery with sides swapped.
    """
    return _match_one_to_many(
        one_lf=right_lf,
        many_lf=left_lf,
        pass_config=pass_config,
        mgid=mgid,
        one_is_left=False,
    )
