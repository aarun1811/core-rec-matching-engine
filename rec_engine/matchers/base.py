"""Shared primitives used by all matcher implementations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from rec_engine.expressions import SUBSTRING_RE
from rec_engine.types import AttributeMatch, MatchPass


IMPLICIT_HARD_KEYS: tuple[str, ...] = ("BANK_ACCOUNT",)


def attr_bare_name(expr: str) -> str:
    m = SUBSTRING_RE.match(expr)
    return m.group(1) if m else expr


def hard_key_columns(pass_config: MatchPass) -> list[str]:
    """
    Columns used for hash-bucketing in this pass:
      - implicit: BANK_ACCOUNT
      - explicit: every EQUALS attribute with aggregation=None AND no SUBSTRING transform
    """
    cols: list[str] = list(IMPLICIT_HARD_KEYS)
    for a in pass_config.attributes_to_match:
        if a.operator != "EQUALS" or a.aggregation is not None:
            continue
        bare_left = attr_bare_name(a.left_attribute)
        bare_right = attr_bare_name(a.right_attribute)
        # Only treat as hard-key if both sides are bare columns AND the columns match.
        if bare_left == a.left_attribute and bare_right == a.right_attribute and bare_left == bare_right:
            if bare_left not in cols:
                cols.append(bare_left)
    return cols


def tolerance_attrs(pass_config: MatchPass) -> list[AttributeMatch]:
    return [a for a in pass_config.attributes_to_match if a.operator == "WITHIN"]


def contains_attrs(pass_config: MatchPass) -> list[AttributeMatch]:
    return [a for a in pass_config.attributes_to_match if a.operator == "CONTAINS"]


def soft_attrs(pass_config: MatchPass) -> list[AttributeMatch]:
    return [a for a in pass_config.attributes_to_match if not a.mandatory]


@dataclass
class MGIDAllocator:
    """
    Per-run allocator. Produces MGID_<setId>_<YYYYMMDD>_<6-digit seq>.
    Sequence is global across passes in a single run.
    """
    set_id: str
    cycle_date: date
    _seq: int = 0

    def next(self) -> str:
        self._seq += 1
        return f"MGID_{self.set_id}_{self.cycle_date.strftime('%Y%m%d')}_{self._seq:06d}"

    def allocate_batch(self, n: int) -> list[str]:
        result = [self.next() for _ in range(n)]
        return result
