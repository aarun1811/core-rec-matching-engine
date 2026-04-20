"""Typed dataclasses representing the parsed JSON config."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

MatchType = Literal["ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY"]
PassStatus = Literal["ACTIVE", "INACTIVE"]
Operator = Literal["EQUALS", "WITHIN", "CONTAINS"]
PopOperator = Literal["=", "!=", ">", "<", ">=", "<=", "IN", "LIKE"]
AggFunction = Literal["SUM", "COUNT", "MIN", "MAX", "AVG"]
AggSide = Literal["LEFT", "RIGHT"]
ExtraColumnType = Literal["string", "integer", "decimal", "date", "datetime", "boolean"]


@dataclass(frozen=True)
class ExtraColumn:
    name: str
    type: ExtraColumnType
    format: str | None = None           # for date/datetime
    precision: int | None = None        # for decimal
    scale: int | None = None            # for decimal


@dataclass(frozen=True)
class Schema:
    extra_columns: dict[str, ExtraColumn] = field(default_factory=dict)


@dataclass(frozen=True)
class PopulationFilter:
    attribute: str
    operator: PopOperator
    value: Any                          # scalar, list (IN), or string (LIKE)


@dataclass(frozen=True)
class Population:
    name: str
    filters: tuple[PopulationFilter, ...]


@dataclass(frozen=True)
class PopulationRule:
    left: Population
    right: Population


@dataclass(frozen=True)
class Aggregation:
    side: AggSide
    function: AggFunction


@dataclass(frozen=True)
class AttributeMatch:
    left_attribute: str
    right_attribute: str
    operator: Operator
    mandatory: bool
    tolerance_days: int | None = None
    tolerance_amount: float | None = None
    tolerance_percent: float | None = None
    aggregation: Aggregation | None = None


@dataclass(frozen=True)
class PQR:
    priority: int
    quality: int
    rule_order: int


@dataclass(frozen=True)
class MatchPass:
    id: str
    name: str
    match_type: MatchType
    status: PassStatus
    population_rule: PopulationRule
    pqr: PQR
    attributes_to_match: tuple[AttributeMatch, ...]


@dataclass(frozen=True)
class Config:
    set_id: str
    cycle_date: str | None
    schema: Schema
    match_passes: tuple[MatchPass, ...]
