"""Load + validate the match-pass config JSON."""

from __future__ import annotations

import json
import re
from pathlib import Path

from rec_engine.types import (
    Aggregation,
    AttributeMatch,
    Config,
    ExtraColumn,
    MatchPass,
    PQR,
    Population,
    PopulationFilter,
    PopulationRule,
    Schema,
)

CANONICAL_COLUMNS = {
    "LS_TYPE", "DR_CR_IND", "STATUS", "AMOUNT", "CURRENCY",
    "VALUE_DATE", "BANK_ACCOUNT", "REFERENCE",
}
SET_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
VALID_MATCH_TYPES = {"ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY"}
VALID_POP_OPS = {"=", "!=", ">", "<", ">=", "<=", "IN", "LIKE"}
VALID_ATTR_OPS = {"EQUALS", "WITHIN", "CONTAINS"}
VALID_EXTRA_TYPES = {"string", "integer", "decimal", "date", "datetime", "boolean"}


class ConfigError(Exception):
    """Raised on any config validation failure."""


def load(path: str | Path) -> Config:
    """Parse JSON from `path` and return a validated Config."""
    raw = json.loads(Path(path).read_text())
    return _parse_config(raw)


def _parse_config(raw: dict) -> Config:
    set_id = raw.get("setId")
    if not isinstance(set_id, str) or not SET_ID_RE.match(set_id):
        raise ConfigError(f"setId must match {SET_ID_RE.pattern!r}, got: {set_id!r}")

    cycle_date = raw.get("cycleDate")
    if cycle_date is not None and not isinstance(cycle_date, str):
        raise ConfigError("cycleDate must be a string (YYYY-MM-DD) or omitted")

    schema = _parse_schema(raw.get("schema", {}))

    passes_raw = raw.get("matchPasses")
    if not isinstance(passes_raw, list) or not passes_raw:
        raise ConfigError("matchPasses must be a non-empty array")
    passes = tuple(_parse_pass(p, schema) for p in passes_raw)

    _cross_validate_passes(passes)

    return Config(
        set_id=set_id,
        cycle_date=cycle_date,
        schema=schema,
        match_passes=passes,
    )


def _parse_schema(raw: dict) -> Schema:
    extras_raw = raw.get("extraColumns", {})
    if not isinstance(extras_raw, dict):
        raise ConfigError("schema.extraColumns must be an object")

    extras: dict[str, ExtraColumn] = {}
    for name, spec in extras_raw.items():
        if not isinstance(spec, dict) or "type" not in spec:
            raise ConfigError(f"extraColumn '{name}' must be an object with a 'type' field")
        t = spec["type"]
        if t not in VALID_EXTRA_TYPES:
            raise ConfigError(f"extraColumn '{name}' has unsupported type: {t!r}")
        extras[name] = ExtraColumn(
            name=name,
            type=t,
            format=spec.get("format"),
            precision=spec.get("precision"),
            scale=spec.get("scale"),
        )
    return Schema(extra_columns=extras)


def _parse_pass(raw: dict, schema: Schema) -> MatchPass:
    pid = raw.get("matchPassId")
    if not isinstance(pid, str) or not pid:
        raise ConfigError("matchPassId must be a non-empty string")

    name = raw.get("matchPassName", pid)
    mtype = raw.get("matchType")
    if mtype not in VALID_MATCH_TYPES:
        raise ConfigError(f"Pass {pid}: invalid matchType {mtype!r}")

    status = raw.get("status", "ACTIVE")
    if status not in ("ACTIVE", "INACTIVE"):
        raise ConfigError(f"Pass {pid}: status must be ACTIVE or INACTIVE")

    pop_rule = _parse_population_rule(raw.get("populationRule", {}), schema, pid)
    pqr = _parse_pqr(raw.get("pqr", {}), pid)
    attrs = tuple(_parse_attr(a, schema, pid, mtype) for a in raw.get("attributesToMatch", []))

    if not attrs:
        raise ConfigError(f"Pass {pid}: attributesToMatch must be non-empty")

    return MatchPass(
        id=pid,
        name=name,
        match_type=mtype,
        status=status,
        population_rule=pop_rule,
        pqr=pqr,
        attributes_to_match=attrs,
    )


def _parse_population_rule(raw: dict, schema: Schema, pid: str) -> PopulationRule:
    left = _parse_population(raw.get("leftPopulation", {}), schema, pid, "left")
    right = _parse_population(raw.get("rightPopulation", {}), schema, pid, "right")
    return PopulationRule(left=left, right=right)


def _parse_population(raw: dict, schema: Schema, pid: str, side: str) -> Population:
    name = raw.get("populationName", f"{pid}_{side}")
    filters = tuple(_parse_filter(f, schema, pid, side) for f in raw.get("filters", []))
    return Population(name=name, filters=filters)


def _parse_filter(raw: dict, schema: Schema, pid: str, side: str) -> PopulationFilter:
    attr = raw.get("attribute")
    op = raw.get("operator")
    if attr is None or op is None or "value" not in raw:
        raise ConfigError(f"Pass {pid} {side} filter: attribute/operator/value required")
    if op not in VALID_POP_OPS:
        raise ConfigError(f"Pass {pid} {side} filter: invalid operator {op!r}")
    if attr not in CANONICAL_COLUMNS and attr not in schema.extra_columns:
        raise ConfigError(f"Pass {pid} {side} filter: unknown attribute {attr!r}")
    return PopulationFilter(attribute=attr, operator=op, value=raw["value"])


def _parse_pqr(raw: dict, pid: str) -> PQR:
    try:
        return PQR(
            priority=int(raw.get("priority", 0)),
            quality=int(raw.get("quality", 0)),
            rule_order=int(raw["ruleOrder"]),
        )
    except (KeyError, ValueError) as e:
        raise ConfigError(f"Pass {pid} pqr: {e}") from e


SUBSTRING_RE = re.compile(r"^SUBSTRING\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$")


def _attr_bare_name(expr: str) -> str:
    m = SUBSTRING_RE.match(expr)
    return m.group(1) if m else expr


def _parse_attr(raw: dict, schema: Schema, pid: str, mtype: str) -> AttributeMatch:
    la = raw.get("leftAttribute")
    ra = raw.get("rightAttribute")
    op = raw.get("operator")
    if not la or not ra or not op:
        raise ConfigError(f"Pass {pid} attr: leftAttribute/rightAttribute/operator required")
    if op not in VALID_ATTR_OPS:
        raise ConfigError(f"Pass {pid} attr: invalid operator {op!r}")

    for e in (la, ra):
        bare = _attr_bare_name(e)
        if bare not in CANONICAL_COLUMNS and bare not in schema.extra_columns:
            raise ConfigError(f"Pass {pid} attr: unknown column {bare!r} in expression {e!r}")

    tol_days = raw.get("toleranceDays")
    tol_amt = raw.get("toleranceAmount")
    tol_pct = raw.get("tolerancePercent")

    if op == "WITHIN":
        bare_left = _attr_bare_name(la)
        is_date = bare_left in ("VALUE_DATE",) or (
            bare_left in schema.extra_columns
            and schema.extra_columns[bare_left].type in ("date", "datetime")
        )
        if is_date:
            if tol_days is None:
                raise ConfigError(f"Pass {pid} attr {la}: WITHIN on date requires toleranceDays")
        else:
            if tol_amt is None and tol_pct is None:
                raise ConfigError(
                    f"Pass {pid} attr {la}: WITHIN on numeric requires toleranceAmount or tolerancePercent"
                )

    agg_raw = raw.get("aggregation")
    agg: Aggregation | None = None
    if agg_raw is not None:
        side = agg_raw.get("side")
        fn = agg_raw.get("function")
        if side not in ("LEFT", "RIGHT") or fn not in ("SUM", "COUNT", "MIN", "MAX", "AVG"):
            raise ConfigError(f"Pass {pid} attr {la}: invalid aggregation {agg_raw!r}")
        agg = Aggregation(side=side, function=fn)

    if mtype in ("ONE_TO_MANY", "MANY_TO_ONE"):
        bare_left = _attr_bare_name(la)
        is_numeric = bare_left == "AMOUNT" or (
            bare_left in schema.extra_columns
            and schema.extra_columns[bare_left].type in ("decimal", "integer")
        )
        if op in ("EQUALS", "WITHIN") and is_numeric and agg is None:
            raise ConfigError(
                f"Pass {pid} attr {la}: non-1:1 pass with numeric {op} requires aggregation block"
            )
        if op in ("EQUALS", "WITHIN") and is_numeric and agg is not None and agg.function != "SUM":
            # POC restriction: only SUM aggregation is implemented for 1:N / N:1.
            # COUNT / MIN / MAX / AVG are allowed in the type enum but not yet wired.
            raise ConfigError(
                f"Pass {pid} attr {la}: only aggregation.function=SUM is supported for 1:N / N:1"
            )

    mandatory = bool(raw.get("mandatory", False))
    return AttributeMatch(
        left_attribute=la,
        right_attribute=ra,
        operator=op,
        mandatory=mandatory,
        tolerance_days=tol_days,
        tolerance_amount=tol_amt,
        tolerance_percent=tol_pct,
        aggregation=agg,
    )


def _cross_validate_passes(passes: tuple[MatchPass, ...]) -> None:
    ids = [p.id for p in passes]
    if len(set(ids)) != len(ids):
        raise ConfigError("matchPassId values must be unique")
    active_orders = [p.pqr.rule_order for p in passes if p.status == "ACTIVE"]
    if len(set(active_orders)) != len(active_orders):
        raise ConfigError("ruleOrder must be unique among ACTIVE passes")
