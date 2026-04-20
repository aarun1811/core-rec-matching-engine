# Reconciliation Matching Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Nostro "fresh" reconciliation matching engine in Python + Polars, modeled on SmartStream TLM, supporting 1:1, 1:N, N:1, and N:M cardinalities with configurable match passes via JSON.

**Architecture:** Modular layered package (`rec_engine/`). Every stage takes `pl.LazyFrame` and returns `pl.LazyFrame` — no `.collect()` except (a) small matched-index tracking between passes, (b) N:M per-bucket subset-sum, (c) final `sink_csv`. This keeps Polars' optimizer in control of the full pipeline and enables streaming execution at scale.

**Tech Stack:** Python 3.11+, Polars (LazyFrame end-to-end), typer for CLI, pytest for the one integration test.

**User instruction override:** Unit tests per module are deliberately skipped (POC). The only test is a single end-to-end integration test covering all four cardinalities.

**Reference spec:** `docs/superpowers/specs/2026-04-21-rec-matching-engine-design.md`

---

## File Structure

```
rec_engine/
├── __init__.py                  # version + public API
├── __main__.py                  # enables `python -m rec_engine`
├── cli.py                       # typer CLI
├── engine.py                    # orchestrator: load → passes → write
├── config.py                    # JSON config loader + validator
├── schema.py                    # canonical columns + extra column schema
├── loader.py                    # scan_csv → LazyFrame + _row_idx
├── populations.py               # filter evaluation (=, !=, IN, LIKE, etc.)
├── expressions.py               # operator/SUBSTRING → Polars expr compiler
├── scorer.py                    # quality score 0-100
├── writer.py                    # sink_csv + manifest
├── types.py                     # config dataclasses
└── matchers/
    ├── __init__.py              # dispatch by matchType
    ├── base.py                  # hard-keys, tolerance/soft attrs, MGIDAllocator
    ├── one_to_one.py
    ├── one_to_many.py
    ├── many_to_one.py
    └── many_to_many.py

tests/
├── __init__.py
├── fixtures/
│   ├── input.csv                # hand-crafted ~60 rows covering all cardinalities
│   ├── config.json              # 4 passes, one per matchType
│   └── expected_output.csv      # byte-exact expected output
└── test_engine_integration.py

sample/                          # demoable set, separate from tests
├── config.json
└── input.csv

scripts/
└── generate_synthetic.py        # scale-test data generator

pyproject.toml
README.md
.gitignore
```

**Principle:** One file = one responsibility. Matchers split by cardinality (each is its own algorithm). Config, schema, and loader are kept separate because they're conceptually independent concerns (validation vs typing vs I/O).

---

## Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `rec_engine/__init__.py`
- Create: `rec_engine/__main__.py`
- Create: `rec_engine/matchers/__init__.py` (empty for now)
- Create: `tests/__init__.py`
- Create: `tests/fixtures/` (directory)
- Create: `sample/` (directory)
- Create: `scripts/` (directory)

- [ ] **Step 1.1: Create `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "rec-engine"
version = "0.1.0"
description = "Nostro reconciliation matching engine (TLM-inspired)"
requires-python = ">=3.11"
dependencies = [
    # Pinned narrow: guards byte-exact expected_output.csv against serialization
    # changes across Polars minor versions. Widen only after regenerating fixtures.
    "polars>=0.20,<0.21",
    "typer>=0.9",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4",
]

[project.scripts]
rec-engine = "rec_engine.cli:app"

[tool.setuptools.packages.find]
where = ["."]
include = ["rec_engine*"]
```

- [ ] **Step 1.2: Create `.gitignore`**

```
__pycache__/
*.pyc
*.pyo
*.egg-info/
.venv/
venv/
.pytest_cache/
*.log
.DS_Store
build/
dist/
```

- [ ] **Step 1.3: Create `rec_engine/__init__.py`**

```python
"""Reconciliation matching engine (TLM-inspired)."""

__version__ = "0.1.0"
```

- [ ] **Step 1.4: Create `rec_engine/__main__.py`**

```python
from rec_engine.cli import app

if __name__ == "__main__":
    app()
```

- [ ] **Step 1.5: Create `rec_engine/matchers/__init__.py`** (placeholder — filled in Task 6)

```python
"""Cardinality-specific match algorithms."""
```

- [ ] **Step 1.6: Create `tests/__init__.py`** (empty file)

```python
```

- [ ] **Step 1.7: Create directories**

```bash
mkdir -p tests/fixtures sample scripts
```

- [ ] **Step 1.8: Install and verify polars works**

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python -c "import polars as pl; print(pl.__version__)"
```

Expected: prints a version number ≥ 0.20.

- [ ] **Step 1.9: Commit**

```bash
git add pyproject.toml .gitignore rec_engine/ tests/ sample/ scripts/
git commit -m "chore: project scaffold with pyproject.toml and package skeleton"
```

---

## Task 2: Config types and loader

**Files:**
- Create: `rec_engine/types.py`
- Create: `rec_engine/config.py`
- Create: `tests/fixtures/config.json`

- [ ] **Step 2.1: Create `rec_engine/types.py`** — typed config dataclasses

```python
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
```

- [ ] **Step 2.2: Create `rec_engine/config.py`** — JSON loader + validator

```python
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
```

- [ ] **Step 2.3: Create `tests/fixtures/config.json`** — covers all 4 cardinalities

```json
{
   "setId": "USD_NOSTRO_CITIBANK",
   "cycleDate": "2024-01-15",
   "schema": { "extraColumns": {} },
   "matchPasses": [
      {
         "matchPassId": "MP_O2O",
         "matchPassName": "One-to-One : Ledger Credit vs Statement Debit",
         "matchType": "ONE_TO_ONE",
         "status": "ACTIVE",
         "populationRule": {
            "leftPopulation":  { "populationName": "GL_CR",   "filters": [
               { "attribute": "LS_TYPE",   "operator": "=", "value": "GL" },
               { "attribute": "DR_CR_IND", "operator": "=", "value": "CR" },
               { "attribute": "STATUS",    "operator": "=", "value": "OPEN" }
            ]},
            "rightPopulation": { "populationName": "STMT_DR", "filters": [
               { "attribute": "LS_TYPE",   "operator": "=", "value": "STMT" },
               { "attribute": "DR_CR_IND", "operator": "=", "value": "DR" },
               { "attribute": "STATUS",    "operator": "=", "value": "OPEN" }
            ]}
         },
         "pqr": { "priority": 10, "quality": 90, "ruleOrder": 1 },
         "attributesToMatch": [
            { "leftAttribute": "AMOUNT",     "rightAttribute": "AMOUNT",     "operator": "EQUALS", "mandatory": true },
            { "leftAttribute": "CURRENCY",   "rightAttribute": "CURRENCY",   "operator": "EQUALS", "mandatory": true },
            { "leftAttribute": "VALUE_DATE", "rightAttribute": "VALUE_DATE", "operator": "WITHIN", "toleranceDays": 1, "mandatory": true },
            { "leftAttribute": "REFERENCE",  "rightAttribute": "REFERENCE",  "operator": "EQUALS", "mandatory": false }
         ]
      },
      {
         "matchPassId": "MP_O2M",
         "matchPassName": "One-to-Many : Ledger Credit vs Statement Debits",
         "matchType": "ONE_TO_MANY",
         "status": "ACTIVE",
         "populationRule": {
            "leftPopulation":  { "populationName": "GL_CR",   "filters": [
               { "attribute": "LS_TYPE",   "operator": "=", "value": "GL" },
               { "attribute": "DR_CR_IND", "operator": "=", "value": "CR" },
               { "attribute": "STATUS",    "operator": "=", "value": "OPEN" }
            ]},
            "rightPopulation": { "populationName": "STMT_DR", "filters": [
               { "attribute": "LS_TYPE",   "operator": "=", "value": "STMT" },
               { "attribute": "DR_CR_IND", "operator": "=", "value": "DR" },
               { "attribute": "STATUS",    "operator": "=", "value": "OPEN" }
            ]}
         },
         "pqr": { "priority": 20, "quality": 85, "ruleOrder": 2 },
         "attributesToMatch": [
            { "leftAttribute": "AMOUNT",     "rightAttribute": "AMOUNT",     "operator": "EQUALS", "mandatory": true,
              "aggregation": { "side": "RIGHT", "function": "SUM" } },
            { "leftAttribute": "CURRENCY",   "rightAttribute": "CURRENCY",   "operator": "EQUALS", "mandatory": true },
            { "leftAttribute": "VALUE_DATE", "rightAttribute": "VALUE_DATE", "operator": "WITHIN", "toleranceDays": 2, "mandatory": true }
         ]
      },
      {
         "matchPassId": "MP_M2O",
         "matchPassName": "Many-to-One : Ledger Credits vs Statement Debit",
         "matchType": "MANY_TO_ONE",
         "status": "ACTIVE",
         "populationRule": {
            "leftPopulation":  { "populationName": "GL_CR",   "filters": [
               { "attribute": "LS_TYPE",   "operator": "=", "value": "GL" },
               { "attribute": "DR_CR_IND", "operator": "=", "value": "CR" },
               { "attribute": "STATUS",    "operator": "=", "value": "OPEN" }
            ]},
            "rightPopulation": { "populationName": "STMT_DR", "filters": [
               { "attribute": "LS_TYPE",   "operator": "=", "value": "STMT" },
               { "attribute": "DR_CR_IND", "operator": "=", "value": "DR" },
               { "attribute": "STATUS",    "operator": "=", "value": "OPEN" }
            ]}
         },
         "pqr": { "priority": 30, "quality": 85, "ruleOrder": 3 },
         "attributesToMatch": [
            { "leftAttribute": "AMOUNT",     "rightAttribute": "AMOUNT",     "operator": "EQUALS", "mandatory": true,
              "aggregation": { "side": "LEFT", "function": "SUM" } },
            { "leftAttribute": "CURRENCY",   "rightAttribute": "CURRENCY",   "operator": "EQUALS", "mandatory": true },
            { "leftAttribute": "VALUE_DATE", "rightAttribute": "VALUE_DATE", "operator": "WITHIN", "toleranceDays": 2, "mandatory": true }
         ]
      },
      {
         "matchPassId": "MP_M2M",
         "matchPassName": "Many-to-Many : Ledger Credits vs Statement Debits",
         "matchType": "MANY_TO_MANY",
         "status": "ACTIVE",
         "populationRule": {
            "leftPopulation":  { "populationName": "GL_CR",   "filters": [
               { "attribute": "LS_TYPE",   "operator": "=", "value": "GL" },
               { "attribute": "DR_CR_IND", "operator": "=", "value": "CR" },
               { "attribute": "STATUS",    "operator": "=", "value": "OPEN" }
            ]},
            "rightPopulation": { "populationName": "STMT_DR", "filters": [
               { "attribute": "LS_TYPE",   "operator": "=", "value": "STMT" },
               { "attribute": "DR_CR_IND", "operator": "=", "value": "DR" },
               { "attribute": "STATUS",    "operator": "=", "value": "OPEN" }
            ]}
         },
         "pqr": { "priority": 40, "quality": 80, "ruleOrder": 4 },
         "attributesToMatch": [
            { "leftAttribute": "AMOUNT",     "rightAttribute": "AMOUNT",     "operator": "WITHIN", "mandatory": true,
              "toleranceAmount": 0.01,
              "aggregation": { "side": "BOTH", "function": "SUM" } },
            { "leftAttribute": "CURRENCY",   "rightAttribute": "CURRENCY",   "operator": "EQUALS", "mandatory": true },
            { "leftAttribute": "VALUE_DATE", "rightAttribute": "VALUE_DATE", "operator": "WITHIN", "toleranceDays": 3, "mandatory": true }
         ]
      }
   ]
}
```

*Note*: The MP_M2M aggregation uses `"side": "BOTH"` as a documented extension-point; the N:M matcher (Task 11) will treat missing side==BOTH as "aggregate both sides with same function." We'll handle this by loosening the Aggregation.side validation in Step 2.1 if needed, or by keeping M2M aggregation optional. **For the POC, simplify: remove the `aggregation` block from M2M entirely** — N:M matcher implicitly uses SUM on both sides. Update the fixture:

```jsonc
// In MP_M2M -> attributesToMatch -> AMOUNT entry, remove the "aggregation" key entirely:
{ "leftAttribute": "AMOUNT", "rightAttribute": "AMOUNT", "operator": "WITHIN", "mandatory": true, "toleranceAmount": 0.01 }
```

And in `_parse_attr` (Step 2.2), change the "non-1:1 numeric needs aggregation" check to **exclude MANY_TO_MANY**:

```python
if mtype in ("ONE_TO_MANY", "MANY_TO_ONE"):
    # ... existing check that requires aggregation
```

- [ ] **Step 2.4: Verify config parses**

Run:
```bash
python -c "from rec_engine.config import load; c = load('tests/fixtures/config.json'); print(c.set_id, len(c.match_passes))"
```
Expected: `USD_NOSTRO_CITIBANK 4`

- [ ] **Step 2.5: Commit**

```bash
git add rec_engine/types.py rec_engine/config.py tests/fixtures/config.json
git commit -m "feat: config types + JSON loader with cross-validation"
```

---

## Task 3: Schema and loader

**Files:**
- Create: `rec_engine/schema.py`
- Create: `rec_engine/loader.py`
- Create: `tests/fixtures/input.csv`

- [ ] **Step 3.1: Create `rec_engine/schema.py`** — build polars dtype mapping

```python
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
```

- [ ] **Step 3.2: Create `rec_engine/loader.py`** — scan_csv + _row_idx

```python
"""Load input CSV as a Polars LazyFrame with canonical + extra types, and a _row_idx column."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from rec_engine.schema import build_dtype_map, check_required_present
from rec_engine.types import Schema


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

    lf = pl.scan_csv(
        path,
        schema_overrides=dtypes,
        try_parse_dates=False,  # we rely on explicit dtypes
    )
    # Add _row_idx as the first column
    lf = lf.with_row_index("_row_idx")
    return lf


def _read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as f:
        header = f.readline().rstrip("\r\n")
    return [c.strip() for c in header.split(",")]
```

- [ ] **Step 3.3: Create `tests/fixtures/input.csv`** — hand-crafted, small, covers all cardinalities

Write this file exactly as shown (no trailing whitespace on any line, use Unix line endings):

```csv
LS_TYPE,DR_CR_IND,STATUS,AMOUNT,CURRENCY,VALUE_DATE,BANK_ACCOUNT,REFERENCE
GL,CR,OPEN,1000.0000,USD,2024-01-15,NOSTRO-USD-001,TRX001
STMT,DR,OPEN,1000.0000,USD,2024-01-15,NOSTRO-USD-001,TRX001
GL,CR,OPEN,2500.0000,USD,2024-01-15,NOSTRO-USD-001,TRX002
STMT,DR,OPEN,2500.0000,USD,2024-01-16,NOSTRO-USD-001,TRX002
GL,CR,OPEN,1500.0000,EUR,2024-01-15,NOSTRO-EUR-001,TRX003
STMT,DR,OPEN,1500.0000,EUR,2024-01-15,NOSTRO-EUR-001,TRX003
GL,CR,OPEN,500.0000,USD,2024-01-16,NOSTRO-USD-002,TRX100O2M
STMT,DR,OPEN,200.0000,USD,2024-01-16,NOSTRO-USD-002,PART-A
STMT,DR,OPEN,300.0000,USD,2024-01-16,NOSTRO-USD-002,PART-B
GL,CR,OPEN,400.0000,USD,2024-01-17,NOSTRO-USD-003,PART-C
GL,CR,OPEN,600.0000,USD,2024-01-17,NOSTRO-USD-003,PART-D
STMT,DR,OPEN,1000.0000,USD,2024-01-17,NOSTRO-USD-003,TRX200M2O
GL,CR,OPEN,100.0000,USD,2024-01-18,NOSTRO-USD-004,PART-E
GL,CR,OPEN,200.0000,USD,2024-01-18,NOSTRO-USD-004,PART-F
STMT,DR,OPEN,120.0000,USD,2024-01-18,NOSTRO-USD-004,PART-G
STMT,DR,OPEN,180.0000,USD,2024-01-18,NOSTRO-USD-004,PART-H
GL,CR,OPEN,999.0000,USD,2024-01-19,NOSTRO-USD-005,ORPHAN1
STMT,DR,OPEN,888.0000,USD,2024-01-19,NOSTRO-USD-005,ORPHAN2
```

*What this covers:*
- Rows 1-2: classic 1:1 (TRX001, exact match)
- Rows 3-4: 1:1 with date tolerance (1 day apart)
- Rows 5-6: 1:1 in EUR (different currency bucket)
- Rows 7-9: 1:N (500 GL vs 200+300 STMT)
- Rows 10-12: N:1 (400+600 GL vs 1000 STMT)
- Rows 13-16: N:M (100+200 GL = 300 matches 120+180 STMT = 300 — trivial equal subset sums)
- Rows 17-18: two unmatched orphans

- [ ] **Step 3.4: Verify loader produces expected shape**

Run:
```bash
python -c "
from rec_engine.config import load as cload
from rec_engine.loader import load
c = cload('tests/fixtures/config.json')
lf = load('tests/fixtures/input.csv', c.schema)
df = lf.collect()
print(df.columns)
print(df.shape)
print(df.head(3))
"
```
Expected: columns include `_row_idx` as first column, shape `(18, 9)`, first 3 rows show correct types.

- [ ] **Step 3.5: Commit**

```bash
git add rec_engine/schema.py rec_engine/loader.py tests/fixtures/input.csv
git commit -m "feat: schema typing + lazy CSV loader with _row_idx"
```

---

## Task 4: Expression compiler and populations

**Files:**
- Create: `rec_engine/expressions.py`
- Create: `rec_engine/populations.py`

- [ ] **Step 4.1: Create `rec_engine/expressions.py`** — attribute expression compilation

```python
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
```

- [ ] **Step 4.2: Create `rec_engine/populations.py`** — filter evaluation

```python
"""Evaluate population filters against a LazyFrame."""

from __future__ import annotations

import polars as pl

from rec_engine.types import Population, PopulationFilter


def apply(lf: pl.LazyFrame, pop: Population) -> pl.LazyFrame:
    """Return `lf` filtered down to rows satisfying all filters in `pop` (AND)."""
    for f in pop.filters:
        lf = lf.filter(_filter_expr(f))
    return lf


def _filter_expr(f: PopulationFilter) -> pl.Expr:
    col = pl.col(f.attribute)
    op = f.operator
    v = f.value
    match op:
        case "=":   return col == v
        case "!=":  return col != v
        case ">":   return col > v
        case "<":   return col < v
        case ">=":  return col >= v
        case "<=":  return col <= v
        case "IN":
            if not isinstance(v, list):
                raise ValueError(f"IN operator requires a list value, got {type(v).__name__}")
            return col.is_in(v)
        case "LIKE":
            if not isinstance(v, str):
                raise ValueError("LIKE operator requires a string pattern")
            regex = _like_to_regex(v)
            return col.cast(pl.Utf8).str.contains(regex)
        case _:
            raise ValueError(f"Unsupported population operator: {op!r}")


def _like_to_regex(pattern: str) -> str:
    """Convert a SQL LIKE pattern (% _) to a Polars-compatible regex."""
    out = ["^"]
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        elif ch in r".\+*?()[]{}|":
            out.append("\\" + ch)
        else:
            out.append(ch)
    out.append("$")
    return "".join(out)
```

- [ ] **Step 4.3: Verify populations work**

Run:
```bash
python -c "
from rec_engine.config import load as cload
from rec_engine.loader import load
from rec_engine.populations import apply
c = cload('tests/fixtures/config.json')
lf = load('tests/fixtures/input.csv', c.schema)
o2o = c.match_passes[0]
left = apply(lf, o2o.population_rule.left).collect()
print(f'GL_CR_OPEN rows: {left.height}')
"
```
Expected: `GL_CR_OPEN rows: 9` (9 GL credits in the fixture).

- [ ] **Step 4.4: Commit**

```bash
git add rec_engine/expressions.py rec_engine/populations.py
git commit -m "feat: expression compiler + population filter evaluation"
```

---

## Task 5: Matcher base + scorer

**Files:**
- Create: `rec_engine/matchers/base.py`
- Create: `rec_engine/scorer.py`

- [ ] **Step 5.1: Create `rec_engine/matchers/base.py`** — shared primitives

```python
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
```

- [ ] **Step 5.2: Create `rec_engine/scorer.py`** — quality scoring as Polars expressions

```python
"""Build a Polars expression that scores each candidate match pair 0-100."""

from __future__ import annotations

import polars as pl

from rec_engine.expressions import compile_attr_expr
from rec_engine.matchers.base import attr_bare_name
from rec_engine.types import AttributeMatch, MatchPass

MANDATORY_WEIGHT = 10
OPTIONAL_WEIGHT = 3
WITHIN_FLOOR = 80.0


def score_expr(
    pass_config: MatchPass,
    left_suffix: str = "",
    right_suffix: str = "_R",
    date_cols: frozenset[str] = frozenset({"VALUE_DATE"}),
) -> pl.Expr:
    """
    Build an expression that evaluates to an integer quality score for each row of
    a join-candidate LazyFrame.
    `right_suffix` is the suffix Polars applied to right-side columns during the join.
    """
    numer_terms: list[pl.Expr] = []
    denom = 0

    for a in pass_config.attributes_to_match:
        weight = MANDATORY_WEIGHT if a.mandatory else OPTIONAL_WEIGHT
        denom += weight

        le = compile_attr_expr(_suffixed(a.left_attribute,  left_suffix))
        re = compile_attr_expr(_suffixed(a.right_attribute, right_suffix))

        if a.operator == "EQUALS":
            per_attr = pl.when(le == re).then(pl.lit(100.0)).otherwise(pl.lit(0.0))
        elif a.operator == "CONTAINS":
            # str.contains accepts an Expr pattern; literal=True disables regex interpretation.
            per_attr = pl.when(le.cast(pl.Utf8).str.contains(re.cast(pl.Utf8), literal=True)).then(pl.lit(100.0)).otherwise(pl.lit(0.0))
        elif a.operator == "WITHIN":
            bare_left = attr_bare_name(a.left_attribute)
            if bare_left in date_cols and a.tolerance_days is not None:
                # Date subtraction yields a Duration; convert to days.
                delta = (le - re).dt.total_days().abs()
                tol = a.tolerance_days
                partial = pl.lit(100.0) - (delta.cast(pl.Float64) / tol * 20.0)
                per_attr = pl.when(delta <= tol).then(partial.clip(WITHIN_FLOOR, 100.0)).otherwise(pl.lit(0.0))
            else:
                delta = (le.cast(pl.Float64) - re.cast(pl.Float64)).abs()
                tol_abs = a.tolerance_amount or 0.0
                tol_pct = a.tolerance_percent or 0.0
                tol_eff = pl.max_horizontal(pl.lit(tol_abs), (le.cast(pl.Float64).abs() * tol_pct))
                partial = pl.lit(100.0) - (delta / tol_eff.clip(lower_bound=1e-12) * 20.0)
                per_attr = pl.when(delta <= tol_eff).then(partial.clip(WITHIN_FLOOR, 100.0)).otherwise(pl.lit(0.0))
        else:
            raise ValueError(f"Unsupported operator in scorer: {a.operator}")

        numer_terms.append(per_attr * weight)

    total = numer_terms[0]
    for t in numer_terms[1:]:
        total = total + t
    return (total / denom).round(0).cast(pl.Int64)


def _suffixed(expr_str: str, suffix: str) -> str:
    """Append suffix to the bare column inside a possibly-SUBSTRING expression."""
    if not suffix:
        return expr_str
    m = __import__("re").match(r"^SUBSTRING\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$", expr_str)
    if m:
        col, s, ln = m.group(1), m.group(2), m.group(3)
        return f"SUBSTRING({col}{suffix}, {s}, {ln})"
    return expr_str + suffix
```

- [ ] **Step 5.3: Verify scorer compiles**

Run:
```bash
python -c "
from rec_engine.config import load as cload
from rec_engine.scorer import score_expr
c = cload('tests/fixtures/config.json')
expr = score_expr(c.match_passes[0])
print('Score expr built:', type(expr).__name__)
"
```
Expected: `Score expr built: Expr`

- [ ] **Step 5.4: Commit**

```bash
git add rec_engine/matchers/base.py rec_engine/scorer.py
git commit -m "feat: matcher base primitives + quality scoring expression"
```

---

## Task 6: ONE_TO_ONE matcher + dispatcher

**Files:**
- Create: `rec_engine/matchers/one_to_one.py`
- Modify: `rec_engine/matchers/__init__.py`

- [ ] **Step 6.1: Create `rec_engine/matchers/one_to_one.py`**

```python
"""1:1 matcher: greedy highest-quality pairing within hard-key buckets."""

from __future__ import annotations

import polars as pl

from rec_engine.expressions import compile_attr_expr
from rec_engine.matchers.base import (
    MGIDAllocator,
    attr_bare_name,
    hard_key_columns,
    tolerance_attrs,
    contains_attrs,
)
from rec_engine.scorer import score_expr
from rec_engine.types import MatchPass


def match(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
) -> pl.LazyFrame:
    """
    Returns a LazyFrame with one row per participating record (left + right):
        columns: _row_idx, MATCHING_ID, MATCHED_BY_PASS, MATCH_QUALITY
    """
    hkeys = hard_key_columns(pass_config)
    left = left_lf.rename({"_row_idx": "_row_idx_L"})
    right = right_lf.rename({"_row_idx": "_row_idx_R"})

    # Right-side column suffix strategy: rename non-key columns of right with _R suffix,
    # but keep the join keys un-suffixed so the inner join works cleanly.
    right_cols = right.collect_schema().names()
    to_suffix = [c for c in right_cols if c not in hkeys and c != "_row_idx_R"]
    right = right.rename({c: f"{c}_R" for c in to_suffix})

    candidates = left.join(right, on=hkeys, how="inner")

    # Apply WITHIN filters
    for a in tolerance_attrs(pass_config):
        bare_left = attr_bare_name(a.left_attribute)
        le = compile_attr_expr(a.left_attribute)
        re_ = compile_attr_expr(a.right_attribute + "_R")
        if bare_left == "VALUE_DATE" and a.tolerance_days is not None:
            candidates = candidates.filter(
                (le - re_).dt.total_days().abs() <= a.tolerance_days
            )
        else:
            tol_abs = a.tolerance_amount or 0.0
            tol_pct = a.tolerance_percent or 0.0
            tol_eff = pl.max_horizontal(pl.lit(tol_abs), (le.cast(pl.Float64).abs() * tol_pct))
            candidates = candidates.filter(
                (le.cast(pl.Float64) - re_.cast(pl.Float64)).abs() <= tol_eff
            )

    # Apply CONTAINS filters (literal=True disables regex; accepts column-vs-column)
    for a in contains_attrs(pass_config):
        le = compile_attr_expr(a.left_attribute).cast(pl.Utf8)
        re_ = compile_attr_expr(a.right_attribute + "_R").cast(pl.Utf8)
        candidates = candidates.filter(le.str.contains(re_, literal=True))

    # Score
    candidates = candidates.with_columns(score_expr(pass_config).alias("_quality"))
    candidates = candidates.filter(pl.col("_quality") >= pass_config.pqr.quality)

    # Greedy 1:1: left-uniqueness then right-uniqueness.
    # sort().unique(keep="first") is unambiguous — takes the first (i.e., highest quality)
    # row per subset. Secondary sort by _row_idx_L/_R makes tie-breaks deterministic
    # across Polars versions and parallel execution orderings.
    candidates = (
        candidates.sort(["_quality", "_row_idx_L"], descending=[True, False])
        .unique(subset=["_row_idx_L"], keep="first", maintain_order=True)
    )
    candidates = (
        candidates.sort(["_quality", "_row_idx_R"], descending=[True, False])
        .unique(subset=["_row_idx_R"], keep="first", maintain_order=True)
    )

    # Collect survivors and allocate MGIDs
    pairs = candidates.select(["_row_idx_L", "_row_idx_R", "_quality"]).collect()
    if pairs.is_empty():
        return _empty_result_lf()

    n = pairs.height
    mgids = mgid.allocate_batch(n)
    pairs = pairs.with_columns(pl.Series("MATCHING_ID", mgids))

    # Explode to long-form: one row per participating record.
    left_rows = pairs.select(
        pl.col("_row_idx_L").alias("_row_idx"),
        pl.col("MATCHING_ID"),
        pl.lit(pass_config.id).alias("MATCHED_BY_PASS"),
        pl.col("_quality").alias("MATCH_QUALITY"),
    )
    right_rows = pairs.select(
        pl.col("_row_idx_R").alias("_row_idx"),
        pl.col("MATCHING_ID"),
        pl.lit(pass_config.id).alias("MATCHED_BY_PASS"),
        pl.col("_quality").alias("MATCH_QUALITY"),
    )
    return pl.concat([left_rows, right_rows]).lazy()


def _empty_result_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            "_row_idx": pl.UInt64,
            "MATCHING_ID": pl.Utf8,
            "MATCHED_BY_PASS": pl.Utf8,
            "MATCH_QUALITY": pl.Int64,
        }
    ).lazy()
```

- [ ] **Step 6.2: Update `rec_engine/matchers/__init__.py`** — dispatcher

```python
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
```

- [ ] **Step 6.3: Commit**

```bash
git add rec_engine/matchers/
git commit -m "feat: ONE_TO_ONE matcher + dispatcher stub"
```

---

## Task 7: Orchestrator, writer, CLI

**Files:**
- Create: `rec_engine/engine.py`
- Create: `rec_engine/writer.py`
- Create: `rec_engine/cli.py`

- [ ] **Step 7.1: Create `rec_engine/engine.py`** — orchestrator

```python
"""Engine orchestrator: load → passes → emit combined match-groups LazyFrame."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from rec_engine.loader import load as load_input
from rec_engine.matchers import dispatch
from rec_engine.matchers.base import MGIDAllocator
from rec_engine.populations import apply as apply_population
from rec_engine.types import Config, MatchPass


@dataclass
class PassStat:
    pass_id: str
    pass_name: str
    match_type: str
    rule_order: int
    matched_groups: int
    matched_rows: int
    avg_quality: float
    duration_sec: float


@dataclass
class EngineResult:
    input_lf: pl.LazyFrame
    matches_lf: pl.LazyFrame            # long-form: _row_idx, MATCHING_ID, MATCHED_BY_PASS, MATCH_QUALITY
    pass_stats: list[PassStat]
    run_started: datetime
    run_duration_sec: float


def run(config: Config, input_path: str | Path, cycle_date_override: str | None = None) -> EngineResult:
    t_start_all = time.time()
    run_started = datetime.now(timezone.utc)

    cycle_date_str = cycle_date_override or config.cycle_date
    if not cycle_date_str:
        raise ValueError("cycleDate must be provided via config or CLI")
    cycle_date = date.fromisoformat(cycle_date_str)

    mgid = MGIDAllocator(set_id=config.set_id, cycle_date=cycle_date)

    input_lf = load_input(input_path, config.schema)

    active_passes: list[MatchPass] = sorted(
        [p for p in config.match_passes if p.status == "ACTIVE"],
        key=lambda p: p.pqr.rule_order,
    )

    matched_idx: set[int] = set()
    all_match_rows: list[pl.DataFrame] = []
    pass_stats: list[PassStat] = []

    for p in active_passes:
        t_start = time.time()

        # Exclude already-matched rows for this pass
        if matched_idx:
            excl_series = pl.Series("_matched_ids", list(matched_idx), dtype=pl.UInt64)
            remaining = input_lf.filter(~pl.col("_row_idx").is_in(excl_series))
        else:
            remaining = input_lf

        left_lf = apply_population(remaining, p.population_rule.left)
        right_lf = apply_population(remaining, p.population_rule.right)

        match_rows_lf = dispatch(left_lf, right_lf, p, mgid)
        match_rows_df = match_rows_lf.collect()

        if not match_rows_df.is_empty():
            all_match_rows.append(match_rows_df)
            new_idx = match_rows_df.get_column("_row_idx").to_list()
            matched_idx.update(new_idx)
            n_groups = match_rows_df.get_column("MATCHING_ID").n_unique()
            n_rows = match_rows_df.height
            avg_q = float(match_rows_df.get_column("MATCH_QUALITY").mean() or 0.0)
        else:
            n_groups = 0
            n_rows = 0
            avg_q = 0.0

        pass_stats.append(PassStat(
            pass_id=p.id, pass_name=p.name, match_type=p.match_type, rule_order=p.pqr.rule_order,
            matched_groups=n_groups, matched_rows=n_rows,
            avg_quality=round(avg_q, 2), duration_sec=round(time.time() - t_start, 3),
        ))

    if all_match_rows:
        matches_df = pl.concat(all_match_rows)
    else:
        matches_df = pl.DataFrame(
            schema={
                "_row_idx": pl.UInt64,
                "MATCHING_ID": pl.Utf8,
                "MATCHED_BY_PASS": pl.Utf8,
                "MATCH_QUALITY": pl.Int64,
            }
        )

    matches_df = matches_df.with_columns(
        pl.lit(cycle_date).alias("MATCH_DATE"),
    )

    return EngineResult(
        input_lf=input_lf,
        matches_lf=matches_df.lazy(),
        pass_stats=pass_stats,
        run_started=run_started,
        run_duration_sec=round(time.time() - t_start_all, 3),
    )
```

- [ ] **Step 7.2: Create `rec_engine/writer.py`** — output assembly + manifest

```python
"""Assemble the output LazyFrame (preserving input column order) and write CSV + manifest."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import polars as pl

from rec_engine.engine import EngineResult
from rec_engine.schema import OUTPUT_COLUMNS


def write(
    result: EngineResult,
    config_path: str,
    input_path: str,
    output_path: str,
) -> dict:
    input_lf = result.input_lf
    matches_lf = result.matches_lf

    original_cols = [c for c in input_lf.collect_schema().names() if c != "_row_idx"]

    joined = input_lf.join(matches_lf, on="_row_idx", how="left")

    # STATUS: MATCHED if MATCHING_ID is not null, else UNMATCHED (overwriting any existing STATUS)
    joined = joined.with_columns([
        pl.when(pl.col("MATCHING_ID").is_not_null())
          .then(pl.lit("MATCHED"))
          .otherwise(pl.lit("UNMATCHED"))
          .alias("STATUS"),
    ])

    # Final column order: original columns first (preserving input order, with STATUS overwritten),
    # then any OUTPUT_COLUMNS not already in original order.
    final_cols: list[str] = []
    for c in original_cols:
        final_cols.append(c)
    for oc in OUTPUT_COLUMNS:
        if oc not in final_cols:
            final_cols.append(oc)

    # Drop the right-side-joined STATUS (from matches_lf this didn't exist; STATUS above is a new expr)
    # But the input might have had STATUS originally -> the join may have produced STATUS_right; handle both.
    schema_cols = set(joined.collect_schema().names())
    if "STATUS_right" in schema_cols:
        joined = joined.drop("STATUS_right")

    # Restore input row order via _row_idx, then drop it
    joined = joined.sort("_row_idx").drop("_row_idx")

    # Select in the finalized order, taking only columns that exist in the frame
    avail = joined.collect_schema().names()
    select_cols = [c for c in final_cols if c in avail]
    joined = joined.select(select_cols)

    # Sink CSV (streaming)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    joined.sink_csv(output_path)

    # Manifest
    manifest = _build_manifest(result, config_path, input_path, output_path)
    manifest_path = Path(str(output_path) + ".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    return manifest


def _build_manifest(result: EngineResult, config_path: str, input_path: str, output_path: str) -> dict:
    # Need totals: collect minimal stats via lazy summaries
    n_input = result.input_lf.select(pl.len()).collect().item()
    matches_df = result.matches_lf.collect()
    matched_rows = matches_df.height
    match_groups = matches_df.get_column("MATCHING_ID").n_unique() if matched_rows else 0

    unmatched_input_lf = result.input_lf.join(result.matches_lf.select("_row_idx"), on="_row_idx", how="anti")
    unmatched_df = unmatched_input_lf.select(["LS_TYPE", "CURRENCY"]).collect()
    by_side = Counter(unmatched_df.get_column("LS_TYPE").to_list())
    by_ccy = Counter(unmatched_df.get_column("CURRENCY").to_list())

    return {
        "setId": result.pass_stats[0].pass_id.split("_")[0] if result.pass_stats else None,
        "cycleDate": str(matches_df.get_column("MATCH_DATE").head(1).item()) if matched_rows else None,
        "configPath": str(Path(config_path).resolve()),
        "inputPath": str(Path(input_path).resolve()),
        "outputPath": str(Path(output_path).resolve()),
        "runTimestamp": result.run_started.isoformat() + "Z",
        "durationSec": result.run_duration_sec,
        "totals": {
            "inputRows": int(n_input),
            "matchedRows": int(matched_rows),
            "unmatchedRows": int(n_input - matched_rows),
            "matchGroups": int(match_groups),
        },
        "passBreakdown": [
            {
                "passId": s.pass_id,
                "passName": s.pass_name,
                "matchType": s.match_type,
                "ruleOrder": s.rule_order,
                "matchedGroups": s.matched_groups,
                "matchedRows": s.matched_rows,
                "avgQuality": s.avg_quality,
                "durationSec": s.duration_sec,
            } for s in result.pass_stats
        ],
        "unmatchedBreakdown": {
            "bySide": dict(by_side),
            "byCurrency": dict(by_ccy),
        },
    }
```

**Note**: The `setId` read from `pass_stats` above is a bug. Fix in next sub-step.

- [ ] **Step 7.3: Fix manifest setId — pass the config through**

In `rec_engine/writer.py`, change `write` signature to accept `Config` and pass `set_id` explicitly:

Replace the `write` function signature and `_build_manifest` signature/call:

```python
from rec_engine.types import Config

def write(
    result: EngineResult,
    config: Config,
    config_path: str,
    input_path: str,
    output_path: str,
) -> dict:
    # ... (body unchanged until manifest build)
    manifest = _build_manifest(result, config, config_path, input_path, output_path)
    # ... rest unchanged

def _build_manifest(result: EngineResult, config: Config, config_path: str, input_path: str, output_path: str) -> dict:
    # Replace the broken setId line with:
    #   "setId": config.set_id,
    # ... rest of dict unchanged
```

Full corrected `_build_manifest`:

```python
def _build_manifest(result: EngineResult, config: Config, config_path: str, input_path: str, output_path: str) -> dict:
    n_input = result.input_lf.select(pl.len()).collect().item()
    matches_df = result.matches_lf.collect()
    matched_rows = matches_df.height
    match_groups = matches_df.get_column("MATCHING_ID").n_unique() if matched_rows else 0

    unmatched_input_lf = result.input_lf.join(result.matches_lf.select("_row_idx"), on="_row_idx", how="anti")
    unmatched_df = unmatched_input_lf.select(["LS_TYPE", "CURRENCY"]).collect()
    by_side = Counter(unmatched_df.get_column("LS_TYPE").to_list())
    by_ccy = Counter(unmatched_df.get_column("CURRENCY").to_list())

    return {
        "setId": config.set_id,
        "cycleDate": config.cycle_date,
        "configPath": str(Path(config_path).resolve()),
        "inputPath": str(Path(input_path).resolve()),
        "outputPath": str(Path(output_path).resolve()),
        "runTimestamp": result.run_started.isoformat() + "Z",
        "durationSec": result.run_duration_sec,
        "totals": {
            "inputRows": int(n_input),
            "matchedRows": int(matched_rows),
            "unmatchedRows": int(n_input - matched_rows),
            "matchGroups": int(match_groups),
        },
        "passBreakdown": [
            {
                "passId": s.pass_id,
                "passName": s.pass_name,
                "matchType": s.match_type,
                "ruleOrder": s.rule_order,
                "matchedGroups": s.matched_groups,
                "matchedRows": s.matched_rows,
                "avgQuality": s.avg_quality,
                "durationSec": s.duration_sec,
            } for s in result.pass_stats
        ],
        "unmatchedBreakdown": {
            "bySide": dict(by_side),
            "byCurrency": dict(by_ccy),
        },
    }
```

- [ ] **Step 7.4: Create `rec_engine/cli.py`**

```python
"""typer CLI entry point."""

from __future__ import annotations

import sys
import traceback
from pathlib import Path
from typing import Annotated

import typer

from rec_engine.config import ConfigError, load as load_config
from rec_engine.engine import run as run_engine
from rec_engine.writer import write as write_output

app = typer.Typer(add_completion=False, help="Nostro reconciliation matching engine.")


@app.command()
def main(
    config: Annotated[Path, typer.Option(help="Path to match-pass config JSON", exists=True, readable=True)],
    input: Annotated[Path,  typer.Option(help="Path to input CSV",           exists=True, readable=True)],
    output: Annotated[Path, typer.Option(help="Output CSV path")],
    cycle_date: Annotated[str | None, typer.Option(help="YYYY-MM-DD; overrides config.cycleDate")] = None,
    verbose: Annotated[bool, typer.Option(help="Print per-pass progress")] = False,
) -> None:
    try:
        cfg = load_config(config)
    except ConfigError as e:
        typer.echo(f"[cfg] ERROR: {e}", err=True)
        raise typer.Exit(code=1)

    if verbose:
        active = [p for p in cfg.match_passes if p.status == "ACTIVE"]
        typer.echo(f"[cfg] Loaded {len(cfg.match_passes)} match passes ({len(active)} ACTIVE)")

    try:
        result = run_engine(cfg, input_path=str(input), cycle_date_override=cycle_date)
    except FileNotFoundError as e:
        typer.echo(f"[load] ERROR: {e}", err=True)
        raise typer.Exit(code=2)
    except ValueError as e:
        typer.echo(f"[load] ERROR: {e}", err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.echo(f"[pass] ERROR: {e}", err=True)
        if verbose:
            traceback.print_exc()
        raise typer.Exit(code=3)

    if verbose:
        for s in result.pass_stats:
            typer.echo(
                f"[pass] {s.pass_id} ({s.match_type}) ... matched {s.matched_groups:>6,} groups  "
                f"[{s.duration_sec:>5.1f}s]"
            )

    try:
        manifest = write_output(result, cfg, str(config), str(input), str(output))
    except Exception as e:
        typer.echo(f"[sink] ERROR: {e}", err=True)
        if verbose:
            traceback.print_exc()
        raise typer.Exit(code=4)

    if verbose:
        tot = manifest["totals"]
        typer.echo(f"[sink] Written {output} + manifest.json ({tot['inputRows']:,} rows)")
        rate = (tot["matchedRows"] / tot["inputRows"]) * 100 if tot["inputRows"] else 0
        typer.echo(f"[done] {result.run_duration_sec}s total | {rate:.2f}% match rate")


if __name__ == "__main__":
    app()
```

- [ ] **Step 7.5: Commit**

```bash
git add rec_engine/engine.py rec_engine/writer.py rec_engine/cli.py
git commit -m "feat: orchestrator + writer + CLI"
```

---

## Task 8: Integration test with 1:1 only (initial end-to-end)

**Files:**
- Create: `tests/fixtures/expected_output.csv` (seed with 1:1 expectations only for now; updated in later tasks)
- Create: `tests/test_engine_integration.py`

- [ ] **Step 8.1: Temporarily deactivate non-1:1 passes in the fixture config**

Edit `tests/fixtures/config.json`: change `"status": "ACTIVE"` to `"status": "INACTIVE"` on `MP_O2M`, `MP_M2O`, and `MP_M2M`. (We'll reactivate them as each matcher is implemented.)

- [ ] **Step 8.2: Run the engine end-to-end once to capture the 1:1-only output**

```bash
python -m rec_engine \
  --config tests/fixtures/config.json \
  --input  tests/fixtures/input.csv \
  --output /tmp/rec_output.csv \
  --cycle-date 2024-01-15 \
  --verbose
```

Expected verbose log: 3 matches for 1:1 pass (TRX001 rows 2, TRX002 rows 2, TRX003 rows 2 = 6 matched rows in 3 groups), leaving 12 unmatched rows.

If verbose output differs, fix the matcher before proceeding. Do not adjust the fixture to match broken output.

- [ ] **Step 8.3: Copy the verified output into `tests/fixtures/expected_output.csv`**

```bash
cp /tmp/rec_output.csv tests/fixtures/expected_output.csv
```

Inspect with `cat tests/fixtures/expected_output.csv` and confirm by hand that the 3 expected pairs are marked `MATCHED` with correct `MATCHING_ID` values (`MGID_USD_NOSTRO_CITIBANK_20240115_000001` / `000002` / `000003`).

- [ ] **Step 8.4: Create `tests/test_engine_integration.py`**

```python
"""End-to-end integration test: run engine on fixture, compare output to expected."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def test_engine_end_to_end(tmp_path: Path) -> None:
    output_path = tmp_path / "output.csv"
    result = subprocess.run(
        [
            sys.executable, "-m", "rec_engine",
            "--config", str(FIXTURES / "config.json"),
            "--input",  str(FIXTURES / "input.csv"),
            "--output", str(output_path),
            "--cycle-date", "2024-01-15",
        ],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, f"engine failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"

    actual = output_path.read_text().splitlines()
    expected = (FIXTURES / "expected_output.csv").read_text().splitlines()

    assert actual == expected, _diff(expected, actual)

    manifest_path = Path(str(output_path) + ".manifest.json")
    assert manifest_path.exists(), "manifest not written"


def _diff(expected: list[str], actual: list[str]) -> str:
    out = []
    for i, (e, a) in enumerate(zip(expected, actual)):
        if e != a:
            out.append(f"line {i + 1}\n  expected: {e!r}\n  actual:   {a!r}")
    if len(expected) != len(actual):
        out.append(f"line-count mismatch: expected {len(expected)} got {len(actual)}")
    return "\n".join(out) or "no per-line diff but lists differ"
```

- [ ] **Step 8.5: Run the integration test**

```bash
pytest tests/test_engine_integration.py -v
```
Expected: PASS.

- [ ] **Step 8.6: Commit**

```bash
git add tests/fixtures/config.json tests/fixtures/expected_output.csv tests/test_engine_integration.py
git commit -m "test: integration test for ONE_TO_ONE matcher end-to-end"
```

---

## Task 9: ONE_TO_MANY matcher

**Files:**
- Create: `rec_engine/matchers/one_to_many.py`

- [ ] **Step 9.1: Create `rec_engine/matchers/one_to_many.py`**

```python
"""1:N matcher: full-bucket aggregation within hard-key buckets."""

from __future__ import annotations

import polars as pl

from rec_engine.expressions import compile_attr_expr
from rec_engine.matchers.base import (
    MGIDAllocator,
    attr_bare_name,
    hard_key_columns,
)
from rec_engine.types import AttributeMatch, MatchPass


def match(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
) -> pl.LazyFrame:
    """
    1:N: one-side is left, many-side is right.
    Full-bucket aggregation: SUM(right rows in bucket) must match left row within tolerance.
    All-within date rule: every right row's date must be within tolerance of left date.
    """
    return _match_one_to_many(left_lf, right_lf, pass_config, mgid, one_is_left=True)


def _match_one_to_many(
    one_lf: pl.LazyFrame,
    many_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
    one_is_left: bool,
) -> pl.LazyFrame:
    hkeys = hard_key_columns(pass_config)

    # Identify aggregation attribute (SUM on the many-side)
    agg_attr = _find_agg_attr(pass_config, many_is_side="RIGHT" if one_is_left else "LEFT")
    if agg_attr is None:
        # Shouldn't happen: config validation enforces it for 1:N/N:1
        return _empty_result_lf()

    many_bare = attr_bare_name(
        agg_attr.right_attribute if one_is_left else agg_attr.left_attribute
    )

    # Identify date WITHIN attribute (for all-within rule), if any
    date_attr: AttributeMatch | None = next(
        (a for a in pass_config.attributes_to_match
         if a.operator == "WITHIN" and attr_bare_name(a.left_attribute) == "VALUE_DATE"),
        None,
    )

    # Bucket the many side
    many_agg_cols = [
        pl.col("_row_idx").alias("_many_idx_list"),
        pl.col(many_bare).sum().alias("_many_sum"),
    ]
    if date_attr is not None:
        many_agg_cols += [
            pl.col("VALUE_DATE").min().alias("_many_date_min"),
            pl.col("VALUE_DATE").max().alias("_many_date_max"),
        ]
    many_agg_cols.append(pl.len().alias("_many_n"))

    many_bucketed = many_lf.group_by(hkeys).agg(many_agg_cols)

    # Only buckets with >= 2 rows on the many side are 1:N candidates
    many_bucketed = many_bucketed.filter(pl.col("_many_n") >= 2)

    # Rename one-side _row_idx
    one_side = one_lf.rename({"_row_idx": "_one_idx"})

    candidates = one_side.join(many_bucketed, on=hkeys, how="inner")

    # Amount-aggregation filter
    one_amt_bare = attr_bare_name(
        agg_attr.left_attribute if one_is_left else agg_attr.right_attribute
    )
    tol_abs = agg_attr.tolerance_amount or 0.0
    tol_pct = agg_attr.tolerance_percent or 0.0
    if agg_attr.operator == "EQUALS":
        candidates = candidates.filter(
            pl.col(one_amt_bare).cast(pl.Float64) == pl.col("_many_sum").cast(pl.Float64)
        )
    else:  # WITHIN
        tol_eff = pl.max_horizontal(
            pl.lit(tol_abs),
            pl.col(one_amt_bare).cast(pl.Float64).abs() * tol_pct,
        )
        candidates = candidates.filter(
            (pl.col(one_amt_bare).cast(pl.Float64) - pl.col("_many_sum").cast(pl.Float64)).abs() <= tol_eff
        )

    # All-within date filter
    if date_attr is not None and date_attr.tolerance_days is not None:
        td = date_attr.tolerance_days
        candidates = candidates.filter(
            ((pl.col("VALUE_DATE") - pl.col("_many_date_min")).dt.total_days().abs() <= td)
            & ((pl.col("VALUE_DATE") - pl.col("_many_date_max")).dt.total_days().abs() <= td)
        )

    # Non-aggregated EQUALS attributes are already in hkeys (handled by hard_key_columns).

    # Quality: we compute a simple score since WITHIN scoring requires pairwise deltas.
    # For 1:N, use: 100 if exact amount match, else 100 - (delta/tol * 20) floored at 80.
    if agg_attr.operator == "EQUALS":
        quality_expr = pl.lit(100).cast(pl.Int64)
    else:
        delta = (pl.col(one_amt_bare).cast(pl.Float64) - pl.col("_many_sum").cast(pl.Float64)).abs()
        tol_eff = pl.max_horizontal(
            pl.lit(tol_abs),
            pl.col(one_amt_bare).cast(pl.Float64).abs() * tol_pct,
        ).clip(lower_bound=1e-12)
        quality_expr = (pl.lit(100.0) - (delta / tol_eff * 20.0)).clip(80.0, 100.0).round(0).cast(pl.Int64)
    candidates = candidates.with_columns(quality_expr.alias("_quality"))
    candidates = candidates.filter(pl.col("_quality") >= pass_config.pqr.quality)

    # Left-uniqueness: each one-side row can join at most one bucket; pick highest quality.
    # Secondary sort by _one_idx makes tie-breaks deterministic.
    candidates = (
        candidates.sort(["_quality", "_one_idx"], descending=[True, False])
        .unique(subset=["_one_idx"], keep="first", maintain_order=True)
    )

    # Collect, allocate MGIDs, explode
    candidates_df = candidates.select([
        "_one_idx", "_many_idx_list", "_quality",
    ]).collect()

    if candidates_df.is_empty():
        return _empty_result_lf()

    n = candidates_df.height
    mgids = mgid.allocate_batch(n)
    candidates_df = candidates_df.with_columns(pl.Series("MATCHING_ID", mgids))

    one_rows = candidates_df.select(
        pl.col("_one_idx").alias("_row_idx"),
        pl.col("MATCHING_ID"),
        pl.lit(pass_config.id).alias("MATCHED_BY_PASS"),
        pl.col("_quality").alias("MATCH_QUALITY"),
    )
    many_rows = candidates_df.select([
        pl.col("_many_idx_list"),
        pl.col("MATCHING_ID"),
        pl.lit(pass_config.id).alias("MATCHED_BY_PASS"),
        pl.col("_quality").alias("MATCH_QUALITY"),
    ]).explode("_many_idx_list").rename({"_many_idx_list": "_row_idx"})

    return pl.concat([one_rows, many_rows]).lazy()


def _find_agg_attr(pass_config: MatchPass, many_is_side: str) -> AttributeMatch | None:
    for a in pass_config.attributes_to_match:
        if a.aggregation and a.aggregation.side == many_is_side and a.aggregation.function == "SUM":
            return a
    return None


def _empty_result_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            "_row_idx": pl.UInt64,
            "MATCHING_ID": pl.Utf8,
            "MATCHED_BY_PASS": pl.Utf8,
            "MATCH_QUALITY": pl.Int64,
        }
    ).lazy()
```

- [ ] **Step 9.2: Reactivate MP_O2M in `tests/fixtures/config.json`**

Change `"status": "INACTIVE"` back to `"status": "ACTIVE"` on the `MP_O2M` pass only.

- [ ] **Step 9.3: Regenerate expected output and verify by eye**

```bash
python -m rec_engine \
  --config tests/fixtures/config.json \
  --input  tests/fixtures/input.csv \
  --output /tmp/rec_output.csv \
  --cycle-date 2024-01-15 \
  --verbose
```

Expected manifest / verbose:
- 1:1 pass: 3 matched groups (6 rows)
- 1:N pass: 1 matched group — the GL 500 row + 2 STMT debits (rows 7, 8, 9 in the fixture)

Inspect `/tmp/rec_output.csv`; verify row 7 (GL 500), row 8 (STMT 200), row 9 (STMT 300) share the same `MATCHING_ID` with `MATCHED_BY_PASS = MP_O2M`.

If correct:
```bash
cp /tmp/rec_output.csv tests/fixtures/expected_output.csv
```

- [ ] **Step 9.4: Run integration test**

```bash
pytest tests/test_engine_integration.py -v
```
Expected: PASS.

- [ ] **Step 9.5: Commit**

```bash
git add rec_engine/matchers/one_to_many.py tests/fixtures/config.json tests/fixtures/expected_output.csv
git commit -m "feat: ONE_TO_MANY matcher with full-bucket aggregation"
```

---

## Task 10: MANY_TO_ONE matcher

**Files:**
- Create: `rec_engine/matchers/many_to_one.py`

- [ ] **Step 10.1: Create `rec_engine/matchers/many_to_one.py`** — thin wrapper over 1:N

```python
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
```

- [ ] **Step 10.2: Reactivate MP_M2O in `tests/fixtures/config.json`**

Set `MP_M2O.status` to `"ACTIVE"`.

- [ ] **Step 10.3: Run and verify**

```bash
python -m rec_engine \
  --config tests/fixtures/config.json \
  --input  tests/fixtures/input.csv \
  --output /tmp/rec_output.csv \
  --cycle-date 2024-01-15 \
  --verbose
```

Expected:
- 1:1 pass: 3 groups
- 1:N pass: 1 group (500 credit → 200+300 debits)
- N:1 pass: 1 group — GL 400 + GL 600 (rows 10, 11) ↔ STMT 1000 (row 12)

Verify, then:
```bash
cp /tmp/rec_output.csv tests/fixtures/expected_output.csv
```

- [ ] **Step 10.4: Run integration test**

```bash
pytest tests/test_engine_integration.py -v
```
Expected: PASS.

- [ ] **Step 10.5: Commit**

```bash
git add rec_engine/matchers/many_to_one.py tests/fixtures/config.json tests/fixtures/expected_output.csv
git commit -m "feat: MANY_TO_ONE matcher via 1:N axis swap"
```

---

## Task 11: MANY_TO_MANY matcher

**Files:**
- Create: `rec_engine/matchers/many_to_many.py`

- [ ] **Step 11.1: Create `rec_engine/matchers/many_to_many.py`** — bucketed meet-in-the-middle

```python
"""N:M matcher: bucketed subset-sum via brute-force enumeration within capped buckets."""

from __future__ import annotations

from itertools import combinations

import polars as pl

from rec_engine.matchers.base import MGIDAllocator, hard_key_columns
from rec_engine.types import AttributeMatch, MatchPass

MAX_N2M_BUCKET_SIDE = 10  # per side. Enumeration is O(2^L × 2^R); 10×10 = ~1M pairs
                          # per bucket is tractable. Raising this materially slows N:M.


def match(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    pass_config: MatchPass,
    mgid: MGIDAllocator,
) -> pl.LazyFrame:
    hkeys = hard_key_columns(pass_config)

    # Identify the amount attribute (there must be exactly one numeric WITHIN/EQUALS)
    amt_attr = _find_amount_attr(pass_config)
    if amt_attr is None:
        return _empty_result_lf()

    # Identify date WITHIN attribute if present
    date_attr = next(
        (a for a in pass_config.attributes_to_match
         if a.operator == "WITHIN" and a.left_attribute == "VALUE_DATE"),
        None,
    )

    # Collect both sides bucketed by hard keys. Bucket rows are small.
    left_groups = (
        left_lf.group_by(hkeys).agg([
            pl.col("_row_idx").alias("idxs"),
            pl.col("AMOUNT").alias("amts"),
            pl.col("VALUE_DATE").alias("dates"),
            pl.len().alias("n"),
        ]).filter(
            (pl.col("n") >= 2) & (pl.col("n") <= MAX_N2M_BUCKET_SIDE)
        ).collect()
    )
    right_groups = (
        right_lf.group_by(hkeys).agg([
            pl.col("_row_idx").alias("idxs"),
            pl.col("AMOUNT").alias("amts"),
            pl.col("VALUE_DATE").alias("dates"),
            pl.len().alias("n"),
        ]).filter(
            (pl.col("n") >= 2) & (pl.col("n") <= MAX_N2M_BUCKET_SIDE)
        ).collect()
    )

    if left_groups.is_empty() or right_groups.is_empty():
        return _empty_result_lf()

    # Align buckets on hard keys via inner join (still small — one row per bucket)
    joined = left_groups.join(
        right_groups, on=hkeys, how="inner", suffix="_R",
    )
    if joined.is_empty():
        return _empty_result_lf()

    tol_abs = amt_attr.tolerance_amount or 0.0
    tol_pct = amt_attr.tolerance_percent or 0.0
    td = date_attr.tolerance_days if date_attr else None

    matched_left_idx: list[list[int]] = []
    matched_right_idx: list[list[int]] = []
    qualities: list[int] = []

    for row in joined.iter_rows(named=True):
        l_idxs = list(row["idxs"])
        l_amts = list(row["amts"])
        l_dates = list(row["dates"])
        r_idxs = list(row["idxs_R"])
        r_amts = list(row["amts_R"])
        r_dates = list(row["dates_R"])

        best = _best_subset_pair(
            l_idxs, l_amts, l_dates,
            r_idxs, r_amts, r_dates,
            tol_abs=tol_abs, tol_pct=tol_pct, tol_days=td,
        )
        if best is None:
            continue

        li, ri, quality = best
        matched_left_idx.append(li)
        matched_right_idx.append(ri)
        qualities.append(quality)

    if not matched_left_idx:
        return _empty_result_lf()

    mgids = mgid.allocate_batch(len(matched_left_idx))

    rows: list[dict] = []
    for mg, li_group, ri_group, q in zip(mgids, matched_left_idx, matched_right_idx, qualities):
        for idx in li_group:
            rows.append({
                "_row_idx": idx, "MATCHING_ID": mg,
                "MATCHED_BY_PASS": pass_config.id, "MATCH_QUALITY": q,
            })
        for idx in ri_group:
            rows.append({
                "_row_idx": idx, "MATCHING_ID": mg,
                "MATCHED_BY_PASS": pass_config.id, "MATCH_QUALITY": q,
            })

    return pl.DataFrame(
        rows,
        schema={
            "_row_idx": pl.UInt64,
            "MATCHING_ID": pl.Utf8,
            "MATCHED_BY_PASS": pl.Utf8,
            "MATCH_QUALITY": pl.Int64,
        },
    ).lazy()


def _best_subset_pair(
    l_idxs: list[int], l_amts: list, l_dates: list,
    r_idxs: list[int], r_amts: list, r_dates: list,
    tol_abs: float, tol_pct: float, tol_days: int | None,
) -> tuple[list[int], list[int], int] | None:
    """
    Brute-force subset-sum: enumerate all size-≥2 subsets on each side, find
    pair with SUM(L) ≈ SUM(R) within tolerance and dates all-within.
    Prefer higher quality (smaller total rows as tiebreaker).
    Returns (l_subset_idxs, r_subset_idxs, quality) or None.
    Safe only because MAX_N2M_BUCKET_SIDE caps the enumeration size.
    """
    l_subsets = _enumerate_subsets(l_amts, l_dates, min_size=2)
    r_subsets = _enumerate_subsets(r_amts, r_dates, min_size=2)
    if not l_subsets or not r_subsets:
        return None

    # best: (quality, neg_total_rows, li_mask, ri_mask) — tuple comparison prefers
    # higher quality, then fewer rows, then lower li_mask, then lower ri_mask (stable).
    best: tuple[int, int, int, int] | None = None

    for li_mask, l_sum, l_dmin, l_dmax in l_subsets:
        tol_eff = max(tol_abs, abs(float(l_sum)) * tol_pct)
        for ri_mask, r_sum, r_dmin, r_dmax in r_subsets:
            delta = abs(float(l_sum) - float(r_sum))
            if delta > tol_eff:
                continue
            if tol_days is not None:
                # Correct "all-within" condition: every date in the union of both
                # subsets must be within tol_days of every other date. Equivalent to:
                #   max(all dates) - min(all dates) <= tol_days
                full_min = min(l_dmin, r_dmin)
                full_max = max(l_dmax, r_dmax)
                if (full_max - full_min).days > tol_days:
                    continue

            if tol_eff <= 0:
                quality = 100 if delta == 0 else 0
            else:
                quality = max(80, int(round(100 - (delta / tol_eff * 20))))

            total_rows = bin(li_mask).count("1") + bin(ri_mask).count("1")
            candidate = (quality, -total_rows, li_mask, ri_mask)
            if best is None or candidate > best:
                best = candidate

    if best is None:
        return None

    quality, _, li_mask, ri_mask = best
    li_out = [l_idxs[i] for i in range(len(l_idxs)) if li_mask & (1 << i)]
    ri_out = [r_idxs[i] for i in range(len(r_idxs)) if ri_mask & (1 << i)]
    return li_out, ri_out, quality


def _enumerate_subsets(amts, dates, min_size: int) -> list[tuple[int, float, object, object]]:
    """
    Return list of (bitmask, sum_amount, min_date, max_date) for each non-empty subset
    with size >= min_size.
    """
    n = len(amts)
    if n < min_size or n > MAX_N2M_BUCKET_SIDE:
        return []
    out: list[tuple[int, float, object, object]] = []
    # We don't enumerate 2^n directly for large n; MAX_N2M_BUCKET_SIDE guards this.
    for size in range(min_size, n + 1):
        for combo in combinations(range(n), size):
            mask = 0
            s = 0.0
            dmin = dates[combo[0]]
            dmax = dates[combo[0]]
            for i in combo:
                mask |= (1 << i)
                s += float(amts[i])
                if dates[i] < dmin: dmin = dates[i]
                if dates[i] > dmax: dmax = dates[i]
            out.append((mask, s, dmin, dmax))
    return out


def _find_amount_attr(pass_config: MatchPass) -> AttributeMatch | None:
    for a in pass_config.attributes_to_match:
        if a.left_attribute == "AMOUNT" and a.operator in ("EQUALS", "WITHIN"):
            return a
    return None


def _empty_result_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            "_row_idx": pl.UInt64,
            "MATCHING_ID": pl.Utf8,
            "MATCHED_BY_PASS": pl.Utf8,
            "MATCH_QUALITY": pl.Int64,
        }
    ).lazy()
```

- [ ] **Step 11.2: Reactivate MP_M2M in `tests/fixtures/config.json`**

Set `MP_M2M.status` to `"ACTIVE"`.

- [ ] **Step 11.3: Run and verify**

```bash
python -m rec_engine \
  --config tests/fixtures/config.json \
  --input  tests/fixtures/input.csv \
  --output /tmp/rec_output.csv \
  --cycle-date 2024-01-15 \
  --verbose
```

Expected:
- 1:1 pass: 3 groups
- 1:N pass: 1 group (500 credit → 200+300 debits)
- N:1 pass: 1 group (400+600 credits → 1000 debit)
- N:M pass: 1 group (100+200 credits = 300 ↔ 120+180 debits = 300, same NOSTRO-USD-004)

Total: 16 matched rows, 2 unmatched (rows 17, 18 — ORPHAN1/ORPHAN2).

Update expected output:
```bash
cp /tmp/rec_output.csv tests/fixtures/expected_output.csv
```

- [ ] **Step 11.4: Run integration test**

```bash
pytest tests/test_engine_integration.py -v
```
Expected: PASS.

- [ ] **Step 11.5: Commit**

```bash
git add rec_engine/matchers/many_to_many.py tests/fixtures/config.json tests/fixtures/expected_output.csv
git commit -m "feat: MANY_TO_MANY matcher via bucketed meet-in-the-middle subset-sum"
```

---

## Task 12: Scale-test data generator

**Files:**
- Create: `scripts/generate_synthetic.py`

- [ ] **Step 12.1: Create `scripts/generate_synthetic.py`** — synthesizes N-row fixtures

```python
"""Generate synthetic Nostro reconciliation CSVs for scale testing.

Usage:
    python scripts/generate_synthetic.py --rows 1000000 --output sample/synth_1m.csv
"""

from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows",    type=int, required=True, help="Total output rows (split half GL / half STMT)")
    p.add_argument("--output",  type=str, required=True)
    p.add_argument("--seed",    type=int, default=42)
    p.add_argument("--match-rate", type=float, default=0.95, help="Target fraction of rows that will match")
    args = p.parse_args()

    random.seed(args.seed)

    n_pairs = args.rows // 2
    matched_pairs = int(n_pairs * args.match_rate)
    unmatched_each_side = n_pairs - matched_pairs

    accounts = [f"NOSTRO-USD-{i:04d}" for i in range(100)]
    ccys = ["USD", "EUR", "GBP", "JPY"]
    base = date(2024, 1, 1)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", encoding="utf-8") as f:
        f.write("LS_TYPE,DR_CR_IND,STATUS,AMOUNT,CURRENCY,VALUE_DATE,BANK_ACCOUNT,REFERENCE\n")

        # Matched pairs: 1:1 — same reference on both sides, same amount
        for i in range(matched_pairs):
            amt = round(random.uniform(10, 100_000), 2)
            ccy = random.choice(ccys)
            vdt = base + timedelta(days=random.randint(0, 30))
            acct = random.choice(accounts)
            ref = f"TRX{i:010d}"
            f.write(f"GL,CR,OPEN,{amt:.4f},{ccy},{vdt.isoformat()},{acct},{ref}\n")
            f.write(f"STMT,DR,OPEN,{amt:.4f},{ccy},{vdt.isoformat()},{acct},{ref}\n")

        # Unmatched GLs
        for i in range(unmatched_each_side):
            amt = round(random.uniform(10, 100_000), 2)
            ccy = random.choice(ccys)
            vdt = base + timedelta(days=random.randint(0, 30))
            acct = random.choice(accounts)
            ref = f"ORPH_GL_{i:08d}"
            f.write(f"GL,CR,OPEN,{amt:.4f},{ccy},{vdt.isoformat()},{acct},{ref}\n")

        # Unmatched STMTs
        for i in range(unmatched_each_side):
            amt = round(random.uniform(10, 100_000), 2)
            ccy = random.choice(ccys)
            vdt = base + timedelta(days=random.randint(0, 30))
            acct = random.choice(accounts)
            ref = f"ORPH_STMT_{i:08d}"
            f.write(f"STMT,DR,OPEN,{amt:.4f},{ccy},{vdt.isoformat()},{acct},{ref}\n")

    print(f"Wrote {out} with {matched_pairs * 2 + unmatched_each_side * 2} rows.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 12.2: Generate 1M / 10M samples**

```bash
python scripts/generate_synthetic.py --rows 1000000  --output sample/synth_1m.csv
python scripts/generate_synthetic.py --rows 10000000 --output sample/synth_10m.csv
```

Expected: two CSV files created, sizes roughly 100MB / 1GB respectively.

- [ ] **Step 12.3: Commit**

```bash
git add scripts/generate_synthetic.py
echo "sample/synth_*.csv" >> .gitignore
git add .gitignore
git commit -m "tooling: synthetic data generator for scale testing"
```

---

## Task 13: Scale benchmark (1M, 10M, 100M)

**Files:**
- Create: `sample/config.json` (copied from test fixture, with only 1:1 active for scale testing)
- No new code.

- [ ] **Step 13.1: Create `sample/config.json`** — same as test fixture but only MP_O2O active (scale tests run on 1:1 only)

```bash
cp tests/fixtures/config.json sample/config.json
```

Edit `sample/config.json` and set `MP_O2M`, `MP_M2O`, `MP_M2M` to `"status": "INACTIVE"` (1:N / N:M scale behavior is not the demo focus; 1:1 is the main throughput claim).

- [ ] **Step 13.2: Run 1M benchmark**

```bash
time python -m rec_engine \
  --config sample/config.json \
  --input  sample/synth_1m.csv \
  --output /tmp/out_1m.csv \
  --cycle-date 2024-01-15 \
  --verbose
```

Record the wall-clock time and match rate. Expected: < 30 seconds total on a modern laptop, ≥ 94% match rate.

- [ ] **Step 13.3: Run 10M benchmark**

```bash
time python -m rec_engine \
  --config sample/config.json \
  --input  sample/synth_10m.csv \
  --output /tmp/out_10m.csv \
  --cycle-date 2024-01-15 \
  --verbose
```

Record time. Expected: a few minutes on a laptop.

- [ ] **Step 13.4: (Optional) Generate and run 100M**

```bash
python scripts/generate_synthetic.py --rows 100000000 --output sample/synth_100m.csv
time python -m rec_engine \
  --config sample/config.json \
  --input  sample/synth_100m.csv \
  --output /tmp/out_100m.csv \
  --cycle-date 2024-01-15 \
  --verbose
```

Disk + memory permitting. If your laptop OOMs, note this in the README and pitch the 100M / 1B number as an architectural claim backed by measured 10M performance + linear scaling under Polars streaming.

- [ ] **Step 13.5: Record results in `sample/benchmarks.md`**

```markdown
# Benchmarks

Run on: <machine spec>, Python <version>, Polars <version>.

| Rows | Match rate | Wall time | Per-pass time (1:1) |
|---|---|---|---|
| 1M   | 95% | <time>  | <time> |
| 10M  | 95% | <time>  | <time> |
| 100M | 95% | <time>  | <time> |
```

- [ ] **Step 13.6: Commit**

```bash
git add sample/config.json sample/benchmarks.md
git commit -m "bench: 1M / 10M (/100M) scale runs on synthetic fixtures"
```

---

## Task 14: README + demo polish

**Files:**
- Create: `README.md`
- Create: `sample/input.csv` (from test fixture, for demo)

- [ ] **Step 14.1: Copy demo input**

```bash
cp tests/fixtures/input.csv sample/input.csv
```

- [ ] **Step 14.2: Create `README.md`**

```markdown
# Reconciliation Matching Engine

A Nostro "fresh" reconciliation matching engine inspired by SmartStream TLM,
built in Python + Polars. Streaming LazyFrame architecture scales from 1M
rows (seconds) to 100M rows (minutes) to 1B rows (architectural claim on
appropriate hardware).

## Features

- Configurable match passes via JSON: populations, PQR, attribute-level rules
- Four cardinalities: `ONE_TO_ONE`, `ONE_TO_MANY`, `MANY_TO_ONE`, `MANY_TO_MANY`
- Operators: `EQUALS`, `WITHIN` (date + amount tolerances), `CONTAINS`, `SUBSTRING(col, start, length)`
- Quality scoring (0-100) with pass-level quality floor
- Pass ordering via `ruleOrder`; matched rows excluded from later passes
- TLM-style output: `STATUS`, `MATCH_DATE`, `MATCHING_ID`, `MATCHED_BY_PASS`, `MATCH_QUALITY`
- Sidecar `manifest.json` with per-pass breakdown

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Quick start

```bash
python -m rec_engine \
    --config sample/config.json \
    --input  sample/input.csv \
    --output /tmp/output.csv \
    --cycle-date 2024-01-15 \
    --verbose
```

Output: `/tmp/output.csv` + `/tmp/output.csv.manifest.json`.

## Architecture

See `docs/superpowers/specs/2026-04-21-rec-matching-engine-design.md` for full design notes.

Every stage is `pl.LazyFrame` → `pl.LazyFrame`. No `.collect()` except for (a) small matched-index tracking between passes, (b) N:M per-bucket subset-sum, (c) final `sink_csv`. This keeps Polars' optimizer in control of the full pipeline.

```
CLI → config.load → loader.scan_csv → engine.run_passes → writer.sink_csv
                                         │
                                         ├── populations.apply (LazyFrame → LazyFrame)
                                         ├── matchers.dispatch (LazyFrame → LazyFrame)
                                         └── scorer (Polars expr)
```

## Config reference

See `docs/superpowers/specs/2026-04-21-rec-matching-engine-design.md` §7.

## Running the integration test

```bash
pytest tests/test_engine_integration.py -v
```

## Benchmarks

See `sample/benchmarks.md`.

## Scope

**POC build.** Unit tests per module deliberately skipped. Subset-sum for partial 1:N / N:M matches is a documented future extension — the current MVP uses full-bucket aggregation for 1:N and N:1, and meet-in-the-middle within size-capped buckets for N:M.
```

- [ ] **Step 14.3: Commit**

```bash
git add README.md sample/input.csv
git commit -m "docs: README + demoable sample input"
```

- [ ] **Step 14.4: Final integration test run**

```bash
pytest tests/test_engine_integration.py -v
```
Expected: PASS.

- [ ] **Step 14.5: Final commit marker**

```bash
git log --oneline
```

Confirm commits for tasks 1-14 are present.

---

## Completion criteria

- [ ] `pytest tests/test_engine_integration.py` passes.
- [ ] `python -m rec_engine --config sample/config.json --input sample/input.csv --output /tmp/demo.csv --cycle-date 2024-01-15 --verbose` runs cleanly and produces a manifest.
- [ ] At least 1M and 10M scale runs recorded in `sample/benchmarks.md`.
- [ ] README is readable and walks a new engineer through the demo in < 2 minutes.

---

## Post-POC extensions (explicitly deferred; do not implement in this plan)

- Per-module unit tests (config, schema, expressions, populations, scorer, each matcher).
- Subset-sum for 1:N / N:M partial-bucket matches.
- Parquet input (`scan_parquet` swap in `loader.py`).
- Column-header renaming (source → canonical mapping in config).
- Carry-forward / in-transit items.
- Suspected-match tier.
