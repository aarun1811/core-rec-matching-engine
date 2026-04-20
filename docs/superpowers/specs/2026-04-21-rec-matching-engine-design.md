# Reconciliation Matching Engine — Design Spec

**Date**: 2026-04-21
**Status**: Approved for implementation
**Scope**: POC / demo build (4-5 hr window)

---

## 1. Overview

A Nostro "fresh" reconciliation matching engine, modeled on SmartStream TLM, built in Python + Polars.

**Input**
- A single CSV containing both sides (LEFT and RIGHT) of a reconciliation, distinguished by an `LS_TYPE` column.
- A JSON configuration defining match passes, populations, attribute rules, and tolerances.

**Output**
- An output CSV preserving all input rows + columns, with five columns populated/updated per row: `STATUS`, `MATCH_DATE`, `MATCHING_ID`, `MATCHED_BY_PASS`, `MATCH_QUALITY`.
- A sidecar `manifest.json` describing run totals, per-pass breakdown, and timing.

**Core behavior**
- Multi-pass matching in `ruleOrder` — earlier passes are tighter; unmatched rows fall through to looser passes.
- A matched row is excluded from subsequent passes.
- Supports four cardinalities: `ONE_TO_ONE`, `ONE_TO_MANY`, `MANY_TO_ONE`, `MANY_TO_MANY`.
- Per-match quality score 0–100; pass rejects matches below `pqr.quality`.

---

## 2. Non-goals (explicitly out of scope for POC)

- Carry-forward / in-transit items (fresh recon only)
- Manual match / forced match UI
- Suspected match workflow (no confidence-tier output)
- Excel / Parquet input (CSV only)
- Column header renaming (source uses canonical column names)
- Distributed execution (single-process)
- Unit tests per component (integration test only for POC; real build would add them)

---

## 3. Technology stack

- **Language**: Python 3.11+
- **Core library**: Polars (LazyFrame end-to-end)
- **CLI**: `typer` (or `argparse` if avoiding a dep)
- **Packaging**: `pyproject.toml` (setuptools backend)

### The LazyFrame invariant

Every stage takes `pl.LazyFrame` and returns `pl.LazyFrame`. No `.collect()` except:

1. Tiny collection of matched row indices between passes (control plane, bounded small).
2. N:M per-bucket subset-sum (bucket size capped, small collection per bucket).
3. Final `sink_csv()` + manifest summary.

This keeps the Polars optimizer in control of the whole pipeline, enables streaming execution at scale, and lets the same code path run at 1M, 10M, 100M, and (architecturally) 1B rows.

---

## 4. Architecture

### Folder layout

```
rec_engine/
├── cli.py               # CLI entry point
├── engine.py            # orchestrator
├── config.py            # JSON config loader + validator
├── schema.py            # canonical + extra column schema, type coercion
├── loader.py            # CSV → LazyFrame
├── populations.py       # leftPopulation / rightPopulation filter evaluation
├── expressions.py       # operator → Polars expr compiler
├── matchers/
│   ├── __init__.py      # dispatch by matchType
│   ├── base.py          # shared: hard-keys, tolerance attrs, MGID allocator
│   ├── one_to_one.py
│   ├── one_to_many.py
│   ├── many_to_one.py
│   └── many_to_many.py
├── scorer.py            # quality score 0-100
├── writer.py            # sink_csv + manifest.json
└── types.py             # dataclasses for config
tests/
├── fixtures/
│   ├── sample_input.csv
│   ├── config.json
│   └── expected_output.csv
└── test_engine_integration.py
sample/
├── config.json
├── input.csv
└── expected_output.csv
```

### Stage flow

```
CLI → config.load → loader.load(input) → engine.run_passes → writer.sink_and_manifest
```

Inside `engine.run_passes`, for each active pass sorted by `ruleOrder`:

```
    ┌── populations.filter_side(lf, LEFT)
    ├── populations.filter_side(lf, RIGHT)
    ├── matchers.dispatch(left_lf, right_lf, pass)
    │      → match_pairs LazyFrame
    ├── scorer.apply_quality_floor(match_pairs, pass)
    ├── collect matched row indices (small)
    ├── append to running match_groups LazyFrame
    └── exclude matched rows from lf for next pass
```

Final output = `input_lf.join(match_groups_lf, on="_row_idx", how="left")` with column fill logic → `sink_csv`.

---

## 5. Canonical input schema

Fixed columns the engine assumes; type-coerced at load.

| Column | Polars dtype | Purpose |
|---|---|---|
| `LS_TYPE` | `Utf8` | Side indicator (free-form string; compared via population filters) |
| `DR_CR_IND` | `Utf8` | `DR` / `CR` |
| `STATUS` | `Utf8` | Lifecycle — `OPEN` on input |
| `AMOUNT` | `Decimal(18,4)` | Unsigned positive; direction carried by `DR_CR_IND` |
| `CURRENCY` | `Utf8` | ISO 4217 |
| `VALUE_DATE` | `Date` | ISO `YYYY-MM-DD` |
| `BANK_ACCOUNT` | `Utf8` | Sub-account — implicit hard key every pass |
| `REFERENCE` | `Utf8` | Transaction reference |

Plus any columns declared in `schema.extraColumns` (user-declared types).

**Internal column (stripped at write)**: `_row_idx` (`UInt64`) added via `pl.with_row_index()` after load.

---

## 6. Output contract

Five columns populated on output. If already present in input → updated in place. Otherwise → appended in the order below.

| Column | Matched rows | Unmatched rows |
|---|---|---|
| `STATUS` | `MATCHED` | `UNMATCHED` |
| `MATCH_DATE` | `cycleDate` | null |
| `MATCHING_ID` | `MGID_<setId>_<YYYYMMDD>_<seq>` | null |
| `MATCHED_BY_PASS` | pass ID | null |
| `MATCH_QUALITY` | 0-100 integer | null |

**MATCHING_ID format**: `MGID_USD_NOSTRO_CITIBANK_20240115_000042`
- `setId`: validated against `[A-Za-z0-9_-]+`
- Sequence: per-run counter, zero-padded to 6 digits, global across all passes.

**Row order**: output preserves input row order (enforced via `_row_idx` on final join).

**Column order**: all input columns first, then any missing output columns appended in the order listed above.

---

## 7. Config JSON schema

### Top-level

```jsonc
{
   "setId":       "USD_NOSTRO_CITIBANK",
   "cycleDate":   "2024-01-15",            // optional; CLI can override
   "schema": {
      "extraColumns": {
         "TRADE_ID":   { "type": "string" },
         "TRADE_DATE": { "type": "date", "format": "%Y-%m-%d" },
         "FEE_AMOUNT": { "type": "decimal", "precision": 18, "scale": 4 }
      }
   },
   "matchPasses": [ /* see below */ ]
}
```

Extra-column types: `string`, `integer`, `decimal`, `date`, `datetime`, `boolean`.

### Match pass

```jsonc
{
   "matchPassId":   "MP_O2O_LEDGER_STMT",
   "matchPassName": "One-to-One : GL Credit vs Stmt Debit",
   "matchType":     "ONE_TO_ONE",          // | ONE_TO_MANY | MANY_TO_ONE | MANY_TO_MANY
   "status":        "ACTIVE",               // | INACTIVE (skipped)

   "populationRule": {
      "leftPopulation":  { "populationName": "...", "filters": [ /* ... */ ] },
      "rightPopulation": { "populationName": "...", "filters": [ /* ... */ ] }
   },

   "pqr": {
      "priority":  10,
      "quality":   95,                      // quality floor (match rejected if below)
      "ruleOrder": 1                        // execution order (uniqueness enforced)
   },

   "attributesToMatch": [
      {
         "leftAttribute":  "AMOUNT",
         "rightAttribute": "AMOUNT",
         "operator":       "EQUALS",        // | WITHIN | CONTAINS
         "mandatory":      true,
         "aggregation":    null             // null for 1:1; {side, function} otherwise
      },
      {
         "leftAttribute":    "VALUE_DATE",
         "rightAttribute":   "VALUE_DATE",
         "operator":         "WITHIN",
         "toleranceDays":    1,
         "mandatory":        true
      },
      {
         "leftAttribute":    "SUBSTRING(REFERENCE, 4, 10)",
         "rightAttribute":   "SUBSTRING(REFERENCE, 4, 10)",
         "operator":         "CONTAINS",
         "mandatory":        false
      }
   ]
}
```

### Population filter operators

| Operator | Value shape | Semantics |
|---|---|---|
| `=` / `!=` / `>` / `<` / `>=` / `<=` | scalar | standard comparison |
| `IN` | array | membership |
| `LIKE` | SQL-style string with `%` `_` | pattern match |

### Match attribute operators

| Operator | Extra params | Semantics |
|---|---|---|
| `EQUALS` | — | exact equality after any transform |
| `WITHIN` (date) | `toleranceDays` | `abs(delta_days) ≤ toleranceDays` |
| `WITHIN` (numeric) | `toleranceAmount` and/or `tolerancePercent` | `abs(delta) ≤ max(toleranceAmount, tolerancePercent × value)` |
| `CONTAINS` | — | `left.contains(right)` — directional |

### Aggregation block

Required on any `EQUALS` / `WITHIN` numeric attribute in non-1:1 passes:

```jsonc
"aggregation": {
   "side":     "RIGHT",      // | LEFT
   "function": "SUM"          // | COUNT | MIN | MAX | AVG
}
```

**Semantics**: aggregate that attribute across rows on the named side within each hard-key bucket, then compare to the single-side value (with tolerance if `WITHIN`).

**Non-amount attributes in 1:N / N:M** (e.g. `VALUE_DATE WITHIN`): "all-within" rule — every row on the N-side must individually be within tolerance of the 1-side.

### SUBSTRING expression

```
SUBSTRING(column, start, length)
```

- 1-based `start` (SQL convention)
- `length` required
- Applied symmetrically on both sides when used on both `leftAttribute` and `rightAttribute`

### Config validation rules (enforced at load)

1. `matchPassId` unique across config
2. `ruleOrder` unique among ACTIVE passes
3. `matchType` is one of the four supported values
4. Non-1:1 passes: every `EQUALS`/`WITHIN` on a numeric attribute must have `aggregation` set
5. `WITHIN` on date: `toleranceDays` required
6. `WITHIN` on numeric: `toleranceAmount` or `tolerancePercent` (or both) required
7. All referenced attributes exist in canonical schema or `extraColumns`

Any failure → exit code 1 with a specific message.

---

## 8. Matcher algorithms

### Shared primitives (`matchers/base.py`)

- `hard_keys(pass_config)` — implicit `BANK_ACCOUNT` + every `EQUALS` attribute with `aggregation is None`
- `tolerance_attrs(pass_config)` — every `WITHIN` attribute (applied post-join)
- `soft_attrs(pass_config)` — every `mandatory: false` attribute (scoring only)
- `MGIDAllocator` — yields `MGID_<setId>_<YYYYMMDD>_<seq>` globally across passes

### ONE_TO_ONE

1. Inner join left / right on hard keys.
2. Apply tolerance filters (`WITHIN`) as post-join predicates.
3. Apply `CONTAINS` filters.
4. Compute quality score per candidate; drop below `pqr.quality`.
5. Greedy 1:1 uniqueness:
   - Sort by quality descending.
   - `group_by(left_idx).head(1)` — best candidate per left row.
   - `group_by(right_idx).head(1)` — resolve cross-collisions on the right.
6. Allocate `matching_id` per surviving pair.

Greedy, not optimal (bipartite assignment). TLM-standard. O(N³) Hungarian is not viable at 100M.

### ONE_TO_MANY (and MANY_TO_ONE — mirrored)

1. Bucket the many-side: `group_by(hard_keys).agg([row_idx list, SUM(agg_attr), min/max date, count])`.
2. Inner join with one-side on hard keys.
3. Amount-aggregation filter: `abs(one_amount - right_sum) ≤ tolerance`.
4. "All-within" date filter: both `right_date_min` and `right_date_max` must satisfy `abs(delta_days) ≤ toleranceDays` against the one-side date.
5. Score, apply quality floor, greedy dedup (left uniqueness + right-bucket uniqueness).
6. Explode bucket row index lists → long-form `match_pairs` (1 + N rows per group, shared matching_id).

**MVP implementation uses full-bucket aggregation** (aggregates all many-side rows within each hard-key bucket). Subset-sum for partial bucket matches is a documented future extension — clean hook in `one_to_many.py`.

MANY_TO_ONE is a thin wrapper that invokes 1:N with axes swapped.

### MANY_TO_MANY

Hard-constrained:
- `L_n ≥ 2` AND `R_n ≥ 2`
- `L_n ≤ MAX_N2M_BUCKET_SIDE` AND `R_n ≤ MAX_N2M_BUCKET_SIDE` (default 20)
- Requires at least one hard-key filter each side (validated at config load)

Algorithm:
1. Bucket both sides by hard keys.
2. Join buckets on hard keys → one row per `(left_bucket, right_bucket)` pair.
3. Apply size filter; collect warnings for any skipped bucket.
4. Per bucket: collect rows (small, bounded), run meet-in-the-middle subset-sum:
   - Enumerate `2^L_n` left-subsets and `2^R_n` right-subsets.
   - Sort one side's sums; binary-search for matching sums on the other side within tolerance.
   - Apply all-within date tolerance.
   - Pick best match per bucket (highest quality, fewest total rows as tiebreaker).
5. Wrap results back as a `LazyFrame` and union with pass output.

---

## 9. Quality scoring (`scorer.py`)

### Formula

```
score = sum(attr_score_i × weight_i) / sum(weight_i)
```

**Weights**: mandatory attribute = 10, optional = 3.

**Per-attribute score**:

| Operator | Exact hit | Partial (within tolerance) | Miss |
|---|---|---|---|
| `EQUALS` | 100 | — | 0 |
| `WITHIN` (date) | 100 at delta=0 | linear `100 - (delta/tolerance × 20)`, floor 80 | 0 |
| `WITHIN` (numeric) | 100 at delta=0 | linear `100 - (delta/tolerance × 20)`, floor 80 | 0 |
| `CONTAINS` | 100 if contains | — | 0 |

### Implementation

Pure Polars expressions compiled from the pass config — one per attribute, then a weighted-average expression. Stays fully lazy.

### Notes

- 80 floor on `WITHIN` hits prevents trivial penalties on FX rounding.
- A `0` on a mandatory is impossible by construction — rows with 0 score on mandatory attributes have already been filtered out by the match filter itself.
- Optional attributes can pull score down slightly but are low-weighted — a good mandatory match with a failed optional still scores ~87.

---

## 10. CLI

```bash
python -m rec_engine \
    --config      ./config.json \
    --input       ./input.csv \
    --output      ./output.csv \
    --cycle-date  2024-01-15 \
    --verbose
```

### Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | config validation error |
| 2 | input file error (missing, missing canonical column, type coercion failure) |
| 3 | runtime error during matching |
| 4 | output write error |

### Verbose output

```
[cfg]  Loaded 3 match passes (2 ACTIVE)
[load] Scanned input.csv
[pass] MP_O2O_LEDGER_STMT (1:1)  ... matched 420,000 groups  [12.1s]
[pass] MP_O2M_LEDGER_STMT (1:N)  ... matched  35,000 groups  [24.6s]
[sink] Written output.csv + manifest.json  (1,000,000 rows)
[done] 87.4s total  |  95.00% match rate
```

---

## 11. Manifest (sidecar JSON)

Emitted as `<output_path>.manifest.json`:

```jsonc
{
   "setId":         "USD_NOSTRO_CITIBANK",
   "cycleDate":     "2024-01-15",
   "configPath":    "/abs/path/config.json",
   "inputPath":     "/abs/path/input.csv",
   "outputPath":    "/abs/path/output.csv",
   "runTimestamp":  "2026-04-21T10:30:00Z",
   "durationSec":   87.4,

   "totals": {
      "inputRows":     1000000,
      "matchedRows":   950000,
      "unmatchedRows": 50000,
      "matchGroups":   456500
   },

   "passBreakdown": [
      {
         "passId":        "MP_O2O_LEDGER_STMT",
         "passName":      "...",
         "matchType":     "ONE_TO_ONE",
         "ruleOrder":     1,
         "matchedGroups": 420000,
         "matchedRows":   840000,
         "avgQuality":    98.3,
         "durationSec":   12.1
      }
   ],

   "unmatchedBreakdown": {
      "bySide":     { "GL": 20000, "STMT": 30000 },
      "byCurrency": { "USD": 48000, "EUR": 2000 }
   }
}
```

---

## 12. Error handling philosophy

Fail fast at boundaries; trust internal state.

- Config / input / output failures: hard exits with specific messages.
- No retries, no silent fallbacks. A Nostro recon must be reproducible — a partial run is worse than a failed run.
- Internal assertion failures inside matchers: exit 3, print pass ID + traceback.

---

## 13. Testing (POC level)

Single end-to-end integration test (`tests/test_engine_integration.py`):

- Hand-crafted `input.csv` (~100 rows covering each cardinality)
- `config.json` with four passes (one per matchType)
- `expected_output.csv` — byte-for-byte expected output
- Test runs the engine and asserts output matches.

Plus a scale sanity run (not CI): 1M / 10M / 100M generated synthetic input, verify perf targets + match rate is sensible.

Unit tests are deliberately skipped for the POC. Real build would add them per module.

---

## 14. Performance targets

| Scale | Per-pass target | Peak memory |
|---|---|---|
| 1M | < 1 sec | < 500 MB |
| 10M | 5–15 sec | < 4 GB |
| 100M | 1–3 min | streaming, ~8 GB peak |
| 1B (claimed, not demoed live) | 15–40 min on beefy box with Parquet input | streaming |

Claims beyond 100M are **architectural** — same code path, requires Parquet input + larger RAM. The linear scaling story is defensible precisely because the engine is LazyFrame-streaming end-to-end.

---

## 15. Build-order plan (4-5 hr window)

| Hour | Deliverable |
|---|---|
| 0:00–0:20 | Project scaffold, `pyproject.toml`, `types.py`, `config.py` |
| 0:20–1:00 | `schema.py`, `loader.py`, `populations.py`, `expressions.py` |
| 1:00–1:45 | `matchers/one_to_one.py` + `scorer.py` + integration test skeleton |
| 1:45–2:30 | `matchers/one_to_many.py` + `matchers/many_to_one.py` |
| 2:30–3:15 | `matchers/many_to_many.py` + `writer.py` + manifest |
| 3:15–3:45 | `cli.py`, end-to-end integration test passes green |
| 3:45–4:30 | Scale testing at 1M / 10M / 100M, sample files for demo |
| 4:30–5:00 | README, demo walkthrough, buffer |

1:1 working end-to-end at the 1:45 mark is the critical checkpoint. Everything after that is mechanical.

---

## 16. Future extensions (explicitly deferred)

- **Subset-sum for 1:N / N:M partial-bucket matches** (bitmask DP per bucket).
- **Parquet input** — `scan_parquet` swap in `loader.py`, no other changes.
- **Carry-forward / in-transit items** — extra status values, cycle-chaining logic.
- **Suspected matches** — lower quality threshold + review flag in output.
- **Column header renaming** — source-to-canonical column map in config.
- **Unit test suite** — per-module tests, 80%+ line coverage.
- **Hungarian-algorithm 1:1** — for tight disputes where greedy isn't optimal.
