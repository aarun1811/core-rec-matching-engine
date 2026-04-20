# Reconciliation Matching Engine

A Nostro "fresh" reconciliation matching engine inspired by SmartStream TLM,
built in Python + Polars. Streaming LazyFrame architecture scales from 1M
rows (~1 second) to 10M rows (~12 seconds) to 100M+ rows on appropriately
sized hardware, with a 1B architectural claim backed by linear scaling.

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

Requires Python 3.11+. Polars is pinned to 0.20.x for byte-exact fixture stability.

## Quick start

```bash
python -m rec_engine \
    --config sample/config_demo.json \
    --input  sample/input.csv \
    --output /tmp/output.csv \
    --cycle-date 2024-01-15 \
    --verbose
```

Output:
- `/tmp/output.csv` — input rows with 5 populated/updated columns (STATUS, MATCH_DATE, MATCHING_ID, MATCHED_BY_PASS, MATCH_QUALITY)
- `/tmp/output.csv.manifest.json` — per-pass breakdown of matched groups / rows / avg quality / duration

## Architecture

See `docs/superpowers/specs/2026-04-21-rec-matching-engine-design.md` for full design notes.

Every stage is `pl.LazyFrame` → `pl.LazyFrame`. The only `.collect()` calls are (a) small matched-index tracking between passes, (b) N:M per-bucket subset-sum (bucket size capped at 10 per side), (c) final `collect(streaming=True).write_csv()` for output (Polars 0.20.x has no `sink_csv`). This keeps Polars' optimizer in control of the full pipeline and enables streaming execution at scale.

```
CLI → config.load → loader.scan_csv → engine.run_passes → writer.sink_csv
                                         │
                                         ├── populations.apply (LazyFrame → LazyFrame)
                                         ├── matchers.dispatch (LazyFrame → LazyFrame)
                                         └── scorer (Polars expr)
```

Module layout:

```
rec_engine/
├── cli.py           # typer CLI entry point
├── engine.py        # orchestrator: load → passes in ruleOrder → write
├── config.py        # JSON loader with cross-pass validation
├── schema.py        # canonical + extra column dtype map
├── loader.py        # pl.scan_csv with explicit dtypes + _row_idx
├── populations.py   # filter evaluation: =, !=, IN, LIKE, etc.
├── expressions.py   # operator / SUBSTRING → pl.Expr compiler
├── scorer.py        # quality score 0-100 as a pl.Expr
├── writer.py        # output assembly + manifest.json
├── types.py         # config dataclasses
└── matchers/
    ├── base.py          # hard keys, soft/tolerance attrs, MGIDAllocator
    ├── one_to_one.py    # 1:1 greedy hash-join
    ├── one_to_many.py   # 1:N full-bucket aggregation
    ├── many_to_one.py   # N:1 via axis-swapped 1:N
    └── many_to_many.py  # N:M bucketed brute-force subset-sum
```

## Config reference

See `docs/superpowers/specs/2026-04-21-rec-matching-engine-design.md` §7 for the full config JSON schema. Example in `sample/config.json`.

Match types: `ONE_TO_ONE`, `ONE_TO_MANY`, `MANY_TO_ONE`, `MANY_TO_MANY`.

Aggregation blocks (required on `ONE_TO_MANY` / `MANY_TO_ONE` numeric attrs):
```json
"aggregation": { "side": "RIGHT", "function": "SUM" }
```
POC supports `function: SUM` only.

## Running the integration test

```bash
pytest tests/test_engine_integration.py -v
```

The test runs the CLI via subprocess and compares output byte-for-byte against `tests/fixtures/expected_output.csv`.

## Benchmarks

See `sample/benchmarks.md` for measured numbers on an M4 MacBook.

## Scope

**POC build.** Unit tests per module deliberately skipped — only one end-to-end integration test. Deferred:
- Subset-sum for partial 1:N / N:M matches (MVP uses full-bucket aggregation for 1:N / N:1; bucket-limited brute force for N:M).
- Parquet input (`scan_parquet` swap in `loader.py`).
- Column-header renaming (source → canonical mapping in config).
- Carry-forward / in-transit items (current engine assumes fresh recon).
- Suspected-match tier.

## Demo

Run the demo end-to-end and inspect the output:

```bash
python -m rec_engine \
  --config sample/config_demo.json \
  --input  sample/input.csv \
  --output /tmp/demo.csv \
  --cycle-date 2024-01-15 \
  --verbose

cat /tmp/demo.csv
cat /tmp/demo.csv.manifest.json
```

Walks through 18 sample rows, demonstrates all 4 cardinalities + orphans, and shows the manifest summary.
