# Reconciliation Matching Engine

A Nostro "fresh" reconciliation matching engine inspired by SmartStream TLM,
built in Python + Polars. Streaming LazyFrame architecture scales from 1M
rows (~1.3 seconds) to 10M rows (~16 seconds) with all 4 cardinalities active,
to 100M+ rows on appropriately sized hardware.

Includes a FastAPI backend for saving configs, triggering runs, paginating
matches/breaks with filters, and downloading outputs — see the **API backend**
section below.

## Features

- Configurable match passes via JSON: populations, PQR, attribute-level rules
- Four cardinalities: `ONE_TO_ONE`, `ONE_TO_MANY`, `MANY_TO_ONE`, `MANY_TO_MANY`
- Operators: `EQUALS`, `WITHIN` (date + amount tolerances), `CONTAINS`, `SUBSTRING(col, start, length)`
- Quality scoring (0-100) with pass-level quality floor
- Pass ordering via `ruleOrder`; matched rows excluded from later passes
- TLM-style output: `STATUS`, `MATCH_DATE`, `MATCHING_ID`, `MATCHED_BY_PASS`, `MATCH_QUALITY`
- Sidecar `manifest.json` with per-pass breakdown
- HTTP API (FastAPI) for config management, runs, and paginated result queries

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
rec_engine/                  # core engine library
├── cli.py                   # typer CLI entry point
├── engine.py                # orchestrator: load → passes in ruleOrder → write
├── config.py                # JSON loader with cross-pass validation
├── schema.py                # canonical + extra column dtype map
├── loader.py                # pl.scan_csv with explicit dtypes + _row_idx
├── populations.py           # filter evaluation: =, !=, IN, LIKE, etc.
├── expressions.py           # operator / SUBSTRING → pl.Expr compiler
├── scorer.py                # quality score 0-100 as a pl.Expr
├── writer.py                # output assembly + manifest.json
├── types.py                 # config dataclasses
└── matchers/
    ├── base.py                # hard keys, tolerance attrs, MGIDAllocator
    ├── one_to_one.py          # 1:1 greedy hash-join
    ├── one_to_many.py         # 1:N full-bucket aggregation
    ├── many_to_one.py         # N:1 via axis-swapped 1:N
    └── many_to_many.py        # N:M bucketed brute-force subset-sum

rec_engine_api/              # FastAPI backend (depends on rec_engine)
├── main.py                  # app + routes + exception handlers
├── schemas.py               # pydantic request/response models
├── storage.py               # filesystem read/write for configs + runs
├── runs.py                  # run execution wrapper
├── filters.py               # minimal filter DSL over output CSV
└── cli.py                   # uvicorn launch entrypoint
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

## Generating synthetic test data

Scale-test fixtures (`sample/synth_*.csv`) are gitignored — regenerate them locally with:

```bash
python scripts/generate_synthetic.py --rows 1000000  --output sample/synth_1m.csv
python scripts/generate_synthetic.py --rows 10000000 --output sample/synth_10m.csv
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--rows` | required | Approximate total rows (may be ±3 due to variable N in 1:N / N:1 groups) |
| `--output` | required | Output CSV path (parent dirs auto-created) |
| `--seed` | `42` | Random seed for reproducibility |
| `--match-rate` | `0.97` | **Deprecated**; ignored (mix is fixed, see below) |

Fixed mix (~97% matched, ~3% orphans):

| Pattern | % of rows | Matched by |
|---|---|---|
| 1:1 exact | 70% | MP_O2O |
| 1:1 with date tolerance (STMT +1 day) | 10% | MP_O2O |
| 1:N (1 GL + 2-4 STMTs summing to GL) | 8% | MP_O2M |
| N:1 (2-4 GLs summing to 1 STMT) | 8% | MP_M2O |
| N:M (2 GLs + 2 STMTs, balanced sums) | 1% | MP_M2M |
| Orphans (GL-only + STMT-only) | 3% | — (UNMATCHED) |

Aggregate groups (1:N / N:1 / N:M) use synthetic unique `BANK_ACCOUNT` names (`NOSTRO-AGG-{kind}-{idx}`) so bucket capacity scales with row count. 1:1 pairs and orphans share the main 100-account pool. Deterministic given the seed.

Then benchmark with all 4 passes active:

```bash
time python -m rec_engine \
  --config sample/config.json \
  --input  sample/synth_10m.csv \
  --output /tmp/out_10m.csv \
  --cycle-date 2024-01-15 \
  --verbose
```

## API backend

HTTP wrapper around the engine with endpoints for saving configs, triggering runs, and paginating matches/breaks with filters.

### Start the server

```bash
source .venv/bin/activate
pip install -e ".[dev]"

# Option A: console script
rec-engine-api

# Option B: module
python -m rec_engine_api

# Option C: uvicorn directly (dev auto-reload)
uvicorn rec_engine_api.main:app --reload --port 8000
```

Server binds to `127.0.0.1:8000` by default. Override via `REC_ENGINE_API_HOST` / `REC_ENGINE_API_PORT` env vars (e.g. `REC_ENGINE_API_PORT=9000 rec-engine-api`). Or use uvicorn directly for more control: `uvicorn rec_engine_api.main:app --port 9000 --host 0.0.0.0 --reload`.

Storage root defaults to the current working directory. Override via `REC_ENGINE_STORAGE_ROOT`. Configs live under `{root}/configs/`, run artifacts under `{root}/runs/{runId}/`. Both directories are gitignored.

**CORS**: permissive by default (`Access-Control-Allow-Origin: *`) so a browser-based UI on any local port (Vite 5173, CRA 3000, etc.) can call the API without preflight failures. Override with the `REC_ENGINE_API_CORS_ORIGINS` env var — comma-separated list of allowed origins. In prod, set it to a specific origin (e.g. `REC_ENGINE_API_CORS_ORIGINS=https://recon.example.com`). When you supply specific origins, `allow_credentials=True` is enabled automatically; the wildcard mode has credentials off (browser spec constraint).

Interactive docs: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/configs/validate` | Validate a match-pass JSON (no save) |
| `POST` | `/configs` | Validate + save, returns `configId` |
| `GET`  | `/configs` | List saved configs |
| `GET`  | `/configs/{configId}` | Fetch full config |
| `POST` | `/runs` | Execute a run against a server-side input CSV path |
| `GET`  | `/runs` | List runs |
| `GET`  | `/runs/{runId}` | Fetch run manifest + meta |
| `GET`  | `/runs/{runId}/matches` | Paginated matched rows, optional filters |
| `GET`  | `/runs/{runId}/breaks` | Paginated unmatched rows, optional filters |
| `GET`  | `/runs/{runId}/download/{kind}` | Download `output` (CSV) or `manifest` (JSON) |

`/matches` and `/breaks` query params: `limit` (default 100, max 10000), `offset` (default 0), `matched_by_pass`, `currency`, `bank_account` (all optional, exact equality).

### Example flow

```bash
# 1. Save a config
curl -s -X POST http://127.0.0.1:8000/configs \
  -H 'Content-Type: application/json' \
  --data @tests/fixtures/config.json | python -m json.tool
# → {"configId": "cfg_USD_NOSTRO_CITIBANK_20260421T...", ...}

# 2. Trigger a run
curl -s -X POST http://127.0.0.1:8000/runs \
  -H 'Content-Type: application/json' \
  -d '{
    "configId": "cfg_USD_NOSTRO_CITIBANK_20260421T143012Z",
    "inputPath": "/absolute/path/to/input.csv",
    "cycleDate": "2024-01-15"
  }' | python -m json.tool
# → {"runId": "run_...", "metrics": {...}, "outputPath": "./runs/.../output.csv", ...}

# 3. Page through matches, filtered by pass
curl -s 'http://127.0.0.1:8000/runs/run_.../matches?matched_by_pass=MP_O2M&limit=50'

# 4. Download the output CSV
curl -s 'http://127.0.0.1:8000/runs/run_.../download/output' -o downloaded_output.csv
```

### Errors

All endpoints return `{"error": "..."}` with appropriate HTTP status:
- `400` — config validation / missing input path / bad request body
- `404` — config or run not found
- `422` — FastAPI query param validation (e.g. `limit` out of range)
- `500` — unexpected server error

### Running the API integration test

```bash
pytest tests/test_api_integration.py -v
```

Exercises all 10 endpoints end-to-end against `tests/fixtures/config.json` + `tests/fixtures/input.csv`. Uses a `tmp_path`-scoped `REC_ENGINE_STORAGE_ROOT` so test storage doesn't pollute the project root.

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
