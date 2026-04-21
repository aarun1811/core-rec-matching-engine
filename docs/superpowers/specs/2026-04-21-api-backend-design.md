# API Backend + Full-Cardinality Synthetic Data — Design

**Date**: 2026-04-21
**Branch**: `feat/api-backend`
**Scope**: FastAPI backend for the reconciliation engine, plus synthetic data generator extension so scale tests exercise all 4 matcher cardinalities.

---

## 1. Goals

1. HTTP API wrapping the existing CLI — configs in, CSV runs, matches/breaks out.
2. Synthetic data generator that produces all 4 cardinality patterns.
3. Update `sample/config.json` to exercise all 4 passes for scale benchmarks.
4. Keep the existing CLI, integration test, and core engine untouched (zero regression risk).

## 2. Scope

### In
- FastAPI app with 6 endpoints (validate/save config, run, list, download, paginated matches, paginated breaks).
- Filesystem-only storage (`./configs/`, `./runs/`).
- Synchronous runs (HTTP blocks until done).
- Server-side input path (user passes a path; API reads it).
- Minimal filter DSL: `matched_by_pass`, `currency`, `bank_account` equality; pagination.
- Synthetic script: produces 1:1 (70% exact + 10% date-tol), 1:N (8%), N:1 (8%), N:M (1%), orphans (3%).
- Updated `sample/config.json` with all 4 passes ACTIVE.

### Out (explicit)
- Auth / multi-user (single-user POC, no tokens).
- Async / queued runs (sync only; clients must handle long-running HTTP).
- File upload (server-side path only).
- Rich filter DSL (no SQL predicates, no date ranges).
- Database (SQLite skipped; filesystem is enough).
- Historical run retention policy (no auto-cleanup).

## 3. Backend layout

```
rec_engine_api/
├── __init__.py
├── main.py                  # FastAPI app, CORS, route mounting
├── schemas.py               # Pydantic request/response models
├── storage.py               # read/write configs + run metadata on disk
├── runs.py                  # run execution + result caching
├── filters.py               # minimal filter DSL over output CSV
└── cli.py                   # uvicorn entry: python -m rec_engine_api
```

Why split into its own package? Keeps the engine (`rec_engine/`) a pure library; the API is an adapter that depends on it. Clean boundaries.

## 4. Endpoints

| Method | Path | Purpose | Body / Query | Response |
|---|---|---|---|---|
| `POST` | `/configs/validate` | Validate a match-pass JSON without saving | JSON body | `{valid: bool, errors: [...]}` |
| `POST` | `/configs` | Validate + save, returns config ID | JSON body | `{configId, savedAt}` |
| `GET`  | `/configs` | List saved configs | — | `[{configId, setId, savedAt, ...}]` |
| `GET`  | `/configs/{configId}` | Fetch one config | — | full JSON |
| `POST` | `/runs` | Run a config against a server-side CSV path | `{configId, inputPath, cycleDate?}` | `{runId, metrics, outputPath, manifestPath}` |
| `GET`  | `/runs` | List runs | — | `[{runId, configId, startedAt, totals}]` |
| `GET`  | `/runs/{runId}` | Fetch run metadata | — | manifest JSON |
| `GET`  | `/runs/{runId}/matches` | Paginated matches | `?limit&offset&matched_by_pass&currency&bank_account` | `{rows, total, limit, offset}` |
| `GET`  | `/runs/{runId}/breaks` | Paginated breaks (unmatched rows) | same filters | same shape |
| `GET`  | `/runs/{runId}/download/{kind}` | Download `output` CSV or `manifest` JSON | — | binary file |

All endpoints return `{"error": "..."}` with appropriate HTTP status on failure.

## 5. Storage layout

```
<project root>/
├── configs/
│   └── {configId}.json              # saved configs
└── runs/
    └── {runId}/
        ├── manifest.json            # run summary
        ├── output.csv               # full output CSV
        └── meta.json                # {configId, inputPath, cycleDate, startedAt}
```

- `configId`: `cfg_<setId>_<timestamp>` (e.g. `cfg_USD_NOSTRO_CITIBANK_20260421T143012`)
- `runId`: `run_<timestamp>_<6-char random>` (e.g. `run_20260421T143012_a1b2c3`)

Both are idempotent names (reruns create new IDs). No deletion endpoint in v1.

## 6. Filter & pagination contract

For `/runs/{runId}/matches` and `/breaks`:

**Query params**:
- `limit` (default 100, max 10000)
- `offset` (default 0)
- `matched_by_pass` — exact string match on `MATCHED_BY_PASS` column
- `currency` — exact match on `CURRENCY`
- `bank_account` — exact match on `BANK_ACCOUNT`

**Response shape**:
```json
{
  "total": 475000,
  "limit": 100,
  "offset": 0,
  "rows": [ { ... first 100 rows as {col: value} ... } ]
}
```

**Implementation**: Polars `scan_csv` on `runs/{runId}/output.csv` → filter by status (MATCHED for `/matches`, UNMATCHED for `/breaks`) + optional filters → `.slice(offset, limit).collect()`. Lazy, fast, no in-memory full-file load.

## 7. Synthetic script extension

New script shape — same CLI (`--rows`, `--output`, `--seed`, `--match-rate` still honored) plus a fixed internal mix:

```
For the pair-generating portion:
  - 70% 1:1 exact (GL + STMT with same ref/amount/date/account/currency)
  - 10% 1:1 with date tolerance (STMT date shifted ±1 day)
  - 8%  1:N (1 GL + 2-4 STMT rows summing to GL amount)
  - 8%  N:1 (2-4 GL rows summing to 1 STMT amount)
  - 1%  N:M (2 GL + 2 STMT rows, GL sum == STMT sum)

Orphans (3% of total rows, split):
  - ~half GL only, ~half STMT only
```

**Determinism**: same seed → same row sequence. Proportions are integer rounded but stable.

**Scale impact**:
- 1M rows → 10K N:M pair-buckets. Each brute-forces 2^2 × 2^2 = 16 subset pairs. Trivial.
- 10M rows → 100K N:M pair-buckets. Python-side loop adds ~30-60s to the 10M run. Acceptable.
- 100M rows → 1M N:M pair-buckets. Not tested live. Documented caveat.

## 8. `sample/config.json` update

Current state: only `MP_O2O` ACTIVE (scale-only 1:1 benchmark).
After update: all 4 passes ACTIVE — scale runs now exercise all matchers.

Rationale: once synthetic data contains all patterns, the scale benchmark demonstrates the full POC.

## 9. Config validation + save behavior

`POST /configs/validate`:
- Body: full match-pass JSON (same shape as `tests/fixtures/config.json`)
- Reuses `rec_engine.config._parse_config()` without touching disk
- Returns `{valid: true}` or `{valid: false, errors: ["Pass MP_O2M attr AMOUNT: ..."]}` — multiple errors possible (extend current loader to accumulate instead of raise-on-first? No — keep raise-on-first for POC; return a single error).

`POST /configs`:
- Same validation
- On success: write to `configs/{configId}.json`, return `{configId, savedAt}`

`POST /runs`:
- Body: `{configId, inputPath, cycleDate?}`
- Load config by ID, verify `inputPath` exists + readable, call `rec_engine.engine.run()`, call `rec_engine.writer.write()` pointing at `runs/{runId}/output.csv`
- Return `{runId, metrics, outputPath, manifestPath}` where `metrics` is the manifest `totals` + `passBreakdown` summary

## 10. Error handling

- All endpoints catch `ConfigError`, `FileNotFoundError`, `ValueError` and return structured JSON with HTTP 4xx.
- Unexpected exceptions return 500 with the error message (POC — not production).

## 11. Testing

- One new integration test: `tests/test_api_integration.py`
  - Starts FastAPI via `TestClient`
  - POSTs the existing test fixture config
  - Triggers a run on `tests/fixtures/input.csv`
  - Asserts metrics match the existing expected output (16 matched, 2 unmatched, 6 groups)
  - Exercises `/matches` and `/breaks` with filters

No changes to the existing `test_engine_integration.py` — core engine must stay byte-identical.

## 12. New runtime deps

Added to `requirements.txt` + `pyproject.toml`:
- `fastapi>=0.110,<0.115` — pinned range
- `uvicorn[standard]>=0.27,<0.32` — ASGI server
- `pydantic>=2.5,<3` — (FastAPI transitive, explicit pin for determinism)
- `python-multipart` — NOT needed since no file upload; skip.

## 13. Startup

```bash
source .venv/bin/activate
pip install -r requirements.txt
uvicorn rec_engine_api.main:app --reload --port 8000
```

Or: `python -m rec_engine_api`.

Swagger UI auto-available at `http://localhost:8000/docs`.

## 14. Deferred (explicit)

- Auth / API keys
- File upload (only server-side path)
- Async / queued runs (use sync + timeout for POC)
- Date-range / numeric filters (equality-only)
- Config versioning, delete, rollback
- Streaming pagination for huge result sets (client must paginate)
- Websocket progress during runs

## 15. Build-order plan (rough sizing)

| Phase | Deliverable | ~hours |
|---|---|---|
| A | `requirements.txt` + `pyproject.toml` bump + scaffold `rec_engine_api/` | 0.5 |
| B | `schemas.py` + `storage.py` — filesystem read/write for configs + runs | 1 |
| C | `main.py` with config endpoints (validate/save/list/get) | 1 |
| D | `runs.py` + `POST /runs` endpoint wired to engine | 1.5 |
| E | `filters.py` + `/matches` / `/breaks` endpoints with pagination | 1.5 |
| F | `/download/{kind}` file-streaming endpoint | 0.5 |
| G | Synthetic script extension — 4-cardinality generator | 1.5 |
| H | `sample/config.json` → all 4 ACTIVE; regenerate bench numbers | 0.5 |
| I | Integration test for API | 1 |
| J | Docs update — extend README with API section | 0.5 |

**Total**: ~9.5 hours for a working backend + full-cardinality synthetic.

## 16. Open questions (none — all resolved)

- Sync vs async → **sync**
- Storage → **filesystem**
- Input → **server-side path**
- Synthetic mix → **70/10/8/8/1/3**
- Filters → **minimal equality + pagination**
- requirements.txt scope → **runtime only** (done; already on master)

## 17. Non-goals for this branch

- Changes to the core engine (`rec_engine/`) — unchanged.
- Changes to the existing integration test — unchanged.
- UI work — separate branch (`feat/ui`) after this backend lands.
