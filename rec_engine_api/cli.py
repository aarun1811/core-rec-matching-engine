"""Uvicorn launch entrypoint for the API console script."""

from __future__ import annotations

import os

import uvicorn


def run() -> None:
    """Entry point for `rec-engine-api` console script and `python -m rec_engine_api`."""
    host = os.environ.get("REC_ENGINE_API_HOST", "127.0.0.1")
    port = int(os.environ.get("REC_ENGINE_API_PORT", "8000"))
    uvicorn.run("rec_engine_api.main:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run()
