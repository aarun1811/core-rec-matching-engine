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
