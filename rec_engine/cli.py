"""typer CLI entry point."""

from __future__ import annotations

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
