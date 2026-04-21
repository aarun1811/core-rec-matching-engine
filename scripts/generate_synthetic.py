"""Generate synthetic Nostro reconciliation CSVs covering all 4 match cardinalities.

Produces a blended fixture hitting every matcher in the engine:

  - 1:1 exact         (~70% of rows)  - MP_O2O
  - 1:1 date tolerance (~10% of rows) - MP_O2O (date within 1 day)
  - 1:N                (~8% of rows)  - MP_O2M (1 GL + k STMTs summing to GL amt)
  - N:1                (~8% of rows)  - MP_M2O (k GLs + 1 STMT summing to STMT amt)
  - N:M                (~1% of rows)  - MP_M2M (2 GLs + 2 STMTs, group sums equal)
  - Orphans            (~3% of rows)  - unmatched GL-only and STMT-only rows

Design notes (must match the matcher invariants in rec_engine/matchers/):

  * All amount splits use integer-cents arithmetic so group sums are EXACT — no
    float rounding that would break aggregate matching.
  * For 1:N / N:1 / N:M groups, each group is assigned a UNIQUE
    (BANK_ACCOUNT, CURRENCY) pair, drawn from a shuffled deck of 400 pairs
    (100 accts x 4 ccys). This matters because the aggregate matchers bucket
    rows by the EQUALS hard keys only (VALUE_DATE uses WITHIN so it is NOT
    part of the hash key). If two different 1:N groups shared a bucket, their
    STMT rows would be summed together and neither GL amount would match the
    combined sum.
  * A small slice of buckets is reserved exclusively for orphans so an orphan
    row's amount never leaks into an aggregate bucket's SUM.
  * 1:1 pairs (EXACT/DTOL) use random (acct, ccy) triples and may land in
    aggregate buckets — that's safe because MP_O2O runs first and consumes
    those rows before any aggregate pass sees them.
  * At fixture sizes above ~10k, the 400-bucket pool starts to saturate and
    a few aggregate groups will overflow into orphans. The overall match rate
    stays within a couple of points of the 97% target per the spec's
    "percentages will vary slightly" clause.

Usage:
    python scripts/generate_synthetic.py --rows 10000 --output /tmp/synth_small.csv
"""

from __future__ import annotations

import argparse
import random
import sys
from datetime import date, timedelta
from pathlib import Path

ACCOUNTS = [f"NOSTRO-USD-{i:04d}" for i in range(100)]
CCYS = ["USD", "EUR", "GBP", "JPY"]
BASE_DATE = date(2024, 1, 1)
DATE_SPAN_DAYS = 31  # 0..30 inclusive

# Amount range in cents: [1000, 10_000_000]  == [$10.00, $100_000.00]
AMT_MIN_CENTS = 1_000
AMT_MAX_CENTS = 10_000_000


def rand_amount_cents() -> int:
    """Uniform random amount in cents in [AMT_MIN_CENTS, AMT_MAX_CENTS]."""
    return random.randint(AMT_MIN_CENTS, AMT_MAX_CENTS)


def rand_date() -> date:
    return BASE_DATE + timedelta(days=random.randint(0, DATE_SPAN_DAYS - 1))


def aggregate_account(kind: str, group_idx: int) -> str:
    """Synthesize a unique BANK_ACCOUNT for one aggregate group.

    Aggregate matchers (MP_O2M, MP_M2O, MP_M2M) bucket rows by the EQUALS
    hard keys only; VALUE_DATE uses WITHIN so it is NOT part of the hash
    bucket. Each aggregate group must therefore own a unique (BANK_ACCOUNT,
    CURRENCY) pair or multiple groups would be summed together and fail to
    match.

    We sidestep the finite 100-account pool entirely by minting a fresh
    account per aggregate group. This lets us scale aggregate counts with
    the fixture size (38K+ aggregate groups at 1M rows is fine).
    """
    return f"NOSTRO-AGG-{kind}-{group_idx:08d}"


def fmt_amt(cents: int) -> str:
    """Render cents as a 4-decimal amount string matching the existing format."""
    whole, frac = divmod(cents, 100)
    return f"{whole}.{frac:02d}00"


def split_cents(total: int, k: int, min_part: int = 1) -> list[int]:
    """Split `total` cents into exactly `k` positive parts summing to `total`.

    Uses the stars-and-bars trick with sorted cut points so every part is >= min_part
    and the sum is EXACT (integer arithmetic, no float drift).
    """
    assert k >= 1
    assert total >= k * min_part, f"total {total} too small to split into {k} parts of >= {min_part}"
    # Pre-allocate the minimum to each part, then distribute the remainder.
    remainder = total - k * min_part
    if k == 1:
        return [total]
    # Choose k-1 cut points in [0, remainder]
    cuts = sorted(random.randint(0, remainder) for _ in range(k - 1))
    parts = []
    prev = 0
    for c in cuts:
        parts.append(min_part + (c - prev))
        prev = c
    parts.append(min_part + (remainder - prev))
    # Sanity: sum matches
    assert sum(parts) == total
    return parts


def write_row(f, ls_type: str, dr_cr: str, amt_cents: int, ccy: str, vdt: date, acct: str, ref: str) -> None:
    f.write(f"{ls_type},{dr_cr},OPEN,{fmt_amt(amt_cents)},{ccy},{vdt.isoformat()},{acct},{ref}\n")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, required=True, help="Approximate total output rows (may be +/- a few due to variable group sizes)")
    p.add_argument("--output", type=str, required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--match-rate", type=float, default=0.97,
                   help="[DEPRECATED] Ignored; the mix now implies ~97%% matched / 3%% orphans.")
    args = p.parse_args()

    if abs(args.match_rate - 0.97) > 1e-9:
        print(
            f"warning: --match-rate={args.match_rate} is deprecated and ignored; "
            f"mix is fixed at ~97% matched / 3% orphans.",
            file=sys.stderr,
        )

    random.seed(args.seed)

    n = args.rows

    # Allocations by row count (percent of n)
    # Units: o2o_exact / o2o_dtol pairs = 2 rows each; n2m units = 4 rows.
    # o2n / n2o units are variable (1 + k where k in [2,4]) -> 3 to 5 rows per unit.
    target_o2o_exact = (int(n * 0.70) // 2) * 2       # force even
    target_o2o_dtol  = (int(n * 0.10) // 2) * 2       # force even
    target_n2m       = (int(n * 0.01) // 4) * 4       # force multiple of 4
    target_o2n_rows  = int(n * 0.08)
    target_n2o_rows  = int(n * 0.08)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    per_block = {"o2o_exact": 0, "o2o_dtol": 0, "o2n": 0, "n2o": 0, "n2m": 0, "orphan_gl": 0, "orphan_stmt": 0}

    # Bucket strategy:
    #
    #   * 1:1 pairs (EXACT + DTOL) use the 100-account main pool. They share
    #     buckets freely — MP_O2O handles pairwise matching and consumes them
    #     before any aggregate pass runs.
    #
    #   * Aggregate groups (1:N, N:1, N:M) use synthetic unique BANK_ACCOUNTs
    #     via `aggregate_account(kind, idx)`. Each group gets a guaranteed-
    #     unique account, so bucket capacity scales with row count.
    #
    #   * Orphans use the same 100-account main pool. They don't match anything
    #     in any pass, and because aggregate groups use disjoint synthetic
    #     accounts (NOSTRO-AGG-*), orphans never contaminate an aggregate SUM.

    def next_main_bucket() -> tuple[str, str, date]:
        """Random bucket from the main 100-account pool (for 1:1 pairs and orphans)."""
        return (random.choice(ACCOUNTS), random.choice(CCYS), rand_date())

    with out.open("w", encoding="utf-8") as f:
        f.write("LS_TYPE,DR_CR_IND,STATUS,AMOUNT,CURRENCY,VALUE_DATE,BANK_ACCOUNT,REFERENCE\n")

        # ---- 1:1 exact ----
        # 1:1 pairs can share (account, ccy, date) buckets freely — MP_O2O matches
        # them pairwise and the optional REFERENCE EQUALS tie-breaks deterministically.
        pairs_exact = target_o2o_exact // 2
        for i in range(pairs_exact):
            amt = rand_amount_cents()
            ccy = random.choice(CCYS)
            vdt = rand_date()
            acct = random.choice(ACCOUNTS)
            ref = f"EXACT_{i:010d}"
            write_row(f, "GL",   "CR", amt, ccy, vdt, acct, ref)
            write_row(f, "STMT", "DR", amt, ccy, vdt, acct, ref)
            per_block["o2o_exact"] += 2
            rows_written += 2

        # ---- 1:1 date tolerance (STMT = GL + 1 day, within MP_O2O toleranceDays=1) ----
        pairs_dtol = target_o2o_dtol // 2
        for i in range(pairs_dtol):
            amt = rand_amount_cents()
            ccy = random.choice(CCYS)
            vdt_gl = rand_date()
            vdt_stmt = vdt_gl + timedelta(days=1)
            acct = random.choice(ACCOUNTS)
            ref = f"DTOL_{i:010d}"
            write_row(f, "GL",   "CR", amt, ccy, vdt_gl,   acct, ref)
            write_row(f, "STMT", "DR", amt, ccy, vdt_stmt, acct, ref)
            per_block["o2o_dtol"] += 2
            rows_written += 2

        # k (STMTs per 1:N group / GLs per N:1 group) uses the spec's
        # [2, 4] range with a weighted distribution toward larger values.
        # E[k] = 3.5 -> avg unit size 4.5 rows.
        def pick_k() -> int:
            return random.choices([2, 3, 4], weights=[1, 2, 5], k=1)[0]

        # ---- N:M groups: 2 GL + 2 STMT; each group uses a unique synthetic account ----
        n2m_units = target_n2m // 4
        for i in range(n2m_units):
            acct = aggregate_account("N2M", i)
            ccy = random.choice(CCYS)
            vdt = rand_date()
            total_cents = rand_amount_cents()
            gl_parts   = split_cents(total_cents, 2, min_part=1)
            stmt_parts = split_cents(total_cents, 2, min_part=1)
            for j, part in enumerate(gl_parts, start=1):
                write_row(f, "GL", "CR", part, ccy, vdt, acct, f"N2M_{i:08d}_L{j}")
            for j, part in enumerate(stmt_parts, start=1):
                write_row(f, "STMT", "DR", part, ccy, vdt, acct, f"N2M_{i:08d}_R{j}")
            per_block["n2m"] += 4
            rows_written += 4

        # ---- 1:N groups: 1 GL + k STMT (k in [2,4]); unique synthetic account per group ----
        i = 0
        while per_block["o2n"] < target_o2n_rows:
            acct = aggregate_account("O2N", i)
            ccy = random.choice(CCYS)
            vdt = rand_date()
            k = pick_k()
            total_cents = rand_amount_cents()
            parts = split_cents(total_cents, k, min_part=1)
            gl_ref = f"O2N_GRP_{i:08d}"
            write_row(f, "GL", "CR", total_cents, ccy, vdt, acct, gl_ref)
            for j, part in enumerate(parts, start=1):
                stmt_ref = f"O2N_{i:08d}_P{j:02d}"
                write_row(f, "STMT", "DR", part, ccy, vdt, acct, stmt_ref)
            unit_rows = 1 + k
            per_block["o2n"] += unit_rows
            rows_written += unit_rows
            i += 1

        # ---- N:1 groups: k GL + 1 STMT (k in [2,4]); unique synthetic account per group ----
        i = 0
        while per_block["n2o"] < target_n2o_rows:
            acct = aggregate_account("N2O", i)
            ccy = random.choice(CCYS)
            vdt = rand_date()
            k = pick_k()
            total_cents = rand_amount_cents()
            parts = split_cents(total_cents, k, min_part=1)
            stmt_ref = f"N2O_GRP_{i:08d}"
            for j, part in enumerate(parts, start=1):
                gl_ref = f"N2O_{i:08d}_P{j:02d}"
                write_row(f, "GL", "CR", part, ccy, vdt, acct, gl_ref)
            write_row(f, "STMT", "DR", total_cents, ccy, vdt, acct, stmt_ref)
            unit_rows = k + 1
            per_block["n2o"] += unit_rows
            rows_written += unit_rows
            i += 1

        # ---- Orphans: fill the rest exactly; split ~half GL-only / ~half STMT-only ----
        # Orphans use the main 100-account pool. Since aggregate groups live on
        # disjoint synthetic accounts (NOSTRO-AGG-*), orphans can never land in
        # an aggregate bucket and corrupt a SUM. Orphans do NOT need unique
        # buckets among themselves — they don't match anything in any pass.
        remaining = n - rows_written
        if remaining < 0:
            # Slight overshoot possible from variable-size o2n/n2o groups; that's fine per spec.
            remaining = 0
        orphan_gl = remaining // 2
        orphan_stmt = remaining - orphan_gl

        for i in range(orphan_gl):
            amt = rand_amount_cents()
            acct, ccy, vdt = next_main_bucket()
            ref = f"ORPH_GL_{i:08d}"
            write_row(f, "GL", "CR", amt, ccy, vdt, acct, ref)
            per_block["orphan_gl"] += 1
            rows_written += 1

        for i in range(orphan_stmt):
            amt = rand_amount_cents()
            acct, ccy, vdt = next_main_bucket()
            ref = f"ORPH_STMT_{i:08d}"
            write_row(f, "STMT", "DR", amt, ccy, vdt, acct, ref)
            per_block["orphan_stmt"] += 1
            rows_written += 1

    print(f"Wrote {out} with {rows_written} rows (requested {n}).")
    print("Breakdown:")
    for k, v in per_block.items():
        pct = (v / rows_written * 100) if rows_written else 0.0
        print(f"  {k:12s} {v:>9d}  ({pct:5.2f}%)")


if __name__ == "__main__":
    main()
