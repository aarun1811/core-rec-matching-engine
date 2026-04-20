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
