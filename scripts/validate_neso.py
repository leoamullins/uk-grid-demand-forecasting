"""Check how closely NESO's ND tracks INDO, on a randomly sampled month.

Backs up the 0.22% / ~60 MW average error claim in the README by comparing
the two series wherever both have a reading for the same settlement period.
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingest import neso
from src.ingest.outturn import KEYS, partition_path

INDO_ROOT = Path("data/raw/indo")
NESO_ROOT = Path("data/raw/neso")

log = logging.getLogger(__name__)


def _available_months(indo_root: Path) -> list[tuple[int, int]]:
    months = []
    for year_dir in sorted(indo_root.glob("year=*")):
        year = int(year_dir.name.removeprefix("year="))
        for month_dir in sorted(year_dir.glob("month=*")):
            months.append((year, int(month_dir.name.removeprefix("month="))))
    return months


def compare_month(year: int, month: int, neso_root: Path = NESO_ROOT) -> pd.DataFrame:
    indo = pd.read_parquet(partition_path(INDO_ROOT, year, month))
    indo["settlement_date"] = pd.to_datetime(indo["settlement_date"]).dt.date

    nd = neso.load(neso_root)[KEYS + ["nd"]]
    df = indo.merge(nd, on=KEYS, how="inner").dropna(subset=["demand_mw", "nd"])

    df["abs_error_mw"] = (df["nd"] - df["demand_mw"]).abs()
    df["pct_error"] = df["abs_error_mw"] / df["demand_mw"]
    return df


def main(seed: int | None = None) -> float:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not any(NESO_ROOT.glob("*.csv")):
        log.info("no NESO data on disk, downloading")
        neso.ingest(NESO_ROOT)

    months = _available_months(INDO_ROOT)
    if not months:
        raise SystemExit(f"no INDO partitions found under {INDO_ROOT}")

    year, month = random.Random(seed).choice(months)
    log.info("validating %s-%02d", year, month)

    df = compare_month(year, month)
    if df.empty:
        raise SystemExit(f"{year}-{month:02d}: no overlapping INDO/NESO periods")

    log.info(
        "%s-%02d: %d overlapping periods, mean error %.1f MW (%.3f%%), max %.1f MW",
        year,
        month,
        len(df),
        df["abs_error_mw"].mean(),
        df["pct_error"].mean() * 100,
        df["abs_error_mw"].max(),
    )

    return df["pct_error"].mean() * 100


if __name__ == "__main__":
    err = []
    for i in range(100):
        err.append(main())

    print(f"The average pct_error over 100 samples: {sum(err) / len(err)}")
