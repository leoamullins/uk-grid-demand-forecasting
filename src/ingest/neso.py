from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests

PACKAGE_URL = (
    "https://api.neso.energy/api/3/action/package_show?id=historic-demand-data"
)
NAME_PREFIX = "Historic Demand Data"
REFRESH_YEARS = 2

log = logging.getLogger(__name__)


def _year_urls() -> dict[int, str]:
    resp = requests.get(PACKAGE_URL, timeout=60)
    resp.raise_for_status()
    return {
        int(r["name"].removeprefix(NAME_PREFIX)): r["url"]
        for r in resp.json()["result"]["resources"]
        if r["name"].startswith(NAME_PREFIX)
    }


def ingest(root: Path, start_year: int = 2016) -> None:
    root.mkdir(parents=True, exist_ok=True)
    urls = _year_urls()
    latest = max(urls)
    for year in range(start_year, latest + 1):
        path = root / f"{year}.csv"
        if path.exists() and year <= latest - REFRESH_YEARS:
            log.info("skip %s exists", year)
            continue

        resp = requests.get(urls[year], timeout=120)
        resp.raise_for_status()
        tmp = path.with_suffix(".csv.tmp")
        tmp.write_bytes(resp.content)
        tmp.replace(path)
        log.info("wrote %s", path)


def _parse_dates(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%b-%Y", "%d-%b-%y"):
        unparsed = out.isna()
        out[unparsed] = pd.to_datetime(s[unparsed], format=fmt, errors="coerce")
    if out.isna().any():
        bad = s[out.isna()]
        raise ValueError(f"unrecognised date format; sample: {bad.head().tolist()}")
    return out.dt.date


def load(root: Path) -> pd.DataFrame:
    df = pd.concat(
        (pd.read_csv(p) for p in sorted(root.glob("*.csv"))), ignore_index=True
    )
    df.columns = df.columns.str.lower()
    df["settlement_date"] = _parse_dates(df["settlement_date"].astype(str))
    df["settlement_period"] = df["settlement_period"].astype("int16")
    return df
