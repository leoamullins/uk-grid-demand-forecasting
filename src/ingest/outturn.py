from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"
# /datasets/INDO only filters on publish time and silently ignores settlement
# date params (returning just the latest record); /demand/outturn filters by
# settlement date but rejects windows longer than 28 days.
ENDPOINT = "/demand/outturn"

PARAM_FROM = "settlementDateFrom"
PARAM_TO = "settlementDateTo"
RECORDS_KEY = "data"
MAX_RANGE_DAYS = 28

FIELD_MAP = {
    "settlementDate": "settlement_date",
    "settlementPeriod": "settlement_period",
    "startTime": "start_time",
    "initialDemandOutturn": "demand_mw",
}

KEYS = ["settlement_date", "settlement_period"]
TZ = "Europe/London"

DEMAND_MIN_MW = 10_000
DEMAND_MAX_MW = 65_000

log = logging.getLogger(__name__)


class IngestError(Exception):
    pass


def _get(params: dict, retries: int = 4, backoff: float = 2.0) -> dict:
    """GET with an exponential backoff. Retries transient failures only"""
    url = f"{BASE_URL}{ENDPOINT}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                return resp.json()

            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f"status {resp.status_code}")
            resp.raise_for_status()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise IngestError(f"failed after {retries} attempts: {exc}") from exc
            sleep = backoff**attempt
            log.warning("request failed (%s), retrying is %.0fs", exc, sleep)
            time.sleep(sleep)
    raise IngestError("unreachable")


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1) - pd.Timedelta(days=1)
    return start, end.date() if hasattr(end, "date") else end


def _windows(start: date, end: date, days: int = MAX_RANGE_DAYS):
    """Split [start, end] into inclusive windows of at most `days` days."""
    while start <= end:
        stop = min(start + timedelta(days=days - 1), end)
        yield start, stop
        start = stop + timedelta(days=1)


def _period_grid(start: date, end: date) -> pd.DataFrame:
    """Every settlement period in [start, end] with its UTC start time.

    Periods count from local midnight, so clock-change days have 46 or 50.
    """
    times = pd.date_range(
        start, end + timedelta(days=1), freq="30min", tz=TZ, inclusive="left"
    )
    grid = pd.DataFrame({"settlement_date": times.date})
    grid["settlement_period"] = (
        grid.groupby("settlement_date").cumcount().add(1).astype("int16")
    )
    grid["start_time"] = times.tz_convert("UTC")
    return grid


def fetch_month(year: int, month: int) -> pd.DataFrame:
    "fetch one calendar month of INDO as a data frame"
    start, end = _month_bounds(year, month)
    records = []
    for lo, hi in _windows(start, end):
        payload = _get(
            {PARAM_FROM: lo.isoformat(), PARAM_TO: hi.isoformat(), "format": "json"}
        )
        chunk = payload.get(RECORDS_KEY) if isinstance(payload, dict) else payload
        records.extend(chunk or [])

    if not records:
        raise IngestError(f"no records returned for {year}-{month:02d}")

    df = pd.DataFrame(records)
    missing = set(FIELD_MAP) - set(df.columns)
    if missing:
        raise IngestError(f"expected fields missing: {missing}; got {list(df.columns)}")

    df = df[list(FIELD_MAP)].rename(columns=FIELD_MAP)

    df["settlement_date"] = pd.to_datetime(df["settlement_date"]).dt.date
    df["settlement_period"] = df["settlement_period"].astype("int16")
    df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
    df["demand_mw"] = pd.to_numeric(df["demand_mw"], errors="coerce").astype("float32")

    # The source has gaps (missing half-hours), so outer-join onto the full grid:
    # gaps become NaN demand rows, and periods the grid doesn't expect are kept
    # for validate() to reject.
    grid = _period_grid(start, end)
    if end >= date.today() - timedelta(days=1):
        grid = grid[grid["start_time"] <= df["start_time"].max()]
    df = grid.merge(df, on=KEYS, how="outer", suffixes=("_grid", ""))
    df["start_time"] = df["start_time"].fillna(df.pop("start_time_grid"))

    return df.sort_values(KEYS).reset_index(drop=True)


def validate(df: pd.DataFrame, year: int, month: int) -> None:
    label = f"{year}-{month:02d}"

    dupes = df.duplicated(KEYS).sum()
    if dupes:
        raise IngestError(f"{label}: {dupes} duplicate (date, period) rows")

    grid = _period_grid(*_month_bounds(year, month))
    grid = grid[grid["start_time"] <= df["start_time"].max()]
    expected = grid.groupby("settlement_date").size()
    counts = df.groupby("settlement_date").size()
    bad = counts[counts.ne(expected.reindex(counts.index))]
    if not bad.empty:
        raise IngestError(f"{label}: unexpected period counts:\n{bad.to_string()}")

    gaps = df[df["demand_mw"].isna()].groupby("settlement_date").size()
    if not gaps.empty:
        log.warning(
            "%s: %d periods missing demand, left as NaN:\n%s",
            label,
            gaps.sum(),
            gaps.to_string(),
        )

    lo, hi = df["demand_mw"].min(), df["demand_mw"].max()
    if lo < DEMAND_MIN_MW or hi > DEMAND_MAX_MW:
        raise IngestError(f"{label}: demand outside band [{lo:.0f}, {hi:.0f}] MW")


def partition_path(root: Path, year: int, month: int) -> Path:
    return root / f"year={year}" / f"month={month:02d}" / "indo.parquet"


def write_partition(df: pd.DataFrame, root: Path, year: int, month: int) -> Path:
    path = partition_path(root, year, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False, compression="snappy")
    tmp.replace(path)
    return path


def _is_complete(path: Path, year: int, month: int) -> bool:
    """returns true if the partition reaches the months last settlement period"""
    last = _period_grid(*_month_bounds(year, month))["start_time"].iloc[-1]
    return pd.read_parquet(path, columns=["start_time"])["start_time"].max() >= last


def backfill(
    root: Path,
    start: date,
    end: date | None = None,
    overwrite: bool = False,
    pause: float = 0.5,
) -> None:
    # defaulting to yesterday so a run on the first doesnt request a month with no date yet
    end = end or date.today() - timedelta(days=1)

    for ts in pd.date_range(start, end, freq="MS"):
        year, month = ts.year, ts.month
        path = partition_path(root, year, month)

        if path.exists() and not overwrite and _is_complete(path, year, month):
            log.info("skip %s-%02d (exists)", year, month)
            continue

        df = fetch_month(year, month)
        validate(df, year, month)
        write_partition(df, root, year, month)
        log.info("wrote %s-%02d (%d rows)", year, month, len(df))
        time.sleep(pause)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    backfill(Path("data/raw/indo"), start=date(2016, 3, 1))
