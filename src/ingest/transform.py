from pathlib import Path

import numpy as np
import pandas as pd

from src.ingest import neso
from src.ingest.outturn import KEYS


def build_demand(indo_root: Path, neso_root: Path, out: Path) -> pd.DataFrame:
    indo = pd.read_parquet(indo_root).drop(columns=["year", "month"])
    indo["settlement_date"] = pd.to_datetime(indo["settlement_date"]).dt.date
    nd = neso.load(neso_root)[KEYS + ["nd"]]

    df = indo.merge(nd, on=KEYS, how="left")
    df["demand_source"] = np.where(
        df["demand_mw"].notna(),
        "indo",
        np.where(df["nd"].notna(), "neso_nd", "missing"),
    )
    df["demand_mw"] = df["demand_mw"].fillna(df.pop("nd")).astype("float32")

    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return df
