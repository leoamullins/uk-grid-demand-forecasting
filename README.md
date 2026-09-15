# UK Demand Forecasting

This project builds a clean, half-hourly series of GB national electricity demand back to 2016, using Elexon's INDO feed and filling gaps from NESO's historic demand archive. Ingestion is done; the forecasting model itself hasn't started yet.

## Data ingestion

Ingestion turns the two raw sources into one validated parquet file. It's more than a single API call: INDO has gaps and a 28-day request limit, settlement periods shift on UK clock-change days, and the pipeline needs to stay current without re-downloading everything each time.

```text
Elexon INDO API ───────► data/raw/indo/  (one parquet per month) ──┐
                                                                    ├─► transform.build_demand ─► data/processed/demand.parquet
NESO Historic Demand ──► data/raw/neso/  (one CSV per year) ───────┘
```

### Quick start

```bash
uv sync
```

`main.py` doesn't yet orchestrate the three ingestion steps, so for now run them directly:

```bash
uv run python -m src.ingest.outturn   # backfills INDO from 2016-03-01
uv run python -c "from pathlib import Path; from src.ingest import neso; neso.ingest(Path('data/raw/neso'))"
uv run python -c "
from pathlib import Path
from src.ingest import transform
transform.build_demand(Path('data/raw/indo'), Path('data/raw/neso'), Path('data/processed/demand.parquet'))
"
```

### Data sources

#### Elexon INDO (primary)

The primary source is Elexon's INDO API — the *initial national demand outturn*, a half-hourly report of demand in MW for each settlement period. It excludes embedded wind and solar, station load, pumped-storage pumping and interconnector exports.

That matters for forecasting because INDO tracks *metered* demand, not *underlying* demand: on sunny days, embedded solar offsets real consumption behind the meter, so INDO dips at midday even though demand hasn't actually fallen. A model trained on INDO alone risks learning that dip as a demand pattern rather than a generation effect, so embedded solar and wind should be added as separate inputs rather than assumed.

The `/demand/outturn` endpoint is used rather than `/datasets/INDO`, which only filters on publish time and ignores the settlement date. Requests are limited to 28 days, so each month is fetched in two parts. History starts 2016-03-01, as the source only goes back part-way through 2016-02-29.

#### NESO Historic Demand Data (gap fill)

NESO publishes one CSV per year of national demand back to 2016. It's used only to fill gaps in INDO, not as the primary series, since it lags INDO by a few weeks.

The `ND` (national demand) column is used. It tracks INDO closely — 0.20% average error, around 60 MW — which is why it's used to fill missing INDO periods rather than interpolating.

One wrinkle: the yearly files don't agree on date format — `settlement_date` shows up as `01-JAN-2016`, `01-Jan-23` or `2025-01-01` depending on the year — so each format is tried in turn rather than assumed.

### Output data

#### Raw

INDO is stored one file per month, under `data/raw/indo/year=YYYY/month=MM/indo.parquet`. NESO is stored one file per year, under `data/raw/neso/YYYY.csv`. Both are kept exactly as published — gaps are left blank rather than filled in at this stage.

INDO files have this schema:

| Column | Type | Description |
| --- | --- | --- |
| settlement_date | date | UK local calendar date |
| settlement_period | int16 | half-hour number within the day, from 1 |
| start_time | timestamp (UTC) | start of the settlement period |
| demand_mw | float32 | initial demand outturn, in MW (blank where missing) |

#### Processed

The combined series is written to `data/processed/demand.parquet`, with the same columns as raw INDO plus `demand_source`, which marks where each row's `demand_mw` came from: `indo`, `neso_nd`, or `missing` if neither source had a value. Use `demand_source` to exclude filled or missing periods from model evaluation, since they aren't genuine INDO readings.

### Limitations

- Recent gaps in INDO stay blank until NESO's data for that period catches up, which lags by a few weeks.
- INDO excludes embedded generation entirely, so a forecasting model that needs it will have to pull NESO's embedded wind and solar columns (or another source) separately — this project doesn't ingest them yet.
- `main.py` doesn't orchestrate the ingestion steps yet; they have to be run manually (see [Quick start](#quick-start)).

### Code layout

- [src/ingest/outturn.py](src/ingest/outturn.py): fetch, validate and write INDO by month.
- [src/ingest/neso.py](src/ingest/neso.py): download and load NESO yearly files.
- [src/ingest/transform.py](src/ingest/transform.py): combine INDO and NESO into the processed series.
- [main.py](main.py): entry point (not yet wired up to run ingestion).
