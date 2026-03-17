# fstore

A lightweight local feature store for incremental, event-driven feature computation. Define features once, feed events row by row with `step()` or replay a full dataset with `run()`. Supports time-windowed aggregations, online serving (in-memory) and offline history (Parquet). No servers, no infra — just Python.

## Installation

```bash
pip install pandas pyarrow
```

## Core concepts

| Concept | What it is |
|---|---|
| `Entity` | The thing you compute features for (e.g. a user, a merchant). Identified by a single key column. |
| `Feature` | A named aggregation over an entity's historical rows, optionally within a time window. |
| `FeatureStore` | Orchestrates registration, computation, and serving. Delegates all logic to the `Engine`. |

The store maintains two backends:
- **Online** — in-memory dict of the latest feature values per entity key. Fast lookups for real-time scoring.
- **Offline** — Parquet file with the full history of every feature computation. Never grows unbounded in RAM.

## Quick start

```python
from src import Entity, Feature, FeatureStore

# 1. Define entities
user  = Entity("user",  "user_id")
store = Entity("store", "store_id")

# 2. Define features
avg_price    = Feature("avg_price",   user, lambda g: g["price"].mean(), ["price"])
tx_count     = Feature("tx_count",    user, lambda g: len(g),            [])
cards_30m    = Feature("cards_30m",   user, lambda g: g["card"].nunique(), ["card"], "30m")

store_rev    = Feature("total_revenue", store, lambda g: g["price"].sum(), ["price"])

# 3. Create store and register features
fs = FeatureStore(timestamp_col="ts")
fs.register(avg_price)
fs.register(tx_count)
fs.register(cards_30m)
fs.register(store_rev)
```

## Usage

### `step(row)` — streaming / real-time

Process one event at a time. Each call updates both stores and returns the current feature values for every entity found in the row.

```python
row = {"user_id": 1, "store_id": 10, "price": 120, "card": "A", "ts": datetime(...)}

features = fs.step(row)
# {
#   "user":  {"avg_price": 120.0, "tx_count": 1, "cards_30m": 1},
#   "store": {"total_revenue": 120}
# }
```

Useful for scoring a new event in real time — call `step()`, inspect the returned features, make a decision.

### `run(df, id_col=None)` — batch

Replay a full DataFrame in timestamp order. Returns a per-row feature snapshot reflecting the state at the moment each row was processed.

```python
results = fs.run(df, id_col="attempt_id")
# {
#   1001: {"user": {"avg_price": 100.0, ...}, "store": {...}},
#   1002: {"user": {"avg_price": 125.0, ...}, "store": {...}},
#   ...
# }
```

If `id_col` is omitted, the DataFrame index is used as the key.

`run()` is equivalent to calling `step()` in a loop over the sorted DataFrame.

### Querying the stores

```python
# Latest values (online store)
fs.get_online_features("user", user_id=1)
# {"avg_price": 115.0, "tx_count": 4, "cards_30m": 2}

# Full history (offline store, read from Parquet)
records = fs.get_offline_features("user", feature_name="avg_price", user_id=1)
for rec in records:
    print(rec.timestamp, rec.value)

# Point-in-time query
records = fs.get_offline_features("user", feature_name="avg_price",
                                  as_of=datetime(2024, 1, 3), user_id=1)
```

## Feature definition

```python
Feature(name, entity, aggregation, columns, window=None)
```

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Feature name, e.g. `"avg_price"` |
| `entity` | `Entity` | The entity this feature belongs to |
| `aggregation` | `Callable` | Function `(DataFrame) -> value`. Receives all historical rows for this entity (within the window, if set). |
| `columns` | `list[str]` | Columns the aggregation reads. Used for upfront validation. Pass `[]` if no columns are needed (e.g. `len(g)`). |
| `window` | `str \| None` | Optional time window: `"30m"`, `"2h"`, `"7d"`, etc. Supported units: `s`, `m`, `h`, `d`. |

### Time windows

When `window` is set, only rows within `[timestamp - window, timestamp]` are passed to the aggregation:

```python
# Distinct cards used by this user in the last 30 minutes
cards_30m = Feature("cards_30m", user, lambda g: g["card"].nunique(), ["card"], "30m")

# Transaction count in the last 7 days
tx_7d = Feature("tx_7d", user, lambda g: len(g), [], "7d")
```

The window string is validated at construction time — an invalid format raises `ValueError` immediately.

## Validation

The store validates inputs before doing any work:

- **`step(row)`** — raises `ValueError` if the timestamp column or any declared feature column is missing from the row dict.
- **`run(df)`** — raises `ValueError` if the timestamp column or `id_col` is missing from the DataFrame columns.
- **`Feature(...)`** — raises `ValueError` at construction if the `window` string is not a valid format.
- A `[warning]` is printed (but execution continues) when an entity key column is missing from a row — the feature is skipped for that row.

## Project structure

```
src/
  entity.py        # Entity definition
  feature.py       # Feature definition + window parsing
  feature_store.py # Public interface (thin wrapper over Engine)
  engine.py        # Core computation: step(), run(), serving
  storage.py       # OnlineStorage (in-memory) + OfflineStorage (Parquet)
main.py            # Usage examples
```

## Configuration

```python
FeatureStore(
    timestamp_col="ts",            # column used as event timestamp (default: "ts")
    offline_path="offline.parquet" # path for the Parquet history file
)
```

The offline Parquet file is created lazily on the first flush. Records are buffered in memory (up to 500 at a time) and written in batches. Call `fs.flush()` to force-write any pending records immediately.
