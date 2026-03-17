# feature_engine

A lightweight **feature engine** for event-driven feature computation. Define features once, feed events one at a time with `update()` or replay a full dataset with `compute()`. Supports time-windowed aggregations, row-level filters, online serving (in-memory) and offline history (Parquet).

## Installation

```bash
pip install pandas pyarrow
```

## Core concepts

| Concept | What it is |
|---|---|
| `Entity` | The thing you compute features for (e.g. a user, a merchant). Identified by a single key column. |
| `Feature` | A named aggregation over an entity's historical rows, optionally within a time window and/or a row filter. |
| `FeatureEngine` | Registers features, drives computation, and serves results. Maintains an online store (latest values) and an offline store (full history). |

The engine maintains two backends:
- **Online** — in-memory dict of the latest feature values per entity key. Fast lookups for real-time scoring.
- **Offline** — append-only Parquet directory with the full history of every feature computation. Never grows unbounded in RAM.

## Execution modes

### `update(row)` — streaming

Process one event at a time. Each call appends the row to an internal buffer and recomputes features over that buffer. Use this when data arrives incrementally (e.g. from a message queue or a real-time stream).

```python
row = {"user_id": 1, "store_id": 10, "price": 120, "card": "A", "ts": datetime(...)}

features = fe.update(row)
# {
#   "user":  {"avg_price": 120.0, "tx_count": 1, "cards_30m": 1},
#   "store": {"total_revenue": 120}
# }
```

Useful for scoring a new event in real time — call `update()`, inspect the returned features, make a decision.

> **Note:** The buffer grows indefinitely. A future improvement is to periodically dump older rows to an optimised data file and reload only the rows within the widest registered window.

### `compute(df)` — batch

Process a full DataFrame at once. The engine works directly on the supplied DataFrame — no internal buffer is used and no copy is made. Feature values are written onto `df` in place as new columns named `"{entity}:{feature}"`. Offline records are written to Parquet in batches so IO is not a per-row bottleneck.

```python
report = fe.compute(df)
# df now has new columns: "user:avg_price", "user:tx_count", "store:total_revenue", ...
# report → {
#   "rows_processed": 6,
#   "features_computed": 7,
#   "records_written": 36,
#   "elapsed_seconds": 0.04,
#   "feature_columns": ["store:avg_price", "store:total_revenue", "user:avg_price", ...]
# }
```

Use this for offline experiments, historical replay, and dataset enrichment.

## Quick start

```python
from src import Entity, Feature, FeatureEngine

# 1. Define entities
user  = Entity("user",  "user_id")
store = Entity("store", "store_id")

# 2. Define features
avg_price = Feature("avg_price",    user,  "mean",    on="price")
tx_count  = Feature("tx_count",     user,  "count")
cards_30m = Feature("cards_30m",    user,  "nunique", on="card", window="30m")
store_rev = Feature("total_revenue",store, "sum",     on="price")

# 3. Create engine and register features
fe = FeatureEngine(timestamp_col="ts")
fe.register(avg_price)
fe.register(tx_count)
fe.register(cards_30m)
fe.register(store_rev)

# 4a. Streaming mode — one row at a time
features = fe.update(row)

# 4b. Batch mode — full DataFrame, enriched in place
report = fe.compute(df)
# df["user:avg_price"], df["user:tx_count"], df["store:total_revenue"] are now populated
```

## Querying the stores

```python
# Latest values (online store)
fe.get_online_features("user", user_id=1)
# {"avg_price": 115.0, "tx_count": 4, "cards_30m": 2}

# Full history (offline store, read from Parquet)
records = fe.get_offline_features("user", feature_name="avg_price", user_id=1)
for rec in records:
    print(rec.timestamp, rec.value)

# Point-in-time query
records = fe.get_offline_features("user", feature_name="avg_price",
                                  as_of=datetime(2024, 1, 3), user_id=1)
```

## Feature definition

```python
Feature(name, entity, aggregation, on=None, window=None, where=None)
```

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Feature name, e.g. `"avg_price"` |
| `entity` | `Entity` | The entity this feature belongs to |
| `aggregation` | `str \| Callable` | Built-in name (see table below) or a custom `(DataFrame) -> value` function |
| `on` | `str \| None` | Column the aggregation reads. Required for all built-ins except `"count"`. For custom callables, used for upfront column validation. |
| `window` | `str \| None` | Optional time window: `"30m"`, `"2h"`, `"7d"`, etc. Supported units: `s`, `m`, `h`, `d`. |
| `where` | `Callable \| None` | Optional row filter applied after windowing. Receives the entity DataFrame, must return a boolean Series. |

### Built-in aggregations

| Name | Equivalent |
|---|---|
| `"mean"` | `g[col].mean()` |
| `"sum"` | `g[col].sum()` |
| `"count"` | `len(g)` |
| `"nunique"` | `g[col].nunique()` |
| `"min"` | `g[col].min()` |
| `"max"` | `g[col].max()` |
| `"std"` | `g[col].std()` |
| `"first"` | `g[col].iloc[0]` |
| `"last"` | `g[col].iloc[-1]` |

For anything else, pass a callable:

```python
Feature("p95_price", user, lambda g: g["price"].quantile(0.95), on="price")
```

### Time windows

When `window` is set, only rows within `[timestamp - window, timestamp]` are passed to the aggregation:

```python
# Distinct cards used by this user in the last 30 minutes
cards_30m = Feature("cards_30m", user, "nunique", on="card", window="30m")

# Transaction count in the last 7 days
tx_7d = Feature("tx_7d", user, "count", window="7d")
```

The window string is validated at construction time — an invalid format raises `ValueError` immediately.

### Row filters

When `where` is set, it is applied after windowing, before the aggregation:

```python
# Transactions at store 10 only
tx_store10 = Feature("tx_count_store10", user, "count",
                     where=lambda g: g["store_id"] == 10)

# Average price of high-value transactions in the last 2 days
avg_high_2d = Feature("avg_high_price_2d", user, "mean", on="price",
                      window="2d", where=lambda g: g["price"] > 100)
```

## Crash recovery (batch mode)

For long-running `compute()` calls, pass `checkpoint_dir` to save progress every N rows. If the process dies, re-run with the same path to resume from the last checkpoint:

```python
report = fe.compute(
    df,
    checkpoint_dir="run_checkpoint",
    checkpoint_every=500_000,
)
```

The checkpoint is deleted automatically on successful completion. The fast (vectorised) path always reruns from scratch; only the slow (row-by-row) path is checkpointed.

## Validation

The engine validates inputs before doing any work:

- **`update(row)`** — raises `ValueError` if the timestamp column or any declared feature column is missing from the row dict.
- **`compute(df)`** — raises `ValueError` if the timestamp column is missing from the DataFrame columns.
- **`Feature(...)`** — raises `ValueError` at construction if the aggregation name is unknown, if a required `on` column is missing, or if the `window` string is not a valid format.
- A warning is logged (but execution continues) when an entity key column is missing from a row or DataFrame.

## Project structure

```
src/
  entity.py         # Entity definition
  feature.py        # Feature definition, built-in aggregation registry, window parsing
  engine.py         # FeatureEngine: computation, streaming, batch, and serving
  storage.py        # OnlineStorage (in-memory) + OfflineStorage (Parquet, append-only)
main.py             # Usage examples
```

## Configuration

```python
FeatureEngine(
    timestamp_col="ts",          # column used as event timestamp (default: "ts")
    offline_path="offline_store" # directory for the Parquet history (default: "offline_store")
)
```

In streaming mode, records are buffered in memory (up to 500 at a time) and flushed automatically; call `fe.flush()` to force-write any pending records. In batch mode, records are flushed in 100 K-row batches during `compute()`.
