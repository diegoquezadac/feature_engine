"""Engine - core computation and storage logic.

Handles all the complex machinery: incremental row processing, time-windowed
aggregations, batch execution, and both online (latest) and offline
(historical) feature serving.
"""

import time
import logging
import pandas as pd
from datetime import datetime
from typing import Any

from src.entity import Entity
from src.feature import Feature, parse_window
from src.storage import OnlineStorage, OfflineStorage, FeatureRecord

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class Engine:
    """Core computation engine for the feature store.

    Maintains an in-memory row buffer and computes features incrementally.
    The online store (latest values) is kept in memory; the offline store
    (full history) is persisted to a Parquet file so it never grows
    unbounded in RAM.

    The storage layer is injected so it can be swapped for remote or
    file-backed implementations without touching this class.

    Args:
        timestamp_col: Name of the column used as the event timestamp.
        offline_path: Path to the Parquet file used by the offline store.
    """

    def __init__(self, timestamp_col: str = "ts", offline_path: str = "offline.parquet"):
        self.timestamp_col = timestamp_col
        self._features: dict[str, Feature] = {}   # "{entity}:{feature}" → Feature
        self._entities: dict[str, Entity] = {}    # entity_name → Entity
        self._buffer: list[dict] = []             # raw rows in arrival order
        self.online = OnlineStorage()
        self.offline = OfflineStorage(path=offline_path)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, feature: Feature):
        """Register a feature (and its entity)."""
        self._features[f"{feature.entity.name}:{feature.name}"] = feature
        self._entities[feature.entity.name] = feature.entity

    # ------------------------------------------------------------------
    # Incremental (streaming) API
    # ------------------------------------------------------------------

    def step(self, row: dict | Any) -> dict[str, dict[str, Any]]:
        """Process a single row and update the feature stores.

        The row is appended to the internal buffer.  For every registered
        feature whose entity key columns are all present in *row*, the
        feature is recomputed over all buffered rows for that entity (subject
        to any time window).

        Args:
            row: A dict or pandas Series representing one event.

        Returns:
            {entity_name: {feature_name: value}} for every entity found in row.
        """
        if hasattr(row, "to_dict"):
            row = row.to_dict()

        if self.timestamp_col not in row:
            raise ValueError(
                f"timestamp column '{self.timestamp_col}' not found in row keys: {list(row.keys())}"
            )
        self._validate_row(row)

        # NOTE: The buffer grows indefinitely. A future improvement would be to
        # periodically dump older rows to an optimised data file (e.g. Parquet
        # or Arrow IPC) and reload only the rows that fall within the widest
        # registered time window, rather than keeping every row in memory.
        self._buffer.append(row)
        timestamp = row.get(self.timestamp_col)

        # Build a DataFrame from the buffer once per step call.
        df = pd.DataFrame(self._buffer)

        updated: dict[str, dict[str, Any]] = {}

        for feature in self._features.values():
            entity = feature.entity

            if entity.key not in row:
                print(f"[warning] feature '{feature.name}' skipped: key '{entity.key}' not in row")
                continue

            entity_key = row[entity.key]
            entity_df = df[df[entity.key] == entity_key].copy()
            entity_df = self._apply_window(entity_df, feature, timestamp)
            if feature.where is not None:
                entity_df = entity_df[feature.where(entity_df)]
            value = feature.aggregation(entity_df)

            self.online.upsert(entity.name, entity_key, feature.name, value)
            self.offline.append(
                FeatureRecord(
                    entity_name=entity.name,
                    entity_key=entity_key,
                    feature_name=feature.name,
                    value=value,
                    timestamp=timestamp,
                )
            )

            updated.setdefault(entity.name, {})[feature.name] = value

        return updated

    # ------------------------------------------------------------------
    # Batch API
    # ------------------------------------------------------------------

    def run(self, df: pd.DataFrame, verbose: bool = False, log_every: int = 100) -> dict[str, Any]:
        """Process a full DataFrame in timestamp order, adding computed features
        as new columns directly on *df*.

        Unlike ``step()``, this method does **not** use the internal buffer — it
        works entirely on the supplied DataFrame, which must contain all rows
        upfront.  The DataFrame is sorted in place by timestamp and feature
        values are written onto it as new columns named
        ``"{entity}__{feature}"`` — no copy is made.

        Offline records are flushed to Parquet in batches of 100 K rows so IO
        is never a per-row bottleneck.

        Args:
            df: Input DataFrame. Must contain ``timestamp_col``. Modified in
                place — feature values are added as new columns.
            verbose: When True, emit detailed timing logs via the ``engine``
                     logger (INFO level). Use ``logging.basicConfig(level=logging.INFO)``
                     to see them. Defaults to False.
            log_every: How often (in rows) to emit a progress log when
                       verbose=True. Defaults to 100.

        Returns:
            A summary report with execution statistics.
        """
        if self.timestamp_col not in df.columns:
            raise ValueError(
                f"timestamp column '{self.timestamp_col}' not found in DataFrame columns: {list(df.columns)}"
            )

        start = time.perf_counter()
        df.sort_values(self.timestamp_col, inplace=True, ignore_index=True)

        _BATCH_SIZE = 100_000
        records: list[FeatureRecord] = []
        total_records_written = 0

        # Timing accumulators (only populated when verbose=True).
        _t: dict[str, float] = {
            "filter": 0.0,   # boolean mask: entity key + timestamp
            "window": 0.0,   # _apply_window()
            "where": 0.0,    # feature.where predicate
            "agg": 0.0,      # feature.aggregation()
            "online": 0.0,   # online store upsert
            "df_write": 0.0, # df.at[] cell write
            "offline_io": 0.0, # batch_write() calls
        }

        n_rows = len(df)

        for i, row in df.iterrows():
            timestamp = row[self.timestamp_col]

            for feature in self._features.values():
                entity = feature.entity

                if entity.key not in df.columns:
                    logger.warning("feature '%s' skipped: key '%s' not in DataFrame", feature.name, entity.key)
                    continue

                entity_key = row[entity.key]

                if verbose:
                    _t0 = time.perf_counter()
                entity_df = df[
                    (df[entity.key] == entity_key) &
                    (df[self.timestamp_col] <= timestamp)
                ]
                if verbose:
                    _t["filter"] += time.perf_counter() - _t0

                if verbose:
                    _t0 = time.perf_counter()
                entity_df = self._apply_window(entity_df, feature, timestamp)
                if verbose:
                    _t["window"] += time.perf_counter() - _t0

                if feature.where is not None:
                    if verbose:
                        _t0 = time.perf_counter()
                    entity_df = entity_df[feature.where(entity_df)]
                    if verbose:
                        _t["where"] += time.perf_counter() - _t0

                if verbose:
                    _t0 = time.perf_counter()
                value = feature.aggregation(entity_df)
                if verbose:
                    _t["agg"] += time.perf_counter() - _t0

                if verbose:
                    _t0 = time.perf_counter()
                self.online.upsert(entity.name, entity_key, feature.name, value)
                if verbose:
                    _t["online"] += time.perf_counter() - _t0

                if verbose:
                    _t0 = time.perf_counter()
                df.at[i, f"{entity.name}__{feature.name}"] = value
                if verbose:
                    _t["df_write"] += time.perf_counter() - _t0

                records.append(FeatureRecord(
                    entity_name=entity.name,
                    entity_key=entity_key,
                    feature_name=feature.name,
                    value=value,
                    timestamp=timestamp,
                ))

            if len(records) >= _BATCH_SIZE:
                if verbose:
                    _t0 = time.perf_counter()
                self.offline.batch_write(records)
                if verbose:
                    _t["offline_io"] += time.perf_counter() - _t0
                total_records_written += len(records)
                records.clear()

            if verbose and (i + 1) % log_every == 0:
                elapsed = time.perf_counter() - start
                rows_s = (i + 1) / elapsed
                logger.info(
                    "[run] %d/%d rows (%.0f rows/s) | filter=%.3fs window=%.3fs "
                    "where=%.3fs agg=%.3fs online=%.3fs df_write=%.3fs offline_io=%.3fs",
                    i + 1, n_rows, rows_s,
                    _t["filter"], _t["window"], _t["where"],
                    _t["agg"], _t["online"], _t["df_write"], _t["offline_io"],
                )

        if records:
            if verbose:
                _t0 = time.perf_counter()
            self.offline.batch_write(records)
            if verbose:
                _t["offline_io"] += time.perf_counter() - _t0
            total_records_written += len(records)

        elapsed = time.perf_counter() - start

        if verbose:
            total_instrumented = sum(_t.values()) or 1.0
            logger.info(
                "[run] DONE — %d rows in %.3fs (%.0f rows/s)",
                n_rows, elapsed, n_rows / elapsed,
            )
            logger.info("[run] Timing breakdown:")
            for phase, t in _t.items():
                logger.info("  %-12s  %7.3fs  (%5.1f%%)", phase, t, 100 * t / total_instrumented)

        feature_cols = sorted({
            f"{feat.entity.name}__{feat.name}" for feat in self._features.values()
        })
        return {
            "rows_processed": n_rows,
            "features_computed": len(self._features),
            "records_written": total_records_written,
            "elapsed_seconds": round(elapsed, 4),
            "feature_columns": feature_cols,
        }

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def get_online_features(
        self, entity_name: str, **entity_keys
    ) -> dict[str, Any]:
        """Return the latest feature values for an entity."""
        entity = self._entities.get(entity_name)
        if not entity:
            return {}
        return self.online.get(entity_name, entity_keys.get(entity.key))

    def get_offline_features(
        self,
        entity_name: str,
        feature_name: str | None = None,
        as_of: datetime | None = None,
        **entity_keys,
    ) -> list[FeatureRecord]:
        """Return historical feature records for an entity."""
        entity = self._entities.get(entity_name)
        if not entity:
            return []

        entity_key = entity_keys.get(entity.key)

        if feature_name:
            return self.offline.get_historical(
                entity_name, entity_key, feature_name, as_of
            )

        all_features = [
            f for f in self._features.values() if f.entity.name == entity_name
        ]
        results: list[FeatureRecord] = []
        for f in all_features:
            results.extend(
                self.offline.get_historical(entity_name, entity_key, f.name, as_of)
            )
        return sorted(results, key=lambda r: r.timestamp)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_window(
        self, entity_df: pd.DataFrame, feature: Feature, timestamp: Any
    ) -> pd.DataFrame:
        if feature.window is None or timestamp is None:
            return entity_df
        cutoff = timestamp - parse_window(feature.window)
        return entity_df[entity_df[self.timestamp_col] >= cutoff]

    def _validate_row(self, row: dict):
        """Raise if any feature's declared columns are missing from a row dict."""
        for feature in self._features.values():
            missing = [c for c in feature.columns if c not in row]
            if missing:
                raise ValueError(
                    f"feature '{feature.name}' requires columns {feature.columns}, "
                    f"but {missing} are missing from the row"
                )
