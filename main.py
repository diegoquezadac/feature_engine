"""Example usage of the local feature store."""

import logging
import shutil

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from src import Entity, Feature, FeatureStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


def make_synthetic_df(n: int = 1_000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    base = datetime(2024, 1, 1)
    seconds = rng.integers(0, 90 * 24 * 3600, size=n)  # spread over ~90 days
    return pd.DataFrame({
        "attempt_id": np.arange(1, n + 1),
        "user_id":    rng.integers(1, 51, size=n),       # 50 users
        "store_id":   rng.integers(1, 11, size=n),       # 10 stores
        "card":       rng.choice(["A", "B", "C", "D", ""], size=n),
        "price":      rng.uniform(10, 500, size=n).round(2),
        "ts":         [base + timedelta(seconds=int(s)) for s in seconds],
    })


def main():
    shutil.rmtree("offline_store", ignore_errors=True)

    df = make_synthetic_df(n=1_000_000)

    # ---------------------------------------------------------------
    # Define entities
    # ---------------------------------------------------------------
    user  = Entity("user",  "user_id")
    store = Entity("store", "store_id")

    # ---------------------------------------------------------------
    # Define features
    # ---------------------------------------------------------------
    user_avg_price   = Feature("avg_price",          user,  "mean",    on="price")
    user_total_spent = Feature("total_spent",         user,  "sum",     on="price")
    user_tx_count    = Feature("tx_count",            user,  "count")
    user_cards_2d    = Feature("distinct_cards_2d",   user,  "nunique", on="card", window="2d")
    user_tx_store1   = Feature("tx_count_store1",     user,  "count",
                               where=lambda g: g["store_id"] == 1)

    store_avg_price  = Feature("avg_price",           store, "mean",    on="price")
    store_total_rev  = Feature("total_revenue",        store, "sum",     on="price")

    # ---------------------------------------------------------------
    # Example 1: run() — experimental mode  (verbose=True to see timings)
    # ---------------------------------------------------------------
    print("=" * 60)
    print("EXAMPLE 1 — run(df)  — experimental mode")
    print("=" * 60)

    fs = FeatureStore(timestamp_col="ts")
    for f in [user_avg_price, user_total_spent, user_tx_count, user_cards_2d,
              user_tx_store1, store_avg_price, store_total_rev]:
        fs.register(f)

    report = fs.run(df, verbose=True, log_every=100_000)
    print("\nReport:", report)

    print("\nFirst 5 rows with feature columns:")
    feature_cols = ["attempt_id", "user_id", "store_id", "ts"] + report["feature_columns"]
    print(df[feature_cols].head().to_string(index=False))

    print("\n--- Online features after run() ---")
    print(f"  User 1:   {fs.get_online_features('user', user_id=1)}")
    print(f"  Store 1:  {fs.get_online_features('store', store_id=1)}")

    # ---------------------------------------------------------------
    # Example 2: step() — streaming mode
    # ---------------------------------------------------------------
    print("\n" + "=" * 60)
    print("EXAMPLE 2 — step(row)  — streaming mode (first 5 rows)")
    print("=" * 60)

    shutil.rmtree("offline_store", ignore_errors=True)

    fs2 = FeatureStore(timestamp_col="ts")
    for f in [user_avg_price, user_tx_count, user_cards_2d]:
        fs2.register(f)

    df_sorted = df.sort_values("ts").reset_index(drop=True)
    for _, row in df_sorted.head(5).iterrows():
        features = fs2.step(row)
        print(f"  attempt {int(row['attempt_id'])} "
              f"(user {int(row['user_id'])}, ts={row['ts'].date()}) → {features}")


if __name__ == "__main__":
    main()
