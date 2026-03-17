"""Feature definition - a named aggregation over data."""

import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Any

from src.entity import Entity


_WINDOW_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}

# Built-in aggregations. Each value is a factory: col -> (DataFrame -> value).
_AGG_REGISTRY: dict[str, Callable] = {
    "mean":    lambda col: lambda g: g[col].mean(),
    "sum":     lambda col: lambda g: g[col].sum(),
    "count":   lambda _: lambda g: len(g),
    "nunique": lambda col: lambda g: g[col].nunique(),
    "min":     lambda col: lambda g: g[col].min(),
    "max":     lambda col: lambda g: g[col].max(),
    "std":     lambda col: lambda g: g[col].std(),
    "first":   lambda col: lambda g: g[col].iloc[0] if len(g) else None,
    "last":    lambda col: lambda g: g[col].iloc[-1] if len(g) else None,
}

# Built-ins that operate on the whole group, not a specific column.
_AGG_NO_COL = {"count"}


def parse_window(window: str) -> timedelta:
    """Parse a window string like '30m', '2h', '7d' into a timedelta.

    Supported units: s (seconds), m (minutes), h (hours), d (days).
    """
    match = re.fullmatch(r"(\d+)([smhd])", window)
    if not match:
        raise ValueError(
            f"Invalid window format: '{window}'. "
            f"Expected a positive integer followed by a unit: s, m, h, or d  (e.g. '30m', '2h', '7d')."
        )
    value, unit = int(match.group(1)), match.group(2)
    return timedelta(**{_WINDOW_UNITS[unit]: value})


@dataclass
class Feature:
    """A feature computed from an aggregation function.

    Args:
        name: Feature name (e.g., 'avg_price').
        entity: The entity this feature belongs to.
        aggregation: Either a built-in name ('mean', 'sum', 'count', 'nunique',
                     'min', 'max', 'std', 'first', 'last') or a custom callable
                     ``(DataFrame) -> value``.
        on: Column the aggregation reads. Required for all built-ins except
            'count'. For custom callables, used to auto-populate ``columns``
            for validation; omit if the callable needs no columns.
        window: Optional time window string (e.g., '30m', '2h', '7d').
                When set, only rows within [timestamp - window, timestamp]
                are passed to the aggregation function.
        where: Optional row filter applied after windowing.  Receives the
               entity DataFrame and must return a boolean Series.
               Example: ``where=lambda g: g["price"] > 100``
    """
    name: str
    entity: Entity
    aggregation: str | Callable[[Any], Any]
    on: str | None = None
    window: str | None = None
    where: Callable[[Any], Any] | None = None

    # Derived in __post_init__ — not part of the public constructor.
    columns: list[str] = field(init=False)
    # Original built-in aggregation name, preserved for the vectorized engine path.
    # None for custom callables.
    _agg_name: str | None = field(init=False)

    def __post_init__(self):
        if isinstance(self.aggregation, str):
            agg_name = self.aggregation
            if agg_name not in _AGG_REGISTRY:
                raise ValueError(
                    f"Unknown aggregation '{agg_name}'. "
                    f"Available: {sorted(_AGG_REGISTRY)}. "
                    f"Pass a callable for custom logic."
                )
            if agg_name not in _AGG_NO_COL and self.on is None:
                raise ValueError(
                    f"Aggregation '{agg_name}' requires an 'on' column."
                )
            self.columns = [self.on] if self.on else []
            self._agg_name = agg_name
            self.aggregation = _AGG_REGISTRY[agg_name](self.on)
        else:
            self.columns = [self.on] if self.on else []
            self._agg_name = None

        if self.window is not None:
            parse_window(self.window)
