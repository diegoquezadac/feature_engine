"""Feature definition - a named aggregation over data."""

import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Any

from src.entity import Entity


_WINDOW_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


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
        name: Feature name (e.g., 'avg_price')
        entity: The entity this feature belongs to
        aggregation: Function that takes a DataFrame group and returns a value.
                     Example: lambda df: df["price"].mean()
        columns: Column names the aggregation reads. Used for upfront validation.
                 Pass [] for aggregations that only need the row count (e.g. len(g)).
        window: Optional time window string (e.g., '30m', '2h', '7d').
                When set, only rows within [timestamp - window, timestamp]
                are passed to the aggregation function.
                Example: Feature("distinct_cards_30m", user, lambda g: g["card"].nunique(), ["card"], "30m")
    """
    name: str
    entity: Entity
    aggregation: Callable[[Any], Any]
    columns: list[str]
    window: str | None = field(default=None)

    def __post_init__(self):
        if self.window is not None:
            parse_window(self.window)  # raises ValueError on invalid format
