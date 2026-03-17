"""Entity definition - represents what we compute features for."""

from dataclasses import dataclass


@dataclass(frozen=True, eq=False)
class Entity:
    """An entity type like 'user' or 'store'.

    Args:
        name: Entity name (e.g., 'user')
        key: Column name that identifies this entity (e.g., 'user_id')
    """
    name: str
    key: str
