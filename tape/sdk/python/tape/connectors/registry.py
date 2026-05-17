"""A tiny in-process registry of connectors keyed by name."""

from __future__ import annotations

from typing import Dict, Iterable

from .base import Connector


class ConnectorRegistry:
    def __init__(self) -> None:
        self._items: Dict[str, Connector] = {}

    def register(self, name: str, connector: Connector) -> None:
        if name in self._items:
            raise ValueError(f"connector {name!r} already registered")
        self._items[name] = connector

    def replace(self, name: str, connector: Connector) -> None:
        self._items[name] = connector

    def get(self, name: str) -> Connector:
        try:
            return self._items[name]
        except KeyError as ex:
            raise KeyError(
                f"no connector registered as {name!r}; "
                f"known: {sorted(self._items)}"
            ) from ex

    def __contains__(self, name: str) -> bool:
        return name in self._items

    def names(self) -> Iterable[str]:
        return tuple(self._items)

    def clear(self) -> None:
        self._items.clear()


CONNECTORS = ConnectorRegistry()
