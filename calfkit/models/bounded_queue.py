from __future__ import annotations

from typing import Generic, TypeVar, overload

from pydantic import ConfigDict, Field

from calfkit.models.types import CompactBaseModel

T = TypeVar("T")


class BoundedQueue(CompactBaseModel, Generic[T]):
    """Bounded queue implementation using python primitive data types,
    in order to preserve itself over network round trip.

    Behaves like ``collections.deque(maxlen=N)`` — appends to the right
    and evicts the oldest items when capacity is exceeded.
    """

    model_config = ConfigDict(extra="forbid")

    maxlen: int = Field(gt=0)
    items: list[T] = Field(default_factory=list)

    def _trim(self) -> None:
        if len(self.items) > self.maxlen:
            self.items[:] = self.items[-self.maxlen :]

    def push(self, x: T) -> None:
        self.items.append(x)
        self._trim()

    def extend(self, xs: list[T]) -> None:
        self.items.extend(xs)
        self._trim()

    def latest(self) -> T | None:
        return self.items[-1] if self.items else None

    # --- container dunder methods ---

    def __bool__(self) -> bool:
        return bool(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def iter_items(self) -> list[T]:
        return list(self.items)

    @overload
    def __getitem__(self, index: int) -> T: ...
    @overload
    def __getitem__(self, index: slice) -> list[T]: ...
    def __getitem__(self, index: int | slice) -> T | list[T]:
        return self.items[index]

    def __contains__(self, value: object) -> bool:
        return value in self.items
