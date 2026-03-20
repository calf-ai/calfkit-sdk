from typing import Any

from typing_extensions import TypeVar

StateT = TypeVar("StateT", default=Any)
DepsT = TypeVar("DepsT", default=Any)
InputT = TypeVar("InputT", default=Any)
