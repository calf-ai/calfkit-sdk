from dataclasses import dataclass
from typing import Generic

from pydantic import BaseModel

from calfkit.experimental._types import DepsT, StateT


class BaseSessionRunContext(BaseModel, Generic[StateT, DepsT]):
    """Developer-facing context for a session — just state + deps."""

    state: StateT
    """The state of the graph. Mutable."""
    deps: DepsT
    """Dependencies for the graph. Immutable."""
