from typing import Generic

from pydantic import BaseModel, Field

from calfkit.experimental._types import DepsT, StateT


class BaseSessionRunContext(BaseModel, Generic[StateT, DepsT]):
    """Base generic context for a session — just state + deps."""

    state: StateT = Field(description="Mutable app state.")
    """The app state. Mutable."""
    deps: DepsT = Field(description="Immutable execution dependencies.")
    """Dependencies for the execution. Immutable."""
