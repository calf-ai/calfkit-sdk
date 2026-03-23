from typing import Generic

from calfkit.experimental._types import StateT


class InvocationHandle(Generic[StateT]):
    def __init__(self, correlation_id: str, state_type: type[StateT]):
        self._correlation_id = correlation_id
        self._state_type = state_type
