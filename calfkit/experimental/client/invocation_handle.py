from calfkit.experimental._types import StateT
from calfkit.experimental.data_model.state_deps import State


class InvocationHandle:
    def __init__(self, correlation_id: str):
        self._correlation_id = correlation_id
