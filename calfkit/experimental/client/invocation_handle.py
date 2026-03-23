from typing import Generic

from calfkit.experimental._types import StateT


class InvocationHandle(Generic[StateT]):
    def __init__(self, correlation_id: str, result_type: type):
        self._correlation_id = correlation_id
        self._result_type = result_type
        
    
