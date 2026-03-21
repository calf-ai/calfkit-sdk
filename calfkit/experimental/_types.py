from typing import Any

from typing_extensions import TypeVar

StateT = TypeVar("StateT", default=Any)

DepsT = TypeVar("DepsT", default=Any)

InputT = TypeVar("InputT", default=Any)

DataT = TypeVar("DataT", default=dict[str, Any])

AgentOutputT = TypeVar("AgentOutputT", default=Any)

AgentInputT = TypeVar("AgentInputT", default=Any)

AgentDepsT = TypeVar("AgentDepsT", default=Any)

# Execution Stack generic type
StackItemT = TypeVar("StackItemT", default=Any)
