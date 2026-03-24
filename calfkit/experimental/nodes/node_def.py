from typing import Generic

from calfkit.experimental._types import AgentDepsT, AgentOutputT
from calfkit.experimental.nodes.base import BaseNodeDef


class NodeDef(Generic[AgentDepsT, AgentOutputT], BaseNodeDef[AgentDepsT]):
    pass
