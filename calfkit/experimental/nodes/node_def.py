from typing import Generic

from calfkit.experimental._types import AgentOutputT
from calfkit.experimental.nodes.base import BaseNodeDef


class NodeDef(Generic[AgentOutputT], BaseNodeDef):
    pass
