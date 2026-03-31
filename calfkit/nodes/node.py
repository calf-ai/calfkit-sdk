from typing import Generic

from calfkit._types import AgentOutputT
from calfkit.nodes.base import BaseNodeDef


class NodeDef(Generic[AgentOutputT], BaseNodeDef):
    pass
