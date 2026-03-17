import json
from dataclasses import dataclass
from typing import Annotated, Any, Generic, Literal

from faststream.types import SendableMessage
from pydantic import BaseModel, Discriminator, Field
from typing_extensions import TypeAliasType, TypeVar

from calfkit.experimental.payload_model import Payload
from calfkit.experimental.state_and_deps_models import AgentDepsT, Deps, State
from calfkit.experimental.utils import generate_payload_id

StateT = TypeVar("StateT", default=Any)
DepsT = TypeVar("DepsT", default=Any)


@dataclass
class Hop:
    input_payload: Payload  # payload to pass into node as input
    result_topic: str  # publish result of node execution to this topic


class BaseSessionRunContext(BaseModel, Generic[StateT, DepsT]):
    """Developer-facing context for a session — just state + deps."""

    state: StateT
    """The state of the graph. Mutable."""
    deps: DepsT
    """Dependencies for the graph. Immutable."""
