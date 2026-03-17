"""Test whether State[TypeVar] resolves anything at runtime."""

from typing import Any, Generic

from pydantic import BaseModel, Field
from typing_extensions import TypeVar

DataT = TypeVar("DataT", default=dict[str, Any])


class MyInput(BaseModel):
    username: str
    score: int


class DataPart(BaseModel, Generic[DataT]):
    kind: str = "data"
    data: DataT


class Payload(BaseModel, Generic[DataT]):
    parts: list[DataPart[DataT]]


class State(BaseModel, Generic[DataT]):
    payload_stack: list[Payload[DataT]] = Field(default_factory=list)


raw_state = {
    "payload_stack": [
        {"parts": [{"kind": "data", "data": {"username": "alice", "score": 42}}]}
    ]
}

AgentInputT = TypeVar("AgentInputT", default=Any)

print("=== State[AgentInputT] (TypeVar) ===")
s1 = State[AgentInputT].model_validate(raw_state)
print(f"  type(data) = {type(s1.payload_stack[0].parts[0].data)}")

print("\n=== State[MyInput] (concrete) ===")
s2 = State[MyInput].model_validate(raw_state)
print(f"  type(data) = {type(s2.payload_stack[0].parts[0].data)}")

print("\n=== State (bare) ===")
s3 = State.model_validate(raw_state)
print(f"  type(data) = {type(s3.payload_stack[0].parts[0].data)}")
