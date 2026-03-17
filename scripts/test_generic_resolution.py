"""Test whether Pydantic resolves generic TypeVars to concrete BaseModel types at runtime."""

from typing import Any, Generic, Literal

from pydantic import BaseModel, Field
from typing_extensions import TypeVar

DataT = TypeVar("DataT", default=dict[str, Any])


class DataPart(BaseModel, Generic[DataT]):
    kind: Literal["data"] = "data"
    data: DataT


class MyInput(BaseModel):
    username: str
    score: int


# --- Test 1: Unparameterized DataPart (simulates unresolved TypeVar) ---
print("=== Test 1: Unparameterized DataPart ===")
raw = {"kind": "data", "data": {"username": "alice", "score": 42}}
part = DataPart.model_validate(raw)
print(f"  type(part.data) = {type(part.data)}")
print(f"  part.data = {part.data}")
print(f"  Is BaseModel? {isinstance(part.data, BaseModel)}")

# --- Test 2: Explicitly parameterized DataPart[MyInput] ---
print("\n=== Test 2: DataPart[MyInput] ===")
ConcreteDataPart = DataPart[MyInput]
part2 = ConcreteDataPart.model_validate(raw)
print(f"  type(part2.data) = {type(part2.data)}")
print(f"  part2.data = {part2.data}")
print(f"  Is BaseModel? {isinstance(part2.data, BaseModel)}")

# --- Test 3: Nested generic chain (simulates the real Envelope → State → Payload → DataPart) ---
print("\n=== Test 3: Nested generic chain ===")

StateT = TypeVar("StateT", default=Any)
DepsT = TypeVar("DepsT", default=Any)


class Payload(BaseModel, Generic[DataT]):
    parts: list[DataPart[DataT]]


class State(BaseModel, Generic[DataT]):
    payload_stack: list[Payload[DataT]] = Field(default_factory=list)


class SessionRunContext(BaseModel, Generic[StateT, DepsT]):
    state: StateT
    deps: DepsT


class Envelope(BaseModel, Generic[StateT, DepsT]):
    context: SessionRunContext[StateT, DepsT]


# 3a: Unparameterized Envelope (what FastStream sees with unresolved TypeVars)
print("\n  --- 3a: Unparameterized Envelope ---")
raw_envelope = {
    "context": {
        "state": {
            "payload_stack": [
                {"parts": [{"kind": "data", "data": {"username": "alice", "score": 42}}]}
            ]
        },
        "deps": {},
    }
}
env = Envelope.model_validate(raw_envelope)
print(f"  type(env.context.state) = {type(env.context.state)}")
print(f"  type(env.context.deps) = {type(env.context.deps)}")
if isinstance(env.context.state, dict):
    print("  state is a plain dict — entire nested chain is unresolved")
else:
    nested_data = env.context.state.payload_stack[0].parts[0].data
    print(f"  type(nested_data) = {type(nested_data)}")
    print(f"  Is BaseModel? {isinstance(nested_data, BaseModel)}")

# 3b: Fully parameterized Envelope[State[MyInput], dict]
print("\n  --- 3b: Envelope[State[MyInput], dict] ---")
ConcreteEnvelope = Envelope[State[MyInput], dict]
env2 = ConcreteEnvelope.model_validate(raw_envelope)
nested_data2 = env2.context.state.payload_stack[0].parts[0].data
print(f"  type(nested_data2) = {type(nested_data2)}")
print(f"  nested_data2 = {nested_data2}")
print(f"  Is BaseModel? {isinstance(nested_data2, BaseModel)}")

# --- Test 4: What does a Generic subclass's method annotation look like at runtime? ---
print("\n=== Test 4: Method annotation resolution on generic subclass ===")
import inspect


class BaseNode(Generic[StateT, DepsT]):
    def handler(self, envelope: Envelope[StateT, DepsT]) -> None: ...


class ConcreteNode(BaseNode[State[MyInput], dict]):
    pass


node = ConcreteNode()
sig = inspect.signature(node.handler)
hints = {}
try:
    hints = {k: v for k, v in node.handler.__annotations__.items()}
except Exception as e:
    print(f"  Error getting annotations: {e}")

print(f"  handler signature: {sig}")
print(f"  handler __annotations__: {hints}")
print(f"  'envelope' annotation is concrete? {hints.get('envelope') is Envelope[State[MyInput], dict]}")

# Also check via get_type_hints
import typing

try:
    resolved = typing.get_type_hints(ConcreteNode.handler)
    print(f"  get_type_hints: {resolved}")
except Exception as e:
    print(f"  get_type_hints error: {e}")
