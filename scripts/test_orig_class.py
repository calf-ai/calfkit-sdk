"""Test __orig_class__ availability and a lazy fallback for _input_type."""

from typing import Any, Generic, get_args, get_origin

from pydantic import BaseModel
from typing_extensions import TypeVar

AgentInputT = TypeVar("AgentInputT", default=Any)
AgentDepsT = TypeVar("AgentDepsT", default=Any)
AgentOutputT = TypeVar("AgentOutputT", default=Any)


class MyInput(BaseModel):
    username: str
    score: int


class BaseAgent(Generic[AgentInputT, AgentDepsT, AgentOutputT]):
    def __init__(self, agent_id: str, *, input_type: type | None = None):
        self._agent_id = agent_id
        self._explicit_input_type = input_type

    @property
    def input_type(self) -> type | None:
        # 1. Explicit wins
        if self._explicit_input_type is not None:
            return self._explicit_input_type

        # 2. Fallback: try __orig_class__ (set after __init__ by Python)
        orig = getattr(self, "__orig_class__", None)
        if orig is not None and get_origin(orig) is type(self):
            args = get_args(orig)
            if args:
                return args[0]

        # 3. Fallback: try __init_subclass__-captured type
        cls_type = getattr(type(self), "_cls_input_type", None)
        if cls_type is not None:
            return cls_type

        return None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            origin = get_origin(base)
            if origin is BaseAgent:
                args = get_args(base)
                if args and not isinstance(args[0], TypeVar):
                    cls._cls_input_type = args[0]
                    break


# --- Test 1: Explicit input_type ---
print("=== Test 1: Explicit input_type ===")
a1 = BaseAgent("a1", input_type=MyInput)
print(f"  input_type = {a1.input_type}")

# --- Test 2: BaseAgent[MyInput]("id") — __orig_class__ ---
print("\n=== Test 2: BaseAgent[MyInput]('a2') ===")
a2 = BaseAgent[MyInput]("a2")
print(f"  has __orig_class__? {hasattr(a2, '__orig_class__')}")
print(f"  __orig_class__ = {getattr(a2, '__orig_class__', None)}")
print(f"  input_type = {a2.input_type}")

# --- Test 3: Subclass with concrete types ---
print("\n=== Test 3: class MyAgent(BaseAgent[MyInput, Any, Any]) ===")


class MyAgent(BaseAgent[MyInput, Any, Any]):
    pass


a3 = MyAgent("a3")
print(f"  has __orig_class__? {hasattr(a3, '__orig_class__')}")
print(f"  _cls_input_type = {getattr(MyAgent, '_cls_input_type', None)}")
print(f"  input_type = {a3.input_type}")

# --- Test 4: Neither provided ---
print("\n=== Test 4: BaseAgent('a4') — no type info ===")
a4 = BaseAgent("a4")
print(f"  input_type = {a4.input_type}")
