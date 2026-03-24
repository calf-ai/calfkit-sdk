"""Simulate generic Pydantic model deserialization with correct defaults.
UserDepsT defaults to Any (matching the real codebase), not None."""

from typing import Any, Generic

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import TypeVar

# --- Mimic the framework types (with CORRECT defaults) ---

UserDepsT = TypeVar("UserDepsT", default=Any)


class Deps(BaseModel, Generic[UserDepsT]):
    model_config = ConfigDict(extra="ignore", frozen=True)
    correlation_id: str
    agent_deps: UserDepsT | None = Field(default=None)


class MyAgentDeps(BaseModel):
    api_key: str
    temperature: float = 0.7


def test():
    original = Deps[MyAgentDeps](
        correlation_id="test-123",
        agent_deps=MyAgentDeps(api_key="sk-test", temperature=0.9),
    )
    json_bytes = original.model_dump_json()
    print(f"JSON: {json_bytes}\n")

    # 1. Deps (unparameterized) — UserDepsT defaults to Any
    print("=== Deps (unparameterized, default=Any) ===")
    try:
        result = Deps.model_validate_json(json_bytes)
        print(f"  agent_deps type: {type(result.agent_deps)}")
        print(f"  agent_deps value: {result.agent_deps}")
        print(f"  isinstance MyAgentDeps: {isinstance(result.agent_deps, MyAgentDeps)}")
        print(f"  isinstance dict: {isinstance(result.agent_deps, dict)}")
    except Exception as e:
        print(f"  FAILED: {e}")

    # 2. Deps[Any]
    print("\n=== Deps[Any] ===")
    try:
        result = Deps[Any].model_validate_json(json_bytes)
        print(f"  agent_deps type: {type(result.agent_deps)}")
        print(f"  isinstance dict: {isinstance(result.agent_deps, dict)}")
    except Exception as e:
        print(f"  FAILED: {e}")

    # 3. Deps[MyAgentDeps] — concrete
    print("\n=== Deps[MyAgentDeps] (concrete) ===")
    try:
        result = Deps[MyAgentDeps].model_validate_json(json_bytes)
        print(f"  agent_deps type: {type(result.agent_deps)}")
        print(f"  isinstance MyAgentDeps: {isinstance(result.agent_deps, MyAgentDeps)}")
    except Exception as e:
        print(f"  FAILED: {e}")

    # 4. Simulate what FastStream sees with the handler type hints
    # Tests use: Envelope[State, Deps] — but Deps is unparameterized
    # Worker uses: node.run which has AgentSessionRunContext[AgentDepsT] — unresolved TypeVar
    print("\n=== What FastStream gets with different handler annotations ===")

    # The test handler uses Envelope[State, Deps] — what is Deps here?
    print(f"\n  Deps (bare):                   {Deps}")
    print(f"  Deps[Any]:                     {Deps[Any]}")
    print(f"  Deps[MyAgentDeps]:             {Deps[MyAgentDeps]}")
    print(f"  Deps.__pydantic_generic_metadata__: {getattr(Deps, '__pydantic_generic_metadata__', 'N/A')}")

    # Check if bare Deps and Deps[Any] produce same schema
    print(f"\n  Deps schema:        {Deps.model_json_schema()}")
    print(f"  Deps[Any] schema:   {Deps[Any].model_json_schema()}")
    print(f"  Deps[MyDeps] schema: {Deps[MyAgentDeps].model_json_schema()}")


if __name__ == "__main__":
    test()
